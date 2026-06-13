#![forbid(unsafe_code)]

//! E3.2 — WindowJoin operator.
//!
//! Buffers rows from two input streams in a shared time window and joins them
//! when the window closes (watermark advance past window end).
//!
//! Design:
//! - Both streams are partitioned by `join_key`.
//! - Each window is identified by `(key, window_start_ms)`.
//! - When the watermark advances past `window_start_ms + window_ms`, all
//!   buffered rows for that window are joined via a hash join and emitted.
//! - Join kind is always `Inner` (extend to Left/Right as needed).
//! - The time column on both inputs must be an Int64 column (event-time ms).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::join::{AggKey, extract_agg_key};
use crate::{ExecError, ExecResult};

// ── WindowJoinSpec ────────────────────────────────────────────────────────────

/// Configuration for a window join operator.
#[derive(Debug, Clone)]
pub struct WindowJoinSpec {
    /// Key column on the left stream.
    pub left_key: String,
    /// Key column on the right stream.
    pub right_key: String,
    /// Event-time column on both inputs (Int64, milliseconds).
    pub time_column: String,
    /// Window size in milliseconds.
    pub window_ms: u64,
    /// Watermark lag applied to the time column.
    pub watermark_lag_ms: u64,
}

// ── WindowJoin ────────────────────────────────────────────────────────────────

/// Buffered-window join operator.
///
/// Accumulate left/right rows per `(key, window_start_ms)`, then
/// emit joined output when the watermark closes that window.
pub struct WindowJoin {
    spec: WindowJoinSpec,
    /// Buffered left rows per `(key_str, window_start_ms)`.
    left_buf: HashMap<(String, i64), Vec<RowBuf>>,
    /// Buffered right rows per `(key_str, window_start_ms)`.
    right_buf: HashMap<(String, i64), Vec<RowBuf>>,
    /// Current watermark (max event-time seen, minus lag).
    watermark_ms: i64,
}

/// Stores one row's key values for later join materialisation.
struct RowBuf {
    /// The batch this row came from (cloned on insertion).
    batch: RecordBatch,
    /// Row index within `batch`.
    row: usize,
}

impl WindowJoin {
    pub fn new(spec: WindowJoinSpec) -> Self {
        Self {
            spec,
            left_buf: HashMap::new(),
            right_buf: HashMap::new(),
            watermark_ms: i64::MIN,
        }
    }

    // ── Feed rows ─────────────────────────────────────────────────────────

    /// Feed a batch of left-stream rows into the operator.
    pub fn push_left(&mut self, batch: &RecordBatch) -> ExecResult<()> {
        self.push_rows(batch, true)
    }

    /// Feed a batch of right-stream rows into the operator.
    pub fn push_right(&mut self, batch: &RecordBatch) -> ExecResult<()> {
        self.push_rows(batch, false)
    }

    fn push_rows(&mut self, batch: &RecordBatch, is_left: bool) -> ExecResult<()> {
        let key_col = if is_left {
            &self.spec.left_key
        } else {
            &self.spec.right_key
        };
        let key_idx = batch
            .schema()
            .index_of(key_col)
            .map_err(|_| ExecError::ColumnNotFound(key_col.clone()))?;
        let time_idx = batch
            .schema()
            .index_of(&self.spec.time_column)
            .map_err(|_| ExecError::ColumnNotFound(self.spec.time_column.clone()))?;
        let time_arr = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ExecError::UnsupportedType(format!(
                    "window join time column '{}' must be Int64",
                    self.spec.time_column
                ))
            })?;

        for row in 0..batch.num_rows() {
            let key = extract_agg_key(batch, key_idx, row)?;
            let key_str = key.to_string();
            let event_ms = time_arr.value(row);
            let window_start = window_start_for(
                event_ms,
                i64::try_from(self.spec.window_ms).unwrap_or(i64::MAX),
            );

            // Advance watermark.
            let lag = i64::try_from(self.spec.watermark_lag_ms).unwrap_or(i64::MAX);
            let new_wm = event_ms.saturating_sub(lag);
            if new_wm > self.watermark_ms {
                self.watermark_ms = new_wm;
            }

            let entry = RowBuf {
                batch: batch.clone(),
                row,
            };
            let buf = if is_left {
                &mut self.left_buf
            } else {
                &mut self.right_buf
            };
            buf.entry((key_str, window_start)).or_default().push(entry);
        }

        Ok(())
    }

    // ── Advance watermark and flush closed windows ─────────────────────────

    /// Advance the watermark to `watermark_ms` and flush all windows
    /// whose end time is ≤ the new watermark.
    pub fn advance_watermark(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        if watermark_ms > self.watermark_ms {
            self.watermark_ms = watermark_ms;
        }
        self.flush_closed_windows()
    }

    fn flush_closed_windows(&mut self) -> ExecResult<Vec<RecordBatch>> {
        let wm = self.watermark_ms;
        let window_ms = self.spec.window_ms as i64;
        let mut closed_keys: Vec<(String, i64)> = Vec::new();

        for k in self.left_buf.keys() {
            let window_end = k.1 + window_ms;
            if window_end <= wm {
                closed_keys.push(k.clone());
            }
        }
        for k in self.right_buf.keys() {
            let window_end = k.1 + window_ms;
            if window_end <= wm && !closed_keys.contains(k) {
                closed_keys.push(k.clone());
            }
        }

        let mut output = Vec::new();
        for key in closed_keys {
            let left_rows = self.left_buf.remove(&key).unwrap_or_default();
            let right_rows = self.right_buf.remove(&key).unwrap_or_default();
            if let Some(batch) = join_row_bufs(&left_rows, &right_rows, &self.spec)? {
                output.push(batch);
            }
        }
        Ok(output)
    }

    /// Current watermark (max event-time seen minus lag, `i64::MIN` initially).
    pub fn watermark_ms(&self) -> i64 {
        self.watermark_ms
    }

    /// Total buffered rows across both inputs (observability/tests).
    pub fn buffered_rows(&self) -> usize {
        self.left_buf.values().map(Vec::len).sum::<usize>()
            + self.right_buf.values().map(Vec::len).sum::<usize>()
    }

    /// Serialize the join's buffered windows and watermark to portable bytes.
    ///
    /// Layout (all integers little-endian):
    /// `[u32 version=1][i64 watermark]` followed by the left then the right
    /// buffer, each as `[u64 group_count]` then per group
    /// `[u32 key_len][key][i64 window_start][u32 ipc_len][Arrow IPC stream]`
    /// where the IPC stream holds the group's buffered rows as one batch.
    pub fn snapshot(&self) -> ExecResult<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&1u32.to_le_bytes());
        out.extend_from_slice(&self.watermark_ms.to_le_bytes());
        encode_row_buf_map(&mut out, &self.left_buf)?;
        encode_row_buf_map(&mut out, &self.right_buf)?;
        Ok(out)
    }

    /// Replace the join's buffered windows and watermark with the contents of
    /// a snapshot produced by [`Self::snapshot`].
    pub fn restore(&mut self, bytes: &[u8]) -> ExecResult<()> {
        let corrupt = |m: &str| ExecError::Arrow(format!("window join snapshot corrupt: {m}"));
        if bytes.len() < 12 {
            return Err(corrupt("too short"));
        }
        let version = u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes"));
        if version != 1 {
            return Err(corrupt(&format!("unsupported version {version}")));
        }
        let watermark_ms = i64::from_le_bytes(bytes[4..12].try_into().expect("8 bytes"));
        let mut pos = 12usize;
        let left_buf = decode_row_buf_map(bytes, &mut pos)?;
        let right_buf = decode_row_buf_map(bytes, &mut pos)?;
        if pos != bytes.len() {
            return Err(corrupt("trailing bytes"));
        }
        self.left_buf = left_buf;
        self.right_buf = right_buf;
        self.watermark_ms = watermark_ms;
        Ok(())
    }

    /// Flush everything unconditionally (end-of-stream).
    pub fn flush_all(&mut self) -> ExecResult<Vec<RecordBatch>> {
        let all_keys: Vec<(String, i64)> = self
            .left_buf
            .keys()
            .chain(self.right_buf.keys())
            .cloned()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let mut output = Vec::new();
        for key in all_keys {
            let left_rows = self.left_buf.remove(&key).unwrap_or_default();
            let right_rows = self.right_buf.remove(&key).unwrap_or_default();
            if let Some(batch) = join_row_bufs(&left_rows, &right_rows, &self.spec)? {
                output.push(batch);
            }
        }
        Ok(output)
    }
}

// ── Snapshot helpers ─────────────────────────────────────────────────────────

fn encode_row_buf_map(
    out: &mut Vec<u8>,
    map: &HashMap<(String, i64), Vec<RowBuf>>,
) -> ExecResult<()> {
    use arrow::ipc::writer::StreamWriter;

    // Deterministic group order so identical state snapshots byte-identically.
    let mut keys: Vec<&(String, i64)> = map.keys().collect();
    keys.sort();

    out.extend_from_slice(&(keys.len() as u64).to_le_bytes());
    for key in keys {
        let rows = &map[key];
        // Materialise the group's buffered rows as one batch.
        let slices: Vec<RecordBatch> = rows.iter().map(|rb| rb.batch.slice(rb.row, 1)).collect();
        let schema = slices
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| ExecError::Arrow("window join snapshot: empty group".into()))?;
        let merged = arrow::compute::concat_batches(&schema, &slices)
            .map_err(|e| ExecError::Arrow(format!("window join snapshot concat: {e}")))?;
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, &schema)
                .map_err(|e| ExecError::Arrow(format!("window join snapshot ipc: {e}")))?;
            writer
                .write(&merged)
                .map_err(|e| ExecError::Arrow(format!("window join snapshot ipc write: {e}")))?;
            writer
                .finish()
                .map_err(|e| ExecError::Arrow(format!("window join snapshot ipc finish: {e}")))?;
        }
        let key_bytes = key.0.as_bytes();
        out.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(key_bytes);
        out.extend_from_slice(&key.1.to_le_bytes());
        out.extend_from_slice(&(ipc.len() as u32).to_le_bytes());
        out.extend_from_slice(&ipc);
    }
    Ok(())
}

fn decode_row_buf_map(
    bytes: &[u8],
    pos: &mut usize,
) -> ExecResult<HashMap<(String, i64), Vec<RowBuf>>> {
    use arrow::ipc::reader::StreamReader;

    let corrupt = |m: &str| ExecError::Arrow(format!("window join snapshot corrupt: {m}"));
    let read = |bytes: &[u8], pos: &mut usize, n: usize| -> ExecResult<Vec<u8>> {
        if bytes.len() < *pos + n {
            return Err(corrupt("truncated"));
        }
        let v = bytes[*pos..*pos + n].to_vec();
        *pos += n;
        Ok(v)
    };

    let group_count =
        u64::from_le_bytes(read(bytes, pos, 8)?.try_into().expect("8 bytes")) as usize;
    const MAX_GROUPS: usize = 10_000_000;
    if group_count > MAX_GROUPS {
        return Err(corrupt("group count exceeds limit"));
    }
    let mut map: HashMap<(String, i64), Vec<RowBuf>> = HashMap::new();
    for _ in 0..group_count {
        let key_len =
            u32::from_le_bytes(read(bytes, pos, 4)?.try_into().expect("4 bytes")) as usize;
        let key =
            String::from_utf8(read(bytes, pos, key_len)?).map_err(|_| corrupt("key not utf8"))?;
        let window_start = i64::from_le_bytes(read(bytes, pos, 8)?.try_into().expect("8 bytes"));
        let ipc_len =
            u32::from_le_bytes(read(bytes, pos, 4)?.try_into().expect("4 bytes")) as usize;
        let ipc = read(bytes, pos, ipc_len)?;
        let reader = StreamReader::try_new(std::io::Cursor::new(ipc), None)
            .map_err(|e| ExecError::Arrow(format!("window join snapshot ipc read: {e}")))?;
        let mut rows = Vec::new();
        for batch in reader {
            let batch =
                batch.map_err(|e| ExecError::Arrow(format!("window join snapshot batch: {e}")))?;
            for row in 0..batch.num_rows() {
                rows.push(RowBuf {
                    batch: batch.clone(),
                    row,
                });
            }
        }
        map.insert((key, window_start), rows);
    }
    Ok(map)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn window_start_for(event_ms: i64, window_ms: i64) -> i64 {
    if window_ms <= 0 {
        return 0;
    }
    // Floor division that works correctly for negative timestamps.
    let q = event_ms / window_ms;
    let r = event_ms % window_ms;
    (if r < 0 { q - 1 } else { q }) * window_ms
}

/// Hash-join left and right row buffers and return the result batch.
fn join_row_bufs(
    left: &[RowBuf],
    right: &[RowBuf],
    spec: &WindowJoinSpec,
) -> ExecResult<Option<RecordBatch>> {
    if left.is_empty() || right.is_empty() {
        return Ok(None);
    }

    // Build a hash map from right key → right row indices.
    let mut right_by_key: HashMap<AggKey, Vec<usize>> = HashMap::new();
    for (ri, rb) in right.iter().enumerate() {
        let key_idx = rb
            .batch
            .schema()
            .index_of(&spec.right_key)
            .map_err(|_| ExecError::ColumnNotFound(spec.right_key.clone()))?;
        let key = extract_agg_key(&rb.batch, key_idx, rb.row)?;
        right_by_key.entry(key).or_default().push(ri);
    }

    let mut left_indices: Vec<u32> = Vec::new();
    let mut right_indices: Vec<u32> = Vec::new();

    for (li, lb) in left.iter().enumerate() {
        let key_idx = lb
            .batch
            .schema()
            .index_of(&spec.left_key)
            .map_err(|_| ExecError::ColumnNotFound(spec.left_key.clone()))?;
        let key = extract_agg_key(&lb.batch, key_idx, lb.row)?;
        if let Some(right_match) = right_by_key.get(&key) {
            for &ri in right_match {
                left_indices.push(li as u32);
                right_indices.push(ri as u32);
            }
        }
    }

    if left_indices.is_empty() {
        return Ok(None);
    }

    // Materialise matched rows into a RecordBatch.
    // Build concatenated schema: all left cols + all right cols.
    let left_schema = left[0].batch.schema();
    let right_schema = right[0].batch.schema();
    let mut fields: Vec<Field> = left_schema.fields().iter().map(|f| (**f).clone()).collect();
    fields.extend(right_schema.fields().iter().map(|f| (**f).clone()));
    let schema = Arc::new(Schema::new(fields));

    let mut columns: Vec<ArrayRef> = Vec::new();

    // Materialise left columns.
    for col_idx in 0..left_schema.fields().len() {
        let mut values: Vec<ArrayRef> = Vec::new();
        for &li in &left_indices {
            let row = left[li as usize].row;
            let src_col = left[li as usize].batch.column(col_idx);
            let idx = UInt32Array::from(vec![row as u32]);
            values.push(
                take(src_col.as_ref(), &idx, None).map_err(|e| ExecError::Arrow(e.to_string()))?,
            );
        }
        columns.push(
            arrow::compute::concat(
                values
                    .iter()
                    .map(|a| a.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .map_err(|e| ExecError::Arrow(e.to_string()))?,
        );
    }

    // Materialise right columns.
    for col_idx in 0..right_schema.fields().len() {
        let mut values: Vec<ArrayRef> = Vec::new();
        for &ri in &right_indices {
            let row = right[ri as usize].row;
            let src_col = right[ri as usize].batch.column(col_idx);
            let idx = UInt32Array::from(vec![row as u32]);
            values.push(
                take(src_col.as_ref(), &idx, None).map_err(|e| ExecError::Arrow(e.to_string()))?,
            );
        }
        columns.push(
            arrow::compute::concat(
                values
                    .iter()
                    .map(|a| a.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .map_err(|e| ExecError::Arrow(e.to_string()))?,
        );
    }

    Ok(Some(
        RecordBatch::try_new(schema, columns).map_err(|e| ExecError::Arrow(e.to_string()))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(keys: &[i32], times: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(times.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn window_join_matches_on_same_key_same_window() {
        let spec = WindowJoinSpec {
            left_key: "key".into(),
            right_key: "key".into(),
            time_column: "ts".into(),
            window_ms: 10_000,
            watermark_lag_ms: 0,
        };
        let mut op = WindowJoin::new(spec);

        // Both rows in window [0, 10_000).
        let left = make_batch(&[1], &[1_000]);
        let right = make_batch(&[1], &[2_000]);
        op.push_left(&left).unwrap();
        op.push_right(&right).unwrap();

        // Advance watermark past window end.
        let result = op.advance_watermark(10_001).unwrap();
        assert_eq!(result.len(), 1, "one joined batch");
        assert_eq!(result[0].num_rows(), 1, "one matched row");
    }

    #[test]
    fn window_join_no_match_returns_empty() {
        let spec = WindowJoinSpec {
            left_key: "key".into(),
            right_key: "key".into(),
            time_column: "ts".into(),
            window_ms: 10_000,
            watermark_lag_ms: 0,
        };
        let mut op = WindowJoin::new(spec);

        let left = make_batch(&[1], &[1_000]);
        let right = make_batch(&[2], &[2_000]); // different key
        op.push_left(&left).unwrap();
        op.push_right(&right).unwrap();

        let result = op.advance_watermark(10_001).unwrap();
        assert!(result.is_empty(), "no match across keys");
    }

    #[test]
    fn window_join_different_windows_not_joined() {
        let spec = WindowJoinSpec {
            left_key: "key".into(),
            right_key: "key".into(),
            time_column: "ts".into(),
            window_ms: 10_000,
            watermark_lag_ms: 0,
        };
        let mut op = WindowJoin::new(spec);

        // Left in window [0, 10_000), right in [10_000, 20_000).
        let left = make_batch(&[1], &[1_000]);
        let right = make_batch(&[1], &[11_000]);
        op.push_left(&left).unwrap();
        op.push_right(&right).unwrap();

        let result = op.advance_watermark(10_001).unwrap();
        assert!(
            result.is_empty(),
            "rows in different windows are not joined"
        );
    }

    #[test]
    fn window_join_flush_all_emits_remaining() {
        let spec = WindowJoinSpec {
            left_key: "key".into(),
            right_key: "key".into(),
            time_column: "ts".into(),
            window_ms: 10_000,
            watermark_lag_ms: 0,
        };
        let mut op = WindowJoin::new(spec);

        let left = make_batch(&[1], &[1_000]);
        let right = make_batch(&[1], &[2_000]);
        op.push_left(&left).unwrap();
        op.push_right(&right).unwrap();

        // No advance — flush everything.
        let result = op.flush_all().unwrap();
        assert_eq!(result.len(), 1);
    }
}
