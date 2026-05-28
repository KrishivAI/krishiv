use std::collections::HashMap;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use krishiv_state::{Namespace, StateBackend, StateError, StateResult};

use crate::aggregate::{AggExpr, AggState};
use crate::join::format_key_value;
use crate::window::tumbling::build_window_record_batch;
use crate::{ExecError, ExecResult};

/// Configuration for a sliding event-time window operator (R5.2).
///
/// A sliding window of size `window_size_ms` that advances by `slide_ms` means
/// an event belongs to `ceil(window_size_ms / slide_ms)` overlapping windows.
#[derive(Debug, Clone)]
pub struct SlidingWindowSpec {
    /// Column used to key the stream.
    pub key_column: String,
    /// Int64 column carrying event time in milliseconds.
    pub event_time_column: String,
    /// Total window duration in milliseconds.
    pub window_size_ms: u64,
    /// Window advance step in milliseconds (must be ≤ `window_size_ms`).
    pub slide_ms: u64,
    /// Aggregate expressions to apply within each window.
    pub agg_exprs: Vec<AggExpr>,
}

/// Sliding event-time window operator (R5.2).
///
/// Each event is placed into every window `[w, w + size)` where
/// `w` is a multiple of `slide_ms` and `w ≤ event_time_ms < w + size`.
#[derive(Debug)]
pub struct SlidingWindowOperator {
    spec: SlidingWindowSpec,
    // (serialised_key, window_start_ms) → aggregate accumulator
    accumulators: HashMap<(String, i64), AggState>,
    prev_watermark_ms: i64,
}

impl SlidingWindowOperator {
    /// Create a new sliding window operator.
    ///
    /// Returns `Err(ExecError::InvalidWindowConfig)` if `spec.slide_ms == 0`,
    /// which would cause an infinite loop in `window_starts`.
    pub fn new(spec: SlidingWindowSpec) -> ExecResult<Self> {
        if spec.slide_ms == 0 {
            return Err(ExecError::InvalidWindowConfig(
                "slide_ms must be greater than zero".into(),
            ));
        }
        Ok(Self {
            spec,
            accumulators: HashMap::new(),
            prev_watermark_ms: i64::MIN,
        })
    }

    /// Number of open (not yet flushed) window buckets.
    pub fn open_window_count(&self) -> usize {
        self.accumulators.len()
    }

    /// Persist open sliding window accumulators to `StateBackend`.
    ///
    /// Clears the namespace first so that stale entries for already-flushed
    /// windows are removed and cannot be re-opened on checkpoint restore.
    pub fn persist_to_state(
        &self,
        backend: &mut dyn StateBackend,
        namespace: &Namespace,
    ) -> StateResult<()> {
        // Remove all previously persisted entries so closed windows don't
        // survive into the next checkpoint snapshot.
        backend.clear_namespace(namespace)?;

        if self.accumulators.is_empty() {
            return Ok(());
        }

        let op_id = namespace.operator_id();
        let name = namespace.state_name();
        let mut state_keys = Vec::with_capacity(self.accumulators.len());
        let mut values = Vec::with_capacity(self.accumulators.len());
        for ((key, win_start), agg) in &self.accumulators {
            let payload = serde_json::json!({
                "values": agg.values,
                "has_value": agg.has_value,
                "avg_sums": agg.avg_sums,
                "avg_counts": agg.avg_counts,
            });
            let bytes = serde_json::to_vec(&payload).map_err(|e| StateError::CorruptEntry {
                message: e.to_string(),
            })?;
            // GAP-18: length-prefix encoding — format: b"sw:" | key_len_le_u32 | key_bytes | win_start_le_i64
            let key_bytes = key.as_bytes();
            let mut state_key = Vec::with_capacity(3 + 4 + key_bytes.len() + 8);
            state_key.extend_from_slice(b"sw:");
            state_key.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            state_key.extend_from_slice(key_bytes);
            state_key.extend_from_slice(&win_start.to_le_bytes());
            state_keys.push(state_key);
            values.push(bytes);
        }
        let batch_entries: Vec<(&str, &str, &[u8], &[u8])> = state_keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| (op_id, name, k.as_slice(), v.as_slice()))
            .collect();
        backend.put_batch(&batch_entries)?;
        Ok(())
    }

    /// Restore open sliding window accumulators from `StateBackend`.
    pub fn restore_from_state(
        &mut self,
        backend: &dyn StateBackend,
        namespace: &Namespace,
    ) -> StateResult<()> {
        let mut restored = HashMap::new();
        for key_bytes in backend.list_keys(namespace)? {
            let Some(payload) = backend.get(namespace, &key_bytes)? else {
                continue;
            };
            let parsed: serde_json::Value =
                serde_json::from_slice(&payload).map_err(|e| StateError::CorruptEntry {
                    message: e.to_string(),
                })?;
            let values: Vec<i64> = parsed["values"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_i64()).collect())
                .unwrap_or_default();
            let has_value: Vec<bool> = parsed["has_value"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_bool()).collect())
                .unwrap_or_default();
            let avg_sums: Vec<f64> = parsed["avg_sums"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_f64()).collect())
                .unwrap_or_default();
            let avg_counts: Vec<u64> = parsed["avg_counts"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
                .unwrap_or_default();
            if let Some((key, win_start)) = parse_sliding_state_key(&key_bytes) {
                restored.insert(
                    (key, win_start),
                    AggState {
                        values,
                        has_value,
                        avg_sums,
                        avg_counts,
                    },
                );
            }
        }
        self.accumulators = restored;
        Ok(())
    }

    /// All window starts (multiples of `slide`) that contain `event_time_ms`.
    fn window_starts(event_time_ms: i64, size_ms: u64, slide_ms: u64) -> Vec<i64> {
        let slide = slide_ms as i64;
        let size = size_ms as i64;
        // The largest multiple of slide that is ≤ event_time_ms.
        let q = event_time_ms / slide;
        let r = event_time_ms % slide;
        let first = if r < 0 { (q - 1) * slide } else { q * slide };
        let mut starts = Vec::new();
        let mut s = first;
        // Walk back until the event is no longer inside the window.
        while event_time_ms < s + size {
            starts.push(s);
            s -= slide;
        }
        starts
    }

    /// Process one `RecordBatch`, returning closed window outputs.
    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        new_watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        let key_idx = batch
            .schema()
            .index_of(&self.spec.key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.spec.key_column.clone()))?;
        let time_idx = batch
            .schema()
            .index_of(&self.spec.event_time_column)
            .map_err(|_| ExecError::ColumnNotFound(self.spec.event_time_column.clone()))?;

        let time_arr = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ExecError::UnsupportedType(format!(
                    "event_time column '{}' must be Int64",
                    self.spec.event_time_column
                ))
            })?;

        let late_threshold = self.prev_watermark_ms;

        for row in 0..batch.num_rows() {
            let event_time_ms = time_arr.value(row);
            if event_time_ms < late_threshold {
                continue;
            }
            let key = format_key_value(batch, key_idx, row)?;
            for win_start in
                Self::window_starts(event_time_ms, self.spec.window_size_ms, self.spec.slide_ms)
            {
                let state = self
                    .accumulators
                    .entry((key.clone(), win_start))
                    .or_insert_with(|| AggState::new(&self.spec.agg_exprs));
                state.update(&self.spec.agg_exprs, batch, row)?;
            }
        }

        self.prev_watermark_ms = new_watermark_ms;
        self.flush_closed_windows(new_watermark_ms)
    }

    /// Flush windows whose end time is ≤ `watermark_ms`.
    pub fn flush_closed_windows(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        let size = self.spec.window_size_ms as i64;
        let mut closed: Vec<(String, i64)> = self
            .accumulators
            .keys()
            .filter(|(_, ws)| ws + size <= watermark_ms)
            .cloned()
            .collect();
        if closed.is_empty() {
            return Ok(vec![]);
        }
        closed.sort_by(|(ka, wa), (kb, wb)| wa.cmp(wb).then(ka.cmp(kb)));
        let mut output = Vec::with_capacity(closed.len());
        for bucket in closed {
            if let Some(state) = self.accumulators.remove(&bucket) {
                output.push(self.build_output_batch(&bucket.0, bucket.1, &state)?);
            }
        }
        Ok(output)
    }

    fn build_output_batch(
        &self,
        key_value: &str,
        window_start_ms: i64,
        state: &AggState,
    ) -> ExecResult<RecordBatch> {
        let window_end_ms = window_start_ms + self.spec.window_size_ms as i64;
        build_window_record_batch(
            &self.spec.key_column,
            key_value,
            window_start_ms,
            window_end_ms,
            &self.spec.agg_exprs,
            state,
        )
    }
}

fn parse_sliding_state_key(bytes: &[u8]) -> Option<(String, i64)> {
    // GAP-18: length-prefix format: b"sw:" | key_len_le_u32 | key_bytes | win_start_le_i64
    const PREFIX: &[u8] = b"sw:";
    if !bytes.starts_with(PREFIX) {
        return None;
    }
    let rest = &bytes[PREFIX.len()..];
    let key_len = u32::from_le_bytes(rest.get(..4)?.try_into().ok()?) as usize;
    let key = std::str::from_utf8(rest.get(4..4 + key_len)?).ok()?.to_string();
    let win_start_offset = 4 + key_len;
    let win_bytes: [u8; 8] = rest
        .get(win_start_offset..win_start_offset + 8)?
        .try_into()
        .ok()?;
    Some((key, i64::from_le_bytes(win_bytes)))
}

#[cfg(test)]
mod sliding_state_tests {
    use std::sync::Arc;

    use super::*;
    use crate::aggregate::AggFunction;
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_state::{InMemoryStateBackend, Namespace};

    #[test]
    fn sliding_state_persist_and_restore_roundtrip() {
        let spec = SlidingWindowSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            window_size_ms: 2000,
            slide_ms: 1000,
            agg_exprs: vec![AggExpr {
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
        };
        let mut op = SlidingWindowOperator::new(spec).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![500])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();
        op.process_batch(&batch, 100).expect("process");
        assert!(op.open_window_count() > 0);

        let mut backend = InMemoryStateBackend::new();
        let ns = Namespace::new("op-sliding", "windows");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = SlidingWindowOperator::new(SlidingWindowSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            window_size_ms: 2000,
            slide_ms: 1000,
            agg_exprs: vec![AggExpr {
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
        })
        .unwrap();
        restored.restore_from_state(&backend, &ns).expect("restore");
        assert!(restored.open_window_count() > 0);
    }

    #[test]
    fn sliding_state_parse_key() {
        // GAP-18: use length-prefix encoding
        let key_str = "mykey";
        let key_bytes = key_str.as_bytes();
        let mut key = Vec::from(b"sw:");
        key.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        key.extend_from_slice(key_bytes);
        key.extend_from_slice(&42i64.to_le_bytes());
        let (k, w) = parse_sliding_state_key(&key).unwrap();
        assert_eq!(k, "mykey");
        assert_eq!(w, 42);
    }

    #[test]
    fn sliding_state_parse_key_with_embedded_null() {
        // GAP-18: keys containing null bytes must parse correctly with length-prefix.
        let key_str = "key\x00with\x00nulls";
        let key_bytes = key_str.as_bytes();
        let mut key = Vec::from(b"sw:");
        key.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        key.extend_from_slice(key_bytes);
        key.extend_from_slice(&100i64.to_le_bytes());
        let (k, w) = parse_sliding_state_key(&key).unwrap();
        assert_eq!(k, "key\x00with\x00nulls");
        assert_eq!(w, 100);
    }

    #[test]
    fn sliding_state_parse_key_bad_prefix_returns_none() {
        let key = b"tw:other";
        assert!(parse_sliding_state_key(key).is_none());
    }
}
