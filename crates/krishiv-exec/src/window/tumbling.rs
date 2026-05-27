use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::aggregate::{AggExpr, AggState};
use crate::join::format_key_value;
use crate::{ExecError, ExecResult};

// ── TumblingWindowSpec ────────────────────────────────────────────────────────

/// Configuration for a tumbling event-time window operator.
#[derive(Debug, Clone)]
pub struct TumblingWindowSpec {
    /// Name of the column to key by (Utf8 or Int64; serialised to String).
    pub key_column: String,
    /// Name of the Int64 column carrying event time in milliseconds.
    pub event_time_column: String,
    /// Window duration in milliseconds.
    pub window_size_ms: u64,
    /// Aggregate expressions to apply within each window.
    pub agg_exprs: Vec<AggExpr>,
}

// ── TumblingWindowOperator ────────────────────────────────────────────────────

/// Tumbling event-time window operator backed by an in-memory accumulation map.
///
/// State structure: `(serialised_key, window_start_ms) → AggState`.
/// Windows are closed and flushed when the watermark reaches their end time.
///
/// **Late-event semantics**: an event is late if its `event_time_ms` is
/// strictly less than the watermark from the *previous* batch (stored as
/// `prev_watermark_ms`).  Events in the current batch are never late relative
/// to the watermark they themselves advance — the caller computes the new
/// watermark from this batch and passes it as `new_watermark_ms`.
///
/// Output schema per closed window:
/// `key_column (Utf8), window_start_ms (Int64), window_end_ms (Int64),
///  …agg output columns (Int64)`.
pub struct TumblingWindowOperator {
    spec: TumblingWindowSpec,
    // (serialised_key, window_start_ms) → aggregate accumulator
    accumulators: HashMap<(String, i64), AggState>,
    // Watermark from before the last processed batch; used for late-event
    // detection.  Initialised to i64::MIN so the first batch is never late.
    prev_watermark_ms: i64,
}

impl TumblingWindowOperator {
    /// Create a new operator.
    pub fn new(spec: TumblingWindowSpec) -> Self {
        Self {
            spec,
            accumulators: HashMap::new(),
            prev_watermark_ms: i64::MIN,
        }
    }

    /// Number of open (not yet flushed) window buckets.
    pub fn open_window_count(&self) -> usize {
        self.accumulators.len()
    }

    /// Persist open window accumulators to `StateBackend` (GAP-I2).
    pub fn persist_to_state(
        &self,
        backend: &mut dyn krishiv_state::StateBackend,
        namespace: &krishiv_state::Namespace,
    ) -> krishiv_state::StateResult<()> {
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
            let bytes = serde_json::to_vec(&payload).map_err(|e| {
                krishiv_state::StateError::CorruptEntry {
                    message: e.to_string(),
                }
            })?;
            let mut state_key = Vec::from(b"tw:");
            state_key.extend_from_slice(key.as_bytes());
            state_key.push(0);
            state_key.extend_from_slice(&win_start.to_le_bytes());
            state_keys.push(state_key);
            values.push(bytes);
        }
        if !state_keys.is_empty() {
            let batch_entries: Vec<(&str, &str, &[u8], &[u8])> = state_keys
                .iter()
                .zip(values.iter())
                .map(|(k, v)| (op_id, name, k.as_slice(), v.as_slice()))
                .collect();
            backend.put_batch(&batch_entries)?;
        }
        Ok(())
    }

    /// Restore open window accumulators from `StateBackend` (GAP-I2).
    pub fn restore_from_state(
        &mut self,
        backend: &dyn krishiv_state::StateBackend,
        namespace: &krishiv_state::Namespace,
    ) -> krishiv_state::StateResult<()> {
        let mut restored = HashMap::new();
        for key_bytes in backend.list_keys(namespace)? {
            let Some(payload) = backend.get(namespace, &key_bytes)? else {
                continue;
            };
            let parsed: serde_json::Value = serde_json::from_slice(&payload).map_err(|e| {
                krishiv_state::StateError::CorruptEntry {
                    message: e.to_string(),
                }
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
            if let Some((key, win_start)) = parse_tumbling_state_key(&key_bytes) {
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

    /// Compute the window start for an event time using floor division.
    fn window_start(event_time_ms: i64, window_size_ms: u64) -> i64 {
        let size = window_size_ms as i64;
        // Integer floor division that works for negative timestamps too.
        let q = event_time_ms / size;
        let r = event_time_ms % size;
        if r < 0 { (q - 1) * size } else { q * size }
    }

    /// Process one `RecordBatch`.
    ///
    /// `new_watermark_ms` is the watermark computed *after* advancing from
    /// this batch's event times.  Events are late only if their
    /// `event_time_ms` is below the watermark from the **previous** batch
    /// (`prev_watermark_ms`).  Windows whose `window_end ≤ new_watermark_ms`
    /// are closed and returned.
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

        let time_col = batch.column(time_idx);
        let time_arr = time_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ExecError::UnsupportedType(format!(
                    "event_time column '{}' must be Int64",
                    self.spec.event_time_column
                ))
            })?;

        // Use the watermark from the PREVIOUS batch as the late threshold.
        let late_threshold = self.prev_watermark_ms;

        for row in 0..batch.num_rows() {
            let event_time_ms = time_arr.value(row);
            // Drop events that arrived late relative to the previous watermark.
            if event_time_ms < late_threshold {
                continue;
            }
            let key = format_key_value(batch, key_idx, row)?;
            let win_start = Self::window_start(event_time_ms, self.spec.window_size_ms);
            let state = self
                .accumulators
                .entry((key, win_start))
                .or_insert_with(|| AggState::new(&self.spec.agg_exprs));
            state.update(&self.spec.agg_exprs, batch, row)?;
        }

        // Advance internal watermark AFTER accumulating this batch.
        self.prev_watermark_ms = new_watermark_ms;

        self.flush_closed_windows(new_watermark_ms)
    }

    /// Flush all window buckets whose end time is ≤ `watermark_ms`.
    ///
    /// Returns one `RecordBatch` per closed window, sorted by
    /// `(window_start_ms, key)` for deterministic output.
    pub fn flush_closed_windows(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        let size = self.spec.window_size_ms as i64;

        let mut closed: Vec<(String, i64)> = self
            .accumulators
            .keys()
            .filter(|(_, win_start)| win_start + size <= watermark_ms)
            .cloned()
            .collect();

        if closed.is_empty() {
            return Ok(vec![]);
        }

        // Deterministic output order.
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

fn parse_tumbling_state_key(bytes: &[u8]) -> Option<(String, i64)> {
    const PREFIX: &[u8] = b"tw:";
    if !bytes.starts_with(PREFIX) {
        return None;
    }
    let rest = &bytes[PREFIX.len()..];
    let sep = rest.iter().position(|b| *b == 0)?;
    let key = std::str::from_utf8(&rest[..sep]).ok()?.to_string();
    let win_bytes: [u8; 8] = rest.get(sep + 1..sep + 9)?.try_into().ok()?;
    Some((key, i64::from_le_bytes(win_bytes)))
}

// ── Shared window output builder ──────────────────────────────────────────────

/// Build a single-row `RecordBatch` representing one closed window.
///
/// Used by both `TumblingWindowOperator` and `SlidingWindowOperator` so that
/// the output schema and column layout stay in sync automatically.
pub(crate) fn build_window_record_batch(
    key_column: &str,
    key_value: &str,
    window_start_ms: i64,
    window_end_ms: i64,
    agg_exprs: &[AggExpr],
    state: &AggState,
) -> ExecResult<RecordBatch> {
    use std::sync::Arc as StdArc;
    let mut fields = vec![
        Field::new(key_column, DataType::Utf8, false),
        Field::new("window_start_ms", DataType::Int64, false),
        Field::new("window_end_ms", DataType::Int64, false),
    ];
    for agg in agg_exprs {
        fields.push(Field::new(&agg.output_column, DataType::Int64, false));
    }
    let schema = StdArc::new(Schema::new(fields));
    let mut columns: Vec<std::sync::Arc<dyn arrow::array::Array>> = vec![
        Arc::new(StringArray::from(vec![key_value])),
        Arc::new(Int64Array::from(vec![window_start_ms])),
        Arc::new(Int64Array::from(vec![window_end_ms])),
    ];
    for (i, agg) in agg_exprs.iter().enumerate() {
        columns.push(Arc::new(Int64Array::from(vec![
            state.finalized_value(i, agg),
        ])));
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod state_tests {
    use super::*;
    use crate::aggregate::AggFunction;
    use krishiv_state::{InMemoryStateBackend, Namespace};

    #[test]
    fn tumbling_state_persist_and_restore_roundtrip() {
        let spec = TumblingWindowSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![AggExpr {
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
        };
        let mut op = TumblingWindowOperator::new(spec);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![500])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();
        op.process_batch(&batch, 100).expect("process");
        assert_eq!(op.open_window_count(), 1);

        let mut backend = InMemoryStateBackend::new();
        let ns = Namespace::new("op-1", "windows");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = TumblingWindowOperator::new(TumblingWindowSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![AggExpr {
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
        });
        restored.restore_from_state(&backend, &ns).expect("restore");
        assert_eq!(restored.open_window_count(), 1);
    }
}
