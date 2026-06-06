use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::aggregate::{AggExpr, AggFunction, AggState};
use crate::join::extract_agg_key;
use crate::window::LateEventHandler;
use crate::{ExecError, ExecResult};

// ── TumblingWindowSpec ────────────────────────────────────────────────────────

/// Configuration for a tumbling event-time window operator.
#[derive(Debug, Clone)]
pub struct TumblingWindowSpec {
    /// Name of the column to key by.
    pub key_column: String,
    /// Arrow type of the key column: `"int32"`, `"int64"`, `"float64"`, `"utf8"`, `"bool"`.
    /// Defaults to `"utf8"`.
    pub key_column_type: String,
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
    accumulators: HashMap<(String, i64), AggState>,
    prev_watermark_ms: i64,
    pub late_events_dropped: u64,
    late_event_handler: Option<Box<dyn LateEventHandler>>,
}

impl TumblingWindowOperator {
    /// Create a new operator.
    pub fn new(spec: TumblingWindowSpec) -> Self {
        Self {
            spec,
            accumulators: HashMap::new(),
            prev_watermark_ms: i64::MIN,
            late_events_dropped: 0,
            late_event_handler: None,
        }
    }

    /// Validate the spec before accepting it.
    pub fn validate_spec(spec: &TumblingWindowSpec) -> ExecResult<()> {
        if spec.window_size_ms == 0 {
            return Err(ExecError::InvalidWindowConfig(
                "tumbling window_size_ms must be non-zero".into(),
            ));
        }
        if spec.window_size_ms > i64::MAX as u64 {
            return Err(ExecError::InvalidWindowConfig(
                format!(
                    "tumbling window_size_ms ({}) exceeds i64::MAX",
                    spec.window_size_ms,
                ),
            ));
        }
        Ok(())
    }

    fn window_start(event_time_ms: i64, window_size_ms: u64) -> i64 {
        let size = window_size_ms as i64;
        let q = event_time_ms / size;
        let r = event_time_ms % size;
        if r < 0 { (q - 1) * size } else { q * size }
    }

    /// Attach a late-event handler that receives each dropped late event.
    pub fn with_late_event_handler(mut self, handler: Box<dyn LateEventHandler>) -> Self {
        self.late_event_handler = Some(handler);
        self
    }

    /// Number of open (not yet flushed) window buckets.
    pub fn open_window_count(&self) -> usize {
        self.accumulators.len()
    }

    /// Persist open window accumulators to `StateBackend` (GAP-I2).
    ///
    /// Clears the namespace first so that stale entries for windows that have
    /// already been flushed (closed) are removed.  Without this, closed windows
    /// would accumulate in the backend across checkpoint cycles and be
    /// incorrectly re-opened on restore, causing double-emission.
    pub fn persist_to_state(
        &self,
        backend: &mut dyn krishiv_state::StateBackend,
        namespace: &krishiv_state::Namespace,
    ) -> krishiv_state::StateResult<()> {
        super::state_persistence::persist_window_accumulators(
            backend,
            namespace,
            &self.accumulators,
            b"tw:",
        )?;
        super::state_persistence::persist_operator_watermark_ms(
            backend,
            namespace,
            self.prev_watermark_ms,
        )
    }

    /// Restore open window accumulators from `StateBackend` (GAP-I2).
    pub fn restore_from_state(
        &mut self,
        backend: &dyn krishiv_state::StateBackend,
        namespace: &krishiv_state::Namespace,
    ) -> krishiv_state::StateResult<()> {
        self.accumulators =
            super::state_persistence::restore_window_accumulators(backend, namespace, b"tw:")?;
        if let Some(wm) =
            super::state_persistence::restore_operator_watermark_ms(backend, namespace)?
        {
            self.prev_watermark_ms = wm;
        }
        Ok(())
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
            if event_time_ms < late_threshold {
                self.late_events_dropped = self.late_events_dropped.saturating_add(1);
                let key = extract_agg_key(batch, key_idx, row)
                    .map(|k| k.to_string())
                    .unwrap_or_default();
                if let Some(ref handler) = self.late_event_handler {
                    handler.on_late_event(&key, event_time_ms, row);
                }
                continue;
            }
            let key = extract_agg_key(batch, key_idx, row)?.to_string();
            let win_start = Self::window_start(event_time_ms, self.spec.window_size_ms);
            let state = self
                .accumulators
                .entry((key, win_start))
                .or_insert_with(|| AggState::new(&self.spec.agg_exprs));
            state.update(&self.spec.agg_exprs, batch, row)?;
        }

        // Advance internal watermark AFTER accumulating this batch.
        if new_watermark_ms >= self.prev_watermark_ms {
            self.prev_watermark_ms = new_watermark_ms;
        }

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
            &self.spec.key_column_type,
            key_value,
            window_start_ms,
            window_end_ms,
            &self.spec.agg_exprs,
            state,
        )
    }
}

// ── Shared window output builder ──────────────────────────────────────────────

/// Build a single-row `RecordBatch` representing one closed window.
///
/// Used by both `TumblingWindowOperator` and `SlidingWindowOperator` so that
/// the output schema and column layout stay in sync automatically.
/// `key_type` is the Arrow type tag for the key column (`"int32"`, `"int64"`,
/// `"float64"`, `"utf8"`, `"bool"`).
pub(crate) fn build_window_record_batch(
    key_column: &str,
    key_type: &str,
    key_value: &str,
    window_start_ms: i64,
    window_end_ms: i64,
    agg_exprs: &[AggExpr],
    state: &AggState,
) -> ExecResult<RecordBatch> {
    use std::sync::Arc as StdArc;
    let key_dtype = key_type_to_arrow_data_type(key_type);
    let mut fields = vec![
        Field::new(key_column, key_dtype, false),
        Field::new("window_start_ms", DataType::Int64, false),
        Field::new("window_end_ms", DataType::Int64, false),
    ];
    for agg in agg_exprs {
        let dtype = match agg.function {
            AggFunction::Avg => DataType::Float64,
            _ => DataType::Int64,
        };
        fields.push(Field::new(&agg.output_column, dtype, false));
    }
    let schema = StdArc::new(Schema::new(fields));
    let mut columns: Vec<std::sync::Arc<dyn arrow::array::Array>> = vec![
        key_value_to_typed_array(key_type, key_value),
        Arc::new(Int64Array::from(vec![window_start_ms])),
        Arc::new(Int64Array::from(vec![window_end_ms])),
    ];
    for (i, agg) in agg_exprs.iter().enumerate() {
        match agg.function {
            AggFunction::Avg => {
                columns.push(Arc::new(Float64Array::from(vec![state.finalized_avg(i)])));
            }
            _ => {
                columns.push(Arc::new(Int64Array::from(vec![
                    state.finalized_value(i, agg),
                ])));
            }
        }
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn key_type_to_arrow_data_type(key_type: &str) -> DataType {
    match key_type {
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "float64" => DataType::Float64,
        "bool" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

fn key_value_to_typed_array(key_type: &str, key_value: &str) -> Arc<dyn arrow::array::Array> {
    match key_type {
        "int32" => {
            let v = key_value.parse::<i32>().unwrap_or_else(|_| {
                tracing::warn!(key = key_value, "failed to parse key as int32, using 0");
                0
            });
            Arc::new(Int32Array::from(vec![v]))
        }
        "int64" => {
            let v = key_value.parse::<i64>().unwrap_or_else(|_| {
                tracing::warn!(key = key_value, "failed to parse key as int64, using 0");
                0
            });
            Arc::new(Int64Array::from(vec![v]))
        }
        "float64" => {
            let v = key_value.parse::<f64>().unwrap_or_else(|_| {
                tracing::warn!(key = key_value, "failed to parse key as float64, using 0.0");
                0.0
            });
            Arc::new(Float64Array::from(vec![v]))
        }
        "bool" => {
            let v = key_value.parse::<bool>().unwrap_or_else(|_| {
                tracing::warn!(key = key_value, "failed to parse key as bool, using false");
                false
            });
            Arc::new(BooleanArray::from(vec![v]))
        }
        _ => Arc::new(StringArray::from(vec![key_value])),
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;
    use crate::aggregate::AggFunction;
    use krishiv_state::{FjallStateBackend, Namespace};

    #[test]
    fn tumbling_state_persist_and_restore_roundtrip() {
        let spec = TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
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

        let mut backend = FjallStateBackend::ephemeral().unwrap();
        let ns = Namespace::new("op-1", "windows");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = TumblingWindowOperator::new(TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
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
