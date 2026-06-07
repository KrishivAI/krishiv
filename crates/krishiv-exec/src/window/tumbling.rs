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
///
/// **Memory bound**: `accumulators` holds one entry per `(key, window_start)`
/// pair until the watermark closes that window, at which point the entry is
/// flushed and removed. There is no key-eviction or TTL on open windows —
/// memory is bounded by `live_key_cardinality × open_window_count`, which the
/// deployment must keep finite by choosing `window_size_ms` and watermark lag
/// appropriate to the expected key cardinality. Pipelines with unbounded or
/// very high-cardinality keys should reduce `window_size_ms` and/or
/// pre-aggregate/filter keys upstream rather than rely on this operator to
/// bound state.
pub struct TumblingWindowOperator {
    spec: TumblingWindowSpec,
    accumulators: HashMap<(String, i64), AggState>,
    prev_watermark_ms: i64,
    pub late_events_dropped: u64,
    late_event_handler: Option<Box<dyn LateEventHandler>>,
    /// Output schema, fixed for the operator's lifetime; cached so closed
    /// windows don't rebuild `Schema`/`Field` vectors per row.
    output_schema: Arc<Schema>,
}

impl TumblingWindowOperator {
    /// Create a new operator.
    pub fn new(spec: TumblingWindowSpec) -> Self {
        let output_schema =
            build_window_output_schema(&spec.key_column, &spec.key_column_type, &spec.agg_exprs);
        Self {
            spec,
            accumulators: HashMap::new(),
            prev_watermark_ms: i64::MIN,
            late_events_dropped: 0,
            late_event_handler: None,
            output_schema,
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
            return Err(ExecError::InvalidWindowConfig(format!(
                "tumbling window_size_ms ({}) exceeds i64::MAX",
                spec.window_size_ms,
            )));
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
            &self.output_schema,
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
/// Build the (fixed) output schema for a tumbling/sliding window operator.
///
/// The schema depends only on `key_column`, `key_type`, and `agg_exprs`,
/// which are immutable for the operator's lifetime — callers should compute
/// this once (e.g. in `new`) and reuse the cached `Arc<Schema>` for every
/// closed window, rather than rebuilding `Schema`/`Field` vectors per row.
pub(crate) fn build_window_output_schema(
    key_column: &str,
    key_type: &str,
    agg_exprs: &[AggExpr],
) -> Arc<Schema> {
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
    Arc::new(Schema::new(fields))
}

pub(crate) fn build_window_record_batch(
    schema: &Arc<Schema>,
    key_type: &str,
    key_value: &str,
    window_start_ms: i64,
    window_end_ms: i64,
    agg_exprs: &[AggExpr],
    state: &AggState,
) -> ExecResult<RecordBatch> {
    let schema = Arc::clone(schema);
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

    /// Regression (Wave 1 — Data Correctness): `validate_spec` must reject
    /// `window_size_ms == 0` and values exceeding `i64::MAX` rather than
    /// letting `window_start`'s `event_time_ms / size` divide by zero or
    /// silently truncate via `as i64`.
    #[test]
    fn validate_spec_rejects_zero_and_overflowing_window_size() {
        let base = TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 0,
            agg_exprs: vec![AggExpr {
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
        };
        assert!(matches!(
            TumblingWindowOperator::validate_spec(&base),
            Err(ExecError::InvalidWindowConfig(_))
        ));

        let overflowing = TumblingWindowSpec {
            window_size_ms: i64::MAX as u64 + 1,
            ..base.clone()
        };
        assert!(matches!(
            TumblingWindowOperator::validate_spec(&overflowing),
            Err(ExecError::InvalidWindowConfig(_))
        ));

        let valid = TumblingWindowSpec {
            window_size_ms: 1000,
            ..base
        };
        assert!(TumblingWindowOperator::validate_spec(&valid).is_ok());
    }
}

/// Property-based aggregation-correctness tests for `TumblingWindowOperator`.
///
/// `cargo-fuzz` requires a nightly toolchain and sanitizer support that this
/// workspace does not provision; `proptest` gives equivalent adversarial-input
/// coverage (arbitrary in-order event sequences, shrinking on failure) entirely
/// on stable, so it is the practical choice for exercising window aggregation
/// invariants such as "every accepted row is counted exactly once" and
/// "the windowed sum equals the sum of its inputs".
#[cfg(test)]
mod aggregation_proptests {
    use super::*;
    use crate::aggregate::AggFunction;
    use proptest::prelude::*;

    /// Arbitrary in-order `(event_time_ms, value)` sequences confined to a
    /// single 1000ms window `[0, 1000)`, with values small enough that their
    /// sum cannot overflow `i64`.
    fn arb_single_window_events() -> impl Strategy<Value = Vec<(i64, i64)>> {
        prop::collection::vec((0i64..1000, -10_000i64..10_000), 0..32).prop_map(|mut events| {
            events.sort_by_key(|(ts, _)| *ts);
            events
        })
    }

    fn make_batch(events: &[(i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let keys = vec!["k"; events.len()];
        let timestamps: Vec<i64> = events.iter().map(|(ts, _)| *ts).collect();
        let values: Vec<i64> = events.iter().map(|(_, v)| *v).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .expect("schema and array lengths match")
    }

    fn spec() -> TumblingWindowSpec {
        TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![
                AggExpr {
                    function: AggFunction::Count,
                    input_column: String::new(),
                    output_column: "cnt".into(),
                },
                AggExpr {
                    function: AggFunction::Sum,
                    input_column: "v".into(),
                    output_column: "sum_v".into(),
                },
            ],
        }
    }

    proptest! {
        /// Closing a single window over an arbitrary in-order event sequence
        /// must never panic, and — when the sequence is non-empty — must
        /// yield exactly one output row whose `cnt`/`sum_v` losslessly
        /// reflect every accepted input row exactly once.
        #[test]
        fn tumbling_window_count_and_sum_are_lossless(events in arb_single_window_events()) {
            let mut op = TumblingWindowOperator::new(spec());
            let batch = make_batch(&events);

            // Watermark = window_size_ms closes the single `[0, 1000)` window.
            let outputs = op.process_batch(&batch, 1000).expect("process_batch");

            if events.is_empty() {
                prop_assert!(outputs.is_empty());
            } else {
                prop_assert_eq!(outputs.len(), 1);
                let out = &outputs[0];
                prop_assert_eq!(out.num_rows(), 1);

                let cnt = out
                    .column(out.schema().index_of("cnt").unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                let sum_v = out
                    .column(out.schema().index_of("sum_v").unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);

                prop_assert_eq!(cnt as usize, events.len());
                prop_assert_eq!(sum_v, events.iter().map(|(_, v)| v).sum::<i64>());
            }

            // No state should remain open once the only window has closed.
            prop_assert_eq!(op.open_window_count(), 0);
        }
    }
}
