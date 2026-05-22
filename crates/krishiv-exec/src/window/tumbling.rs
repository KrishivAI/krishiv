use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};
use crate::aggregate::{AggExpr, AggState};
use crate::join::format_key_value;

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
