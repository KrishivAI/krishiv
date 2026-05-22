use std::collections::HashMap;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};
use crate::aggregate::{AggExpr, AggState};
use crate::join::format_key_value;
use crate::window::tumbling::build_window_record_batch;

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
