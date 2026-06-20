use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use krishiv_state::{Namespace, StateBackend, StateResult};

use crate::aggregate::{AggExpr, AggState};
use crate::join::extract_agg_key;
use crate::window::tumbling::{
    WindowRecordBatchInput, build_window_output_schema, build_window_record_batch,
};
use crate::{ExecError, ExecResult};

/// Configuration for a sliding event-time window operator (R5.2).
///
/// A sliding window of size `window_size_ms` that advances by `slide_ms` means
/// an event belongs to `ceil(window_size_ms / slide_ms)` overlapping windows.
#[derive(Debug, Clone)]
pub struct SlidingWindowSpec {
    /// Column used to key the stream.
    pub key_column: String,
    /// Arrow type of the key column: `"int32"`, `"int64"`, `"float64"`, `"utf8"`, `"bool"`.
    /// Defaults to `"utf8"`.
    pub key_column_type: String,
    /// Int64 column carrying event time in milliseconds.
    pub event_time_column: String,
    /// Total window duration in milliseconds.
    pub window_size_ms: u64,
    /// Window advance step in milliseconds (must be ≤ `window_size_ms`).
    pub slide_ms: u64,
    /// Aggregate expressions to apply within each window.
    pub agg_exprs: Vec<AggExpr>,
    /// Per-aggregate float flag: `true` when the aggregate input column is `Float64`.
    pub agg_is_float: Vec<bool>,
}

/// Sliding event-time window operator (R5.2).
///
/// Each event is placed into every window `[w, w + size)` where
/// `w` is a multiple of `slide_ms` and `w ≤ event_time_ms < w + size`.
///
/// **Memory bound**: like [`TumblingWindowOperator`](super::tumbling::TumblingWindowOperator),
/// `accumulators` retains one entry per `(key, window_start)` until the
/// watermark closes that window; sliding windows additionally fan each event
/// out into `ceil(window_size_ms / slide_ms)` overlapping windows, so live
/// state is roughly that factor larger than tumbling for the same key
/// cardinality. There is no key-eviction or TTL — bound memory by choosing
/// `window_size_ms`/`slide_ms` and watermark lag appropriate to the expected
/// key cardinality, and pre-aggregate/filter upstream for high-cardinality keys.
#[derive(Debug)]
pub struct SlidingWindowOperator {
    spec: SlidingWindowSpec,
    // (serialised_key, window_start_ms) → aggregate accumulator
    accumulators: HashMap<(String, i64), AggState>,
    prev_watermark_ms: i64,
    /// Total late events dropped by this operator since creation.
    pub late_events_dropped: u64,
    /// Output schema, fixed for the operator's lifetime; cached so closed
    /// windows don't rebuild `Schema`/`Field` vectors per row.
    output_schema: Arc<Schema>,
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
        if spec.window_size_ms == 0 {
            return Err(ExecError::InvalidWindowConfig(
                "window_size_ms must be non-zero".into(),
            ));
        }
        if spec.window_size_ms > i64::MAX as u64 || spec.slide_ms > i64::MAX as u64 {
            return Err(ExecError::InvalidWindowConfig(format!(
                "sliding window size ({}) or slide ({}) exceeds i64::MAX",
                spec.window_size_ms, spec.slide_ms,
            )));
        }
        let output_schema = build_window_output_schema(
            &spec.key_column,
            &spec.key_column_type,
            &spec.agg_exprs,
            &spec.agg_is_float,
        );
        Ok(Self {
            spec,
            accumulators: HashMap::new(),
            prev_watermark_ms: i64::MIN,
            late_events_dropped: 0,
            output_schema,
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
        super::state_persistence::persist_window_accumulators(
            backend,
            namespace,
            &self.accumulators,
            b"sw:",
        )?;
        super::state_persistence::persist_operator_watermark_ms(
            backend,
            namespace,
            self.prev_watermark_ms,
        )
    }

    /// Restore open sliding window accumulators from `StateBackend`.
    pub fn restore_from_state(
        &mut self,
        backend: &dyn StateBackend,
        namespace: &Namespace,
    ) -> StateResult<()> {
        self.accumulators =
            super::state_persistence::restore_window_accumulators(backend, namespace, b"sw:")?;
        if let Some(wm) =
            super::state_persistence::restore_operator_watermark_ms(backend, namespace)?
        {
            self.prev_watermark_ms = wm;
        }
        Ok(())
    }

    /// All window starts (multiples of `slide`) that contain `event_time_ms`.
    ///
    /// Uses checked arithmetic throughout to avoid overflow for timestamps near
    /// i64::MIN or i64::MAX.  Returns an empty vec when all window starts would
    /// underflow rather than panicking.
    fn window_starts(event_time_ms: i64, size_ms: u64, slide_ms: u64) -> Vec<i64> {
        let slide = slide_ms as i64;
        let size = size_ms as i64;

        let q = event_time_ms / slide;
        let r = event_time_ms % slide;
        // Compute the latest window start ≤ event_time_ms with checked arithmetic
        // so that event times near i64::MIN cannot overflow.
        let first = if r < 0 {
            match q.checked_sub(1).and_then(|q1| q1.checked_mul(slide)) {
                Some(f) => f,
                None => return vec![],
            }
        } else {
            match q.checked_mul(slide) {
                Some(f) => f,
                None => return vec![],
            }
        };

        let count = match (size as u64)
            .checked_add(slide_ms)
            .and_then(|n| n.checked_sub(1))
        {
            Some(n) => (n / slide_ms) as usize,
            None => usize::MAX,
        };
        let mut starts = Vec::with_capacity(count.min(1024));
        let mut s = first;
        while let Some(sum) = s.checked_add(size) {
            if sum <= event_time_ms {
                break;
            }
            starts.push(s);
            match s.checked_sub(slide) {
                Some(next) => s = next,
                None => break,
            }
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
                self.late_events_dropped = self.late_events_dropped.saturating_add(1);
                continue;
            }
            let key = extract_agg_key(batch, key_idx, row)?.to_string();
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

        if new_watermark_ms >= self.prev_watermark_ms {
            self.prev_watermark_ms = new_watermark_ms;
        }
        self.flush_closed_windows(new_watermark_ms)
    }

    /// Flush windows whose end time is ≤ `watermark_ms`.
    pub fn flush_closed_windows(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        let size = self.spec.window_size_ms as i64;
        let mut closed: Vec<(String, i64)> = self
            .accumulators
            .keys()
            .filter(|(_, ws)| ws.saturating_add(size) <= watermark_ms)
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
        build_window_record_batch(WindowRecordBatchInput {
            schema: &self.output_schema,
            key_type: &self.spec.key_column_type,
            key_value,
            window_start_ms,
            window_end_ms,
            agg_exprs: &self.spec.agg_exprs,
            state,
            agg_is_float: &self.spec.agg_is_float,
        })
    }
}

#[cfg(test)]
fn parse_sliding_state_key(bytes: &[u8]) -> Option<(String, i64)> {
    // GAP-18: length-prefix format: b"sw:" | key_len_le_u32 | key_bytes | win_start_le_i64
    const PREFIX: &[u8] = b"sw:";
    if !bytes.starts_with(PREFIX) {
        return None;
    }
    let rest = &bytes[PREFIX.len()..];
    let key_len = u32::from_le_bytes(rest.get(..4)?.try_into().ok()?) as usize;
    let key = std::str::from_utf8(rest.get(4..4 + key_len)?)
        .ok()?
        .to_string();
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
    use krishiv_state::{Namespace, RocksDbStateBackend};

    #[test]
    fn sliding_state_persist_and_restore_roundtrip() {
        let spec = SlidingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
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

        let mut backend = RocksDbStateBackend::ephemeral().unwrap();
        let ns = Namespace::new("op-sliding", "windows");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = SlidingWindowOperator::new(SlidingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
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

    #[test]
    fn window_starts_near_i64_min_does_not_panic() {
        // Timestamps near i64::MIN should return an empty vec or a valid small
        // set of starts rather than panicking or overflowing.
        let very_negative = i64::MIN + 1;
        let starts = SlidingWindowOperator::window_starts(very_negative, 1000, 500);
        // Result must either be empty (underflow path) or contain valid i64 values.
        for &s in &starts {
            let end = s.checked_add(1000).expect("window end must not overflow");
            assert!(
                end > very_negative,
                "every window start must produce an end > event_time"
            );
        }
    }

    #[test]
    fn window_starts_i64_min_exactly_does_not_panic() {
        let starts = SlidingWindowOperator::window_starts(i64::MIN, 2000, 1000);
        for &s in &starts {
            assert!(
                s.checked_add(2000).is_some(),
                "window end must not overflow i64"
            );
        }
    }

    /// Regression (Wave 1 — Data Correctness): `new()` must reject
    /// `slide_ms == 0` (would loop forever in `window_starts`),
    /// `window_size_ms == 0`, and sizes/slides exceeding `i64::MAX` (would
    /// overflow the `checked_add`/`s + size` arithmetic in `window_starts`).
    #[test]
    fn new_rejects_zero_and_overflowing_size_or_slide() {
        let base = SlidingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            slide_ms: 0,
            agg_exprs: vec![AggExpr {
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
        };
        assert!(matches!(
            SlidingWindowOperator::new(base.clone()),
            Err(ExecError::InvalidWindowConfig(_))
        ));

        let zero_window = SlidingWindowSpec {
            window_size_ms: 0,
            slide_ms: 500,
            ..base.clone()
        };
        assert!(matches!(
            SlidingWindowOperator::new(zero_window),
            Err(ExecError::InvalidWindowConfig(_))
        ));

        let overflowing_size = SlidingWindowSpec {
            window_size_ms: i64::MAX as u64 + 1,
            slide_ms: 500,
            ..base.clone()
        };
        assert!(matches!(
            SlidingWindowOperator::new(overflowing_size),
            Err(ExecError::InvalidWindowConfig(_))
        ));

        let overflowing_slide = SlidingWindowSpec {
            window_size_ms: 1000,
            slide_ms: i64::MAX as u64 + 1,
            ..base.clone()
        };
        assert!(matches!(
            SlidingWindowOperator::new(overflowing_slide),
            Err(ExecError::InvalidWindowConfig(_))
        ));

        let valid = SlidingWindowSpec {
            window_size_ms: 1000,
            slide_ms: 500,
            ..base
        };
        assert!(SlidingWindowOperator::new(valid).is_ok());
    }
}
