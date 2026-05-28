//! Stateful continuous window execution across drain cycles.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use krishiv_plan::window::{WindowExecutionSpec, WindowKind};
use krishiv_state::{InMemoryStateBackend, StateBackend, TtlConfig, TtlStateBackend};

use crate::operator_runtime::window_agg_to_expr;
use crate::watermark_util::advance_effective_watermark;
use crate::window::MultiSourceWatermarkState;
use crate::{
    AggExpr, ExecError, ExecResult, SessionWindowOperator, SessionWindowSpec,
    SlidingWindowOperator, SlidingWindowSpec, StateBackedSessionWindowOperator,
    StateBackedSlidingWindowOperator, StateBackedTumblingWindowOperator, TumblingWindowOperator,
    TumblingWindowSpec, WatermarkState,
};

enum WindowOperatorState {
    Tumbling(TumblingWindowOperator),
    TumblingState(Box<StateBackedTumblingWindowOperator>),
    Sliding(SlidingWindowOperator),
    SlidingState(Box<StateBackedSlidingWindowOperator>),
    Session(SessionWindowOperator),
    SessionState(Box<StateBackedSessionWindowOperator>),
}

/// Tracks single- or multi-source watermark state for continuous execution.
struct WatermarkTracker {
    single: WatermarkState,
    multi: MultiSourceWatermarkState,
    source_lags: HashMap<String, u64>,
    source_id_column: Option<String>,
    event_time_column: String,
}

impl WatermarkTracker {
    fn new(spec: &WindowExecutionSpec) -> Self {
        Self {
            single: WatermarkState::new(spec.watermark_lag_ms),
            multi: MultiSourceWatermarkState::new(),
            source_lags: spec.source_watermark_lags.clone(),
            source_id_column: spec.source_id_column.clone(),
            event_time_column: spec.event_time_column.clone(),
        }
    }

    fn advance(&mut self, batch: &RecordBatch) -> ExecResult<i64> {
        advance_effective_watermark(
            batch,
            &self.event_time_column,
            self.source_id_column.as_deref(),
            &self.source_lags,
            &mut self.single,
            &mut self.multi,
        )
    }
}

fn build_operator(
    spec: &WindowExecutionSpec,
    agg_exprs: &[AggExpr],
) -> ExecResult<WindowOperatorState> {
    match spec.window_kind {
        WindowKind::Tumbling => {
            let tw_spec = TumblingWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                agg_exprs: agg_exprs.to_vec(),
            };
            if let Some(ttl_ms) = spec.state_ttl_ms {
                let inner = InMemoryStateBackend::new();
                let state: Box<dyn StateBackend> =
                    Box::new(TtlStateBackend::new(inner, TtlConfig::new(ttl_ms)));
                let op = StateBackedTumblingWindowOperator::new(
                    tw_spec,
                    state,
                    "continuous-window",
                    "tumbling",
                )
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
                Ok(WindowOperatorState::TumblingState(Box::new(op)))
            } else {
                Ok(WindowOperatorState::Tumbling(TumblingWindowOperator::new(
                    tw_spec,
                )))
            }
        }
        WindowKind::Sliding => {
            let slide_ms = spec.slide_ms.unwrap_or(spec.window_size_ms);
            let sw_spec = SlidingWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                slide_ms,
                agg_exprs: agg_exprs.to_vec(),
            };
            if let Some(ttl_ms) = spec.state_ttl_ms {
                let inner = InMemoryStateBackend::new();
                let state: Box<dyn StateBackend> =
                    Box::new(TtlStateBackend::new(inner, TtlConfig::new(ttl_ms)));
                let op = StateBackedSlidingWindowOperator::new(
                    sw_spec,
                    state,
                    "continuous-window",
                    "sliding",
                )
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
                Ok(WindowOperatorState::SlidingState(Box::new(op)))
            } else {
                Ok(WindowOperatorState::Sliding(SlidingWindowOperator::new(
                    sw_spec,
                )?))
            }
        }
        WindowKind::Session => {
            let gap_ms = spec.session_gap_ms.unwrap_or(spec.window_size_ms);
            let sess_spec = SessionWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                session_gap_ms: gap_ms,
                agg_exprs: agg_exprs.to_vec(),
            };
            if let Some(ttl_ms) = spec.state_ttl_ms {
                let inner = InMemoryStateBackend::new();
                let state: Box<dyn StateBackend> =
                    Box::new(TtlStateBackend::new(inner, TtlConfig::new(ttl_ms)));
                let op = StateBackedSessionWindowOperator::new(
                    sess_spec,
                    state,
                    "continuous-window",
                    "session",
                )
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
                Ok(WindowOperatorState::SessionState(Box::new(op)))
            } else {
                Ok(WindowOperatorState::Session(SessionWindowOperator::new(
                    sess_spec,
                )))
            }
        }
    }
}

impl WindowOperatorState {
    fn process_batch(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        match self {
            Self::Tumbling(op) => op.process_batch(batch, watermark_ms),
            Self::TumblingState(op) => op.process_batch(batch, watermark_ms),
            Self::Sliding(op) => op.process_batch(batch, watermark_ms),
            Self::SlidingState(op) => op.process_batch(batch, watermark_ms),
            Self::Session(op) => op.process_batch(batch, watermark_ms),
            Self::SessionState(op) => op.process_batch(batch, watermark_ms),
        }
    }

    /// GAP-15: Evict expired entries from TTL-backed state variants.
    ///
    /// Non-TTL variants (Tumbling, Sliding, Session) are no-ops.
    /// State-backed variants delegate to the underlying `StateBackend::purge_expired`.
    fn purge_expired(&mut self) -> ExecResult<usize> {
        match self {
            Self::Tumbling(_) | Self::Sliding(_) | Self::Session(_) => Ok(0),
            Self::TumblingState(op) => op.purge_expired(),
            Self::SlidingState(op) => op.purge_expired(),
            Self::SessionState(op) => op.purge_expired(),
        }
    }

    /// Propagate the event-time watermark to the underlying TTL state backend.
    ///
    /// For TTL-backed variants (`TumblingState`, `SlidingState`, `SessionState`)
    /// this forwards to the operator's `set_watermark`, which in turn calls
    /// `StateBackend::set_watermark` on the inner `TtlStateBackend`.  Subsequent
    /// calls to `purge_expired` and lazy read-time expiry checks will then use
    /// event time rather than wall-clock time.
    ///
    /// Non-TTL variants are no-ops (the method is still valid to call; the
    /// underlying plain operators carry no TTL state to evict).
    fn set_watermark(&mut self, watermark_ms: i64) {
        match self {
            Self::Tumbling(_) | Self::Sliding(_) | Self::Session(_) => {}
            Self::TumblingState(op) => op.set_watermark(watermark_ms),
            Self::SlidingState(op) => op.set_watermark(watermark_ms),
            Self::SessionState(op) => op.set_watermark(watermark_ms),
        }
    }
}

// ── StreamQualityHook ─────────────────────────────────────────────────────────

/// Optional quality-gate hook for the streaming drain cycle (R10).
///
/// Implementations run data-quality rules against each emitted output batch.
/// Accepted rows are returned; rejected rows are routed to a dead-letter output.
/// The trait is defined here (in exec) so that `ContinuousWindowExecutor` can
/// hold it without creating a circular dependency on `krishiv-connectors`.
///
/// Implement this trait in `krishiv-connectors` using `CompiledDataQualityConfig`
/// and `DeadLetterSink`, then inject it via
/// [`ContinuousWindowExecutor::with_quality_hook`].
pub trait StreamQualityHook: Send {
    /// Apply quality rules to one output `batch`.
    ///
    /// Returns the accepted sub-batch (possibly smaller than the input) and
    /// the number of rejected rows routed to the dead-letter output.
    fn filter(&mut self, batch: RecordBatch) -> ExecResult<(RecordBatch, usize)>;
}

/// Retains window operator state between continuous streaming drain cycles.
pub struct ContinuousWindowExecutor {
    watermark: WatermarkTracker,
    operator: WindowOperatorState,
    /// Optional data-quality gate applied to each emitted output batch.
    quality_hook: Option<Box<dyn StreamQualityHook>>,
    /// Most recently computed event-time watermark in milliseconds.
    ///
    /// Persisted across drain cycles so that `purge_expired` at the start of
    /// each cycle uses the watermark from the previous cycle rather than falling
    /// back to wall-clock time.  Starts at `i64::MIN` (no watermark seen yet).
    last_watermark_ms: i64,
}

impl ContinuousWindowExecutor {
    /// Create a new continuous executor for `spec`.
    pub fn new(spec: WindowExecutionSpec) -> ExecResult<Self> {
        if spec.agg_exprs.is_empty() {
            return Err(ExecError::InvalidWindowConfig(
                "window execution requires at least one aggregate".into(),
            ));
        }
        let agg_exprs: Vec<AggExpr> = spec.agg_exprs.iter().map(window_agg_to_expr).collect();
        Ok(Self {
            watermark: WatermarkTracker::new(&spec),
            operator: build_operator(&spec, &agg_exprs)?,
            quality_hook: None,
            last_watermark_ms: i64::MIN,
        })
    }

    /// Attach a data-quality hook that filters each output batch.
    ///
    /// When set, every batch emitted by the window operator passes through
    /// [`StreamQualityHook::filter`] before being returned from [`drain`].
    /// Rejected rows are handled by the hook implementation (e.g. written to a
    /// dead-letter Parquet file or logged).
    #[must_use]
    pub fn with_quality_hook(mut self, hook: Box<dyn StreamQualityHook>) -> Self {
        self.quality_hook = Some(hook);
        self
    }

    /// Process newly arrived input batches and return any emitted output.
    ///
    /// GAP-15: At the start of each drain cycle we call `purge_expired` on the
    /// underlying window operator.  For non-TTL operators this is a no-op
    /// (returns 0).  For TTL-backed operators it walks all namespaces once and
    /// deletes entries that have passed their `expires_at_ms` timestamp.
    /// This prevents unbounded growth from entries that were written once and
    /// never read again after expiry (lazy-delete alone is insufficient).
    ///
    /// Watermark propagation: before eviction, the operator's TTL state backend
    /// is updated with the watermark computed during the *previous* drain cycle
    /// (`last_watermark_ms`).  This ensures that `purge_expired` uses event time
    /// rather than wall-clock time even for keys that were never read again after
    /// expiry.  Within the batch loop, `set_watermark` is called again after each
    /// watermark advance so that lazy read-time expiry also reflects event time.
    pub fn drain(&mut self, input_batches: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>> {
        // Propagate the most recently known event-time watermark to the TTL
        // state backend before eviction so that purge_expired uses event time.
        // On the very first drain cycle last_watermark_ms == i64::MIN and the
        // backend falls back to wall-clock time (no-op for non-TTL operators).
        if self.last_watermark_ms != i64::MIN {
            self.operator.set_watermark(self.last_watermark_ms);
        }

        // Eagerly evict stale TTL entries before processing new data.
        self.operator.purge_expired()?;

        if input_batches.is_empty() {
            return Ok(Vec::new());
        }
        let mut raw: Vec<RecordBatch> = Vec::new();
        for batch in &input_batches {
            let wm = self.watermark.advance(batch)?;
            // Keep the TTL backend's event-time reference current as the
            // watermark advances within this drain cycle.
            self.operator.set_watermark(wm);
            self.last_watermark_ms = wm;
            raw.extend(self.operator.process_batch(batch, wm)?);
        }
        if self.quality_hook.is_none() || raw.is_empty() {
            return Ok(raw);
        }
        let hook = self.quality_hook.as_mut().unwrap();
        let mut output = Vec::with_capacity(raw.len());
        for batch in raw {
            let (accepted, _rejected_count) = hook.filter(batch)?;
            if accepted.num_rows() > 0 {
                output.push(accepted);
            }
        }
        Ok(output)
    }

    /// Borrow the underlying window spec fields (for diagnostics).
    pub fn uses_multi_source_watermark(&self) -> bool {
        !self.watermark.source_lags.is_empty()
    }
}

impl std::fmt::Debug for ContinuousWindowExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContinuousWindowExecutor")
            .field(
                "multi_source",
                &self.watermark.source_lags.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_plan::window::WindowExecutionSpec;

    use super::*;

    fn events_batch(ts: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![ts])) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn continuous_executor_emits_window_after_watermark_passes_boundary() {
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        let mut spec = spec;
        spec.watermark_lag_ms = 0;
        let mut exec = ContinuousWindowExecutor::new(spec).expect("create");
        // Events at timestamp 1_000 and 12_000 span two tumbling windows [0, 10000) and [10000, 20000).
        let _ = exec.drain(vec![events_batch(1_000)]).expect("drain1");
        // After receiving event at 12_000, watermark advances past the first window.
        let second = exec.drain(vec![events_batch(12_000)]).expect("drain2");
        // First window [0, 10000) should be emitted (at least one row).
        assert!(!second.is_empty(), "expected first window to be emitted");
    }

    #[test]
    fn multi_source_watermark_configured() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.source_id_column = Some("source_id".into());
        spec.source_watermark_lags =
            HashMap::from([("src-a".into(), 1_000), ("src-b".into(), 2_000)]);
        let exec = ContinuousWindowExecutor::new(spec).expect("create");
        assert!(exec.uses_multi_source_watermark());
    }
}
