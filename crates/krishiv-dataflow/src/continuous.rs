//! Stateful continuous window execution across drain cycles.

use std::collections::HashMap;

use crate::operator_runtime::{open_state_backend, window_agg_to_expr};
use crate::watermark_util::advance_effective_watermark;
use crate::window::MultiSourceWatermarkState;
use crate::window::{CountWindowOperator, CountWindowSpec};
use crate::{
    AggExpr, ExecError, ExecResult, SessionWindowSpec, SlidingWindowSpec,
    StateBackedSessionWindowOperator, StateBackedSlidingWindowOperator,
    StateBackedTumblingWindowOperator, TumblingWindowSpec, WatermarkState,
};
use arrow::record_batch::RecordBatch;
use krishiv_plan::window::{WindowExecutionSpec, WindowKind, validate_window_execution_spec};

enum WindowOperatorState {
    Tumbling(Box<StateBackedTumblingWindowOperator>),
    Sliding(Box<StateBackedSlidingWindowOperator>),
    Session(Box<StateBackedSessionWindowOperator>),
    Count(Box<CountWindowOperator>),
}

/// Tracks single- or multi-source watermark state for continuous execution.
#[derive(Clone)]
struct WatermarkTracker {
    single: WatermarkState,
    multi: MultiSourceWatermarkState,
    source_lags: HashMap<String, u64>,
    source_id_column: Option<String>,
    event_time_column: String,
}

impl WatermarkTracker {
    fn new(spec: &WindowExecutionSpec) -> Self {
        let mut multi = MultiSourceWatermarkState::new();
        if !spec.source_watermark_lags.is_empty() {
            // C2: Configure idle-source policy to prevent one stalled source
            // from freezing all windows. Default: mark idle after 5 min.
            multi = multi.with_idle_source_policy(300_000, i64::MAX);
        }
        Self {
            single: WatermarkState::new(spec.watermark_lag_ms),
            multi,
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

    /// C2: Apply idle-source policy so stalled sources don't hold back all windows.
    fn apply_idle_source_policy(&mut self) {
        self.multi.apply_idle_source_policy();
    }
}

fn build_operator(
    spec: &WindowExecutionSpec,
    agg_exprs: &[AggExpr],
    state_dir: Option<&std::path::Path>,
) -> ExecResult<WindowOperatorState> {
    match spec.window_kind {
        WindowKind::Tumbling => {
            let tw_spec = TumblingWindowSpec {
                key_column: spec.key_column.clone(),
                key_column_type: spec.key_column_type.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                agg_exprs: agg_exprs.to_vec(),
            };
            let state = open_state_backend(state_dir, "tumbling", spec.state_ttl_ms)?;
            let op = StateBackedTumblingWindowOperator::new(
                tw_spec,
                state,
                "continuous-window",
                "tumbling",
            )
            .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            Ok(WindowOperatorState::Tumbling(Box::new(op)))
        }
        WindowKind::Sliding => {
            let slide_ms = spec.slide_ms.unwrap_or(spec.window_size_ms);
            let sw_spec = SlidingWindowSpec {
                key_column: spec.key_column.clone(),
                key_column_type: spec.key_column_type.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                slide_ms,
                agg_exprs: agg_exprs.to_vec(),
            };
            let state = open_state_backend(state_dir, "sliding", spec.state_ttl_ms)?;
            let op = StateBackedSlidingWindowOperator::new(
                sw_spec,
                state,
                "continuous-window",
                "sliding",
            )
            .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            Ok(WindowOperatorState::Sliding(Box::new(op)))
        }
        WindowKind::Session => {
            let gap_ms = spec.session_gap_ms.unwrap_or(spec.window_size_ms);
            let sess_spec = SessionWindowSpec {
                key_column: spec.key_column.clone(),
                key_column_type: spec.key_column_type.clone(),
                event_time_column: spec.event_time_column.clone(),
                session_gap_ms: gap_ms,
                agg_exprs: agg_exprs.to_vec(),
            };
            let state = open_state_backend(state_dir, "session", spec.state_ttl_ms)?;
            let op = StateBackedSessionWindowOperator::new(
                sess_spec,
                state,
                "continuous-window",
                "session",
            )
            .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            Ok(WindowOperatorState::Session(Box::new(op)))
        }
        WindowKind::Count { size, slide } => {
            let count_spec = CountWindowSpec {
                key_column: spec.key_column.clone(),
                key_column_type: spec.key_column_type.clone(),
                size,
                slide,
                agg_exprs: agg_exprs.to_vec(),
            };
            let op = CountWindowOperator::new(count_spec)
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            Ok(WindowOperatorState::Count(Box::new(op)))
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
            Self::Sliding(op) => op.process_batch(batch, watermark_ms),
            Self::Session(op) => op.process_batch(batch, watermark_ms),
            // Count windows are row-indexed, watermark is unused.
            Self::Count(op) => op.process_batch(batch),
        }
    }

    fn purge_expired(&mut self) -> ExecResult<usize> {
        match self {
            Self::Tumbling(op) => op.purge_expired(),
            Self::Sliding(op) => op.purge_expired(),
            Self::Session(op) => op.purge_expired(),
            Self::Count(_) => Ok(0), // no time-based eviction
        }
    }

    fn set_watermark(&mut self, watermark_ms: i64) {
        match self {
            Self::Tumbling(op) => op.set_watermark(watermark_ms),
            Self::Sliding(op) => op.set_watermark(watermark_ms),
            Self::Session(op) => op.set_watermark(watermark_ms),
            Self::Count(_) => {} // count windows have no time-watermark
        }
    }

    /// C3: Persist operator state so crash recovery can restore open windows.
    fn checkpoint(&mut self) -> ExecResult<()> {
        match self {
            Self::Tumbling(op) => op.checkpoint(),
            Self::Sliding(op) => op.checkpoint(),
            Self::Session(op) => op.checkpoint(),
            Self::Count(_) => Ok(()), // in-memory; no persistence
        }
    }

    /// C9: Serialize state backend contents to portable bytes.
    fn snapshot_state_bytes(&self) -> krishiv_state::StateResult<Vec<u8>> {
        match self {
            Self::Tumbling(op) => op.snapshot_state_bytes(),
            Self::Sliding(op) => op.snapshot_state_bytes(),
            Self::Session(op) => op.snapshot_state_bytes(),
            Self::Count(_) => Ok(vec![]), // no persistent state
        }
    }

    /// C9: Restore state backend from bytes produced by `snapshot_state_bytes`.
    fn load_snapshot_bytes(&mut self, bytes: &[u8]) -> krishiv_state::StateResult<()> {
        match self {
            Self::Tumbling(op) => op.load_snapshot_bytes(bytes),
            Self::Sliding(op) => op.load_snapshot_bytes(bytes),
            Self::Session(op) => op.load_snapshot_bytes(bytes),
            Self::Count(_) => Ok(()),
        }
    }

    /// Merge a snapshot additively (existing entries preserved).
    fn merge_snapshot_bytes(&mut self, bytes: &[u8]) -> krishiv_state::StateResult<()> {
        match self {
            Self::Tumbling(op) => op.merge_snapshot_bytes(bytes),
            Self::Sliding(op) => op.merge_snapshot_bytes(bytes),
            Self::Session(op) => op.merge_snapshot_bytes(bytes),
            Self::Count(_) => Ok(()),
        }
    }
}

// ── StreamQualityHook ─────────────────────────────────────────────────────────

/// Optional quality-gate hook for the streaming drain cycle (R10).
pub use krishiv_common::StreamQualityHook;

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
    /// Total rows rejected by `quality_hook` across all drain cycles.
    rejected_rows_total: u64,
}

impl ContinuousWindowExecutor {
    /// Create a new continuous executor for `spec` using ephemeral (in-memory) state.
    ///
    /// Correct for `DevLocal` / embedded execution. For single-node-durable or distributed
    /// deployments use [`new_with_state_dir`] so window state survives executor restarts.
    pub fn new(spec: WindowExecutionSpec) -> ExecResult<Self> {
        Self::new_with_state_dir(spec, None)
    }

    /// Create a new continuous executor with an optional persistent state directory.
    ///
    /// - `state_dir = None` → ephemeral (dev-local, same as [`new`])
    /// - `state_dir = Some(path)` → file-backed Fjall state under `path/` (single-node-durable
    ///   or distributed-durable); survives executor restart
    pub fn new_with_state_dir(
        spec: WindowExecutionSpec,
        state_dir: Option<&std::path::Path>,
    ) -> ExecResult<Self> {
        validate_window_execution_spec(&spec)
            .map_err(|error| ExecError::InvalidWindowConfig(error.to_string()))?;
        let agg_exprs: Vec<AggExpr> = spec.agg_exprs.iter().map(window_agg_to_expr).collect();
        Ok(Self {
            watermark: WatermarkTracker::new(&spec),
            operator: build_operator(&spec, &agg_exprs, state_dir)?,
            quality_hook: None,
            last_watermark_ms: i64::MIN,
            rejected_rows_total: 0,
        })
    }

    /// Total rows rejected by the data-quality hook across all drain cycles.
    ///
    /// `0` if no [`StreamQualityHook`] is attached.
    pub fn rejected_rows_total(&self) -> u64 {
        self.rejected_rows_total
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
        // C2: Apply idle-source policy before processing so idle sources don't
        // freeze all windows when they stop emitting data.
        self.watermark.apply_idle_source_policy();

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
            // G4: Warn when the watermark stalls so operators can detect idle sources.
            if self
                .watermark
                .multi
                .is_stalled(std::time::Duration::from_secs(60))
                && let Some(dur) = self.watermark.multi.stall_duration()
            {
                tracing::warn!(
                    stall_secs = dur.as_secs(),
                    "watermark has not advanced for {}s — check for idle or slow sources",
                    dur.as_secs()
                );
            }
            // Keep the TTL backend's event-time reference current as the
            // watermark advances within this drain cycle.
            self.operator.set_watermark(wm);
            self.last_watermark_ms = wm;
            raw.extend(self.operator.process_batch(batch, wm)?);
        }
        if raw.is_empty() {
            return Ok(raw);
        }
        if let Some(hook) = self.quality_hook.as_mut() {
            let mut output = Vec::with_capacity(raw.len());
            for batch in raw {
                let (accepted, rejected_count) = hook.filter(batch).map_err(ExecError::Arrow)?;
                if rejected_count > 0 {
                    self.rejected_rows_total = self
                        .rejected_rows_total
                        .saturating_add(rejected_count as u64);
                    tracing::debug!(
                        rejected_count,
                        rejected_rows_total = self.rejected_rows_total,
                        "quality hook rejected rows from continuous window output"
                    );
                }
                if accepted.num_rows() > 0 {
                    output.push(accepted);
                }
            }
            Ok(output)
        } else {
            Ok(raw)
        }
    }

    /// Process one drain cycle atomically with respect to retained operator state.
    ///
    /// The current state and watermark trackers are snapshotted before
    /// processing. If any input batch fails, both are restored so callers may
    /// retain and retry the same queued input without duplicating partial
    /// aggregation state.
    pub fn drain_transactional(
        &mut self,
        input_batches: Vec<RecordBatch>,
    ) -> ExecResult<Vec<RecordBatch>> {
        if self.quality_hook.is_some() {
            return Err(ExecError::InvalidWindowConfig(
                "transactional continuous drain does not support a side-effecting quality hook"
                    .into(),
            ));
        }
        if input_batches.is_empty() {
            return self.drain(input_batches);
        }

        // Eagerly purge TTL-expired entries BEFORE taking the snapshot so that
        // the snapshot captures post-purge state. If we took the snapshot first
        // and purge_expired ran inside drain(), a rollback would restore pre-purge
        // state while the state backend retained its post-purge writes — leaving
        // the in-memory and backend views inconsistent.
        if self.last_watermark_ms != i64::MIN {
            self.operator.set_watermark(self.last_watermark_ms);
        }
        self.operator.purge_expired()?;

        let state_snapshot = self.snapshot()?;
        let watermark_snapshot = self.watermark.clone();
        let last_watermark_snapshot = self.last_watermark_ms;

        match self.drain(input_batches) {
            Ok(output) => Ok(output),
            Err(process_error) => {
                self.operator
                    .load_snapshot_bytes(&state_snapshot)
                    .map_err(|restore_error| {
                        ExecError::InvalidWindowConfig(format!(
                            "continuous drain failed ({process_error}); rollback failed: \
                             {restore_error}"
                        ))
                    })?;
                self.watermark = watermark_snapshot;
                self.last_watermark_ms = last_watermark_snapshot;
                Err(process_error)
            }
        }
    }

    /// Borrow the underlying window spec fields (for diagnostics).
    pub fn uses_multi_source_watermark(&self) -> bool {
        !self.watermark.source_lags.is_empty()
    }

    /// C3: Persist operator state to the state backend for crash recovery.
    ///
    /// Delegates to the underlying operator's `checkpoint()` which writes
    /// accumulated window state to the configured `StateBackend`.  Callers
    /// (the runtime drain loop) should invoke this periodically so that open
    /// windows survive executor restarts.
    pub fn checkpoint(&mut self) -> ExecResult<()> {
        self.operator.checkpoint()
    }

    /// C9: Serialize the current window state to bytes for cross-session
    /// persistence.
    ///
    /// Calls `checkpoint()` first (writes to the in-memory state backend),
    /// then extracts the state backend's snapshot bytes. The bytes can be
    /// stored externally (file, etcd, object store) and later restored via
    /// [`restore_from_snapshot`] on a new executor.
    pub fn snapshot(&mut self) -> ExecResult<Vec<u8>> {
        self.operator.checkpoint()?;
        self.operator
            .snapshot_state_bytes()
            .map_err(|e| ExecError::InvalidWindowConfig(format!("snapshot failed: {e}")))
    }

    /// Most recently observed watermark, used to restore `last_watermark_ms`
    /// after a snapshot/restore cycle.
    pub fn last_watermark_ms(&self) -> i64 {
        self.last_watermark_ms
    }

    /// C9: Replace the current window state with the contents of a snapshot
    /// previously produced by [`snapshot`].
    ///
    /// The watermark is reset to `i64::MIN` (the executor will advance it on
    /// the first batch). Call this immediately after [`new`] and before any
    /// [`drain`] calls when resuming an executor from a checkpoint.
    pub fn restore_from_snapshot(&mut self, bytes: &[u8]) -> ExecResult<()> {
        self.operator
            .load_snapshot_bytes(bytes)
            .map_err(|e| ExecError::InvalidWindowConfig(format!("restore failed: {e}")))?;
        self.last_watermark_ms = i64::MIN;
        Ok(())
    }

    /// Merge an additional snapshot into the current window state additively.
    ///
    /// Used after [`restore_from_snapshot`] when this process hosts several
    /// tasks of one job and must union their per-task checkpoint snapshots.
    /// Existing entries are preserved; entries present in `bytes` overwrite
    /// same-key entries (per-task snapshots of the same epoch are disjoint by
    /// key group after a rescaled restore, and identical for duplicated keys).
    pub fn merge_snapshot(&mut self, bytes: &[u8]) -> ExecResult<()> {
        self.operator
            .merge_snapshot_bytes(bytes)
            .map_err(|e| ExecError::InvalidWindowConfig(format!("merge restore failed: {e}")))
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

    fn invalid_events_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"])) as _]).unwrap()
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

    #[test]
    fn transactional_drain_rolls_back_partial_window_and_watermark_state() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.watermark_lag_ms = 0;
        let mut exec = ContinuousWindowExecutor::new(spec).expect("create");

        exec.drain_transactional(vec![events_batch(1_000), invalid_events_batch()])
            .expect_err("invalid second batch must fail the whole drain cycle");

        let output = exec
            .drain_transactional(vec![events_batch(12_000)])
            .expect("executor must remain usable after rollback");
        assert!(
            output.is_empty(),
            "the failed cycle's first batch must not remain in window state"
        );
    }
}
