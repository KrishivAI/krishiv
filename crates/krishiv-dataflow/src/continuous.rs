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
    agg_is_float: &[bool],
) -> ExecResult<WindowOperatorState> {
    match spec.window_kind {
        WindowKind::Tumbling => {
            let tw_spec = TumblingWindowSpec {
                key_column: spec.key_column.clone(),
                key_column_type: spec.key_column_type.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                agg_exprs: agg_exprs.to_vec(),
                agg_is_float: agg_is_float.to_vec(),
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
                agg_is_float: agg_is_float.to_vec(),
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
                agg_is_float: agg_is_float.to_vec(),
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
                agg_is_float: agg_is_float.to_vec(),
            };
            let op = CountWindowOperator::new(count_spec)
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            Ok(WindowOperatorState::Count(Box::new(op)))
        }
    }
}

fn infer_agg_is_float(batch: &RecordBatch, agg_exprs: &[AggExpr]) -> ExecResult<Vec<bool>> {
    agg_exprs
        .iter()
        .map(|expr| {
            if expr.input_column.is_empty() {
                return Ok(false);
            }
            batch
                .schema()
                .field_with_name(&expr.input_column)
                .map(|field| *field.data_type() == arrow::datatypes::DataType::Float64)
                .map_err(|_| {
                    ExecError::InvalidWindowConfig(format!(
                        "aggregate input column '{}' not found in batch schema",
                        expr.input_column
                    ))
                })
        })
        .collect()
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

    /// Emit all windows whose close time is ≤ `watermark_ms` without requiring
    /// new row data. Used by the idle-tick path to close session/tumbling/sliding
    /// windows when the source goes quiet.
    fn flush_due(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        match self {
            Self::Tumbling(op) => op.flush_closed_windows(watermark_ms),
            Self::Sliding(op) => op.flush_closed_windows(watermark_ms),
            Self::Session(op) => op.flush_closed_sessions(watermark_ms),
            Self::Count(_) => Ok(Vec::new()),
        }
    }

    /// Emit **every** open window, whatever the watermark — the end-of-stream
    /// close for a bounded source.
    ///
    /// Distinct from [`Self::flush_due`] because the two callers want opposite
    /// things from the same operator. An *idle tick* must not close a
    /// tumbling/sliding window on wall-clock, since more events may still
    /// arrive and closing early would silently drop them as late (STREAM-1).
    /// An *end-of-stream flush* knows no more events are coming, so every
    /// remaining window is final.
    ///
    /// This mirrors `operator_runtime::WindowOperator::flush`, which is the
    /// bounded path's equivalent and the reason `execute_bounded_window`
    /// already emits trailing windows correctly.
    fn flush_end_of_stream(&mut self) -> ExecResult<Vec<RecordBatch>> {
        match self {
            Self::Tumbling(op) => op.flush_closed_windows(i64::MAX),
            Self::Sliding(op) => op.flush_closed_windows(i64::MAX),
            Self::Session(op) => op.flush_closed_sessions(i64::MAX),
            // A count window's trailing partial window has a well-defined
            // aggregate, and `flush` is documented as the end-of-stream emit.
            Self::Count(op) => op.flush(),
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

    /// Borrow the operator's state backend, when it has one.
    fn state_backend(&self) -> Option<&dyn krishiv_state::StateBackend> {
        match self {
            Self::Tumbling(op) => Some(op.state_backend()),
            Self::Sliding(op) => Some(op.state_backend()),
            Self::Session(op) => Some(op.state_backend()),
            Self::Count(_) => None,
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
    spec: WindowExecutionSpec,
    agg_exprs: Vec<AggExpr>,
    state_dir: Option<std::path::PathBuf>,
    watermark: WatermarkTracker,
    operator: Option<WindowOperatorState>,
    /// Snapshot bytes to load as a full restore on the next operator init.
    /// Set by `restore_from_snapshot` when the operator hasn't been built yet.
    pending_restore: Option<Vec<u8>>,
    /// Additional snapshots to merge additively after `pending_restore` is applied.
    pending_merges: Vec<Vec<u8>>,
    /// Optional data-quality gate applied to each emitted output batch.
    quality_hook: Option<Box<dyn StreamQualityHook>>,
    /// Most recently computed event-time watermark in milliseconds.
    ///
    /// Persisted across drain cycles so that `purge_expired` at the start of
    /// each cycle uses the watermark from the previous cycle rather than falling
    /// back to wall-clock time.  `None` when no watermark has been observed yet.
    last_watermark_ms: Option<i64>,
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
    /// - `state_dir = Some(path)` → file-backed RocksDB state under `path/` (single-node-durable
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
            spec,
            agg_exprs,
            state_dir: state_dir.map(|p| p.to_path_buf()),
            operator: None,
            pending_restore: None,
            pending_merges: Vec::new(),
            quality_hook: None,
            last_watermark_ms: None,
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
    /// Apply the spec's row filter (a streaming `WHERE`), if any.
    ///
    /// Reuses `eval_agg_filter` — the same evaluator the per-aggregate
    /// `FILTER (WHERE …)` clause uses — so the two cannot disagree about NULL
    /// handling or comparison semantics. Batches that filter down to nothing
    /// are dropped rather than passed on empty.
    fn apply_row_filter(&self, batches: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>> {
        let Some(filter) = self.spec.row_filter.as_ref() else {
            return Ok(batches);
        };
        let mut kept = Vec::with_capacity(batches.len());
        for batch in batches {
            // NULL in the mask means "row excluded", which is what
            // `filter_record_batch` does with a null predicate and what SQL
            // `WHERE` requires — a row whose predicate is unknown is not
            // selected.
            let mask = crate::aggregate::eval_agg_filter(filter, &batch)?;
            let filtered = arrow::compute::filter_record_batch(&batch, &mask)
                .map_err(|e| crate::ExecError::Arrow(format!("row filter: {e}")))?;
            if filtered.num_rows() > 0 {
                kept.push(filtered);
            }
        }
        Ok(kept)
    }

    pub fn drain(&mut self, input_batches: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>> {
        // A streaming `WHERE` runs HERE — before grouping, before the watermark
        // advance, before the operator exists.
        //
        // Placement is the whole design. Every driver loop reaches the operator
        // through this method, so one application covers all five; and applying
        // it before grouping is what makes it a `WHERE` rather than a
        // per-aggregate filter. A predicate that removes every row of a key
        // group must remove the GROUP — a per-aggregate filter would leave the
        // group emitting a zero count, which is a different query.
        //
        // Filtering before the watermark advance is also deliberate: a row the
        // predicate rejects is not part of this query's stream at all, so it
        // must not move the watermark and close a window on the strength of an
        // event the query never sees.
        let input_batches = self.apply_row_filter(input_batches)?;

        // Lazy-init: build the window operator from the first batch's schema so
        // that `agg_is_float` reflects the actual aggregate input types instead
        // of hardcoding `false` (which silently truncates Float64 to Int64).
        if self.operator.is_none() && !input_batches.is_empty() {
            let first = input_batches
                .first()
                .ok_or_else(|| crate::ExecError::InvalidInput("empty input_batches".into()))?;
            let agg_is_float = infer_agg_is_float(first, &self.agg_exprs)?;
            let mut op = build_operator(
                &self.spec,
                &self.agg_exprs,
                self.state_dir.as_deref(),
                &agg_is_float,
            )?;
            if let Some(bytes) = self.pending_restore.take() {
                op.load_snapshot_bytes(&bytes)
                    .map_err(|e| ExecError::InvalidWindowConfig(format!("restore failed: {e}")))?;
            }
            for bytes in std::mem::take(&mut self.pending_merges) {
                op.merge_snapshot_bytes(&bytes).map_err(|e| {
                    ExecError::InvalidWindowConfig(format!("merge restore failed: {e}"))
                })?;
            }
            self.operator = Some(op);
        }

        let Some(op) = &mut self.operator else {
            return Ok(Vec::new());
        };

        // C2: Apply idle-source policy before processing so idle sources don't
        // freeze all windows when they stop emitting data.
        self.watermark.apply_idle_source_policy();

        // Propagate the most recently known event-time watermark to the TTL
        // state backend before eviction so that purge_expired uses event time.
        // On the very first drain cycle last_watermark_ms is None and the
        // backend falls back to wall-clock time (no-op for non-TTL operators).
        if let Some(wm) = self.last_watermark_ms {
            op.set_watermark(wm);
        }

        // Eagerly evict stale TTL entries before processing new data.
        op.purge_expired()?;

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
            op.set_watermark(wm);
            self.last_watermark_ms = Some(wm);
            raw.extend(op.process_batch(batch, wm)?);
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

        // Lazy-init operator from first batch's schema (Float64 awareness).
        if self.operator.is_none() {
            let first = input_batches.first().ok_or_else(|| {
                crate::ExecError::InvalidInput("empty input_batches in lazy-init".into())
            })?;
            let agg_is_float = infer_agg_is_float(first, &self.agg_exprs)?;
            let mut op = build_operator(
                &self.spec,
                &self.agg_exprs,
                self.state_dir.as_deref(),
                &agg_is_float,
            )?;
            if let Some(bytes) = self.pending_restore.take() {
                op.load_snapshot_bytes(&bytes)
                    .map_err(|e| ExecError::InvalidWindowConfig(format!("restore failed: {e}")))?;
            }
            for bytes in std::mem::take(&mut self.pending_merges) {
                op.merge_snapshot_bytes(&bytes).map_err(|e| {
                    ExecError::InvalidWindowConfig(format!("merge restore failed: {e}"))
                })?;
            }
            self.operator = Some(op);
        }

        // Eagerly purge TTL-expired entries BEFORE taking the snapshot so that
        // the snapshot captures post-purge state.
        {
            let Some(op) = self.operator.as_mut() else {
                return Err(ExecError::InvalidWindowConfig(
                    "continuous operator was not initialized after schema inference".into(),
                ));
            };
            if let Some(wm) = self.last_watermark_ms {
                op.set_watermark(wm);
            }
            op.purge_expired()?;
        }

        let state_snapshot = self.snapshot()?;
        let watermark_snapshot = self.watermark.clone();
        let last_watermark_snapshot = self.last_watermark_ms;

        match self.drain(input_batches) {
            Ok(output) => Ok(output),
            Err(process_error) => {
                // Rollback restores the in-memory operator state from the
                // pre-drain snapshot. However, RocksDB-backed operators may
                // have written partial state to the persistent backend during
                // `drain` (e.g. via `process_batch` window aggregations).
                // `load_snapshot_bytes` only restores the in-memory view —
                // it does NOT revert RocksDB entries. A `checkpoint()`
                // immediately after rollback syncs the restored in-memory
                // state back to the persistent backend, overwriting any
                // partial writes from the failed drain cycle. Without it,
                // the in-memory view may diverge from persisted state on
                // crash, causing duplicate or inconsistent window results
                // after recovery.
                let Some(op) = self.operator.as_mut() else {
                    return Err(ExecError::InvalidWindowConfig(format!(
                        "continuous drain failed ({process_error}); rollback failed: operator missing"
                    )));
                };
                op.load_snapshot_bytes(&state_snapshot)
                    .map_err(|restore_error| {
                        ExecError::InvalidWindowConfig(format!(
                            "continuous drain failed ({process_error}); rollback failed: \
                             {restore_error}"
                        ))
                    })?;
                op.checkpoint().map_err(|checkpoint_error| {
                    ExecError::InvalidWindowConfig(format!(
                        "continuous drain failed ({process_error}); rollback checkpoint failed: \
                         {checkpoint_error}"
                    ))
                })?;
                self.watermark = watermark_snapshot;
                self.last_watermark_ms = last_watermark_snapshot;
                Err(process_error)
            }
        }
    }

    /// Borrow the underlying window spec fields (for diagnostics).
    /// Advance the watermark to `wall_clock_ms` and emit any windows that have
    /// closed due to inactivity, without requiring new row data.
    ///
    /// Called by the streaming engine's idle-tick path (ST-4) so session windows
    /// whose inactivity gap has elapsed can close even when the source is quiet.
    pub fn tick(&mut self, wall_clock_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        // STREAM-1: Only session windows should advance the watermark to
        // wall-clock on idle (inactivity = close is correct for sessions).
        let is_session = matches!(self.operator, Some(WindowOperatorState::Session(_)));
        let Some(op) = &mut self.operator else {
            return Ok(Vec::new()); // operator not yet initialised; no open windows
        };
        self.watermark.apply_idle_source_policy();
        // Tumbling/sliding windows have pure event-time semantics; advancing
        // their watermark to wall-clock during source idle closes windows that
        // may still receive events, silently dropping them as late.
        let wm = if is_session {
            self.last_watermark_ms
                .unwrap_or(i64::MIN)
                .max(wall_clock_ms)
        } else {
            // For tumbling/sliding: use the last event-time watermark only.
            // Flush windows that are due based on event time, not wall clock.
            self.last_watermark_ms.unwrap_or(i64::MIN)
        };
        op.set_watermark(wm);
        if is_session {
            self.last_watermark_ms = Some(wm);
        }
        op.purge_expired()?;
        op.flush_due(wm)
    }

    /// Close every open window because the source is exhausted (bounded runs).
    ///
    /// `tick(i64::MAX)` cannot express this. `tick` deliberately ignores its
    /// argument for tumbling/sliding windows and flushes against the last
    /// *event-time* watermark instead (STREAM-1, see [`Self::tick`]) — which is
    /// correct for an idle tick and wrong for an end-of-stream flush, because
    /// the final window's own events are what set that watermark, so it can
    /// never close itself. A bounded tumbling job therefore dropped its
    /// trailing window entirely.
    ///
    /// `last_watermark_ms` is deliberately left untouched: the checkpoint that
    /// follows a bounded run should still record the watermark the source
    /// actually reported, so a restore resumes consistently.
    /// True when a restore landed but no batch has arrived since, so the
    /// operator was never built and the restored windows are unflushable.
    ///
    /// The window operator initialises lazily from the first batch's Arrow
    /// schema, so between `restore_from_snapshot` and the first `drain` the
    /// state lives in `pending_restore` and `self.operator` is `None`. Callers
    /// that treat "no operator" as "no state" get it wrong — `snapshot` and
    /// `peek_snapshot_bytes` each carry a comment about exactly that hazard.
    pub fn has_unflushed_restored_state(&self) -> bool {
        self.operator.is_none()
            && self
                .pending_restore
                .as_ref()
                .is_some_and(|bytes| !bytes.is_empty())
    }

    /// True when this operator is holding window state that a stop would
    /// discard — accumulated windows, or a restore that was never consumed.
    ///
    /// Used to tell an ordinary quiet shutdown from one that loses work, so the
    /// run-loop can say which it is. Deliberately built on
    /// [`peek_snapshot_bytes`], which already handles the lazy-init hazard
    /// ("no operator" is not "no state"); asking `self.operator` directly would
    /// report "nothing to lose" for exactly the job that restored a checkpoint
    /// and stopped before its first batch.
    /// The window spec this executor validates and aggregates against.
    ///
    /// Exposed so a driver can coerce input toward exactly this spec rather than
    /// a separately-threaded copy that could drift from it.
    #[must_use]
    pub fn spec(&self) -> &WindowExecutionSpec {
        &self.spec
    }

    pub fn has_open_windows(&self) -> bool {
        self.peek_snapshot_bytes()
            .map(|bytes| !bytes.is_empty())
            .unwrap_or(false)
    }

    pub fn flush_all(&mut self) -> ExecResult<Vec<RecordBatch>> {
        let Some(op) = &mut self.operator else {
            // "No operator" does NOT mean "no state". The operator initialises
            // lazily from the first batch's schema, so a job that restored a
            // checkpoint and then reached end-of-stream without seeing a single
            // batch still holds every restored window in `pending_restore`.
            //
            // Returning an empty vec here would report "nothing to flush" for a
            // job whose windows are all sitting unflushed — the bounded caller
            // then writes no rows and reports Completed. `snapshot` and
            // `peek_snapshot_bytes` both already guard exactly this case, each
            // with a comment about persisting an empty checkpoint that wipes
            // the job's state; this method was added without the same guard.
            //
            // The state is not lost — it is still in `pending_restore` and the
            // next drain applies it — so the honest answer is to refuse rather
            // than to claim emptiness. Rebuilding the operator here is not
            // possible: it needs an Arrow schema, and end-of-stream means no
            // batch ever arrived to supply one.
            // Do NOT error here. "Restored a checkpoint, found no new data,
            // exited" is a normal resume, and the restored windows are not lost
            // — they are still in the checkpoint and the next run restores them
            // again. An error would turn an ordinary resume into a failure, and
            // two existing engine tests said so immediately.
            //
            // But it is not nothing either: the caller asked to flush and this
            // returns empty while state exists, so say so once, loudly enough
            // to find in a log. `has_unflushed_restored_state` exposes the same
            // fact to callers and tests.
            if self.has_unflushed_restored_state() {
                tracing::warn!(
                    "flush_all: restored checkpoint state was never applied because no \
                     batch arrived after the restore, so no window could be flushed. \
                     The state remains in the checkpoint and is restored again next run."
                );
            }
            return Ok(Vec::new()); // genuinely nothing: no operator, no restored state
        };
        op.purge_expired()?;
        op.flush_end_of_stream()
    }

    /// H-14 (audit): speculatively emit currently-open tumbling windows
    /// without mutating the operator state. Returns `None` when the
    /// active operator is not a tumbling window (e.g. session or sliding
    /// windows), or when there are no open windows. The runtime should
    /// call this on a configurable cadence
    /// (`KRISHIV_STREAM_EARLY_FIRE_MS`) and route the snapshot to a
    /// downstream upsert sink keyed on `(key, window_start)`.
    pub fn emit_open_windows_speculative(&self) -> Option<Vec<RecordBatch>> {
        let Some(WindowOperatorState::Tumbling(op)) = &self.operator else {
            return None;
        };
        match op.emit_open_windows() {
            Ok(batches) if batches.is_empty() => None,
            Ok(batches) => Some(batches),
            Err(err) => {
                // Best-effort: a speculative snapshot failing never blocks
                // the normal close-time flush, which still holds the durable
                // state. Log and skip this round's early fire.
                tracing::warn!(
                    error = %err,
                    "speculative early-fire snapshot failed; skipping this round"
                );
                None
            }
        }
    }

    pub fn uses_multi_source_watermark(&self) -> bool {
        !self.watermark.source_lags.is_empty()
    }

    /// Phase 56: run `f` against the operator's concrete RocksDB state
    /// backend, when the executor is file-backed. Returns `None` for
    /// in-memory / not-yet-initialised operators — callers fall back to the
    /// portable full snapshot. Call [`Self::checkpoint`] first so the live
    /// window panes are actually in the backend the SST checkpoint captures.
    pub fn with_rocksdb_state<R>(
        &self,
        f: impl FnOnce(&krishiv_state::RocksDbStateBackend) -> R,
    ) -> Option<R> {
        self.operator
            .as_ref()
            .and_then(|op| op.state_backend())
            .and_then(|backend| backend.as_rocksdb())
            .map(f)
    }

    /// C3: Persist operator state to the state backend for crash recovery.
    ///
    /// Delegates to the underlying operator's `checkpoint()` which writes
    /// accumulated window state to the configured `StateBackend`.  Callers
    /// (the runtime drain loop) should invoke this periodically so that open
    /// windows survive executor restarts.
    pub fn checkpoint(&mut self) -> ExecResult<()> {
        match &mut self.operator {
            Some(op) => op.checkpoint(),
            None => Ok(()),
        }
    }

    /// C9: Serialize the current window state to bytes for cross-session
    /// persistence.
    ///
    /// Calls `checkpoint()` first (writes to the in-memory state backend),
    /// then extracts the state backend's snapshot bytes. The bytes can be
    /// stored externally (file, etcd, object store) and later restored via
    /// [`restore_from_snapshot`] on a new executor.
    pub fn snapshot(&mut self) -> ExecResult<Vec<u8>> {
        match &mut self.operator {
            Some(op) => {
                op.checkpoint()?;
                op.snapshot_state_bytes()
                    .map_err(|e| ExecError::InvalidWindowConfig(format!("snapshot failed: {e}")))
            }
            // Phase 56: a snapshot taken between restore and the first batch
            // (the operator initializes lazily) must carry the restored state
            // forward — returning empty here let a barrier in that window
            // persist an EMPTY checkpoint that would wipe the job's state on
            // the next restore.
            None => Ok(self.pending_restore.clone().unwrap_or_default()),
        }
    }

    /// Serialize state backend contents without running `checkpoint()`.
    ///
    /// Use this for read-only observation (e.g. queryable state) where you
    /// must not mutate the operator state as a side effect.
    pub fn peek_snapshot_bytes(&self) -> ExecResult<Vec<u8>> {
        match &self.operator {
            Some(op) => op
                .snapshot_state_bytes()
                .map_err(|e| ExecError::InvalidWindowConfig(format!("peek snapshot failed: {e}"))),
            // See `snapshot`: restored-but-not-yet-consumed state must not
            // read back as empty.
            None => Ok(self.pending_restore.clone().unwrap_or_default()),
        }
    }

    /// Most recently observed watermark, used to restore `last_watermark_ms`
    /// after a snapshot/restore cycle.
    pub fn last_watermark_ms(&self) -> i64 {
        self.last_watermark_ms.unwrap_or(i64::MIN)
    }

    /// C9: Replace the current window state with the contents of a snapshot
    /// previously produced by [`snapshot`].
    ///
    /// The watermark is reset to `None` (the executor will advance it on
    /// the first batch). Call this immediately after [`new`] and before any
    /// [`drain`] calls when resuming an executor from a checkpoint.
    pub fn restore_from_snapshot(&mut self, bytes: &[u8]) -> ExecResult<()> {
        match &mut self.operator {
            Some(op) => {
                op.load_snapshot_bytes(bytes)
                    .map_err(|e| ExecError::InvalidWindowConfig(format!("restore failed: {e}")))?;
            }
            None => {
                // Operator is built lazily on the first batch; stash the
                // snapshot so it is applied immediately after initialization.
                self.pending_restore = Some(bytes.to_vec());
                self.pending_merges.clear();
            }
        }
        self.last_watermark_ms = None;
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
        match &mut self.operator {
            Some(op) => op
                .merge_snapshot_bytes(bytes)
                .map_err(|e| ExecError::InvalidWindowConfig(format!("merge restore failed: {e}"))),
            None => {
                // Operator is built lazily; stash the merge to apply after init.
                // A merge without a prior restore is still valid (idempotent merge
                // of disjoint task snapshots during a rescaled recovery).
                self.pending_merges.push(bytes.to_vec());
                Ok(())
            }
        }
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

    /// `(key, ts, v)` rows — the shape the row-filter tests aggregate over.
    fn key_ts_val_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(k, _, _)| *k).collect::<Vec<_>>(),
                )) as _,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, t, _)| *t).collect::<Vec<_>>(),
                )) as _,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, v)| *v).collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

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

    /// `tick` must not close a tumbling window on wall-clock (STREAM-1) but
    /// `flush_all` must, because the source is over. Pinning both halves in one
    /// test keeps a future "simplification" from collapsing them back together.
    #[test]
    fn flush_all_closes_a_trailing_tumbling_window_that_tick_leaves_open() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.watermark_lag_ms = 0;
        let mut exec = ContinuousWindowExecutor::new(spec).expect("create");

        // One event in [0,10000). Nothing else arrives, so no event-time
        // watermark ever passes the window's end.
        let emitted = exec.drain(vec![events_batch(1_000)]).expect("drain");
        assert!(emitted.is_empty(), "no window closes from one event");

        // An idle tick, even at i64::MAX, must leave it open: more events could
        // still arrive and closing early would drop them as late.
        let ticked = exec.tick(i64::MAX).expect("tick");
        assert!(
            ticked.is_empty(),
            "tick must not close a tumbling window on wall-clock (STREAM-1); \
             this is exactly why the bounded final flush could not use it"
        );

        // End of stream: the window is final and must emit.
        let flushed = exec.flush_all().expect("flush_all");
        let rows: usize = flushed.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "flush_all closes the trailing tumbling window");
    }

    /// Count windows returned nothing from the idle-tick flush path, so a
    /// bounded count-windowed job dropped its trailing partial window too.
    /// `CountWindowOperator::flush` is documented as the end-of-stream emit and
    /// is what `flush_all` routes to.
    /// A job that restored a checkpoint and then hit end-of-stream without
    /// receiving a batch must NOT report "nothing to flush".
    ///
    /// The operator initialises lazily from the first batch's schema, so
    /// restored state sits in `pending_restore` until a drain applies it.
    /// `flush_all` returning an empty vec there let a bounded run write no rows
    /// and report Completed while every restored window stayed unflushed.
    /// `snapshot` and `peek_snapshot_bytes` each already guard this case with a
    /// comment about wiping job state; `flush_all` shipped without it.
    #[test]
    fn unapplied_restored_state_is_reported_not_read_back_as_empty() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.watermark_lag_ms = 0;

        // Produce a real snapshot holding one open window.
        let mut source = ContinuousWindowExecutor::new(spec.clone()).expect("create");
        let _ = source.drain(vec![events_batch(1_000)]).expect("drain");
        let bytes = source.snapshot().expect("snapshot");
        assert!(!bytes.is_empty(), "fixture must carry real restored state");

        // Restore into a fresh executor and immediately hit end-of-stream.
        let mut restored = ContinuousWindowExecutor::new(spec).expect("create");
        restored.restore_from_snapshot(&bytes).expect("restore");
        assert!(
            restored.has_unflushed_restored_state(),
            "restored state with no operator yet must be reported as unflushed, not \
             read back as 'no open windows' — this is the state `flush_all` used to \
             silently report as empty"
        );
        // Flushing is still not an error: a resume that finds no new data is
        // ordinary, and the state survives in the checkpoint.
        assert!(
            restored
                .flush_all()
                .expect("resume with no new data is not an error")
                .is_empty()
        );
        // Once a batch arrives the operator is built and the state is real.
        let emitted = restored.drain(vec![events_batch(12_000)]).expect("drain");
        let _ = emitted;
        assert!(
            !restored.has_unflushed_restored_state(),
            "after the first batch the restored state has been applied"
        );
    }

    /// `has_open_windows` is what lets a stop say whether it lost anything.
    ///
    /// The run-loop's teardown neither flushes nor snapshots (a forced flush
    /// would emit partial aggregates as complete), so a stop with state open is
    /// a real loss and a stop with nothing open is not. Reporting them the same
    /// way makes the warning noise, which is how a warning stops being read.
    ///
    /// The lazy-init hazard is the reason this delegates to
    /// `peek_snapshot_bytes` rather than testing `self.operator`: a job that
    /// restored a checkpoint and stopped before its first batch has NO operator
    /// and its entire state to lose.
    #[test]
    fn open_windows_are_reported_including_before_the_first_batch() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.watermark_lag_ms = 0;

        // Nothing has happened: a stop here costs nothing.
        let fresh = ContinuousWindowExecutor::new(spec.clone()).expect("create");
        assert!(
            !fresh.has_open_windows(),
            "an untouched executor must not claim a stop would lose state"
        );

        // Data accumulated into an open window: a stop here loses it.
        let mut live = ContinuousWindowExecutor::new(spec.clone()).expect("create");
        let _ = live.drain(vec![events_batch(1_000)]).expect("drain");
        assert!(
            live.has_open_windows(),
            "an accumulated window must be reported as at risk on stop"
        );

        // Restored but not yet applied — no operator, full state. Asking
        // `self.operator` here would answer "nothing to lose" for the job with
        // the most to lose.
        let bytes = live.snapshot().expect("snapshot");
        let mut restored = ContinuousWindowExecutor::new(spec).expect("create");
        restored.restore_from_snapshot(&bytes).expect("restore");
        assert!(
            restored.has_open_windows(),
            "restored-but-unapplied state is still state a stop would discard"
        );
    }

    /// With no operator AND no restored state there is genuinely nothing to
    /// flush, and that must stay quiet — otherwise the guard above turns every
    /// empty bounded run into an error.
    #[test]
    fn flush_all_is_quiet_when_there_is_genuinely_no_state() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.watermark_lag_ms = 0;
        let mut exec = ContinuousWindowExecutor::new(spec).expect("create");
        assert!(
            exec.flush_all()
                .expect("no state is not an error")
                .is_empty()
        );
    }

    #[test]
    fn flush_all_emits_a_partial_count_window() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.watermark_lag_ms = 0;
        spec.window_kind = krishiv_plan::window::WindowKind::Count {
            size: 10,
            slide: 10,
        };
        let mut exec = ContinuousWindowExecutor::new(spec).expect("create");

        // Three rows against a size-10 count window: the window never fills, so
        // nothing closes on its own.
        let emitted = exec
            .drain(vec![
                events_batch(1_000),
                events_batch(2_000),
                events_batch(3_000),
            ])
            .expect("drain");
        assert!(
            emitted.is_empty(),
            "a partial count window does not self-close"
        );

        let flushed = exec.flush_all().expect("flush_all");
        let rows: usize = flushed.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "the partial count window emits at end of stream");
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

    #[test]
    fn emit_open_windows_speculative_returns_none_before_any_drain() {
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        let exec = ContinuousWindowExecutor::new(spec).expect("create");
        assert!(
            exec.emit_open_windows_speculative().is_none(),
            "no operator has been created yet (no drain call), so there is nothing to peek at"
        );
    }

    #[test]
    fn emit_open_windows_speculative_snapshots_an_open_tumbling_window_without_consuming_it() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.watermark_lag_ms = 0;
        let mut exec = ContinuousWindowExecutor::new(spec).expect("create");

        // Window [0, 10000) is still open: watermark (1_000) has not passed
        // its end.
        let closed = exec.drain(vec![events_batch(1_000)]).expect("drain1");
        assert!(closed.is_empty(), "window must not have closed yet");

        let speculative = exec
            .emit_open_windows_speculative()
            .expect("H-14: an open tumbling window must produce a speculative snapshot");
        assert_eq!(speculative.len(), 1);
        assert_eq!(
            speculative[0].num_rows(),
            1,
            "one key (\"a\") has one open window"
        );

        // The peek must not have mutated state: the real close still sees
        // the same row when the watermark actually passes the boundary.
        let final_output = exec.drain(vec![events_batch(12_000)]).expect("drain2");
        assert!(
            !final_output.is_empty(),
            "the window must still close normally after being speculatively peeked"
        );
    }

    #[test]
    fn drain_errors_when_aggregate_input_column_is_missing() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.agg_exprs = vec![krishiv_plan::window::WindowAgg {
            filter: None,
            kind: krishiv_plan::window::WindowAggKind::Sum,
            input_column: "amount".into(),
            output_column: "total_amount".into(),
        }];
        let mut exec = ContinuousWindowExecutor::new(spec).expect("create");

        let error = exec
            .drain(vec![events_batch(1_000)])
            .expect_err("missing aggregate input column must fail during lazy init");

        assert!(matches!(error, ExecError::InvalidWindowConfig(_)));
        assert!(
            error
                .to_string()
                .contains("aggregate input column 'amount' not found")
        );
    }

    #[test]
    fn restore_before_first_drain_applies_snapshot_on_next_batch() {
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);

        // First executor: run one batch and snapshot.
        let mut exec1 = ContinuousWindowExecutor::new(spec.clone()).expect("create");
        exec1.drain(vec![events_batch(5_000)]).expect("drain1");
        let snap = exec1.snapshot().expect("snapshot");

        // Second executor: restore BEFORE any drain (operator is None).
        let mut exec2 = ContinuousWindowExecutor::new(spec).expect("create");
        exec2
            .restore_from_snapshot(&snap)
            .expect("restore before first drain must not fail");

        // The first drain after restore should succeed and apply the snapshot.
        exec2
            .drain(vec![events_batch(15_000)])
            .expect("drain after deferred restore must succeed");
    }

    #[test]
    fn transactional_drain_errors_when_aggregate_input_column_is_missing() {
        let mut spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        spec.agg_exprs = vec![krishiv_plan::window::WindowAgg {
            filter: None,
            kind: krishiv_plan::window::WindowAggKind::Sum,
            input_column: "amount".into(),
            output_column: "total_amount".into(),
        }];
        let mut exec = ContinuousWindowExecutor::new(spec).expect("create");

        let error = exec
            .drain_transactional(vec![events_batch(1_000)])
            .expect_err("missing aggregate input column must fail before state changes");

        assert!(matches!(error, ExecError::InvalidWindowConfig(_)));
        assert!(
            error
                .to_string()
                .contains("aggregate input column 'amount' not found")
        );
    }

    /// A `row_filter` removes rows BEFORE grouping, so an excluded key's group
    /// does not exist at all.
    ///
    /// This is the property that separates a streaming `WHERE` from a
    /// per-aggregate `FILTER (WHERE …)`. Both suppress the row's contribution;
    /// only the former suppresses the GROUP. Asserting "key b is absent" rather
    /// than "b's count is 0" is what makes this test about the right thing —
    /// a per-aggregate filter would emit b with 0 and pass a count-based check.
    #[test]
    fn a_row_filter_removes_the_group_not_just_the_rows() {
        use krishiv_plan::window::{AggFilterCompareOp, AggFilterValue, WindowAggFilter};

        let mut spec = WindowExecutionSpec::tumbling("k", "ts", 10_000);
        spec.watermark_lag_ms = 0;
        // WHERE v > 100 — key "b" has only a row that fails it.
        spec.row_filter = Some(WindowAggFilter::Compare {
            column: String::from("v"),
            op: AggFilterCompareOp::Gt,
            value: AggFilterValue::Int(100),
        });

        let mut exec = ContinuousWindowExecutor::new(spec).expect("executor");
        let batch = key_ts_val_batch(&[("a", 1_000, 500), ("b", 2_000, 5)]);
        // A later event closes the window.
        let closer = key_ts_val_batch(&[("a", 25_000, 900)]);

        let mut out = exec.drain(vec![batch]).expect("drain");
        out.extend(exec.drain(vec![closer]).expect("drain closer"));

        let keys: Vec<String> = out
            .iter()
            .flat_map(|b| {
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("key column is Utf8");
                (0..b.num_rows())
                    .map(|i| col.value(i).to_owned())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert!(
            keys.iter().any(|k| k == "a"),
            "key a had a row passing the predicate and must appear; got {keys:?}"
        );
        assert!(
            !keys.iter().any(|k| k == "b"),
            "key b's only row failed the predicate, so its GROUP must not exist — \
             emitting b with a zero count is what a per-aggregate filter would do, \
             and is a different query; got {keys:?}"
        );
    }

    /// With no filter configured, nothing is removed.
    ///
    /// Guards the other direction: an `apply_row_filter` that dropped rows
    /// unconditionally would pass the test above while silently breaking every
    /// query in the engine.
    #[test]
    fn no_row_filter_passes_every_row_through() {
        let spec = WindowExecutionSpec::tumbling("k", "ts", 10_000);
        let mut exec = ContinuousWindowExecutor::new(spec).expect("executor");
        exec.drain(vec![key_ts_val_batch(&[("a", 1_000, 5), ("b", 2_000, 5)])])
            .expect("drain");
        // Nothing closed yet, but the rows must have been accepted — a flush
        // proves they are in state.
        let flushed = exec.flush_all().expect("flush");
        let rows: usize = flushed.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "both keys must survive when no filter is set");
    }
}
