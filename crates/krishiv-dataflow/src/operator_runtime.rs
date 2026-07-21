//! Unified bounded window execution for batch and streaming (all deployment modes).

use arrow::record_batch::RecordBatch;
use krishiv_common::resolve_durability_profile;
use krishiv_plan::window::{
    WindowAgg, WindowAggKind, WindowExecutionSpec, WindowKind, validate_window_execution_spec,
};
use krishiv_state::{
    InMemoryStateBackend, RocksDbStateBackend, StateBackend, TtlConfig, TtlStateBackend,
};

/// Open or create a state backend for a window operator.
///
/// Shared between `operator_runtime.rs` (bounded/streaming execution) and
/// `continuous.rs` (continuous drain-cycle execution).
///
/// # Placement-aware backend selection
///
/// - `state_dir = None` (embedded / in-process cluster) ⇒
///   [`InMemoryStateBackend`]. Zero disk I/O, microsecond `get`/`put` —
///   the right shape for tests and single-host embedded use. The window
///   operator's active state is held in an in-memory `HashMap` already;
///   the state backend is only touched on `checkpoint()` and
///   `purge_expired`, so making the backend itself in-memory eliminates
///   the tempdir-on-disk roundtrip those paths were paying.
///
/// - `state_dir = Some(path)` (single-node-durable / distributed-durable) ⇒
///   [`RocksDbStateBackend`] at the given path, opened with
///   `durable_fsync = false`: window state is written on every batch but
///   only synced to disk once per checkpoint epoch (via
///   `StateBackend::sync()` inside `checkpoint()`), so the crash-durability
///   boundary is "state as of the last checkpoint" rather than "state as of
///   the last write." See the inline comment at the call site below for why
///   this is the intended, batched-WAL behavior rather than a gap.
pub(crate) fn open_state_backend(
    state_dir: Option<&std::path::Path>,
    tag: &str,
    ttl_ms: Option<u64>,
) -> ExecResult<Box<dyn StateBackend>> {
    // Branch on TTL first so the typed `RocksDbStateBackend` / `InMemoryStateBackend`
    // flows into the generic `TtlStateBackend<B>` without needing trait-object
    // boxing before the wrap.
    let backend: Box<dyn StateBackend> = match (state_dir, ttl_ms) {
        (None, None) => Box::new(InMemoryStateBackend::default()),
        (None, Some(ms)) => {
            let ttl = TtlStateBackend::new(InMemoryStateBackend::default(), TtlConfig::new(ms));
            Box::new(ttl)
        }
        (Some(dir), None) => {
            let path = dir.join(tag);
            std::fs::create_dir_all(&path).map_err(|e| {
                ExecError::InvalidWindowConfig(format!(
                    "failed to create state dir '{}': {e}",
                    path.display()
                ))
            })?;
            // Batched-WAL durability: window state is persisted only at
            // `checkpoint()`, which calls `StateBackend::sync()` once per epoch.
            // Opening with `durable_fsync = false` therefore keeps the same
            // crash-durability boundary (state as of the last checkpoint) while
            // collapsing the multiple per-checkpoint writes (clear + accumulator
            // batch + watermark) into a single fsync.
            let r = RocksDbStateBackend::open(&path)
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?
                .with_durable_fsync(false);
            Box::new(r)
        }
        (Some(dir), Some(ms)) => {
            let path = dir.join(tag);
            std::fs::create_dir_all(&path).map_err(|e| {
                ExecError::InvalidWindowConfig(format!(
                    "failed to create state dir '{}': {e}",
                    path.display()
                ))
            })?;
            let r = RocksDbStateBackend::open(&path)
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?
                .with_durable_fsync(false);
            let ttl = TtlStateBackend::new(r, TtlConfig::new(ms));
            Box::new(ttl)
        }
    };
    // Touch the profile so a future caller can branch on it without a
    // silent dead-store warning; today the placement-aware behavior lives
    // in the engine runtime, not the backend selector.
    let _ = resolve_durability_profile();
    Ok(backend)
}

use crate::watermark_util::advance_effective_watermark;
use crate::window::MultiSourceWatermarkState;
use crate::window::tumbling::TumblingWindowOperator;
use crate::{
    AggExpr, AggFunction, ExecError, ExecResult, SessionWindowSpec, SlidingWindowSpec,
    StateBackedSessionWindowOperator, StateBackedSlidingWindowOperator,
    StateBackedTumblingWindowOperator, TumblingWindowSpec, WatermarkState,
};
use futures::stream::{Stream, StreamExt};
use std::pin::Pin;

pub(crate) fn window_agg_to_expr(agg: &WindowAgg) -> AggExpr {
    let function = match agg.kind {
        WindowAggKind::Count => AggFunction::Count,
        WindowAggKind::Sum => AggFunction::Sum,
        WindowAggKind::Min => AggFunction::Min,
        WindowAggKind::Max => AggFunction::Max,
        WindowAggKind::Avg => AggFunction::Avg,
        WindowAggKind::Stddev => AggFunction::Stddev,
    };
    AggExpr {
        function,
        input_column: agg.input_column.clone(),
        output_column: agg.output_column.clone(),
        filter: agg.filter.clone(),
    }
}

/// Execute a bounded windowed stream over in-memory batches (canonical semantics).
///
/// Watermark advances to the max event time per input batch, then the whole batch
/// is passed to the window operator (see `streaming-execution-model.md`).
///
/// `state_dir` controls where the operator's window state is persisted:
/// - `Some(dir)`: state lives under `dir/{window_kind}/` and survives process
///   restarts. Required for exactly-once semantics across multiple bounded
///   window invocations against the same key space (e.g. an executor's
///   `state:store:` fragment).
/// - `None`: state is ephemeral (in a `tempdir`) and lives only for the
///   duration of this call. Suitable for one-shot batch SQL where the
///   operator's state does not need to outlive the call.
pub fn execute_bounded_window(
    input_batches: Vec<RecordBatch>,
    spec: &WindowExecutionSpec,
    state_dir: Option<&std::path::Path>,
) -> ExecResult<Vec<RecordBatch>> {
    validate_window_execution_spec(spec)
        .map_err(|error| ExecError::InvalidWindowConfig(error.to_string()))?;
    if input_batches.is_empty() {
        return Ok(Vec::new());
    }

    let agg_exprs: Vec<AggExpr> = spec.agg_exprs.iter().map(window_agg_to_expr).collect();

    // Determine which aggregates take Float64 input so downstream operators
    // can emit Float64 output arrays (instead of silently truncating to Int64).
    let agg_is_float: Vec<bool> = match input_batches.first() {
        Some(b) => agg_exprs
            .iter()
            .map(|e| {
                if e.input_column.is_empty() {
                    return Ok(false);
                }
                b.schema()
                    .field_with_name(&e.input_column)
                    .map(|f| *f.data_type() == arrow::datatypes::DataType::Float64)
                    .map_err(|_| {
                        ExecError::InvalidWindowConfig(format!(
                            "aggregate input column '{}' not found in batch schema",
                            e.input_column
                        ))
                    })
            })
            .collect::<ExecResult<Vec<_>>>()?,
        None => vec![false; agg_exprs.len()],
    };

    let mut single_watermark = WatermarkState::new(spec.watermark_lag_ms);
    let mut multi_watermark =
        MultiSourceWatermarkState::new().with_idle_source_policy(60_000, i64::MAX);
    let mut output = Vec::new();

    // Clone the shared spec fields once; only the arm matching `window_kind`
    // runs, so these are moved (not re-cloned) into whichever operator spec
    // gets constructed below.
    let key_column = spec.key_column.clone();
    let key_column_type = spec.key_column_type.clone();
    let event_time_column = spec.event_time_column.clone();

    match spec.window_kind {
        WindowKind::Tumbling => {
            let tw_spec = TumblingWindowSpec {
                key_column,
                key_column_type,
                event_time_column,
                window_size_ms: spec.window_size_ms,
                agg_exprs: agg_exprs.clone(),
                agg_is_float: agg_is_float.clone(),
            };
            TumblingWindowOperator::validate_spec(&tw_spec)
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let state = open_state_backend(state_dir, "tumbling", spec.state_ttl_ms)?;
            let mut op =
                StateBackedTumblingWindowOperator::new(tw_spec, state, "window-exec", "tumbling")
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            for batch in &input_batches {
                multi_watermark.apply_idle_source_policy();
                let wm = advance_effective_watermark(
                    batch,
                    &spec.event_time_column,
                    spec.source_id_column.as_deref(),
                    &spec.source_watermark_lags,
                    &mut single_watermark,
                    &mut multi_watermark,
                )?;
                output.extend(op.process_batch(batch, wm)?);
            }
            output.extend(op.flush_closed_windows(i64::MAX)?);
        }
        WindowKind::Sliding => {
            let slide_ms = spec.slide_ms.unwrap_or(spec.window_size_ms);
            let sw_spec = SlidingWindowSpec {
                key_column,
                key_column_type,
                event_time_column,
                window_size_ms: spec.window_size_ms,
                slide_ms,
                agg_exprs,
                agg_is_float: agg_is_float.clone(),
            };
            let state = open_state_backend(state_dir, "sliding", spec.state_ttl_ms)?;
            let mut op =
                StateBackedSlidingWindowOperator::new(sw_spec, state, "window-exec", "sliding")
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            for batch in &input_batches {
                multi_watermark.apply_idle_source_policy();
                let wm = advance_effective_watermark(
                    batch,
                    &spec.event_time_column,
                    spec.source_id_column.as_deref(),
                    &spec.source_watermark_lags,
                    &mut single_watermark,
                    &mut multi_watermark,
                )?;
                output.extend(op.process_batch(batch, wm)?);
            }
            output.extend(op.flush_closed_windows(i64::MAX)?);
        }
        WindowKind::Session => {
            let gap_ms = spec.session_gap_ms.unwrap_or(spec.window_size_ms);
            let sess_spec = SessionWindowSpec {
                key_column,
                key_column_type,
                event_time_column,
                session_gap_ms: gap_ms,
                agg_exprs,
                agg_is_float,
            };
            let state = open_state_backend(state_dir, "session", spec.state_ttl_ms)?;
            let mut op =
                StateBackedSessionWindowOperator::new(sess_spec, state, "window-exec", "session")
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            for batch in &input_batches {
                multi_watermark.apply_idle_source_policy();
                let wm = advance_effective_watermark(
                    batch,
                    &spec.event_time_column,
                    spec.source_id_column.as_deref(),
                    &spec.source_watermark_lags,
                    &mut single_watermark,
                    &mut multi_watermark,
                )?;
                output.extend(op.process_batch(batch, wm)?);
            }
            output.extend(op.flush_closed_sessions(i64::MAX)?);
        }
        WindowKind::Count { size, slide } => {
            use crate::window::{CountWindowOperator, CountWindowSpec};
            let count_spec = CountWindowSpec {
                key_column,
                key_column_type,
                size,
                slide,
                agg_exprs: agg_exprs.clone(),
                agg_is_float: agg_is_float.clone(),
            };
            let mut op = CountWindowOperator::new(count_spec)
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            for batch in &input_batches {
                output.extend(op.process_batch(batch)?);
            }
            output.extend(op.flush()?);
        }
    }

    Ok(output)
}

/// Dispatches `process_batch`/`flush` across window operator kinds.
enum StreamingWindowOp {
    Tumbling(StateBackedTumblingWindowOperator),
    Sliding(StateBackedSlidingWindowOperator),
    Session(StateBackedSessionWindowOperator),
    Count(crate::window::CountWindowOperator),
}

impl StreamingWindowOp {
    fn process_batch(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        match self {
            Self::Tumbling(op) => op.process_batch(batch, watermark_ms),
            Self::Sliding(op) => op.process_batch(batch, watermark_ms),
            Self::Session(op) => op.process_batch(batch, watermark_ms),
            Self::Count(op) => op.process_batch(batch),
        }
    }

    fn flush(&mut self) -> ExecResult<Vec<RecordBatch>> {
        match self {
            Self::Tumbling(op) => op.flush_closed_windows(i64::MAX),
            Self::Sliding(op) => op.flush_closed_windows(i64::MAX),
            Self::Session(op) => op.flush_closed_sessions(i64::MAX),
            Self::Count(op) => op.flush(),
        }
    }

    /// Forward the event-time watermark to the operator's state backend so that
    /// TTL expiry checks use event time instead of wall-clock time (ST7).
    ///
    /// The `CountWindowOperator` is stateless; the call is a no-op for it.
    fn set_watermark(&mut self, watermark_ms: i64) {
        match self {
            Self::Tumbling(op) => op.set_watermark(watermark_ms),
            Self::Sliding(op) => op.set_watermark(watermark_ms),
            Self::Session(op) => op.set_watermark(watermark_ms),
            Self::Count(_) => {}
        }
    }
}

/// Build a state-backed streaming window operator for `spec`.
///
/// Constructed lazily (once the first input batch's schema is known) so that
/// `agg_is_float` can reflect the real aggregate input types — mirroring the
/// schema probe in [`execute_bounded_window`]. Previously the streaming path
/// hardcoded `agg_is_float = false`, silently truncating `Float64`
/// `SUM`/`MIN`/`MAX`/`AVG` results to `Int64`.
fn build_streaming_window_op(
    spec: &WindowExecutionSpec,
    state_dir: Option<&std::path::Path>,
    agg_exprs: &[AggExpr],
    agg_is_float: &[bool],
) -> ExecResult<StreamingWindowOp> {
    let key_column = spec.key_column.clone();
    let key_column_type = spec.key_column_type.clone();
    let event_time_column = spec.event_time_column.clone();
    let agg_exprs = agg_exprs.to_vec();
    let agg_is_float = agg_is_float.to_vec();

    let op = match spec.window_kind {
        WindowKind::Tumbling => {
            let tw_spec = TumblingWindowSpec {
                key_column,
                key_column_type,
                event_time_column,
                window_size_ms: spec.window_size_ms,
                agg_exprs,
                agg_is_float,
            };
            TumblingWindowOperator::validate_spec(&tw_spec)
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let state = open_state_backend(state_dir, "tumbling", spec.state_ttl_ms)?;
            StreamingWindowOp::Tumbling(
                StateBackedTumblingWindowOperator::new(tw_spec, state, "window-exec", "tumbling")
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?,
            )
        }
        WindowKind::Sliding => {
            let slide_ms = spec.slide_ms.unwrap_or(spec.window_size_ms);
            let sw_spec = SlidingWindowSpec {
                key_column,
                key_column_type,
                event_time_column,
                window_size_ms: spec.window_size_ms,
                slide_ms,
                agg_exprs,
                agg_is_float,
            };
            let state = open_state_backend(state_dir, "sliding", spec.state_ttl_ms)?;
            StreamingWindowOp::Sliding(
                StateBackedSlidingWindowOperator::new(sw_spec, state, "window-exec", "sliding")
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?,
            )
        }
        WindowKind::Session => {
            let session_gap_ms = spec.session_gap_ms.unwrap_or(spec.window_size_ms);
            let sess_spec = SessionWindowSpec {
                key_column,
                key_column_type,
                event_time_column,
                session_gap_ms,
                agg_exprs,
                agg_is_float: agg_is_float.clone(),
            };
            let state = open_state_backend(state_dir, "session", spec.state_ttl_ms)?;
            StreamingWindowOp::Session(
                StateBackedSessionWindowOperator::new(sess_spec, state, "window-exec", "session")
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?,
            )
        }
        WindowKind::Count { size, slide } => {
            use crate::window::{CountWindowOperator, CountWindowSpec};
            let count_spec = CountWindowSpec {
                key_column,
                key_column_type,
                size,
                slide,
                agg_exprs,
                agg_is_float,
            };
            StreamingWindowOp::Count(
                CountWindowOperator::new(count_spec)
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?,
            )
        }
    };
    Ok(op)
}

pub fn execute_streaming_window(
    mut input: Pin<Box<dyn Stream<Item = ExecResult<RecordBatch>> + Send>>,
    spec: WindowExecutionSpec,
    state_dir: Option<&std::path::Path>,
) -> ExecResult<Pin<Box<dyn Stream<Item = ExecResult<RecordBatch>> + Send>>> {
    if spec.agg_exprs.is_empty() {
        return Err(ExecError::InvalidWindowConfig(
            "window execution requires at least one aggregate".into(),
        ));
    }

    let agg_exprs: Vec<AggExpr> = spec.agg_exprs.iter().map(window_agg_to_expr).collect();
    // Own the state dir so the returned `'static` stream can open its state
    // backend lazily, after the first batch reveals the aggregate input types.
    let state_dir = state_dir.map(|p| p.to_path_buf());

    let mut single_watermark = WatermarkState::new(spec.watermark_lag_ms);
    let mut multi_watermark =
        MultiSourceWatermarkState::new().with_idle_source_policy(60_000, i64::MAX);

    let stream = async_stream::stream! {
        // Peek the first batch so the operator is built with correct Float64
        // awareness (mirrors `execute_bounded_window`). The first batch is then
        // processed normally via `pending_first` — it is not dropped.
        let first_batch = match input.next().await {
            None => return,
            Some(Err(e)) => {
                yield Err(e);
                return;
            }
            Some(Ok(batch)) => batch,
        };
        let agg_is_float: Vec<bool> = match agg_exprs
            .iter()
            .map(|e| {
                if e.input_column.is_empty() {
                    return Ok(false);
                }
                first_batch
                    .schema()
                    .field_with_name(&e.input_column)
                    .map(|f| *f.data_type() == arrow::datatypes::DataType::Float64)
                    .map_err(|_| {
                        ExecError::InvalidWindowConfig(format!(
                            "aggregate input column '{}' not found in batch schema",
                            e.input_column
                        ))
                    })
            })
            .collect::<ExecResult<Vec<_>>>()
        {
            Ok(v) => v,
            Err(e) => {
                yield Err(e);
                return;
            }
        };
        // State backend initialization (create_dir_all + RocksDB open) is
        // technically blocking I/O. It happens exactly once per streaming job
        // (on the first batch) and is sub-millisecond on local filesystems.
        // With state_dir=None (embedded mode) no I/O occurs at all. Upgrading
        // to spawn_blocking requires StreamingWindowOp: Send + 'static; the
        // trait objects it contains preclude that without a larger refactor.
        // Accepted: brief reactor stall at job start, not in the hot path.
        let mut op = match build_streaming_window_op(
            &spec,
            state_dir.as_deref(),
            &agg_exprs,
            &agg_is_float,
        ) {
            Ok(op) => op,
            Err(e) => {
                yield Err(e);
                return;
            }
        };

        let mut pending_first = Some(first_batch);
        loop {
            let batch = match pending_first.take() {
                Some(batch) => batch,
                None => match input.next().await {
                    Some(Ok(batch)) => batch,
                    Some(Err(e)) => {
                        yield Err(e);
                        return;
                    }
                    None => break,
                },
            };
            multi_watermark.apply_idle_source_policy();
            let wm = match advance_effective_watermark(
                &batch,
                &spec.event_time_column,
                spec.source_id_column.as_deref(),
                &spec.source_watermark_lags,
                &mut single_watermark,
                &mut multi_watermark,
            ) {
                Ok(wm) => wm,
                Err(e) => {
                    yield Err(e);
                    continue;
                }
            };
            // ST7: forward the watermark to the operator's state backend so TTL
            // eviction is event-time-driven instead of wall-clock-driven.
            op.set_watermark(wm);
            match op.process_batch(&batch, wm) {
                Ok(output_batches) => {
                    for out_batch in output_batches {
                        yield Ok(out_batch);
                    }
                }
                Err(e) => {
                    yield Err(e);
                    return;
                }
            }
        }
        match op.flush() {
            Ok(output_batches) => {
                for out_batch in output_batches {
                    yield Ok(out_batch);
                }
            }
            Err(e) => yield Err(e),
        }
    };
    Ok(Box::pin(stream))
}

/// Parameters for converting a legacy local window spec into a `WindowExecutionSpec`.
pub struct LocalWindowParams {
    pub key_column: String,
    pub key_column_type: String,
    pub event_time_column: String,
    pub watermark_lag_ms: u64,
    pub window_kind: LocalWindowKindBridge,
    pub window_size_ms: u64,
    pub agg_exprs: Vec<AggExpr>,
    pub state_ttl_ms: Option<u64>,
}

/// Convert legacy runtime local spec fields into a plan `WindowExecutionSpec`.
pub fn local_spec_to_window_execution(params: LocalWindowParams) -> WindowExecutionSpec {
    let LocalWindowParams {
        key_column,
        key_column_type,
        event_time_column,
        watermark_lag_ms,
        window_kind,
        window_size_ms,
        agg_exprs,
        state_ttl_ms,
    } = params;
    let (kind, slide_ms, session_gap_ms) = match window_kind {
        LocalWindowKindBridge::Tumbling => (WindowKind::Tumbling, None, None),
        LocalWindowKindBridge::Sliding { slide_ms } => (WindowKind::Sliding, Some(slide_ms), None),
        LocalWindowKindBridge::Session { gap_ms } => (WindowKind::Session, None, Some(gap_ms)),
    };
    WindowExecutionSpec {
        key_column,
        key_column_type,
        event_time_column,
        watermark_lag_ms,
        window_kind: kind,
        window_size_ms,
        slide_ms,
        session_gap_ms,
        agg_exprs: agg_exprs
            .iter()
            .map(|a| {
                let kind = match a.function {
                    AggFunction::Count => WindowAggKind::Count,
                    AggFunction::Sum => WindowAggKind::Sum,
                    AggFunction::Min => WindowAggKind::Min,
                    AggFunction::Max => WindowAggKind::Max,
                    AggFunction::Avg => WindowAggKind::Avg,
                    AggFunction::Stddev => WindowAggKind::Stddev,
                };
                WindowAgg {
                    kind,
                    input_column: a.input_column.clone(),
                    output_column: a.output_column.clone(),
                    filter: a.filter.clone(),
                }
            })
            .collect(),
        state_ttl_ms,
        allowed_lateness_ms: None,
        source_watermark_lags: std::collections::HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    }
}

/// ST8: Execute a stream-stream watermark-bounded window join over pre-collected batches.
///
/// Routes left and right batches (interleaved by index) through a
/// [`WatermarkWindowJoinOperator`].  After processing, calls
/// `advance_watermark(final_watermark_ms)` to flush any remaining state.
/// Returns joined `RecordBatch`es (left cols ∥ right cols, with `left_` /
/// `right_` prefixes when column names collide).
pub fn execute_window_join(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    spec: crate::watermark_join::WatermarkWindowJoinSpec,
    final_watermark_ms: i64,
) -> ExecResult<Vec<RecordBatch>> {
    use crate::watermark_join::WatermarkWindowJoinOperator;
    let mut op = WatermarkWindowJoinOperator::new(spec);
    let mut out = Vec::new();
    let max_side = left_batches.len().max(right_batches.len());
    for i in 0..max_side {
        if let Some(lb) = left_batches.get(i) {
            out.extend(op.process_left(lb));
        }
        if let Some(rb) = right_batches.get(i) {
            out.extend(op.process_right(rb));
        }
    }
    op.advance_watermark(final_watermark_ms);
    Ok(out)
}

/// A typed event in a **barrier-aligned** join input stream.
#[derive(Debug, Clone)]
pub enum JoinStreamEvent {
    /// A batch on the left input.
    Left(RecordBatch),
    /// A batch on the right input.
    Right(RecordBatch),
    /// The checkpoint barrier for `epoch` on the left input.
    LeftBarrier(u64),
    /// The checkpoint barrier for `epoch` on the right input.
    RightBarrier(u64),
    /// Advance the event-time watermark.
    Watermark(i64),
}

/// The output of a barrier-aligned join run.
#[derive(Debug, Default)]
pub struct AlignedJoinOutput {
    /// Joined rows emitted across all epochs (left cols ∥ right cols).
    pub joined: Vec<RecordBatch>,
    /// `(epoch, snapshot_bytes)` captured at each aligned checkpoint, in order.
    pub snapshots: Vec<(u64, Vec<u8>)>,
}

/// Drive a windowed join continuously, taking a consistent snapshot each time a
/// checkpoint barrier **aligns** across both inputs (Chandy–Lamport).
///
/// This is the multi-input counterpart of the single-operator continuous loop:
/// the [`WatermarkWindowJoinOperator`]'s [`BarrierAligner`](crate::BarrierAligner)
/// blocks the input that delivered a barrier first — buffering its post-barrier
/// batches — until the other input's barrier arrives. On alignment the operator
/// is snapshotted and the buffered batches are replayed into the next epoch, so
/// no input is dropped or double-counted across the checkpoint boundary.
pub fn execute_window_join_aligned(
    spec: crate::watermark_join::WatermarkWindowJoinSpec,
    events: impl IntoIterator<Item = JoinStreamEvent>,
    final_watermark_ms: i64,
) -> ExecResult<AlignedJoinOutput> {
    use crate::BarrierEvent;
    use crate::watermark_join::WatermarkWindowJoinOperator;

    let mut op = WatermarkWindowJoinOperator::new(spec);
    let mut out = AlignedJoinOutput::default();
    for ev in events {
        match ev {
            JoinStreamEvent::Left(b) => out.joined.extend(op.process_left(&b)),
            JoinStreamEvent::Right(b) => out.joined.extend(op.process_right(&b)),
            JoinStreamEvent::Watermark(w) => op.advance_watermark(w),
            JoinStreamEvent::LeftBarrier(epoch) => {
                if op.record_left_barrier(epoch) == BarrierEvent::Aligned {
                    snapshot_and_replay_join(&mut op, epoch, &mut out)?;
                }
            }
            JoinStreamEvent::RightBarrier(epoch) => {
                if op.record_right_barrier(epoch) == BarrierEvent::Aligned {
                    snapshot_and_replay_join(&mut op, epoch, &mut out)?;
                }
            }
        }
    }
    op.advance_watermark(final_watermark_ms);
    Ok(out)
}

/// On an aligned epoch: snapshot the operator, then replay the inputs buffered
/// while it was barrier-blocked into the post-snapshot epoch.
fn snapshot_and_replay_join(
    op: &mut crate::watermark_join::WatermarkWindowJoinOperator,
    epoch: u64,
    out: &mut AlignedJoinOutput,
) -> ExecResult<()> {
    let bytes = op
        .snapshot_bytes()
        .map_err(|e| ExecError::InvalidInput(format!("join checkpoint snapshot: {e}")))?;
    out.snapshots.push((epoch, bytes));
    let (left, right) = op.take_realigned_input();
    for b in &left {
        out.joined.extend(op.process_left(b));
    }
    for b in &right {
        out.joined.extend(op.process_right(b));
    }
    Ok(())
}

/// Bridge enum matching the runtime local window kind (avoids circular deps).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalWindowKindBridge {
    Tumbling,
    Sliding { slide_ms: u64 },
    Session { gap_ms: u64 },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn local_spec_to_window_execution_int32_key_type() {
        let spec = local_spec_to_window_execution(LocalWindowParams {
            key_column: "user_id".into(),
            key_column_type: "int32".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: LocalWindowKindBridge::Tumbling,
            window_size_ms: 5_000,
            agg_exprs: vec![],
            state_ttl_ms: None,
        });
        assert_eq!(spec.key_column_type, "int32");
        assert_eq!(spec.key_column, "user_id");
        assert_eq!(spec.window_size_ms, 5_000);
    }

    #[test]
    fn multi_source_watermark_min_across_sources() {
        use std::collections::HashMap;

        let spec = WindowExecutionSpec {
            key_column: "user_id".into(),
            key_column_type: String::from("utf8"),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: WindowKind::Tumbling,
            window_size_ms: 10_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: WindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            allowed_lateness_ms: None,
            source_watermark_lags: HashMap::from([("src-a".into(), 0), ("src-b".into(), 0)]),
            source_id_column: Some("source_id".into()),
            window_timezone: None,
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("source_id", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![5_000])) as _,
                Arc::new(StringArray::from(vec!["src-a"])) as _,
            ],
        )
        .unwrap();
        let out = execute_bounded_window(vec![batch], &spec, None).expect("execute");
        assert!(!out.is_empty());
    }

    fn join_batch(id: &str, ts: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![id])) as _,
                Arc::new(Int64Array::from(vec![ts])) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn aligned_join_snapshots_on_alignment_and_replays_without_loss() {
        use crate::watermark_join::WatermarkWindowJoinSpec;
        let spec = WatermarkWindowJoinSpec {
            time_column: "ts".into(),
            left_key_column: "id".into(),
            right_key_column: "id".into(),
            window_ms: 500,
        };
        // left k@1000; left barrier ep1 → left blocks; left k@1100 is buffered;
        // right k@1200 (matches the in-epoch left k@1000); right barrier ep1 →
        // aligns → snapshot + replay the buffered left k@1100 (matches right k@1200).
        let events = vec![
            JoinStreamEvent::Left(join_batch("k", 1000)),
            JoinStreamEvent::LeftBarrier(1),
            JoinStreamEvent::Left(join_batch("k", 1100)),
            JoinStreamEvent::Right(join_batch("k", 1200)),
            JoinStreamEvent::RightBarrier(1),
        ];
        let out = execute_window_join_aligned(spec, events, i64::MAX).expect("aligned join");

        assert_eq!(out.snapshots.len(), 1, "exactly one aligned checkpoint");
        assert_eq!(out.snapshots[0].0, 1, "the snapshot carries epoch 1");
        assert!(!out.snapshots[0].1.is_empty(), "snapshot bytes captured");
        let joined_rows: usize = out.joined.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            joined_rows, 2,
            "both the in-epoch match and the replayed match are emitted"
        );
    }

    #[test]
    fn aligned_join_with_no_barriers_matches_one_shot() {
        use crate::watermark_join::WatermarkWindowJoinSpec;
        let spec = WatermarkWindowJoinSpec {
            time_column: "ts".into(),
            left_key_column: "id".into(),
            right_key_column: "id".into(),
            window_ms: 500,
        };
        let events = vec![
            JoinStreamEvent::Left(join_batch("k", 1000)),
            JoinStreamEvent::Right(join_batch("k", 1200)),
        ];
        let out = execute_window_join_aligned(spec, events, i64::MAX).expect("aligned join");
        assert!(out.snapshots.is_empty(), "no barriers ⇒ no checkpoints");
        let joined_rows: usize = out.joined.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(joined_rows, 1, "the single match is emitted");
    }
}
