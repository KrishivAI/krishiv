//! Unified bounded window execution for batch and streaming (all deployment modes).

use arrow::record_batch::RecordBatch;
use krishiv_plan::window::{
    WindowAgg, WindowAggKind, WindowExecutionSpec, WindowKind, validate_window_execution_spec,
};
use krishiv_state::{RocksDbStateBackend, StateBackend, TtlConfig, TtlStateBackend};

/// Open or create a state backend for a window operator.
///
/// Shared between `operator_runtime.rs` (bounded/streaming execution) and
/// `continuous.rs` (continuous drain-cycle execution).
pub(crate) fn open_state_backend(
    state_dir: Option<&std::path::Path>,
    tag: &str,
    ttl_ms: Option<u64>,
) -> ExecResult<Box<dyn StateBackend>> {
    let backend = match state_dir {
        None => RocksDbStateBackend::ephemeral()
            .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?,
        Some(dir) => {
            let path = dir.join(tag);
            std::fs::create_dir_all(&path).map_err(|e| {
                ExecError::InvalidWindowConfig(format!(
                    "failed to create state dir '{}': {e}",
                    path.display()
                ))
            })?;
            RocksDbStateBackend::open(&path)
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?
        }
    };
    if let Some(ms) = ttl_ms {
        Ok(Box::new(TtlStateBackend::new(backend, TtlConfig::new(ms))))
    } else {
        Ok(Box::new(backend))
    }
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
    };
    AggExpr {
        function,
        input_column: agg.input_column.clone(),
        output_column: agg.output_column.clone(),
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
    let agg_is_float: Vec<bool> = input_batches
        .first()
        .map(|b| {
            agg_exprs
                .iter()
                .map(|e| {
                    b.schema()
                        .field_with_name(&e.input_column)
                        .map(|f| *f.data_type() == arrow::datatypes::DataType::Float64)
                        .unwrap_or(false)
                })
                .collect()
        })
        .unwrap_or_else(|| vec![false; agg_exprs.len()]);

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
        let agg_is_float: Vec<bool> = agg_exprs
            .iter()
            .map(|e| {
                first_batch
                    .schema()
                    .field_with_name(&e.input_column)
                    .map(|f| *f.data_type() == arrow::datatypes::DataType::Float64)
                    .unwrap_or(false)
            })
            .collect();
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
                };
                WindowAgg {
                    kind,
                    input_column: a.input_column.clone(),
                    output_column: a.output_column.clone(),
                }
            })
            .collect(),
        state_ttl_ms,
        source_watermark_lags: std::collections::HashMap::new(),
        source_id_column: None,
    }
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
            source_watermark_lags: HashMap::from([("src-a".into(), 0), ("src-b".into(), 0)]),
            source_id_column: Some("source_id".into()),
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
}
