//! Unified bounded window execution for batch and streaming (all deployment modes).

use arrow::record_batch::RecordBatch;
use krishiv_plan::window::{WindowAgg, WindowAggKind, WindowExecutionSpec, WindowKind};
use krishiv_state::{FjallStateBackend, StateBackend, TtlConfig, TtlStateBackend};

use crate::watermark_util::advance_effective_watermark;
use crate::window::MultiSourceWatermarkState;
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
pub fn execute_bounded_window(
    input_batches: Vec<RecordBatch>,
    spec: &WindowExecutionSpec,
) -> ExecResult<Vec<RecordBatch>> {
    if input_batches.is_empty() {
        return Ok(Vec::new());
    }
    if spec.agg_exprs.is_empty() {
        return Err(ExecError::InvalidWindowConfig(
            "window execution requires at least one aggregate".into(),
        ));
    }

    let agg_exprs: Vec<AggExpr> = spec.agg_exprs.iter().map(window_agg_to_expr).collect();
    let mut single_watermark = WatermarkState::new(spec.watermark_lag_ms);
    let mut multi_watermark =
        MultiSourceWatermarkState::new().with_idle_source_policy(60_000, i64::MAX);
    let mut output = Vec::new();

    match spec.window_kind {
        WindowKind::Tumbling => {
            let tw_spec = TumblingWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                agg_exprs: agg_exprs.clone(),
            };
            let redb = FjallStateBackend::ephemeral()
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let state: Box<dyn StateBackend> = if let Some(ttl_ms) = spec.state_ttl_ms {
                Box::new(TtlStateBackend::new(redb, TtlConfig::new(ttl_ms)))
            } else {
                Box::new(redb)
            };
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
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                slide_ms,
                agg_exprs,
            };
            let redb = FjallStateBackend::ephemeral()
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let state: Box<dyn StateBackend> = if let Some(ttl_ms) = spec.state_ttl_ms {
                Box::new(TtlStateBackend::new(redb, TtlConfig::new(ttl_ms)))
            } else {
                Box::new(redb)
            };
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
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                session_gap_ms: gap_ms,
                agg_exprs,
            };
            let redb = FjallStateBackend::ephemeral()
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let state: Box<dyn StateBackend> = if let Some(ttl_ms) = spec.state_ttl_ms {
                Box::new(TtlStateBackend::new(redb, TtlConfig::new(ttl_ms)))
            } else {
                Box::new(redb)
            };
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
    }

    Ok(output)
}

pub fn execute_streaming_window(
    mut input: Pin<Box<dyn Stream<Item = ExecResult<RecordBatch>> + Send>>,
    spec: WindowExecutionSpec,
) -> ExecResult<Pin<Box<dyn Stream<Item = ExecResult<RecordBatch>> + Send>>> {
    if spec.agg_exprs.is_empty() {
        return Err(ExecError::InvalidWindowConfig(
            "window execution requires at least one aggregate".into(),
        ));
    }

    let agg_exprs: Vec<AggExpr> = spec.agg_exprs.iter().map(window_agg_to_expr).collect();

    match spec.window_kind {
        WindowKind::Tumbling => {
            let tw_spec = TumblingWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                agg_exprs: agg_exprs.clone(),
            };
            let redb = FjallStateBackend::ephemeral()
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let state: Box<dyn StateBackend> = if let Some(ttl_ms) = spec.state_ttl_ms {
                Box::new(TtlStateBackend::new(redb, TtlConfig::new(ttl_ms)))
            } else {
                Box::new(redb)
            };
            let mut op =
                StateBackedTumblingWindowOperator::new(tw_spec, state, "window-exec", "tumbling")
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let mut single_watermark = WatermarkState::new(spec.watermark_lag_ms);
            let mut multi_watermark =
                MultiSourceWatermarkState::new().with_idle_source_policy(60_000, i64::MAX);

            let stream = async_stream::stream! {
                while let Some(batch_res) = input.next().await {
                    match batch_res {
                        Ok(batch) => {
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
                        Err(e) => {
                            yield Err(e);
                            return;
                        }
                    }
                }
                match op.flush_closed_windows(i64::MAX) {
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
        WindowKind::Sliding => {
            let slide_ms = spec.slide_ms.unwrap_or(spec.window_size_ms);
            let sw_spec = SlidingWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                slide_ms,
                agg_exprs,
            };
            let redb = FjallStateBackend::ephemeral()
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let state: Box<dyn StateBackend> = if let Some(ttl_ms) = spec.state_ttl_ms {
                Box::new(TtlStateBackend::new(redb, TtlConfig::new(ttl_ms)))
            } else {
                Box::new(redb)
            };
            let mut op =
                StateBackedSlidingWindowOperator::new(sw_spec, state, "window-exec", "sliding")
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let mut single_watermark = WatermarkState::new(spec.watermark_lag_ms);
            let mut multi_watermark =
                MultiSourceWatermarkState::new().with_idle_source_policy(60_000, i64::MAX);

            let stream = async_stream::stream! {
                while let Some(batch_res) = input.next().await {
                    match batch_res {
                        Ok(batch) => {
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
                        Err(e) => {
                            yield Err(e);
                            return;
                        }
                    }
                }
                match op.flush_closed_windows(i64::MAX) {
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
        WindowKind::Session => {
            let session_gap_ms = spec.session_gap_ms.unwrap_or(spec.window_size_ms);
            let sess_spec = SessionWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                session_gap_ms,
                agg_exprs,
            };
            let redb = FjallStateBackend::ephemeral()
                .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let state: Box<dyn StateBackend> = if let Some(ttl_ms) = spec.state_ttl_ms {
                Box::new(TtlStateBackend::new(redb, TtlConfig::new(ttl_ms)))
            } else {
                Box::new(redb)
            };
            let mut op =
                StateBackedSessionWindowOperator::new(sess_spec, state, "window-exec", "session")
                    .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
            let mut single_watermark = WatermarkState::new(spec.watermark_lag_ms);
            let mut multi_watermark =
                MultiSourceWatermarkState::new().with_idle_source_policy(60_000, i64::MAX);

            let stream = async_stream::stream! {
                while let Some(batch_res) = input.next().await {
                    match batch_res {
                        Ok(batch) => {
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
                        Err(e) => {
                            yield Err(e);
                            return;
                        }
                    }
                }
                match op.flush_closed_sessions(i64::MAX) {
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
    }
}

/// Convert legacy runtime local spec fields into a plan `WindowExecutionSpec`.
pub fn local_spec_to_window_execution(
    key_column: String,
    event_time_column: String,
    watermark_lag_ms: u64,
    window_kind: LocalWindowKindBridge,
    window_size_ms: u64,
    agg_exprs: Vec<AggExpr>,
    state_ttl_ms: Option<u64>,
) -> WindowExecutionSpec {
    let (kind, slide_ms, session_gap_ms) = match window_kind {
        LocalWindowKindBridge::Tumbling => (WindowKind::Tumbling, None, None),
        LocalWindowKindBridge::Sliding { slide_ms } => (WindowKind::Sliding, Some(slide_ms), None),
        LocalWindowKindBridge::Session { gap_ms } => (WindowKind::Session, None, Some(gap_ms)),
    };
    WindowExecutionSpec {
        key_column,
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
    fn multi_source_watermark_min_across_sources() {
        use std::collections::HashMap;

        let spec = WindowExecutionSpec {
            key_column: "user_id".into(),
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
        let out = execute_bounded_window(vec![batch], &spec).expect("execute");
        assert!(!out.is_empty());
    }
}
