//! Execute a Python [`StreamPipeline`] through the unified session execution runtime.

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use krishiv_api::{AggExpr, AggFunction, LocalWindowExecutionSpec, LocalWindowKind};
use pyo3::prelude::*;

use crate::agg::{AggDescriptor, AggKind};
use crate::batch::PyBatch;
use crate::errors::map_krishiv_error;
use crate::pipeline::{StreamPipeline, WindowKind};
use crate::session::block_on_async;

pub(crate) fn agg_descriptor_to_expr(desc: &AggDescriptor) -> AggExpr {
    let function = match desc.function {
        AggKind::Count => AggFunction::Count,
        AggKind::Sum => AggFunction::Sum,
        AggKind::Min => AggFunction::Min,
        AggKind::Max => AggFunction::Max,
        AggKind::Mean => AggFunction::Avg,
    };
    AggExpr {
        filter: desc.filter.clone(),
        function,
        input_column: desc.input_column.clone().unwrap_or_default(),
        output_column: desc.output_name.clone(),
    }
}

pub(crate) async fn resolve_input_batches(
    pipeline: &StreamPipeline,
) -> Result<Vec<arrow::record_batch::RecordBatch>, krishiv_api::KrishivError> {
    if let Some(name) = pipeline.source_id.strip_prefix("memory:") {
        return pipeline.session.memory_stream_batches(name).ok_or_else(|| {
            krishiv_api::KrishivError::unsupported(format!(
                "memory stream '{name}' is not registered on this session"
            ))
        });
    }
    let upper = pipeline.source_id.to_ascii_uppercase();
    if upper.contains("SELECT") || upper.contains("FROM") {
        let df = pipeline.session.sql_async(&pipeline.source_id).await?;
        let result = df.collect_async().await?;
        return Ok(result.batches().to_vec());
    }
    Err(krishiv_api::KrishivError::unsupported(
        "streaming source must be SQL (Session.stream) or memory:<name> (Session.memory_stream)",
    ))
}

pub(crate) fn spec_from_pipeline(pipeline: &StreamPipeline) -> PyResult<LocalWindowExecutionSpec> {
    let window = pipeline.window.as_ref().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "windowed stream requires tumbling_window() or KeyedStream.window() before collect",
        )
    })?;
    let event_time = pipeline
        .event_time_column
        .clone()
        .filter(|c| !c.is_empty())
        .unwrap_or_else(|| pipeline.watermark_column.clone());
    if event_time.is_empty() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "streaming execution requires a watermark or event-time column",
        ));
    }
    // Empty stays empty here; the shared builder's `with_aggs` keeps the default
    // COUNT(*) when no aggregations are supplied.
    let aggs: Vec<AggExpr> = pipeline
        .aggregations
        .iter()
        .map(agg_descriptor_to_expr)
        .collect();
    // B1: reject multi-column key_by — the runtime key_column field is a single string.
    if pipeline.key_columns.len() > 1 {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "multi-column key_by is not yet supported for windowed aggregation; \
             provide a single key column (got: [{}])",
            pipeline.key_columns.join(", ")
        )));
    }
    let key_column = pipeline
        .key_columns
        .first()
        .cloned()
        .ok_or_else(|| {
            krishiv_api::KrishivError::unsupported(
                "streaming execution requires key_by() before window collect",
            )
        })
        .map_err(map_krishiv_error)?;
    // B3: validate window-type-specific fields.
    let window_kind = match window.kind {
        WindowKind::Tumbling => LocalWindowKind::Tumbling,
        WindowKind::Sliding => LocalWindowKind::Sliding {
            slide_ms: window.slide_ms.unwrap_or(window.size_ms),
        },
        WindowKind::Session => {
            let gap = window.gap_ms.ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "session window requires a gap_ms value; use session_window(gap_ms=N)",
                )
            })?;
            LocalWindowKind::Session { gap_ms: gap }
        }
    };
    let state_ttl_ms = pipeline
        .state_ttl_ms
        .or_else(|| pipeline.session.state_ttl().map(|c| c.ttl_ms()));
    // G3: thread per-source watermark lags when set.
    let source_watermark_lags = pipeline.source_watermarks.clone();
    let source_id_column = if source_watermark_lags.is_empty() {
        None
    } else {
        pipeline
            .source_id_column
            .clone()
            .or_else(|| Some("source_id".to_string()))
    };
    Ok(
        LocalWindowExecutionSpec::windowed(key_column, event_time, window_kind, window.size_ms)
            .with_watermark_lag_ms(pipeline.max_lateness_ms)
            .with_aggs(aggs)
            .with_state_ttl_ms(state_ttl_ms)
            .with_source_watermarks(source_watermark_lags, source_id_column),
    )
}

pub(crate) fn execute_pipeline(pipeline: &StreamPipeline) -> PyResult<Vec<PyBatch>> {
    let spec = spec_from_pipeline(pipeline)?;
    let input = block_on_async(resolve_input_batches(pipeline)).map_err(map_krishiv_error)?;
    let output = pipeline
        .session
        .execution_runtime()
        .collect_bounded_window(&pipeline.source_id, input, &spec)
        .map_err(|e| map_krishiv_error(krishiv_api::KrishivError::from(e)))?;
    Ok(output.into_iter().map(PyBatch::from_record_batch).collect())
}

