//! Execute a Python [`StreamPipeline`] through the unified session execution runtime.

use krishiv_api::{AggExpr, AggFunction, LocalWindowExecutionSpec, LocalWindowKind};
use pyo3::prelude::*;

use crate::agg::{AggDescriptor, AggKind};
use crate::batch::PyBatch;
use crate::errors::map_krishiv_error;
use crate::pipeline::{StreamPipeline, WindowKind};
use crate::session::block_on_async;

fn agg_descriptor_to_expr(desc: &AggDescriptor) -> AggExpr {
    let function = match desc.function {
        AggKind::Count => AggFunction::Count,
        AggKind::Sum => AggFunction::Sum,
        AggKind::Min => AggFunction::Min,
        AggKind::Max => AggFunction::Max,
        AggKind::Mean => AggFunction::Avg,
    };
    AggExpr {
        function,
        input_column: desc.input_column.clone().unwrap_or_default(),
        output_column: desc.output_name.clone(),
    }
}

fn resolve_input_batches(
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
        let df = block_on_async(pipeline.session.sql_async(&pipeline.source_id))?;
        let result = block_on_async(df.collect_async())?;
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
    let agg_exprs = if pipeline.aggregations.is_empty() {
        LocalWindowExecutionSpec::default_count_agg()
    } else {
        pipeline
            .aggregations
            .iter()
            .map(agg_descriptor_to_expr)
            .collect()
    };
    let window_kind = match window.kind {
        WindowKind::Tumbling => LocalWindowKind::Tumbling,
        WindowKind::Sliding => LocalWindowKind::Sliding {
            slide_ms: window.slide_ms.unwrap_or(window.size_ms),
        },
        WindowKind::Session => LocalWindowKind::Session {
            gap_ms: window.gap_ms.unwrap_or(window.size_ms),
        },
    };
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
    let state_ttl_ms = pipeline.session.state_ttl().map(|c| c.ttl_ms());
    Ok(LocalWindowExecutionSpec {
        key_column,
        event_time_column: event_time,
        watermark_lag_ms: pipeline.max_lateness_ms,
        window_kind,
        window_size_ms: window.size_ms,
        agg_exprs,
        state_ttl_ms,
        source_watermark_lags: std::collections::HashMap::new(),
        source_id_column: None,
    })
}

pub(crate) fn execute_pipeline(pipeline: &StreamPipeline) -> PyResult<Vec<PyBatch>> {
    let spec = spec_from_pipeline(pipeline)?;
    let input = resolve_input_batches(pipeline).map_err(map_krishiv_error)?;
    let output = krishiv_api::execute_windowed_stream(input, &spec)
        .map_err(|e| map_krishiv_error(krishiv_api::KrishivError::from(e)))?;
    Ok(output.into_iter().map(PyBatch::from_record_batch).collect())
}
