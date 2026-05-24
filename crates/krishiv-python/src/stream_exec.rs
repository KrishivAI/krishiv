//! Execute a Python [`StreamPipeline`] through the in-process window runtime.

use krishiv_api::{AggExpr, AggFunction, LocalWindowExecutionSpec, LocalWindowKind};
use krishiv_runtime::execute_windowed_stream;
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

fn resolve_input_batches(pipeline: &StreamPipeline) -> Result<Vec<arrow::record_batch::RecordBatch>, krishiv_api::KrishivError> {
    let upper = pipeline.source_id.to_ascii_uppercase();
    if upper.contains("SELECT") || upper.contains("FROM") {
        let df = block_on_async(pipeline.session.sql(&pipeline.source_id))?;
        let result = df.collect()?;
        return Ok(result.batches().to_vec());
    }
    Err(krishiv_api::KrishivError::unsupported(
        "streaming source must be a SQL query from Session.stream(); use memory_stream_collect for in-memory sources",
    ))
}

pub(crate) fn execute_pipeline(pipeline: &StreamPipeline) -> PyResult<Vec<PyBatch>> {
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
    let key_column = pipeline.key_columns.first().cloned().ok_or_else(|| {
        krishiv_api::KrishivError::unsupported(
            "streaming execution requires key_by() before window collect",
        )
    })?;
    let spec = LocalWindowExecutionSpec {
        key_column,
        event_time_column: event_time,
        watermark_lag_ms: pipeline.max_lateness_ms,
        window_kind,
        window_size_ms: window.size_ms,
        agg_exprs,
    };
    let input = resolve_input_batches(pipeline).map_err(map_krishiv_error)?;
    let output = execute_windowed_stream(input, &spec).map_err(map_krishiv_error)?;
    Ok(output
        .into_iter()
        .map(|batch| PyBatch::from_record_batch(batch))
        .collect())
}
