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

async fn resolve_input_batches(
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

async fn resolve_input_stream(
    pipeline: &StreamPipeline,
) -> Result<krishiv_api::KrishivStream, krishiv_api::KrishivError> {
    if let Some(name) = pipeline.source_id.strip_prefix("memory:") {
        let batches = pipeline
            .session
            .memory_stream_batches(name)
            .ok_or_else(|| {
                krishiv_api::KrishivError::unsupported(format!(
                    "memory stream '{name}' is not registered on this session"
                ))
            })?;
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        return Ok(Box::pin(stream));
    }
    let upper = pipeline.source_id.to_ascii_uppercase();
    if upper.contains("SELECT") || upper.contains("FROM") {
        let df = pipeline.session.sql_async(&pipeline.source_id).await?;
        return df.execute_stream_async().await;
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
    Ok(LocalWindowExecutionSpec {
                key_column_type: String::from("utf8"),
        key_column,
        event_time_column: event_time,
        watermark_lag_ms: pipeline.max_lateness_ms,
        window_kind,
        window_size_ms: window.size_ms,
        agg_exprs,
        state_ttl_ms,
        source_watermark_lags,
        source_id_column,
    })
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

pub(crate) fn spawn_pipeline_stream(
    pipeline: StreamPipeline,
) -> PyResult<tokio::sync::mpsc::Receiver<PyResult<PyBatch>>> {
    let spec = spec_from_pipeline(&pipeline)?;
    let (tx, rx) = tokio::sync::mpsc::channel(16);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let input_res = resolve_input_stream(&pipeline).await;
            let input_stream = match input_res {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(map_krishiv_error(e))).await;
                    return;
                }
            };

            use futures::StreamExt;
            let mapped_input_stream = input_stream
                .map(|res| res.map_err(|e| krishiv_dataflow::ExecError::InvalidWindowConfig(e)));

            let output_res =
                krishiv_api::execute_streaming_window(Box::pin(mapped_input_stream), &spec);
            let mut output_stream = match output_res {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx
                        .send(Err(map_krishiv_error(krishiv_api::KrishivError::from(e))))
                        .await;
                    return;
                }
            };

            while let Some(batch_res) = output_stream.next().await {
                match batch_res {
                    Ok(batch) => {
                        if tx
                            .send(Ok(PyBatch::from_record_batch(batch)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(map_krishiv_error(
                                krishiv_api::KrishivError::unsupported(e.to_string()),
                            )))
                            .await;
                        break;
                    }
                }
            }
        });
    });

    Ok(rx)
}
