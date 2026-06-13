#![forbid(unsafe_code)]

//! High-level stateful process API for streaming DataFrames.
//!
//! This module re-exports the core dataflow types needed to build stateful
//! per-key streaming operators, and provides convenience functions for
//! attaching them to a [`KrishivStream`][crate::streaming_dataframe::KrishivStream].

pub use krishiv_dataflow::broadcast_state::{
    BroadcastContext, BroadcastProcessExecutor, BroadcastProcessFunction,
};
pub use krishiv_dataflow::connected_streams::{
    CoProcessExecutor, CoProcessFunction, ConnectedStreams,
};
pub use krishiv_dataflow::operator_config::OperatorConfig;
pub use krishiv_dataflow::process_fn::{ProcessContext, ProcessFunction, ProcessFunctionExecutor};
pub use krishiv_dataflow::state_descriptor::{
    ListState, MapState, ReducingState, StateError, StateValue, ValueState,
};

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use krishiv_dataflow::ProcessFunctionExecutor as DataflowExecutor;

use crate::streaming_dataframe::KrishivStream;

// ── apply_process_function ────────────────────────────────────────────────────

/// Apply a `ProcessFunction` to each batch of a streaming `KrishivStream`,
/// executing per-row and firing event-time timers as the watermark advances.
///
/// Returns a new `KrishivStream` of emitted output batches.
///
/// # Watermark
/// Because the stream adapter here does not track a separate watermark signal,
/// each batch is processed with `watermark_ms = 0`. For real event-time timer
/// support, call [`ProcessFunctionExecutor`] directly inside a custom streaming
/// task.
pub fn apply_process_function(
    input: KrishivStream,
    key_column: impl Into<String>,
    func: Box<dyn ProcessFunction>,
    _config: OperatorConfig,
) -> KrishivStream {
    let key_column: String = key_column.into();
    let mut executor = DataflowExecutor::new(key_column, func);

    let output = input.flat_map(move |result| {
        let batches: Vec<std::result::Result<RecordBatch, String>> = match result {
            Err(e) => vec![Err(e)],
            Ok(batch) => {
                match executor.process_batch(&batch, 0) {
                    Err(e) => vec![Err(e.to_string())],
                    Ok(mut out) => {
                        // Also fire timers at watermark 0 to collect any pending outputs.
                        if let Ok(timer_out) = executor.fire_timers(0) {
                            out.extend(timer_out);
                        }
                        out.into_iter().map(Ok).collect()
                    }
                }
            }
        };
        futures::stream::iter(batches)
    });

    Box::pin(output)
}

// ── apply_async_io ────────────────────────────────────────────────────────────

/// Apply an async I/O function to each batch in the stream.
///
/// The async lookup is applied to each `RecordBatch` independently. The
/// `batch_size` parameter is currently informational (reserved for future
/// windowed concurrency); the function is applied batch-by-batch.
pub fn apply_async_io<F, Fut>(input: KrishivStream, _batch_size: usize, lookup: F) -> KrishivStream
where
    F: Fn(RecordBatch) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = crate::error::Result<RecordBatch>> + Send + 'static,
{
    let output = input.then(move |result| {
        let maybe_batch = result;
        let fut: std::pin::Pin<
            Box<dyn std::future::Future<Output = std::result::Result<RecordBatch, String>> + Send>,
        > = match maybe_batch {
            Err(e) => Box::pin(futures::future::ready(Err(e))),
            Ok(batch) => {
                let inner = lookup(batch);
                Box::pin(async move { inner.await.map_err(|e| e.to_string()) })
            }
        };
        fut
    });

    Box::pin(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use std::sync::Arc;

    fn int_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    fn stream_from_batches(batches: Vec<RecordBatch>) -> KrishivStream {
        Box::pin(futures::stream::iter(
            batches.into_iter().map(Ok::<_, String>),
        ))
    }

    /// A ProcessFunction that emits a count batch for each key.
    struct EchoCountFn {
        counts: std::collections::HashMap<String, i64>,
    }

    impl EchoCountFn {
        fn new() -> Self {
            Self {
                counts: std::collections::HashMap::new(),
            }
        }
    }

    impl ProcessFunction for EchoCountFn {
        fn on_event(
            &mut self,
            key: &str,
            _batch: &RecordBatch,
            _row: usize,
            ctx: &mut ProcessContext<'_>,
        ) -> krishiv_dataflow::ExecResult<()> {
            let count = self.counts.entry(key.to_owned()).or_default();
            *count += 1;
            // Emit a single-row batch with the running count.
            let schema = Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::Int64,
                false,
            )]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![*count]))])
                    .unwrap();
            ctx.emit(batch);
            Ok(())
        }

        fn on_timer(
            &mut self,
            _key: &str,
            _fire_time_ms: i64,
            _ctx: &mut ProcessContext<'_>,
        ) -> krishiv_dataflow::ExecResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn apply_process_function_emits_output_batches() {
        let batches = vec![int_batch(&[1, 2, 1])];
        let stream = stream_from_batches(batches);

        let func = EchoCountFn::new();
        let config = OperatorConfig::new("test-op");
        let out_stream = apply_process_function(stream, "id", Box::new(func), config);

        let results: Vec<_> = out_stream.collect().await;
        // 3 events → 3 emitted batches from on_event.
        assert_eq!(results.len(), 3, "one output batch per input row");
        for r in results {
            assert!(r.is_ok());
        }
    }

    #[tokio::test]
    async fn apply_async_io_transforms_batches() {
        let batches = vec![int_batch(&[10, 20])];
        let stream = stream_from_batches(batches);

        let out_stream = apply_async_io(stream, 8, |batch| async move {
            // Identity: just pass the batch through.
            Ok(batch)
        });

        let results: Vec<_> = out_stream.collect().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        assert_eq!(results[0].as_ref().unwrap().num_rows(), 2);
    }
}
