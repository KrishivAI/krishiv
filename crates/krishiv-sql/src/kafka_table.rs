use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::catalog::streaming::StreamingTable;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use krishiv_connectors::Source;
use krishiv_connectors::kafka::{KafkaConfig, KafkaSource};

// Auto-commit interval for dev-local streaming SQL (at-least-once). Durable profiles
// use manual commit aligned with checkpoint barriers.
const STREAMING_AUTO_COMMIT_MS: u64 = 1_000;

pub(crate) fn kafka_auto_commit_interval_ms() -> Option<u64> {
    let profile = std::env::var("KRISHIV_DURABILITY_PROFILE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(krishiv_common::DurabilityProfile::DevLocal);
    if krishiv_common::requires_manual_kafka_commit(profile) {
        None
    } else {
        Some(STREAMING_AUTO_COMMIT_MS)
    }
}

pub(crate) struct KafkaPartitionStream {
    schema: SchemaRef,
    source: Arc<tokio::sync::Mutex<KafkaSource>>,
}

impl KafkaPartitionStream {
    pub fn new(schema: SchemaRef, source: KafkaSource) -> Self {
        Self {
            schema,
            source: Arc::new(tokio::sync::Mutex::new(source)),
        }
    }
}

impl std::fmt::Debug for KafkaPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaPartitionStream").finish()
    }
}

impl PartitionStream for KafkaPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        let source = self.source.clone();
        let schema = self.schema.clone();
        let manual_commit = kafka_auto_commit_interval_ms().is_none();

        // Use an async channel so the polling loop can run indefinitely.
        // `Ok(None)` from `read_batch` means "no message on this poll cycle"
        // for an unbounded topic — we keep looping rather than ending the stream.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch, DataFusionError>>(64);

        tokio::spawn(async move {
            loop {
                // Check cancellation before doing any I/O: if the DataFusion
                // executor dropped the stream, stop immediately rather than
                // waiting up to poll_timeout_ms to detect it on the next send.
                if tx.is_closed() {
                    break;
                }
                let res = {
                    let mut guard = source.lock().await;
                    guard.read_batch().await
                };
                match res {
                    Ok(Some(batch)) if batch.num_rows() == 0 => {
                        // Empty batch (tombstone / non-UTF-8 skip) — keep polling.
                    }
                    Ok(Some(batch)) => {
                        let send_result = match project_batch(&batch, &schema) {
                            Ok(projected) => tx.send(Ok(projected)).await,
                            Err(e) => tx.send(Err(DataFusionError::ArrowError(Box::new(e), None))).await,
                        };
                        if send_result.is_err() {
                            break; // receiver dropped — query cancelled
                        }
                        if manual_commit {
                            let guard = source.lock().await;
                            guard.commit_current_offset();
                        }
                    }
                    Ok(None) => {
                        // Poll timeout — no message ready; yield and retry.
                        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
                        break;
                    }
                }
            }
        });

        let recv_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            recv_stream,
        ))
    }
}

/// Project and cast a raw connector batch to the declared table schema.
///
/// Missing columns → typed null arrays.
/// Cast failures → null arrays with a tracing warning (no silent data loss).
pub(crate) fn project_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut cols = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let col = if let Ok(idx) = batch.schema().index_of(field.name()) {
            let src = batch.column(idx);
            arrow::compute::cast(src, field.data_type()).map_err(|e| {
                arrow::error::ArrowError::CastError(format!(
                    "Kafka column '{}': cast from {} to {} failed: {e}",
                    field.name(),
                    src.data_type(),
                    field.data_type(),
                ))
            })?
        } else {
            arrow::array::new_null_array(field.data_type(), batch.num_rows())
        };
        cols.push(col);
    }
    RecordBatch::try_new(schema.clone(), cols)
}

/// Build a DataFusion `StreamingTable` backed by a live Kafka/Redpanda topic.
///
/// Enables rdkafka auto-commit at 1 s intervals for at-least-once delivery.
/// Callers that prefer SQL DDL can use `CREATE EXTERNAL TABLE … STORED AS KAFKA`.
pub fn create_kafka_streaming_table(
    schema: SchemaRef,
    config: KafkaConfig,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let config = match kafka_auto_commit_interval_ms() {
        Some(ms) => config.with_auto_commit(ms),
        None => config,
    };
    let source = KafkaSource::new(config).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition = Arc::new(KafkaPartitionStream::new(schema.clone(), source));
    let table = StreamingTable::try_new(schema, vec![partition])?;
    Ok(Arc::new(table))
}
