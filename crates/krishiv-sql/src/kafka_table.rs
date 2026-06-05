use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::catalog::TableProviderFactory;
use datafusion::catalog::streaming::StreamingTable;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use krishiv_connectors::Source;
use krishiv_connectors::kafka::{KafkaConfig, KafkaSource};

// Auto-commit interval used for the streaming SQL path (at-least-once delivery).
const STREAMING_AUTO_COMMIT_MS: u64 = 1_000;

pub struct KafkaPartitionStream {
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
                        let projected = project_batch(&batch, &schema);
                        if tx.send(Ok(projected)).await.is_err() {
                            break; // receiver dropped — query cancelled
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

/// Project and cast a raw Kafka batch to the declared table schema.
///
/// Missing columns → typed null arrays.
/// Cast failures → null arrays with a tracing warning (no silent data loss).
fn project_batch(batch: &RecordBatch, schema: &SchemaRef) -> RecordBatch {
    let mut cols = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let col = if let Ok(idx) = batch.schema().index_of(field.name()) {
            let src = batch.column(idx);
            match arrow::compute::cast(src, field.data_type()) {
                Ok(casted) => casted,
                Err(e) => {
                    tracing::warn!(
                        field = field.name(),
                        from_type = %src.data_type(),
                        to_type = %field.data_type(),
                        error = %e,
                        "Kafka column cast failed; filling with nulls to preserve row count"
                    );
                    arrow::array::new_null_array(field.data_type(), batch.num_rows())
                }
            }
        } else {
            arrow::array::new_null_array(field.data_type(), batch.num_rows())
        };
        cols.push(col);
    }
    match RecordBatch::try_new(schema.clone(), cols) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("Kafka project_batch: RecordBatch construction failed: {e}");
            RecordBatch::new_empty(schema.clone())
        }
    }
}

/// Build a DataFusion `StreamingTable` backed by a live Kafka/Redpanda topic.
///
/// Enables rdkafka auto-commit at 1 s intervals for at-least-once delivery.
/// Callers that prefer SQL DDL can use `CREATE EXTERNAL TABLE … STORED AS KAFKA`.
pub fn create_kafka_streaming_table(
    schema: SchemaRef,
    config: KafkaConfig,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let config = config.with_auto_commit(STREAMING_AUTO_COMMIT_MS);
    let source = KafkaSource::new(config).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition = Arc::new(KafkaPartitionStream::new(schema.clone(), source));
    let table = StreamingTable::try_new(schema, vec![partition])?;
    Ok(Arc::new(table))
}

/// DataFusion `TableProviderFactory` for `CREATE EXTERNAL TABLE … STORED AS KAFKA`.
///
/// Shares the engine's `streaming_sources` set so `SqlEngine::is_streaming_query`
/// correctly identifies DDL-registered Kafka tables.
pub struct KafkaTableFactory {
    /// Shared with the owning `SqlEngine` so DDL-created tables are tracked.
    pub streaming_sources: std::sync::Arc<std::sync::RwLock<std::collections::HashSet<String>>>,
}

impl std::fmt::Debug for KafkaTableFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaTableFactory").finish()
    }
}

#[async_trait]
impl TableProviderFactory for KafkaTableFactory {
    async fn create(
        &self,
        _state: &dyn datafusion::catalog::Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let topic = cmd.location.clone();
        let mut bootstrap_servers = "127.0.0.1:9092".to_string();
        let mut group_id = "krishiv-sql".to_string();
        for (k, v) in &cmd.options {
            if k == "bootstrap.servers" {
                bootstrap_servers = v.clone();
            }
            if k == "group.id" {
                group_id = v.clone();
            }
        }

        let schema: Arc<arrow::datatypes::Schema> = cmd.schema.as_ref().inner().clone();
        let config = KafkaConfig {
            bootstrap_servers,
            topic,
            group_id,
            auto_commit_interval_ms: Some(STREAMING_AUTO_COMMIT_MS),
            security_protocol: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            sasl_username: None,
            sasl_password: None,
            sasl_mechanisms: None,
            enable_idempotence: None,
            transactional_id: None,
        };

        let source =
            KafkaSource::new(config).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partition = Arc::new(KafkaPartitionStream::new(schema.clone(), source));
        let table = StreamingTable::try_new(schema, vec![partition])?;

        // Register the table name so SqlEngine::is_streaming_query detects it.
        let table_name = cmd.name.table().to_string();
        self.streaming_sources
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(table_name);

        Ok(Arc::new(table))
    }
}
