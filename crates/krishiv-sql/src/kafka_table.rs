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
    auto_commit_interval_for(profile)
}

/// The auto-commit decision itself, separated from reading the environment.
///
/// Kept pure so it can be tested directly: mutating process environment from
/// a test is unsound under a multi-threaded runner, and the workspace denies
/// the `unsafe` that edition 2024 requires for `set_var`.
pub(crate) fn auto_commit_interval_for(
    profile: krishiv_common::DurabilityProfile,
) -> Option<u64> {
    if krishiv_common::requires_manual_kafka_commit(profile) {
        None
    } else {
        Some(STREAMING_AUTO_COMMIT_MS)
    }
}

pub(crate) struct KafkaPartitionStream {
    schema: SchemaRef,
    source: Arc<tokio::sync::Mutex<KafkaSource>>,
    /// Handle to the spawned Kafka consumer task; stored so it can be aborted
    /// if the stream is dropped before the consumer loop exits.
    consumer_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl KafkaPartitionStream {
    pub fn new(schema: SchemaRef, source: KafkaSource) -> Self {
        Self {
            schema,
            source: Arc::new(tokio::sync::Mutex::new(source)),
            consumer_task: std::sync::Mutex::new(None),
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

        let task = tokio::spawn(async move {
            // Coalesce the source's per-message (typically single-row) batches
            // into larger record batches for downstream throughput, while still
            // flushing promptly on a poll gap so streaming latency stays low.
            // Every projected batch shares the declared table schema, so
            // concatenation is always valid.
            const COALESCE_MAX_ROWS: usize = 1024;
            let mut pending: Vec<RecordBatch> = Vec::new();
            let mut pending_rows: usize = 0;
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
                        match project_batch(&batch, &schema) {
                            Ok(projected) => {
                                pending_rows += projected.num_rows();
                                pending.push(projected);
                            }
                            Err(e) => {
                                let _ = flush_pending(&tx, &schema, &mut pending).await;
                                let _ = tx
                                    .send(Err(DataFusionError::ArrowError(Box::new(e), None)))
                                    .await;
                                break;
                            }
                        }
                        if manual_commit {
                            let guard = source.lock().await;
                            guard.commit_current_offset();
                        }
                        if pending_rows >= COALESCE_MAX_ROWS {
                            pending_rows = 0;
                            if flush_pending(&tx, &schema, &mut pending).await.is_err() {
                                break; // receiver dropped — query cancelled
                            }
                        }
                    }
                    Ok(None) => {
                        // Poll gap — flush what we have so consumers see low
                        // latency, then yield and retry.
                        pending_rows = 0;
                        if flush_pending(&tx, &schema, &mut pending).await.is_err() {
                            break;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    }
                    Err(e) => {
                        let _ = flush_pending(&tx, &schema, &mut pending).await;
                        let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
                        break;
                    }
                }
            }
            // Best-effort final flush before the task exits.
            let _ = flush_pending(&tx, &schema, &mut pending).await;
        });
        *self.consumer_task.lock().unwrap_or_else(|p| p.into_inner()) = Some(task);

        let recv_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            recv_stream,
        ))
    }
}

/// Concatenate the buffered per-message batches into one and send it downstream.
///
/// All buffered batches share the declared table schema (they come from
/// `project_batch`), so concatenation always succeeds. Returns `Err(())` if the
/// receiver has been dropped (query cancelled) so the caller can stop polling.
async fn flush_pending(
    tx: &tokio::sync::mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    schema: &SchemaRef,
    pending: &mut Vec<RecordBatch>,
) -> Result<(), ()> {
    if pending.is_empty() {
        return Ok(());
    }
    let coalesced = if pending.len() == 1 {
        pending.remove(0)
    } else {
        match arrow::compute::concat_batches(schema, pending.iter()) {
            Ok(batch) => {
                pending.clear();
                batch
            }
            Err(e) => {
                pending.clear();
                return tx
                    .send(Err(DataFusionError::ArrowError(Box::new(e), None)))
                    .await
                    .map_err(|_| ());
            }
        }
    };
    tx.send(Ok(coalesced)).await.map_err(|_| ())
}

/// Project and cast a raw connector batch to the declared table schema.
///
/// A column the message does not carry becomes a typed null array, so a
/// sparse payload still matches the declared schema.
///
/// A value that is present but does not parse into the declared type becomes
/// **null**: `arrow::compute::cast` is lenient by default, so casting the
/// string `"not-a-number"` to `Int64` yields null rather than an error. That
/// is a row silently losing a field, and this function warns when it happens
/// — counting the nulls the cast introduced, since the kernel does not report
/// them. Without the warning the loss is invisible, which is what "no silent
/// data loss" was always supposed to rule out.
///
/// A type *pair* with no cast kernel at all is still a hard error, and the
/// caller turns that into a terminated stream.
pub(crate) fn project_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut cols = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let col = if let Ok(idx) = batch.schema().index_of(field.name()) {
            let src = batch.column(idx);
            let casted = arrow::compute::cast(src, field.data_type()).map_err(|e| {
                arrow::error::ArrowError::CastError(format!(
                    "Kafka column '{}': cast from {} to {} failed: {e}",
                    field.name(),
                    src.data_type(),
                    field.data_type(),
                ))
            })?;
            // A lenient cast reports nothing when it cannot parse a value — it
            // just emits null. The only evidence is that nulls appeared where
            // the source had none, so that is what we count.
            let dropped = casted.null_count().saturating_sub(src.null_count());
            if dropped > 0 {
                tracing::warn!(
                    column = %field.name(),
                    from = %src.data_type(),
                    to = %field.data_type(),
                    dropped,
                    "Kafka value(s) did not parse into the declared column type and \
                     became null; the rows are kept and only this field is lost"
                );
            }
            casted
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
    // A Kafka topic never ends: `KafkaPartitionStream::execute` treats a poll
    // gap as "retry", never as end-of-stream. Left at DataFusion's default
    // (`infinite: false`) the plan claims `Boundedness::Bounded`, so a
    // pipeline-breaking operator — `ORDER BY`, an unwindowed aggregate — is
    // accepted and then blocks forever waiting for an end that never comes.
    // Declaring it unbounded turns that silent hang into a plan-time error.
    let table = StreamingTable::try_new(schema, vec![partition])?.with_infinite_table(true);
    Ok(Arc::new(table))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn declared() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch_of(fields: Vec<Field>, cols: Vec<arrow::array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    /// A widening cast is applied, not merely accepted: the output must carry
    /// the *declared* type, since the reduce side labels data with it.
    #[test]
    fn a_castable_column_is_cast_to_the_declared_type() {
        let raw = batch_of(
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        );
        let out = project_batch(&raw, &declared()).expect("castable");
        assert_eq!(out.schema(), declared());
        let ids = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cast to int64");
        assert_eq!(ids.values(), &[1i64, 2]);
    }

    /// A column the message omits becomes typed nulls rather than a failure —
    /// this is what lets a sparse payload satisfy the declared schema.
    #[test]
    fn a_missing_column_becomes_typed_nulls() {
        let raw = batch_of(
            vec![Field::new("id", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![7]))],
        );
        let out = project_batch(&raw, &declared()).expect("missing column is allowed");
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.column(1).null_count(), 1, "absent column must be null");
        assert_eq!(out.schema(), declared());
    }

    /// A value that does not parse becomes null rather than failing the
    /// batch — `arrow::compute::cast` is lenient — and the row survives with
    /// only that field lost.
    ///
    /// This is the case the doc comment promised would not be *silent*. The
    /// cast kernel reports nothing, so the only signal is the null that
    /// appeared where the source had a value; `project_batch` counts exactly
    /// that and warns.
    #[test]
    fn an_unparseable_value_becomes_null_and_keeps_the_row() {
        let raw = batch_of(
            vec![Field::new("id", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec![Some("7"), Some("nope")]))],
        );
        let out = project_batch(&raw, &declared()).expect("lenient cast does not fail");
        assert_eq!(out.num_rows(), 2, "both rows must survive");
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(!ids.is_null(0), "the parseable value is kept");
        assert_eq!(ids.value(0), 7);
        assert!(ids.is_null(1), "the unparseable value becomes null");
    }

    /// Columns are selected by name, so a different message field order is
    /// not a reordering of the output.
    #[test]
    fn columns_are_matched_by_name_not_position() {
        let raw = batch_of(
            vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("id", DataType::Int64, true),
            ],
            vec![
                Arc::new(StringArray::from(vec!["z"])),
                Arc::new(Int64Array::from(vec![9])),
            ],
        );
        let out = project_batch(&raw, &declared()).expect("reordered");
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[9i64], "id must come from the 'id' field");
    }

    /// Fields the table did not declare are dropped.
    #[test]
    fn undeclared_columns_are_ignored() {
        let raw = batch_of(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("extra", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["ignored"])),
            ],
        );
        let out = project_batch(&raw, &declared()).expect("extra column");
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema(), declared());
    }

    #[tokio::test]
    async fn flushing_nothing_sends_nothing() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let mut pending = Vec::new();
        flush_pending(&tx, &declared(), &mut pending).await.unwrap();
        drop(tx);
        assert!(rx.recv().await.is_none(), "an empty flush must not send");
    }

    /// Several buffered messages arrive downstream as one batch, with every
    /// row preserved and in order — that is the whole point of coalescing.
    #[tokio::test]
    async fn flushing_coalesces_into_one_batch_preserving_order() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let schema = declared();
        let mut pending: Vec<RecordBatch> = (0..3)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![i])),
                        Arc::new(StringArray::from(vec![format!("r{i}")])),
                    ],
                )
                .unwrap()
            })
            .collect();
        flush_pending(&tx, &schema, &mut pending).await.unwrap();
        assert!(pending.is_empty(), "flush must drain the buffer");

        let got = rx.recv().await.expect("one batch").expect("ok");
        assert_eq!(got.num_rows(), 3, "three messages must arrive as three rows");
        let ids = got.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[0i64, 1, 2], "order must be preserved");
    }

    /// A dropped receiver is how a cancelled query reaches the consumer loop;
    /// the flush must report it so polling stops instead of spinning forever.
    #[tokio::test]
    async fn flushing_to_a_dropped_receiver_reports_the_cancellation() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        drop(rx);
        let schema = declared();
        let mut pending = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec!["a"])),
                ],
            )
            .unwrap(),
        ];
        assert!(
            flush_pending(&tx, &schema, &mut pending).await.is_err(),
            "a dropped receiver must surface as an error so the loop breaks"
        );
    }

    /// A profile that owns its offsets must not also have rdkafka committing
    /// behind its back: both durable profiles align commits with checkpoint
    /// barriers, so auto-commit has to be off unconditionally.
    #[test]
    fn durable_profiles_disable_auto_commit() {
        use krishiv_common::DurabilityProfile;
        for profile in [
            DurabilityProfile::SingleNodeDurable,
            DurabilityProfile::DistributedDurable,
        ] {
            assert_eq!(
                auto_commit_interval_for(profile),
                None,
                "{profile:?} commits on checkpoint barriers, not on a timer"
            );
        }
    }

    /// Dev-local is at-least-once via auto-commit — but only when the process
    /// is not in production mode, since `requires_manual_kafka_commit`
    /// consults that too. Asserting a fixed value here would make the test
    /// depend on ambient environment, so it pins the interval only in the
    /// case where auto-commit applies at all.
    #[test]
    fn dev_local_auto_commit_uses_the_documented_interval() {
        use krishiv_common::DurabilityProfile;
        if let Some(ms) = auto_commit_interval_for(DurabilityProfile::DevLocal) {
            assert_eq!(ms, STREAMING_AUTO_COMMIT_MS);
        }
    }
}
