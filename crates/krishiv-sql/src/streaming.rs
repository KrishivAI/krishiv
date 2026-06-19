use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use core::fmt;

/// Default per-continuous-table channel capacity. Bounds the in-memory
/// queue between a producer and the DataFusion consumer: a slow consumer
/// (e.g. an expensive join downstream) cannot cause an unbounded producer
/// to grow memory without limit. 64 batches × ~1k rows/batch ≈ 64k rows
/// of inflight buffering, which is enough to absorb short stalls without
/// imposing visible backpressure on typical CDC / streaming-SQL workloads.
pub const CONTINUOUS_TABLE_CHANNEL_CAPACITY: usize = 64;

/// Errors returned by a continuous table producer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ContinuousInputError {
    /// Submitted batch schema does not match the registered table schema.
    #[error("continuous table batch schema mismatch: expected {expected}, got {actual}")]
    SchemaMismatch { expected: String, actual: String },
    /// The bounded producer queue has no remaining capacity.
    #[error("continuous table input queue is full")]
    QueueFull,
    /// The producer was explicitly closed or its consumer was dropped.
    #[error("continuous table input is closed")]
    Closed,
    /// Internal producer state was poisoned by a panic while locked.
    #[error("continuous table input lock is poisoned: {0}")]
    LockPoisoned(String),
}

/// A partition stream that reads from an MPSC channel.
pub struct ChannelPartitionStream {
    schema: SchemaRef,
    receiver: AsyncMutex<Option<mpsc::Receiver<RecordBatch>>>,
}

impl fmt::Debug for ChannelPartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl ChannelPartitionStream {
    pub fn new(schema: SchemaRef, receiver: mpsc::Receiver<RecordBatch>) -> Self {
        Self {
            schema,
            receiver: AsyncMutex::new(Some(receiver)),
        }
    }

    fn error_stream(&self, message: impl Into<String>) -> SendableRecordBatchStream {
        let message = message.into();
        let stream = futures::stream::once(async move { Err(DataFusionError::Execution(message)) });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

impl PartitionStream for ChannelPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut rx_guard = match self.receiver.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                return self.error_stream(
                    "continuous table partition is already executing in another query",
                );
            }
        };
        let Some(rx) = rx_guard.take() else {
            return self.error_stream(
                "continuous table partition has already been consumed by another query",
            );
        };

        let stream = ReceiverStream::new(rx).map(Ok::<RecordBatch, DataFusionError>);
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Schema-bound producer handle for one continuous SQL table.
pub struct ContinuousTableInput {
    schema: SchemaRef,
    sender: StdMutex<Option<mpsc::Sender<RecordBatch>>>,
}

impl fmt::Debug for ContinuousTableInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContinuousTableInput")
            .field("schema", &self.schema)
            .field("closed", &self.is_closed().ok())
            .finish()
    }
}

impl ContinuousTableInput {
    fn new(schema: SchemaRef, sender: mpsc::Sender<RecordBatch>) -> Self {
        Self {
            schema,
            sender: StdMutex::new(Some(sender)),
        }
    }

    /// Expected Arrow schema for every submitted batch.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Submit a batch without waiting for queue capacity.
    pub fn try_send(&self, batch: RecordBatch) -> Result<(), ContinuousInputError> {
        self.validate_schema(&batch)?;
        let sender = self.sender_clone()?;
        sender.try_send(batch).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => ContinuousInputError::QueueFull,
            mpsc::error::TrySendError::Closed(_) => ContinuousInputError::Closed,
        })
    }

    /// Submit a batch, asynchronously waiting for queue capacity.
    pub async fn send(&self, batch: RecordBatch) -> Result<(), ContinuousInputError> {
        self.validate_schema(&batch)?;
        self.sender_clone()?
            .send(batch)
            .await
            .map_err(|_| ContinuousInputError::Closed)
    }

    /// Close the input. The consumer observes end-of-stream after queued data.
    ///
    /// Returns `true` when this call closed an open input and `false` when it
    /// was already closed.
    pub fn close(&self) -> Result<bool, ContinuousInputError> {
        let mut sender = self
            .sender
            .lock()
            .map_err(|error| ContinuousInputError::LockPoisoned(error.to_string()))?;
        Ok(sender.take().is_some())
    }

    /// Whether the producer side has been closed.
    pub fn is_closed(&self) -> Result<bool, ContinuousInputError> {
        self.sender
            .lock()
            .map(|sender| sender.is_none())
            .map_err(|error| ContinuousInputError::LockPoisoned(error.to_string()))
    }

    fn sender_clone(&self) -> Result<mpsc::Sender<RecordBatch>, ContinuousInputError> {
        self.sender
            .lock()
            .map_err(|error| ContinuousInputError::LockPoisoned(error.to_string()))?
            .clone()
            .ok_or(ContinuousInputError::Closed)
    }

    fn validate_schema(&self, batch: &RecordBatch) -> Result<(), ContinuousInputError> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(ContinuousInputError::SchemaMismatch {
                expected: format!("{:?}", self.schema),
                actual: format!("{:?}", batch.schema()),
            });
        }
        Ok(())
    }
}

/// Creates a new continuous-table provider and its schema-bound producer.
/// The channel is bounded (capacity
/// `CONTINUOUS_TABLE_CHANNEL_CAPACITY`) so a slow DataFusion consumer
/// applies backpressure via [`ContinuousTableInput::send`], or
/// [`ContinuousTableInput::try_send`] returns a resource-exhausted error.
pub fn create_continuous_table(
    schema: SchemaRef,
) -> datafusion::error::Result<(Arc<dyn TableProvider>, Arc<ContinuousTableInput>)> {
    create_continuous_table_with_capacity(schema, CONTINUOUS_TABLE_CHANNEL_CAPACITY)
}

/// Same as [`create_continuous_table`] but with a caller-supplied
/// capacity. Useful for tests that want to exercise the full/empty
/// channel boundary without needing to push 64 batches.
pub fn create_continuous_table_with_capacity(
    schema: SchemaRef,
    capacity: usize,
) -> datafusion::error::Result<(Arc<dyn TableProvider>, Arc<ContinuousTableInput>)> {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    let partition = Arc::new(ChannelPartitionStream::new(schema.clone(), rx));
    let table = StreamingTable::try_new(schema.clone(), vec![partition])?;
    Ok((
        Arc::new(table),
        Arc::new(ContinuousTableInput::new(schema, tx)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
    }

    fn make_batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(make_schema(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[tokio::test]
    async fn create_continuous_table_with_capacity_zero_is_clamped_to_one() {
        let schema = make_schema();
        let (table, tx) = create_continuous_table_with_capacity(schema, 0).unwrap();
        // Capacity 0 is clamped to 1: a `mpsc::channel(0)` would deadlock
        // the sender before the receiver is even polled. The clamp is
        // documented in `create_continuous_table_with_capacity`.
        tx.try_send(make_batch(vec![1]))
            .expect("capacity should be >= 1");
        // The second try_send should fail with Full, not deadlock.
        assert!(tx.try_send(make_batch(vec![2])).is_err());
        drop(table);
    }

    #[tokio::test]
    async fn bounded_channel_rejects_oversized_queue_via_try_send() {
        let schema = make_schema();
        let (table, tx) = create_continuous_table_with_capacity(schema, 2).unwrap();
        // Fill to capacity (DataFusion does not pull until execute is
        // called by the query plan). try_send must return Full once full.
        assert!(tx.try_send(make_batch(vec![1])).is_ok());
        assert!(tx.try_send(make_batch(vec![2])).is_ok());
        let third = tx.try_send(make_batch(vec![3]));
        assert!(
            matches!(third, Err(ContinuousInputError::QueueFull)),
            "expected Full, got {third:?}"
        );
        drop(table);
    }

    #[tokio::test]
    async fn continuous_input_rejects_schema_mismatch_and_close_is_idempotent() {
        let (table, input) = create_continuous_table(make_schema()).unwrap();
        let wrong_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let wrong_batch = RecordBatch::try_new(
            wrong_schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();

        let error = input
            .try_send(wrong_batch)
            .expect_err("schema mismatch must fail");
        assert!(matches!(error, ContinuousInputError::SchemaMismatch { .. }));
        assert!(input.close().unwrap());
        assert!(!input.close().unwrap());
        assert!(input.is_closed().unwrap());
        assert!(matches!(
            input.try_send(make_batch(vec![1])),
            Err(ContinuousInputError::Closed)
        ));
        drop(table);
    }
}
