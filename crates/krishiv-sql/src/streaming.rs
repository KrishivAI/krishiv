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
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Set by [`ContinuousTableInput::cancel`]. Checked before each batch is
    /// yielded, which is what makes cancellation *hard* — dropping the sender
    /// alone does not discard what is already queued.
    cancelled: Arc<AtomicBool>,
}

impl fmt::Debug for ChannelPartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl ChannelPartitionStream {
    /// Build a stream sharing `cancelled` with its [`ContinuousTableInput`].
    ///
    /// This is the only constructor on purpose. A stream built with its own
    /// fresh flag could never be cancelled by any producer, and the resulting
    /// half-wired pair would look correct at every call site.
    fn with_cancel_flag(
        schema: SchemaRef,
        receiver: mpsc::Receiver<RecordBatch>,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            schema,
            receiver: AsyncMutex::new(Some(receiver)),
            cancelled,
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

        // Stop yielding as soon as the producer cancels, even if the channel
        // still holds buffered batches. Dropping the sender is not enough: a
        // tokio mpsc receiver drains its buffer before reporting end-of-stream.
        let cancelled = Arc::clone(&self.cancelled);
        let stream = ReceiverStream::new(rx)
            .take_while(move |_| {
                let stop = cancelled.load(Ordering::Acquire);
                futures::future::ready(!stop)
            })
            .map(Ok::<RecordBatch, DataFusionError>);
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Schema-bound producer handle for one continuous SQL table.
pub struct ContinuousTableInput {
    schema: SchemaRef,
    sender: StdMutex<Option<mpsc::Sender<RecordBatch>>>,
    /// Shared with the [`ChannelPartitionStream`] this input feeds, so
    /// [`Self::cancel`] can stop the consumer rather than only closing the
    /// producer.
    cancelled: Arc<AtomicBool>,
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
    /// Build an input sharing `cancelled` with the stream it feeds. Sole
    /// constructor for the same reason as [`ChannelPartitionStream`]'s.
    fn with_cancel_flag(
        schema: SchemaRef,
        sender: mpsc::Sender<RecordBatch>,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            schema,
            sender: StdMutex::new(Some(sender)),
            cancelled,
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

    /// A-8: hard-cancel the stream. Queued batches are discarded and the
    /// consumer sees an immediate end-of-stream without flushing. Idempotent.
    ///
    /// Use [`Self::close`] instead to end the stream *after* what is already
    /// queued has been delivered.
    ///
    /// Dropping the sender is not sufficient on its own, which is what this
    /// used to do: a tokio mpsc receiver drains everything still buffered
    /// before it reports `None`, so a "cancelled" stream went on delivering
    /// every queued row and `cancel` was indistinguishable from `close`. The
    /// flag is what the consumer actually stops on.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
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
    // One flag shared by both ends: the producer sets it, the consumer stops on it.
    let cancelled = Arc::new(AtomicBool::new(false));
    let partition = Arc::new(ChannelPartitionStream::with_cancel_flag(
        schema.clone(),
        rx,
        Arc::clone(&cancelled),
    ));
    let table = StreamingTable::try_new(schema.clone(), vec![partition])?;
    Ok((
        Arc::new(table),
        Arc::new(ContinuousTableInput::with_cancel_flag(schema, tx, cancelled)),
    ))
}

#[cfg(test)]
mod cancel_semantics_tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    /// Build a producer/consumer pair sharing one cancellation flag, exactly as
    /// `create_continuous_table_with_capacity` does.
    fn linked_pair(capacity: usize) -> (SchemaRef, ChannelPartitionStream, ContinuousTableInput) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let (tx, rx) = mpsc::channel(capacity);
        let cancelled = Arc::new(AtomicBool::new(false));
        (
            Arc::clone(&schema),
            ChannelPartitionStream::with_cancel_flag(
                Arc::clone(&schema),
                rx,
                Arc::clone(&cancelled),
            ),
            ContinuousTableInput::with_cancel_flag(schema, tx, cancelled),
        )
    }

    fn queue(input: &ContinuousTableInput, schema: &SchemaRef, values: [i32; 3]) {
        for value in values {
            let batch = RecordBatch::try_new(
                Arc::clone(schema),
                vec![Arc::new(Int32Array::from(vec![value]))],
            )
            .unwrap();
            input.try_send(batch).expect("queue has capacity");
        }
    }

    async fn drain(partition: ChannelPartitionStream) -> usize {
        let mut stream = partition.execute(Arc::new(TaskContext::default()));
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.expect("no error expected").num_rows();
        }
        rows
    }

    /// `cancel()` must discard what is already queued.
    ///
    /// It used to only drop the sender, and a tokio mpsc receiver drains its
    /// buffer before reporting end-of-stream — so every queued row was still
    /// delivered and `cancel` was indistinguishable from `close`.
    #[tokio::test]
    async fn cancel_discards_already_queued_batches() {
        let (schema, partition, input) = linked_pair(8);
        queue(&input, &schema, [1, 2, 3]);

        input.cancel();

        assert_eq!(
            drain(partition).await,
            0,
            "a hard cancel must not deliver queued rows"
        );
    }

    /// ...and `close()` must still flush them, or the graceful path is gone.
    #[tokio::test]
    async fn close_still_delivers_already_queued_batches() {
        let (schema, partition, input) = linked_pair(8);
        queue(&input, &schema, [1, 2, 3]);

        input.close().expect("close should succeed");

        assert_eq!(
            drain(partition).await,
            3,
            "close() ends the stream after queued data, unlike cancel()"
        );
    }
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
