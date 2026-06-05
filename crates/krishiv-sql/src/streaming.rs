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
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use core::fmt;

/// Default per-continuous-table channel capacity. Bounds the in-memory
/// queue between a producer and the DataFusion consumer: a slow consumer
/// (e.g. an expensive join downstream) cannot cause an unbounded producer
/// to grow memory without limit. 64 batches × ~1k rows/batch ≈ 64k rows
/// of inflight buffering, which is enough to absorb short stalls without
/// imposing visible backpressure on typical CDC / streaming-SQL workloads.
pub const CONTINUOUS_TABLE_CHANNEL_CAPACITY: usize = 64;

/// A partition stream that reads from an MPSC channel.
pub struct ChannelPartitionStream {
    schema: SchemaRef,
    receiver: Mutex<Option<mpsc::Receiver<RecordBatch>>>,
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
            receiver: Mutex::new(Some(receiver)),
        }
    }
}

impl PartitionStream for ChannelPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut rx_guard = self
            .receiver
            .try_lock()
            .expect("Partition executed multiple times or concurrently");
        let rx = rx_guard.take().expect("Partition stream already consumed");

        let stream = ReceiverStream::new(rx).map(Ok::<RecordBatch, DataFusionError>);
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Creates a new continuous-table provider and returns it along with the
/// sender half of the channel. The channel is bounded (capacity
/// `CONTINUOUS_TABLE_CHANNEL_CAPACITY`) so a slow DataFusion consumer
/// applies backpressure to the producer via `Sender::send(...).await`
/// blocking, or `Sender::try_send(...)` returning `TrySendError::Full`
/// if the caller prefers drop-on-full semantics.
pub fn create_continuous_table(
    schema: SchemaRef,
) -> datafusion::error::Result<(Arc<dyn TableProvider>, mpsc::Sender<RecordBatch>)> {
    create_continuous_table_with_capacity(schema, CONTINUOUS_TABLE_CHANNEL_CAPACITY)
}

/// Same as [`create_continuous_table`] but with a caller-supplied
/// capacity. Useful for tests that want to exercise the full/empty
/// channel boundary without needing to push 64 batches.
pub fn create_continuous_table_with_capacity(
    schema: SchemaRef,
    capacity: usize,
) -> datafusion::error::Result<(Arc<dyn TableProvider>, mpsc::Sender<RecordBatch>)> {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    let partition = Arc::new(ChannelPartitionStream::new(schema.clone(), rx));
    let table = StreamingTable::try_new(schema, vec![partition])?;
    Ok((Arc::new(table), tx))
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
            matches!(third, Err(mpsc::error::TrySendError::Full(_))),
            "expected Full, got {third:?}"
        );
        drop(table);
    }
}
