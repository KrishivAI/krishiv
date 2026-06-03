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
use tokio_stream::wrappers::UnboundedReceiverStream;

use core::fmt;

/// A partition stream that reads from an MPSC channel.
pub struct ChannelPartitionStream {
    schema: SchemaRef,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<RecordBatch>>>,
}

impl fmt::Debug for ChannelPartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl ChannelPartitionStream {
    pub fn new(schema: SchemaRef, receiver: mpsc::UnboundedReceiver<RecordBatch>) -> Self {
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

        let stream = UnboundedReceiverStream::new(rx).map(Ok::<RecordBatch, DataFusionError>);
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Creates a new unbounded table provider and returns it along with the sender half of the channel.
pub fn create_continuous_table(
    schema: SchemaRef,
) -> datafusion::error::Result<(Arc<dyn TableProvider>, mpsc::UnboundedSender<RecordBatch>)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let partition = Arc::new(ChannelPartitionStream::new(schema.clone(), rx));
    let table = StreamingTable::try_new(schema, vec![partition])?;
    Ok((Arc::new(table), tx))
}
