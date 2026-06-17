use crate::ShuffleResult;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::future::Future;

/// Identifies a shuffle partition uniquely within a job.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId {
    pub job_id: String,
    pub stage_id: String,
    pub partition: u32,
}

/// A single shuffle partition: schema + ordered record batches.
#[derive(Debug, Clone)]
pub struct ShufflePartition {
    pub id: PartitionId,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// A streaming shuffle partition.
pub struct ShuffleStream {
    pub id: PartitionId,
    pub schema: SchemaRef,
    pub batches: futures::stream::BoxStream<'static, ShuffleResult<RecordBatch>>,
}

/// An async shuffle store that persists inter-stage partition data.
///
/// Implementations must be `Send + Sync` so they can be shared across async
/// task boundaries inside the executor runtime.
pub trait ShuffleStore: Send + Sync {
    /// Register the currently valid assignment lease token for a partition.
    ///
    /// Executors should call this when a task assignment is launched so a
    /// zombie attempt cannot win a race by writing before the replacement
    /// attempt commits data. Subsequent writes for the partition must present
    /// exactly this token until a newer assignment registers a replacement.
    fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> impl Future<Output = ShuffleResult<()>> + Send;

    /// Write a partition. `lease_token` must match the current assignment
    /// token for this partition; stale tokens are rejected.
    fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> impl Future<Output = ShuffleResult<()>> + Send;

    /// Read a partition. Returns `None` if not yet written.
    fn read_partition(
        &self,
        id: &PartitionId,
    ) -> impl Future<Output = ShuffleResult<Option<ShufflePartition>>> + Send;

    /// Stream a partition. Default implementation buffers via read_partition.
    fn stream_partition(
        &self,
        id: &PartitionId,
    ) -> impl Future<Output = ShuffleResult<Option<ShuffleStream>>> + Send {
        let id = id.clone();
        async move {
            if let Some(partition) = self.read_partition(&id).await? {
                Ok(Some(ShuffleStream {
                    id: partition.id,
                    schema: partition.schema,
                    batches: Box::pin(futures::stream::iter(partition.batches.into_iter().map(Ok))),
                }))
            } else {
                Ok(None)
            }
        }
    }

    /// Delete all partitions for a job (called on job completion or cancellation).
    fn delete_job_partitions(&self, job_id: &str)
    -> impl Future<Output = ShuffleResult<()>> + Send;
}

/// Compound key used for lease maps: `(job_id, stage_id, partition_index)`.
pub type PartitionKey = (String, String, u32);

/// Shared lease-token map type used by both in-memory and disk-backed stores.
pub type LeaseMap = std::sync::Arc<std::sync::RwLock<ahash::AHashMap<PartitionKey, u64>>>;

#[derive(Clone)]
pub enum ShuffleBackend {
    Local(std::sync::Arc<crate::LocalDiskShuffleStore>),
    InMemory(std::sync::Arc<crate::InMemoryShuffleStore>),
    Tiered(std::sync::Arc<crate::tiered_store::TieredShuffleStore>),
    Object(std::sync::Arc<crate::ObjectStoreShuffleStore>),
}

impl ShuffleStore for ShuffleBackend {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        match self {
            Self::Local(s) => s.register_partition_lease(id, lease_token).await,
            Self::InMemory(s) => s.register_partition_lease(id, lease_token).await,
            Self::Tiered(s) => s.register_partition_lease(id, lease_token).await,
            Self::Object(s) => s.register_partition_lease(id, lease_token).await,
        }
    }

    async fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        match self {
            Self::Local(s) => s.write_partition(partition, lease_token).await,
            Self::InMemory(s) => s.write_partition(partition, lease_token).await,
            Self::Tiered(s) => s.write_partition(partition, lease_token).await,
            Self::Object(s) => s.write_partition(partition, lease_token).await,
        }
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        match self {
            Self::Local(s) => s.read_partition(id).await,
            Self::InMemory(s) => s.read_partition(id).await,
            Self::Tiered(s) => s.read_partition(id).await,
            Self::Object(s) => s.read_partition(id).await,
        }
    }

    async fn stream_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShuffleStream>> {
        match self {
            Self::Local(s) => s.stream_partition(id).await,
            Self::InMemory(s) => s.stream_partition(id).await,
            Self::Tiered(s) => s.stream_partition(id).await,
            Self::Object(s) => s.stream_partition(id).await,
        }
    }

    fn delete_job_partitions(
        &self,
        job_id: &str,
    ) -> impl Future<Output = ShuffleResult<()>> + Send {
        let job_id = job_id.to_string();
        async move {
            match self {
                Self::Local(s) => s.delete_job_partitions(&job_id).await,
                Self::InMemory(s) => s.delete_job_partitions(&job_id).await,
                Self::Tiered(s) => s.delete_job_partitions(&job_id).await,
                Self::Object(s) => s.delete_job_partitions(&job_id).await,
            }
        }
    }
}
