use crate::ShuffleResult;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

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
    pub batches: ShuffleBatchStream,
}

/// An owned stream of a single partition's batches, in write order.
///
/// Used by both the read side ([`ShuffleStream`]) and the write side
/// ([`ShuffleStore::write_partition_stream`]).
pub type ShuffleBatchStream = futures::stream::BoxStream<'static, ShuffleResult<RecordBatch>>;

/// An async shuffle store that persists inter-stage partition data.
///
/// Implementations must be `Send + Sync` so they can be shared across async
/// task boundaries inside the executor runtime.
///
/// The trait is object-safe via `async_trait` so callers can use
/// `Arc<dyn ShuffleStore + Send + Sync>`.
#[async_trait::async_trait]
pub trait ShuffleStore: Send + Sync {
    /// Register the currently valid assignment lease token for a partition.
    ///
    /// Executors should call this when a task assignment is launched so a
    /// zombie attempt cannot win a race by writing before the replacement
    /// attempt commits data. Subsequent writes for the partition must present
    /// exactly this token until a newer assignment registers a replacement.
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()>;

    /// Write a partition. `lease_token` must match the current assignment
    /// token for this partition; stale tokens are rejected.
    async fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> ShuffleResult<()>;

    /// Write a partition from a stream of batches, so the producer never needs
    /// the whole partition resident.
    ///
    /// # Why this exists
    ///
    /// [`write_partition`](Self::write_partition) takes a complete
    /// `Vec<RecordBatch>`, which made "one output partition" the smallest unit
    /// a map task could hand over. At SF100 that is hundreds of megabytes held
    /// as an *unspillable* consumer of the DataFusion pool — and because
    /// `FairSpillPool` computes availability as
    /// `pool_size - (unspillable + spillable)` in both branches, one oversized
    /// unspillable reservation saturates availability to zero for *every*
    /// consumer. TPC-H q10 and q21 both died on the resulting
    /// `Failed to allocate additional 877.0 B for HashJoinInput`: a few hundred
    /// bytes refused by a multi-gigabyte pool because a drain had already
    /// declared more than the pool holds.
    ///
    /// The default implementation collects and delegates, so a store that
    /// genuinely needs the whole partition (in-memory, tee-ing tiered writes)
    /// keeps working unchanged. Stores whose serialiser accepts batches
    /// incrementally — [`crate::LocalDiskShuffleStore`] writes through
    /// `ArrowWriter` — override it and never materialise the partition.
    async fn write_partition_stream(
        &self,
        id: PartitionId,
        schema: SchemaRef,
        batches: ShuffleBatchStream,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        use futures::StreamExt as _;
        let mut batches = batches;
        let mut collected = Vec::new();
        while let Some(batch) = batches.next().await {
            collected.push(batch?);
        }
        self.write_partition(
            ShufflePartition {
                id,
                schema,
                batches: collected,
            },
            lease_token,
        )
        .await
    }

    /// Read a partition. Returns `None` if not yet written.
    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>>;

    /// On-disk size of a partition, **without reading it**.
    ///
    /// Exists so the shuffle Flight server can admit a `do_get` by the bytes it
    /// is about to hold resident rather than by counting open responses. Those
    /// are not the same bound: a response-count cap has to assume every response
    /// is the largest one allowed, so it admits a handful of 3 MB fragments as
    /// though each were 32 MB. That over-charging is why the reduce side had to
    /// be pinned at one fetch in flight (see `DEFAULT_SHUFFLE_FETCH_BUFFER`),
    /// which serialises every reduce task's fetches end to end.
    ///
    /// `None` means "unknown" — the caller must fall back to the conservative
    /// assumption, which is exactly the old behaviour. Implementations that
    /// cannot answer cheaply should return `None` rather than read the data.
    async fn partition_bytes(&self, _id: &PartitionId) -> ShuffleResult<Option<u64>> {
        Ok(None)
    }

    /// Stream a partition. Default implementation buffers via read_partition.
    async fn stream_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShuffleStream>> {
        let id = id.clone();
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

    /// Delete all partitions for a job (called on job completion or cancellation).
    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()>;
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

#[async_trait::async_trait]
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

    async fn write_partition_stream(
        &self,
        id: PartitionId,
        schema: SchemaRef,
        batches: ShuffleBatchStream,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        // Dispatch to the concrete store so `LocalDiskShuffleStore`'s genuine
        // streaming override is reached. Forwarding to the trait default here
        // would collect the partition first and quietly undo the point of the
        // call — the exact shape of bug this audit keeps finding.
        match self {
            Self::Local(s) => {
                s.write_partition_stream(id, schema, batches, lease_token)
                    .await
            }
            Self::InMemory(s) => {
                s.write_partition_stream(id, schema, batches, lease_token)
                    .await
            }
            Self::Tiered(s) => {
                s.write_partition_stream(id, schema, batches, lease_token)
                    .await
            }
            Self::Object(s) => {
                s.write_partition_stream(id, schema, batches, lease_token)
                    .await
            }
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

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        match self {
            Self::Local(s) => s.delete_job_partitions(job_id).await,
            Self::InMemory(s) => s.delete_job_partitions(job_id).await,
            Self::Tiered(s) => s.delete_job_partitions(job_id).await,
            Self::Object(s) => s.delete_job_partitions(job_id).await,
        }
    }
}
