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

    /// Delete all partitions for a job (called on job completion or cancellation).
    fn delete_job_partitions(&self, job_id: &str)
    -> impl Future<Output = ShuffleResult<()>> + Send;
}

/// Compound key used for lease maps: `(job_id, stage_id, partition_index)`.
pub type PartitionKey = (String, String, u32);

/// Shared lease-token map type used by both in-memory and disk-backed stores.
pub type LeaseMap =
    std::sync::Arc<std::sync::RwLock<std::collections::BTreeMap<PartitionKey, u64>>>;
