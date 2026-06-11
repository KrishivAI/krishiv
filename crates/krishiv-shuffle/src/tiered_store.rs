use crate::{
    PartitionId, ShufflePartition, ShuffleResult, ShuffleStore, ShuffleStream,
    disk_store::LocalDiskShuffleStore, object_store::ObjectStoreShuffleStore,
};
use std::sync::Arc;

/// A hybrid tiered shuffle store:
/// 1. Low-latency P2P fetches via local disk.
/// 2. Fault-tolerant availability via a remote object-store copy.
///
/// Writes are acknowledged only after both tiers commit. That keeps retry and
/// node-loss behavior fail-closed: callers never observe a successful write
/// that exists only on ephemeral local disk.
pub struct TieredShuffleStore {
    local: Arc<LocalDiskShuffleStore>,
    remote: Arc<ObjectStoreShuffleStore>,
}

impl TieredShuffleStore {
    pub fn new(local: Arc<LocalDiskShuffleStore>, remote: Arc<ObjectStoreShuffleStore>) -> Self {
        Self { local, remote }
    }
}

impl ShuffleStore for TieredShuffleStore {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        self.local
            .register_partition_lease(id.clone(), lease_token)
            .await?;
        self.remote.register_partition_lease(id, lease_token).await
    }

    async fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        // Commit local first for fast same-node reads, then synchronously commit
        // remote before acknowledging the write. If remote fails, the caller
        // sees an error and can retry the partition.
        self.local
            .write_partition(partition.clone(), lease_token)
            .await?;
        self.remote.write_partition(partition, lease_token).await
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        // Try local disk first. Only fall through on a clean miss (Ok(None));
        // propagate real errors rather than silently falling back to remote
        // and potentially returning corrupt data to the caller.
        match self.local.read_partition(id).await? {
            Some(part) => return Ok(Some(part)),
            None => {}
        }

        // Local miss after executor restart or eviction; fall back to the
        // committed remote copy.
        tracing::info!(
            "Tiered Shuffle: Local miss for {:?}, falling back to remote object store",
            id
        );
        self.remote.read_partition(id).await
    }

    async fn stream_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShuffleStream>> {
        // Same fall-through policy as read_partition: only on clean miss.
        match self.local.stream_partition(id).await? {
            Some(stream) => return Ok(Some(stream)),
            None => {}
        }

        tracing::info!(
            "Tiered Shuffle: Local stream miss for {:?}, falling back to remote object store",
            id
        );
        self.remote.stream_partition(id).await
    }

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        // Best-effort local delete; log failures but don't let them mask the
        // remote delete result. Orphan scanner will reclaim any leftovers.
        if let Err(e) = self.local.delete_job_partitions(job_id).await {
            tracing::warn!(
                job_id,
                error = %e,
                "TieredShuffleStore: local partition delete failed; remote delete proceeding"
            );
        }
        self.remote.delete_job_partitions(job_id).await
    }
}
