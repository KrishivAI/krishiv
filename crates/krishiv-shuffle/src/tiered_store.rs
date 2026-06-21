use crate::{
    PartitionId, ShuffleError, ShufflePartition, ShuffleResult, ShuffleStore, ShuffleStream,
    disk_store::LocalDiskShuffleStore, object_store::ObjectStoreShuffleStore,
};
use std::sync::Arc;

fn is_corruption_error(e: &ShuffleError) -> bool {
    matches!(e, ShuffleError::ContentHashMismatch { .. })
}

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

#[async_trait::async_trait]
impl ShuffleStore for TieredShuffleStore {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        tokio::try_join!(
            self.local.register_partition_lease(id.clone(), lease_token),
            self.remote.register_partition_lease(id, lease_token),
        )?;
        Ok(())
    }

    async fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        // Fail-closed: the write is acknowledged only after BOTH tiers commit.
        // `tokio::join!` drives both futures to completion without cancelling
        // either, so a failure on one side never abandons the other's write
        // mid-flight — the surviving copy stays durable for retry/cleanup, and
        // the caller never observes success unless both tiers committed.
        let (local_result, remote_result) = tokio::join!(
            self.local.write_partition(partition.clone(), lease_token),
            self.remote.write_partition(partition, lease_token),
        );
        // Surface a local failure first (low-latency primary tier); otherwise
        // propagate any remote failure.
        local_result?;
        remote_result
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        // Try local disk first. On a clean miss (Ok(None)), fall through to
        // remote. On a corruption-class error (ContentHashMismatch), also fall
        // back to remote — the remote tier performs its own independent BLAKE3
        // verification, so falling back is safe and preserves availability.
        // Non-corruption errors (auth, permission, I/O) are propagated.
        match self.local.read_partition(id).await {
            Ok(Some(part)) => return Ok(Some(part)),
            Ok(None) => {}
            Err(e) if is_corruption_error(&e) => {
                tracing::warn!(
                    partition = ?id,
                    error = %e,
                    "Tiered Shuffle: local partition corrupt, falling back to remote object store"
                );
            }
            Err(e) => return Err(e),
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
        // Same fall-through policy as read_partition: clean miss or corruption.
        match self.local.stream_partition(id).await {
            Ok(Some(stream)) => return Ok(Some(stream)),
            Ok(None) => {}
            Err(e) if is_corruption_error(&e) => {
                tracing::warn!(
                    partition = ?id,
                    error = %e,
                    "Tiered Shuffle: local stream corrupt, falling back to remote object store"
                );
            }
            Err(e) => return Err(e),
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
