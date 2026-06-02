use crate::{
    PartitionId, ShuffleResult, ShufflePartition, ShuffleStream, ShuffleStore,
    disk_store::LocalDiskShuffleStore, object_store::ObjectStoreShuffleStore,
};
use std::sync::Arc;

/// A hybrid tiered shuffle store that provides the best of both worlds:
/// 1. Low-latency P2P fetches via the fast local disk.
/// 2. Fault-tolerant durability via asynchronous S3/Object Store backups.
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
        self.local.register_partition_lease(id.clone(), lease_token).await?;
        self.remote.register_partition_lease(id, lease_token).await
    }

    async fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        // 1. Write to local disk synchronously (for blazing fast P2P access)
        self.local.write_partition(partition.clone(), lease_token).await?;

        // 2. Asynchronously stream to S3 in the background to prevent blocking the executor.
        // This solves the S3 latency bottleneck while ensuring K8s Spot Instance resilience.
        let remote = self.remote.clone();
        let part_clone = partition.clone();
        tokio::spawn(async move {
            if let Err(e) = remote.write_partition(part_clone, lease_token).await {
                tracing::warn!("Tiered Shuffle: Background S3 upload failed for partition {:?}: {}", partition.id, e);
            }
        });

        Ok(())
    }

    async fn read_partition(
        &self,
        id: &PartitionId,
    ) -> ShuffleResult<Option<ShufflePartition>> {
        // First try the ultra-fast local disk
        if let Ok(Some(part)) = self.local.read_partition(id).await {
            return Ok(Some(part));
        }
        
        // If the pod was restarted or data was evicted, fallback to the indestructible S3 object store
        tracing::info!("Tiered Shuffle: Local miss for {:?}, falling back to S3 Object Store", id);
        self.remote.read_partition(id).await
    }

    async fn stream_partition(
        &self,
        id: &PartitionId,
    ) -> ShuffleResult<Option<ShuffleStream>> {
        // First try the ultra-fast local disk
        if let Ok(Some(stream)) = self.local.stream_partition(id).await {
            return Ok(Some(stream));
        }
        
        tracing::info!("Tiered Shuffle: Local stream miss for {:?}, falling back to S3 Object Store", id);
        self.remote.stream_partition(id).await
    }

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        let _ = self.local.delete_job_partitions(job_id).await;
        self.remote.delete_job_partitions(job_id).await
    }
}
