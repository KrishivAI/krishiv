use crate::{
    LocalDiskShuffleStore, PartitionId, ShuffleError, ShufflePartition, ShuffleResult,
    ShuffleStore,
    compression::partition_memory_bytes,
    error::{shuffle_read_lock, shuffle_write_lock},
    store::{LeaseMap, PartitionKey},
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, RwLock};

/// An in-memory shuffle store backed by a `BTreeMap` under an `RwLock`.
///
/// Used for testing and single-node deployments where shuffle data does
/// not need to survive process restarts. When configured with
/// [`Self::with_max_bytes`] and [`Self::with_spill_store`], partitions are
/// spilled to a [`LocalDiskShuffleStore`] once the in-memory byte cap is exceeded.
pub struct InMemoryShuffleStore {
    // key: (job_id, stage_id, partition) → latest accepted partition
    partitions: Arc<RwLock<BTreeMap<PartitionKey, ShufflePartition>>>,
    // key: (job_id, stage_id, partition) → current assignment lease token
    lease_tokens: LeaseMap,
    max_bytes: Option<usize>,
    bytes_used: Arc<RwLock<usize>>,
    spill_store: Option<Arc<LocalDiskShuffleStore>>,
    spill_order: Arc<RwLock<VecDeque<PartitionKey>>>,
    spilled: Arc<RwLock<BTreeSet<PartitionKey>>>,
}

impl Default for InMemoryShuffleStore {
    fn default() -> Self {
        Self {
            partitions: Arc::new(RwLock::new(BTreeMap::new())),
            lease_tokens: Arc::new(RwLock::new(BTreeMap::new())),
            max_bytes: None,
            bytes_used: Arc::new(RwLock::new(0)),
            spill_store: None,
            spill_order: Arc::new(RwLock::new(VecDeque::new())),
            spilled: Arc::new(RwLock::new(BTreeSet::new())),
        }
    }
}

impl InMemoryShuffleStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the in-memory byte cap. When exceeded, oldest partitions spill to disk.
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    /// Attach a disk store used to spill partitions evicted from memory.
    #[must_use]
    pub fn with_spill_store(mut self, spill_store: Arc<LocalDiskShuffleStore>) -> Self {
        self.spill_store = Some(spill_store);
        self
    }

    async fn ensure_memory_capacity(
        &self,
        incoming_key: &PartitionKey,
        incoming_size: usize,
    ) -> ShuffleResult<()> {
        let Some(max_bytes) = self.max_bytes else {
            return Ok(());
        };
        let Some(spill) = self.spill_store.as_ref() else {
            return Ok(());
        };

        loop {
            let projected = {
                let used = shuffle_read_lock(&self.bytes_used)?;
                used.saturating_add(incoming_size)
            };
            if projected <= max_bytes {
                return Ok(());
            }

            let key_to_spill = {
                let order = shuffle_read_lock(&self.spill_order)?;
                let parts = shuffle_read_lock(&self.partitions)?;
                order
                    .iter()
                    .find(|k| **k != *incoming_key && parts.contains_key(*k))
                    .cloned()
            };
            let Some(key_to_spill) = key_to_spill else {
                return Ok(());
            };

            let (spill_partition, spill_size, spill_token) = {
                let mut parts = shuffle_write_lock(&self.partitions)?;
                let Some(partition) = parts.remove(&key_to_spill) else {
                    continue;
                };
                let spill_size = partition_memory_bytes(&partition);
                let spill_token = shuffle_read_lock(&self.lease_tokens)?
                    .get(&key_to_spill)
                    .copied()
                    .unwrap_or(0);
                (partition, spill_size, spill_token)
            };

            {
                let mut used = shuffle_write_lock(&self.bytes_used)?;
                *used = used.saturating_sub(spill_size);
            }

            spill.write_partition(spill_partition, spill_token).await?;
            shuffle_write_lock(&self.spilled)?.insert(key_to_spill);
        }
    }
}

impl ShuffleStore for InMemoryShuffleStore {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        let key = (id.job_id, id.stage_id, id.partition);
        let mut leases = shuffle_write_lock(&self.lease_tokens)?;
        if let Some(&expected) = leases.get(&key)
            && lease_token != expected
        {
            return Err(ShuffleError::StaleLeaseToken {
                expected,
                actual: lease_token,
            });
        }
        leases.insert(key, lease_token);
        Ok(())
    }

    async fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        let key = (
            partition.id.job_id.clone(),
            partition.id.stage_id.clone(),
            partition.id.partition,
        );
        {
            let mut leases = shuffle_write_lock(&self.lease_tokens)?;
            if let Some(&expected) = leases.get(&key) {
                // P1.25: use `<` (monotonic-token semantics) — reject stale writes,
                // accept equal or newer tokens.
                if lease_token < expected {
                    return Err(ShuffleError::StaleLeaseToken {
                        expected,
                        actual: lease_token,
                    });
                }
            } else {
                // Compatibility path for direct single-attempt writes: the first
                // writer establishes the expected token for this partition.
                leases.insert(key.clone(), lease_token);
            }
        }

        let new_size = partition_memory_bytes(&partition);
        {
            let mut used = shuffle_write_lock(&self.bytes_used)?;
            if let Some(old) = shuffle_read_lock(&self.partitions)?.get(&key) {
                *used = used.saturating_sub(partition_memory_bytes(old));
            }
        }
        shuffle_write_lock(&self.spilled)?.remove(&key);
        self.ensure_memory_capacity(&key, new_size).await?;

        {
            let mut parts = shuffle_write_lock(&self.partitions)?;
            parts.insert(key.clone(), partition);
            let mut order = shuffle_write_lock(&self.spill_order)?;
            order.retain(|existing| existing != &key);
            order.push_back(key);
            let mut used = shuffle_write_lock(&self.bytes_used)?;
            *used += new_size;
        }
        Ok(())
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);
        if shuffle_read_lock(&self.spilled)?.contains(&key)
            && let Some(spill) = &self.spill_store
        {
            return spill.read_partition(id).await;
        }
        let guard = shuffle_read_lock(&self.partitions)?;
        Ok(guard.get(&key).cloned())
    }

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        shuffle_write_lock(&self.partitions)?.retain(|(jid, _, _), _| jid != job_id);
        shuffle_write_lock(&self.lease_tokens)?.retain(|(jid, _, _), _| jid != job_id);
        shuffle_write_lock(&self.spilled)?.retain(|(jid, _, _)| jid != job_id);
        shuffle_write_lock(&self.spill_order)?.retain(|(jid, _, _)| jid != job_id);
        if let Some(spill) = &self.spill_store {
            spill.delete_job_partitions(job_id).await?;
        }
        let mut total = 0usize;
        for partition in shuffle_read_lock(&self.partitions)?.values() {
            total += partition_memory_bytes(partition);
        }
        *shuffle_write_lock(&self.bytes_used)? = total;
        Ok(())
    }
}
