use crate::{
    LocalDiskShuffleStore, PartitionId, ShuffleError, ShufflePartition, ShuffleResult,
    ShuffleStore,
    compression::partition_memory_bytes,
    error::{shuffle_read_lock, shuffle_write_lock},
    store::{LeaseMap, PartitionKey},
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

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
    spill_lock: Arc<Mutex<()>>,
    // Content hashes for partition determinism verification
    content_hashes: Arc<RwLock<BTreeMap<PartitionKey, [u8; 32]>>>,
}

/// Simple stable hash for partition determinism verification.
fn compute_simple_partition_hash(partition: &ShufflePartition) -> [u8; 32] {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    partition.id.job_id.hash(&mut hasher);
    partition.id.stage_id.hash(&mut hasher);
    partition.id.partition.hash(&mut hasher);
    partition.batches.len().hash(&mut hasher);

    let h = hasher.finish();
    let mut out = [0u8; 32];
    out[0..8].copy_from_slice(&h.to_be_bytes());
    out
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
            spill_lock: Arc::new(Mutex::new(())),
            content_hashes: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

/// Default in-memory shuffle capacity (128 MiB).
pub const DEFAULT_SHUFFLE_MEMORY_BYTES: usize = 128 * 1024 * 1024;

impl InMemoryShuffleStore {
    pub fn new() -> Self {
        let max_bytes = std::env::var("KRISHIV_SHUFFLE_MEMORY_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SHUFFLE_MEMORY_BYTES);
        Self {
            partitions: Arc::new(RwLock::new(BTreeMap::new())),
            lease_tokens: Arc::new(RwLock::new(BTreeMap::new())),
            max_bytes: Some(max_bytes),
            bytes_used: Arc::new(RwLock::new(0)),
            spill_store: None,
            spill_order: Arc::new(RwLock::new(VecDeque::new())),
            spilled: Arc::new(RwLock::new(BTreeSet::new())),
            spill_lock: Arc::new(Mutex::new(())),
            content_hashes: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn new_unbounded() -> Self {
        Self {
            partitions: Arc::new(RwLock::new(BTreeMap::new())),
            lease_tokens: Arc::new(RwLock::new(BTreeMap::new())),
            max_bytes: None,
            bytes_used: Arc::new(RwLock::new(0)),
            spill_store: None,
            spill_order: Arc::new(RwLock::new(VecDeque::new())),
            spilled: Arc::new(RwLock::new(BTreeSet::new())),
            spill_lock: Arc::new(Mutex::new(())),
            content_hashes: Arc::new(RwLock::new(BTreeMap::new())),
        }
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

    async fn ensure_memory_capacity_locked(
        &self,
        incoming_key: &PartitionKey,
        incoming_size: usize,
        old_size: usize,
    ) -> ShuffleResult<()> {
        let Some(max_bytes) = self.max_bytes else {
            return Ok(());
        };

        loop {
            let current_bytes = {
                let used = shuffle_read_lock(&self.bytes_used)?;
                used.saturating_sub(old_size)
            };
            let projected = current_bytes.saturating_add(incoming_size);
            if projected <= max_bytes {
                return Ok(());
            }

            let Some(spill) = self.spill_store.as_ref() else {
                return Err(ShuffleError::MemoryLimitExceeded {
                    max_bytes,
                    current_bytes,
                    incoming_bytes: incoming_size,
                });
            };

            let key_to_spill = {
                let order = shuffle_read_lock(&self.spill_order)?;
                let parts = shuffle_read_lock(&self.partitions)?;
                order
                    .iter()
                    .find(|k| **k != *incoming_key && parts.contains_key(*k))
                    .cloned()
            };
            let Some(key_to_spill) = key_to_spill else {
                return Err(ShuffleError::MemoryLimitExceeded {
                    max_bytes,
                    current_bytes,
                    incoming_bytes: incoming_size,
                });
            };

            // Read partition data under lock (clone is cheap — Arc bumps).
            // Do NOT remove from partitions yet; only remove after the spill
            // write succeeds, so a spill failure doesn't lose data.
            let (spill_partition, spill_size, spill_token, spill_hash) = {
                let parts = shuffle_read_lock(&self.partitions)?;
                let Some(partition) = parts.get(&key_to_spill).cloned() else {
                    continue;
                };
                let spill_size = partition_memory_bytes(&partition);
                let spill_token = shuffle_read_lock(&self.lease_tokens)?
                    .get(&key_to_spill)
                    .copied()
                    .unwrap_or(0);
                let spill_hash = compute_simple_partition_hash(&partition);
                (partition, spill_size, spill_token, spill_hash)
            };

            // Spill to disk. If this fails, the partition stays in memory
            // and no data is lost — the caller can retry.
            spill.write_partition(spill_partition, spill_token).await?;

            // Spill succeeded — now safely remove from memory and account.
            // Acquire all three locks in the same scope to prevent a window
            // where the partition is absent from both partitions and spilled
            // (which would cause read_partition to return None).
            {
                let mut parts = shuffle_write_lock(&self.partitions)?;
                let mut spilled = shuffle_write_lock(&self.spilled)?;
                let mut used = shuffle_write_lock(&self.bytes_used)?;
                let mut hashes = shuffle_write_lock(&self.content_hashes)?;
                let current_token = shuffle_read_lock(&self.lease_tokens)?
                    .get(&key_to_spill)
                    .copied()
                    .unwrap_or(0);
                if current_token == spill_token
                    && let Some(current_part) = parts.get(&key_to_spill)
                {
                    let current_hash = compute_simple_partition_hash(current_part);
                    if current_hash == spill_hash {
                        parts.remove(&key_to_spill);
                        hashes.remove(&key_to_spill);
                        spilled.insert(key_to_spill);
                        *used = used.saturating_sub(spill_size);
                    } else {
                        tracing::info!(
                            "spill-to-disk cleanup skipped for {:?}: partition was modified during write",
                            key_to_spill
                        );
                    }
                }
            }
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
            && lease_token < expected
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

        let computed_hash = compute_simple_partition_hash(&partition);
        let new_size = partition_memory_bytes(&partition);

        {
            let leases = shuffle_read_lock(&self.lease_tokens)?;
            if let Some(&expected) = leases.get(&key) {
                if lease_token < expected {
                    return Err(ShuffleError::StaleLeaseToken {
                        expected,
                        actual: lease_token,
                    });
                }
            }
        }

        let _write_guard = self.spill_lock.lock().await;
        let old_size = {
            let parts = shuffle_read_lock(&self.partitions)?;
            parts.get(&key).map(partition_memory_bytes).unwrap_or(0)
        };

        if let (Some(max_bytes), Some(spill)) = (self.max_bytes, self.spill_store.as_ref())
            && new_size > max_bytes
        {
            spill.write_partition(partition, lease_token).await?;

            {
                let mut leases = shuffle_write_lock(&self.lease_tokens)?;
                if let Some(&expected) = leases.get(&key)
                    && lease_token < expected
                {
                    return Err(ShuffleError::StaleLeaseToken {
                        expected,
                        actual: lease_token,
                    });
                }
                leases.insert(key.clone(), lease_token);
            }

            {
                let mut parts = shuffle_write_lock(&self.partitions)?;
                let removed_size = parts
                    .remove(&key)
                    .map(|p| partition_memory_bytes(&p))
                    .unwrap_or(0);
                let mut order = shuffle_write_lock(&self.spill_order)?;
                order.retain(|existing| existing != &key);
                let mut spilled = shuffle_write_lock(&self.spilled)?;
                spilled.insert(key.clone());
                let mut used = shuffle_write_lock(&self.bytes_used)?;
                *used = used.saturating_sub(removed_size);
                let mut hashes = shuffle_write_lock(&self.content_hashes)?;
                hashes.remove(&key);
            }
            return Ok(());
        }

        // Ensure capacity BEFORE mutating accounting state. If the spill
        // fails, no state has changed yet and we can safely retry.
        self.ensure_memory_capacity_locked(&key, new_size, old_size)
            .await?;

        {
            let mut leases = shuffle_write_lock(&self.lease_tokens)?;
            if let Some(&expected) = leases.get(&key)
                && lease_token < expected
            {
                return Err(ShuffleError::StaleLeaseToken {
                    expected,
                    actual: lease_token,
                });
            }
            leases.insert(key.clone(), lease_token);
        }

        // Update partitions and bytes_used atomically in a single lock scope.
        // This avoids the tear where bytes_used undercounts between the old-size
        // subtraction and new-size addition, which would cause ensure_memory_capacity
        // to incorrectly skip needed spills.
        {
            let committed_key = key.clone();
            let mut parts = shuffle_write_lock(&self.partitions)?;
            let old_size = parts.get(&key).map(partition_memory_bytes).unwrap_or(0);
            parts.insert(key.clone(), partition);
            let mut order = shuffle_write_lock(&self.spill_order)?;
            order.retain(|existing| existing != &key);
            order.push_back(committed_key.clone());
            let mut spilled = shuffle_write_lock(&self.spilled)?;
            spilled.remove(&committed_key);
            let mut used = shuffle_write_lock(&self.bytes_used)?;
            *used = used.saturating_sub(old_size).saturating_add(new_size);
            let mut hashes = shuffle_write_lock(&self.content_hashes)?;
            hashes.insert(committed_key, computed_hash);
        }
        Ok(())
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);

        let (from_spill, data) = {
            let parts_guard = shuffle_read_lock(&self.partitions)?;
            let spilled_guard = shuffle_read_lock(&self.spilled)?;
            let hashes_guard = shuffle_read_lock(&self.content_hashes)?;

            if spilled_guard.contains(&key) {
                (true, None)
            } else if let Some(partition) = parts_guard.get(&key) {
                if let Some(&stored_hash) = hashes_guard.get(&key) {
                    let computed = compute_simple_partition_hash(partition);
                    if computed != stored_hash {
                        return Err(ShuffleError::ContentHashMismatch {
                            partition: format!("{:?}", key),
                            expected: format!("{:02x?}", stored_hash),
                            actual: format!("{:02x?}", computed),
                        });
                    }
                }
                (false, Some(partition.clone()))
            } else {
                (false, None)
            }
        };

        if from_spill {
            match &self.spill_store {
                Some(spill) => spill.read_partition(id).await,
                None => Ok(None),
            }
        } else {
            Ok(data)
        }
    }

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        // Acquire locks in the same order as write_partition: lease_tokens → partitions → spilled → spill_order.
        shuffle_write_lock(&self.lease_tokens)?.retain(|(jid, _, _), _| jid != job_id);
        shuffle_write_lock(&self.partitions)?.retain(|(jid, _, _), _| jid != job_id);
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
