use crate::{
    LocalDiskShuffleStore, PartitionId, ShuffleError, ShufflePartition, ShuffleResult,
    ShuffleStore, compression::partition_memory_bytes, store::PartitionKey,
};
use ahash::{AHashMap, AHashSet};
use indexmap::IndexSet;
use std::sync::{Arc, Mutex};

/// Default in-memory shuffle capacity (128 MiB).
pub const DEFAULT_SHUFFLE_MEMORY_BYTES: usize = 128 * 1024 * 1024;

// A4: All mutable shuffle state is consolidated into a single Mutex<InMemoryState>
// so that any code path acquires exactly one lock, eliminating the deadlock risk
// that existed when 7 separate RwLocks had to be acquired in consistent order.
#[derive(Default)]
struct InMemoryState {
    partitions: AHashMap<PartitionKey, ShufflePartition>,
    lease_tokens: AHashMap<PartitionKey, u64>,
    spilled: AHashSet<PartitionKey>,
    spill_order: IndexSet<PartitionKey>,
    bytes_used: usize,
    content_hashes: AHashMap<PartitionKey, [u8; 32]>,
}

/// An in-memory shuffle store backed by a single `Mutex<InMemoryState>`.
///
/// Used for testing and single-node deployments. When configured with
/// [`Self::with_max_bytes`] and [`Self::with_spill_store`], partitions are
/// spilled to a [`LocalDiskShuffleStore`] once the in-memory byte cap is exceeded.
pub struct InMemoryShuffleStore {
    state: Mutex<InMemoryState>,
    max_bytes: Option<usize>,
    spill_store: Option<Arc<LocalDiskShuffleStore>>,
}

fn compute_simple_partition_hash(partition: &ShufflePartition) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(partition.id.job_id.as_bytes());
    hasher.update(partition.id.stage_id.as_bytes());
    hasher.update(&partition.id.partition.to_le_bytes());
    for batch in &partition.batches {
        for col in batch.columns() {
            for buf in col.to_data().buffers() {
                hasher.update(buf.as_slice());
            }
        }
    }
    *hasher.finalize().as_bytes()
}

impl Default for InMemoryShuffleStore {
    fn default() -> Self {
        Self {
            state: Mutex::new(InMemoryState::default()),
            max_bytes: None,
            spill_store: None,
        }
    }
}

impl InMemoryShuffleStore {
    pub fn new() -> Self {
        let max_bytes = std::env::var("KRISHIV_SHUFFLE_MEMORY_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SHUFFLE_MEMORY_BYTES);
        Self {
            state: Mutex::new(InMemoryState::default()),
            max_bytes: Some(max_bytes),
            spill_store: None,
        }
    }

    pub fn new_unbounded() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    #[must_use]
    pub fn with_spill_store(mut self, spill_store: Arc<LocalDiskShuffleStore>) -> Self {
        self.spill_store = Some(spill_store);
        self
    }

    /// Returns `true` if the partition is currently in memory (not spilled).
    ///
    /// Synchronous: callers (e.g. `SpillableShuffleBackend`) can call this
    /// without `.await` to decide whether to release budget after a write.
    pub fn is_partition_in_memory(&self, id: &PartitionId) -> bool {
        let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);
        self.state
            .lock()
            .map(|st| st.partitions.contains_key(&key))
            .unwrap_or(false)
    }

    /// Returns the total in-memory bytes for non-spilled partitions of `job_id`.
    pub fn bytes_for_job(&self, job_id: &str) -> usize {
        self.state
            .lock()
            .map(|st| {
                st.partitions
                    .iter()
                    .filter(|((jid, _, _), _)| jid == job_id)
                    .map(|(_, p)| partition_memory_bytes(p))
                    .sum()
            })
            .unwrap_or(0)
    }
}

enum ReadResult {
    Data(ShufflePartition),
    Spilled,
    NotFound,
}

#[async_trait::async_trait]
impl ShuffleStore for InMemoryShuffleStore {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        let key = (id.job_id, id.stage_id, id.partition);
        let mut st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
        if let Some(&expected) = st.lease_tokens.get(&key)
            && lease_token < expected
        {
            return Err(ShuffleError::StaleLeaseToken {
                expected,
                actual: lease_token,
            });
        }
        st.lease_tokens.insert(key, lease_token);
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

        // Initial token validation — synchronous, no I/O.
        {
            let st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
            if let Some(&expected) = st.lease_tokens.get(&key)
                && lease_token < expected
            {
                return Err(ShuffleError::StaleLeaseToken {
                    expected,
                    actual: lease_token,
                });
            }
        } // guard dropped before any .await

        // Direct-to-disk: single partition exceeds the total memory cap.
        if let (Some(max_bytes), Some(spill)) = (self.max_bytes, self.spill_store.as_ref())
            && new_size > max_bytes
        {
            spill.write_partition(partition, lease_token).await?;
            let mut st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
            if let Some(&expected) = st.lease_tokens.get(&key)
                && lease_token < expected
            {
                return Err(ShuffleError::StaleLeaseToken {
                    expected,
                    actual: lease_token,
                });
            }
            let old = st
                .partitions
                .remove(&key)
                .map(|p| partition_memory_bytes(&p))
                .unwrap_or(0);
            st.lease_tokens.insert(key.clone(), lease_token);
            st.spill_order.swap_remove(&key);
            st.spilled.insert(key.clone());
            st.bytes_used = st.bytes_used.saturating_sub(old);
            st.content_hashes.remove(&key);
            return Ok(());
        }

        // Evict LRU victims until there is room for the incoming partition.
        if let Some(max_bytes) = self.max_bytes {
            loop {
                // Determine capacity and pick a victim in one lock acquisition.
                let (have_room, victim) = {
                    let st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
                    let old = st
                        .partitions
                        .get(&key)
                        .map(partition_memory_bytes)
                        .unwrap_or(0);
                    let projected = st.bytes_used.saturating_sub(old).saturating_add(new_size);
                    if projected <= max_bytes {
                        (true, None)
                    } else {
                        let v = st
                            .spill_order
                            .iter()
                            .find(|k| **k != key && st.partitions.contains_key(*k))
                            .cloned();
                        (false, v)
                    }
                }; // guard dropped

                if have_room {
                    break;
                }

                let Some(victim_key) = victim else {
                    // No evictable partition — return hard limit error.
                    let st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
                    let old = st
                        .partitions
                        .get(&key)
                        .map(partition_memory_bytes)
                        .unwrap_or(0);
                    return Err(ShuffleError::MemoryLimitExceeded {
                        max_bytes,
                        current_bytes: st.bytes_used.saturating_sub(old),
                        incoming_bytes: new_size,
                    });
                };

                // Snapshot victim data without holding the lock across I/O.
                let victim_snapshot = {
                    let st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
                    st.partitions.get(&victim_key).map(|p| {
                        (
                            p.clone(),
                            partition_memory_bytes(p),
                            st.lease_tokens.get(&victim_key).copied().unwrap_or(0),
                            compute_simple_partition_hash(p),
                        )
                    })
                }; // guard dropped

                let Some((victim_partition, victim_size, victim_token, victim_hash)) =
                    victim_snapshot
                else {
                    continue; // Victim was removed concurrently; retry.
                };

                let Some(spill) = self.spill_store.as_ref() else {
                    let st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
                    return Err(ShuffleError::MemoryLimitExceeded {
                        max_bytes,
                        current_bytes: st.bytes_used,
                        incoming_bytes: new_size,
                    });
                };
                spill
                    .write_partition(victim_partition, victim_token)
                    .await?;

                // Update accounting — skip if victim was overwritten during spill.
                {
                    let mut st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
                    if st.content_hashes.get(&victim_key).copied() == Some(victim_hash) {
                        st.partitions.remove(&victim_key);
                        st.content_hashes.remove(&victim_key);
                        st.spilled.insert(victim_key.clone());
                        st.spill_order.swap_remove(&victim_key);
                        st.bytes_used = st.bytes_used.saturating_sub(victim_size);
                    } else {
                        tracing::info!(
                            "spill cleanup skipped for {victim_key:?}: partition modified during spill"
                        );
                    }
                } // guard dropped; loop to recheck capacity
            }
        }

        // Commit partition to memory.
        {
            let mut st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
            // Re-validate: token may have been superseded while we were spilling.
            if let Some(&expected) = st.lease_tokens.get(&key)
                && lease_token < expected
            {
                return Err(ShuffleError::StaleLeaseToken {
                    expected,
                    actual: lease_token,
                });
            }
            let old_size = st
                .partitions
                .get(&key)
                .map(partition_memory_bytes)
                .unwrap_or(0);
            st.partitions.insert(key.clone(), partition);
            st.lease_tokens.insert(key.clone(), lease_token);
            st.spill_order.swap_remove(&key);
            st.spill_order.insert(key.clone());
            st.spilled.remove(&key);
            st.bytes_used = st
                .bytes_used
                .saturating_sub(old_size)
                .saturating_add(new_size);
            st.content_hashes.insert(key, computed_hash);
        }

        Ok(())
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);

        // Lock once, read state, drop guard — then do async I/O outside the lock.
        // The `?` inside the block propagates LockPoisoned to the function caller.
        let action: ReadResult = {
            let st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
            if st.spilled.contains(&key) {
                ReadResult::Spilled
            } else if let Some(partition) = st.partitions.get(&key) {
                if let Some(&stored_hash) = st.content_hashes.get(&key) {
                    let computed = compute_simple_partition_hash(partition);
                    if computed != stored_hash {
                        return Err(ShuffleError::ContentHashMismatch {
                            partition: format!("{key:?}"),
                            expected: format!("{stored_hash:02x?}"),
                            actual: format!("{computed:02x?}"),
                        });
                    }
                }
                ReadResult::Data(partition.clone())
            } else {
                // Not in memory and not spilled — either never written or the write
                // failed (e.g. stale token). The caller can retry; returning Ok(None)
                // is correct semantics because no committed data exists yet.
                ReadResult::NotFound
            }
        }; // guard dropped here, before any .await

        match action {
            ReadResult::Data(p) => Ok(Some(p)),
            ReadResult::NotFound => Ok(None),
            ReadResult::Spilled => match &self.spill_store {
                Some(spill) => spill.read_partition(id).await,
                None => Ok(None),
            },
        }
    }

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        {
            let mut st = self.state.lock().map_err(|_| ShuffleError::LockPoisoned)?;
            // Compute freed bytes before removing so bytes_used stays accurate.
            let freed: usize = st
                .partitions
                .iter()
                .filter(|((jid, _, _), _)| jid == job_id)
                .map(|(_, p)| partition_memory_bytes(p))
                .sum();
            st.content_hashes.retain(|(jid, _, _), _| jid != job_id);
            st.lease_tokens.retain(|(jid, _, _), _| jid != job_id);
            st.partitions.retain(|(jid, _, _), _| jid != job_id);
            st.spilled.retain(|(jid, _, _)| jid != job_id);
            st.spill_order.retain(|(jid, _, _)| jid != job_id);
            st.bytes_used = st.bytes_used.saturating_sub(freed);
        } // guard dropped before .await
        if let Some(spill) = &self.spill_store {
            spill.delete_job_partitions(job_id).await?;
        }
        Ok(())
    }
}
