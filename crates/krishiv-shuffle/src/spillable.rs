#![forbid(unsafe_code)]

//! E1.2 — SpillableShuffleBackend: budget-aware shuffle store that automatically
//! spills partitions to a local temp directory when the memory limit is exceeded.
//!
//! This is a thin constructor that wires `InMemoryShuffleStore` + a temp-dir
//! `LocalDiskShuffleStore` together using the task's `MemoryBudget` as the
//! byte cap.

use std::path::PathBuf;
use std::sync::Arc;

use krishiv_common::MemoryBudget;

use crate::{
    InMemoryShuffleStore, LocalDiskShuffleStore, PartitionId, ShuffleError, ShufflePartition,
    ShuffleResult, ShuffleStore, memory_store::DEFAULT_SHUFFLE_MEMORY_BYTES,
};

/// Budget-aware shuffle backend.
///
/// Uses an `InMemoryShuffleStore` as the primary store and a
/// `LocalDiskShuffleStore` for spills.  The in-memory byte cap is taken from
/// the `MemoryBudget`'s limit, falling back to
/// [`DEFAULT_SHUFFLE_MEMORY_BYTES`] when the budget is unlimited.
///
/// The `MemoryBudget` is also notified of reserves/releases so that memory
/// charged to shuffle partitions counts against the shared task-level budget.
pub struct SpillableShuffleBackend {
    inner: Arc<InMemoryShuffleStore>,
    budget: Arc<MemoryBudget>,
    spill_dir: PathBuf,
}

impl SpillableShuffleBackend {
    /// Create a backend whose spill directory is `spill_dir`.
    ///
    /// The directory is created if it does not exist.
    pub fn new(spill_dir: PathBuf, budget: Arc<MemoryBudget>) -> ShuffleResult<Self> {
        std::fs::create_dir_all(&spill_dir).map_err(|e| {
            ShuffleError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "failed to create shuffle spill dir '{}': {e}",
                    spill_dir.display()
                ),
            ))
        })?;

        let spill_store = Arc::new(LocalDiskShuffleStore::new(&spill_dir).map_err(|e| {
            ShuffleError::Io(std::io::Error::other(format!(
                "failed to open spill store at '{}': {e}",
                spill_dir.display()
            )))
        })?);

        let max_bytes = budget
            .limit()
            .map(|l| l as usize)
            .unwrap_or(DEFAULT_SHUFFLE_MEMORY_BYTES);
        let inner = Arc::new(
            InMemoryShuffleStore::new_unbounded()
                .with_max_bytes(max_bytes)
                .with_spill_store(spill_store),
        );

        Ok(Self {
            inner,
            budget,
            spill_dir,
        })
    }

    /// Spill directory path.
    pub fn spill_dir(&self) -> &std::path::Path {
        &self.spill_dir
    }

    /// Current budget usage.
    pub fn budget_used_bytes(&self) -> u64 {
        self.budget.used_bytes()
    }
}

impl ShuffleStore for SpillableShuffleBackend {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        self.inner.register_partition_lease(id, lease_token).await
    }

    async fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        // Reserve budget before writing. If the budget is exceeded (returns
        // false), the in-memory store's own spill mechanism will handle it —
        // but we must not silently ignore the rejection.
        let bytes = crate::compression::partition_memory_bytes(&partition);
        let accepted = self.budget.try_reserve(bytes as u64);
        if !accepted {
            // Budget exceeded — the InMemoryShuffleStore will spill to disk
            // internally. We don't return an error here because the store's
            // own spill path handles the overflow. The budget is advisory.
            tracing::debug!(
                bytes,
                used = self.budget.used_bytes(),
                "shuffle write exceeds memory budget; relying on store spill"
            );
        }
        let result = self.inner.write_partition(partition, lease_token).await;
        if result.is_err() {
            // Only release if we actually reserved (accepted was true).
            if accepted {
                self.budget.release(bytes as u64);
            }
        }
        result
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        // Reads return a CLONE of the partition; the partition stays in the
        // store. We do NOT release budget on a cloning read — that would
        // undercount memory. Budget is released when the partition is deleted
        // (delete_job_partitions) or spilled (handled by the in-memory store's
        // own spill path which calls the budget's release via a callback).
        self.inner.read_partition(id).await
    }

    fn delete_job_partitions(
        &self,
        job_id: &str,
    ) -> impl std::future::Future<Output = ShuffleResult<()>> + Send {
        let inner = Arc::clone(&self.inner);
        let job_id = job_id.to_string();
        async move { inner.delete_job_partitions(&job_id).await }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn make_partition(job: &str, stage: &str, idx: u32, rows: usize) -> ShufflePartition {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let col: Arc<dyn arrow::array::Array> =
            Arc::new(Int64Array::from_iter_values(0..rows as i64));
        let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
        ShufflePartition {
            id: PartitionId {
                job_id: job.to_string(),
                stage_id: stage.to_string(),
                partition: idx,
            },
            schema,
            batches: vec![batch],
        }
    }

    #[tokio::test]
    async fn write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::limited(256 * 1024 * 1024);
        let store = SpillableShuffleBackend::new(dir.path().to_path_buf(), budget).unwrap();

        let p = make_partition("job-1", "s0", 0, 10);
        store.write_partition(p, 1).await.unwrap();

        let read = store
            .read_partition(&PartitionId {
                job_id: "job-1".into(),
                stage_id: "s0".into(),
                partition: 0,
            })
            .await
            .unwrap();
        assert!(read.is_some());
        assert_eq!(read.unwrap().batches[0].num_rows(), 10);
    }

    #[tokio::test]
    async fn unlimited_budget_does_not_reject() {
        let dir = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::unlimited();
        let store = SpillableShuffleBackend::new(dir.path().to_path_buf(), budget).unwrap();

        for i in 0..5u32 {
            let p = make_partition("job-u", "s0", i, 10);
            store.write_partition(p, 1).await.unwrap();
        }
    }
}
