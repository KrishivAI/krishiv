//! T12: Push-based shuffle — map-side merge before reduce fetch.
//!
//! In pull-based shuffle (T10/T11), each reduce task must open N connections to
//! fetch its partition from each of the N map tasks.  Push-based shuffle moves
//! the merge work to the map side:
//!
//! 1. Each map task **pushes** its Arrow IPC partition data directly to a
//!    shared [`PushShuffleStore`] (or to the ESS HTTP endpoint
//!    `POST /ess/push/{job}/{stage}/{task}/{partition}`).
//! 2. After all map tasks push, the store holds the union of every map task's
//!    contribution for each partition.
//! 3. A reduce task fetches partition `p` via
//!    `GET /ess/merged/{job}/{stage}/{partition}` which returns the
//!    concatenated Arrow IPC stream from all map tasks — one round-trip instead
//!    of N.
//!
//! # Layout
//!
//! ```text
//!   push_store[(job, stage, partition)] = [ ipc_task_0, ipc_task_1, … ]
//! ```
//!
//! The Arrow IPC payloads are stored raw so the store is format-agnostic.
//! Callers write individual streams (one per map task push), and readers
//! receive them concatenated in push order.

use std::sync::Arc;

use dashmap::DashMap;

/// In-process store for push-based shuffle data.
///
/// Shared (via `Arc`) between the executor push path and the ESS HTTP handler.
#[derive(Clone, Default)]
pub struct PushShuffleStore {
    /// (job_id, stage_id, partition_idx) → ordered list of IPC payloads
    inner: Arc<DashMap<(String, String, u32), Vec<Vec<u8>>>>,
}

impl PushShuffleStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accept one map-task push: append `ipc_bytes` for `(job_id, stage_id, partition)`.
    ///
    /// This is the hot path — called for every (partition, task) combination
    /// during map-stage execution.  The call is O(1) amortised.
    pub fn push(&self, job_id: &str, stage_id: &str, partition: u32, ipc_bytes: Vec<u8>) {
        self.inner
            .entry((job_id.to_owned(), stage_id.to_owned(), partition))
            .or_default()
            .push(ipc_bytes);
    }

    /// Return the merged Arrow IPC stream for `(job_id, stage_id, partition)`.
    ///
    /// The stream is the **concatenation** of all pushed IPC payloads in the
    /// order they were pushed.  Returns `None` if no data has been pushed.
    pub fn merge_read(&self, job_id: &str, stage_id: &str, partition: u32) -> Option<Vec<u8>> {
        let chunks: Vec<Vec<u8>> = {
            let entry = self
                .inner
                .get(&(job_id.to_owned(), stage_id.to_owned(), partition))?;
            if entry.is_empty() {
                return None;
            }
            entry.clone()
        };
        if chunks.len() == 1 {
            return Some(chunks.into_iter().next().unwrap());
        }
        let total: usize = chunks.iter().map(|b| b.len()).sum();
        let mut merged = Vec::with_capacity(total);
        for chunk in &chunks {
            merged.extend_from_slice(chunk);
        }
        Some(merged)
    }

    /// Number of pushed segments for `(job_id, stage_id, partition)`.
    pub fn push_count(&self, job_id: &str, stage_id: &str, partition: u32) -> usize {
        self.inner
            .get(&(job_id.to_owned(), stage_id.to_owned(), partition))
            .map(|e| e.len())
            .unwrap_or(0)
    }

    /// Release all data for `job_id`.  Called after the job completes or fails.
    pub fn gc_job(&self, job_id: &str) {
        self.inner.retain(|(jid, _, _), _| jid != job_id);
    }

    /// Release data for a specific `(job_id, stage_id)` stage.
    pub fn gc_stage(&self, job_id: &str, stage_id: &str) {
        self.inner
            .retain(|(jid, sid, _), _| jid != job_id || sid != stage_id);
    }

    /// Total number of bytes held in the store (all jobs, all partitions).
    pub fn total_bytes(&self) -> usize {
        self.inner
            .iter()
            .map(|entry| entry.value().iter().map(|b| b.len()).sum::<usize>())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ipc(val: u8, n: usize) -> Vec<u8> {
        vec![val; n]
    }

    #[test]
    fn push_then_merge_read_concatenates() {
        let store = PushShuffleStore::new();
        store.push("job-1", "stage-0", 0, ipc(0xAA, 10));
        store.push("job-1", "stage-0", 0, ipc(0xBB, 20));
        store.push("job-1", "stage-0", 0, ipc(0xCC, 5));

        let merged = store.merge_read("job-1", "stage-0", 0).unwrap();
        assert_eq!(merged.len(), 35);
        assert_eq!(&merged[..10], &[0xAAu8; 10]);
        assert_eq!(&merged[10..30], &[0xBBu8; 20]);
        assert_eq!(&merged[30..35], &[0xCCu8; 5]);
    }

    #[test]
    fn merge_read_returns_none_when_empty() {
        let store = PushShuffleStore::new();
        assert!(store.merge_read("job-empty", "s0", 0).is_none());
    }

    #[test]
    fn push_count_tracks_per_partition() {
        let store = PushShuffleStore::new();
        store.push("j", "s", 0, ipc(1, 4));
        store.push("j", "s", 0, ipc(2, 4));
        store.push("j", "s", 1, ipc(3, 4));
        assert_eq!(store.push_count("j", "s", 0), 2);
        assert_eq!(store.push_count("j", "s", 1), 1);
        assert_eq!(store.push_count("j", "s", 2), 0);
    }

    #[test]
    fn gc_job_removes_all_job_data() {
        let store = PushShuffleStore::new();
        store.push("gc-job", "s0", 0, ipc(1, 10));
        store.push("gc-job", "s0", 1, ipc(2, 10));
        store.push("other-job", "s0", 0, ipc(3, 10));

        store.gc_job("gc-job");

        assert!(store.merge_read("gc-job", "s0", 0).is_none());
        assert!(store.merge_read("gc-job", "s0", 1).is_none());
        assert!(store.merge_read("other-job", "s0", 0).is_some());
    }

    #[test]
    fn gc_stage_removes_only_that_stage() {
        let store = PushShuffleStore::new();
        store.push("j", "s0", 0, ipc(1, 10));
        store.push("j", "s1", 0, ipc(2, 10));

        store.gc_stage("j", "s0");

        assert!(store.merge_read("j", "s0", 0).is_none());
        assert!(store.merge_read("j", "s1", 0).is_some());
    }

    #[test]
    fn total_bytes_sums_all_payloads() {
        let store = PushShuffleStore::new();
        store.push("j", "s", 0, ipc(1, 100));
        store.push("j", "s", 1, ipc(2, 200));
        assert_eq!(store.total_bytes(), 300);
    }
}
