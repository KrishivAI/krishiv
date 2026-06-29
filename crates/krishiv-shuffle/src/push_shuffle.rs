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
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;

/// Composite key for the push-shuffle store: `(job_id, stage_id, partition_idx)`.
pub type PushShuffleKey = (String, String, u32);

/// Inner map for [`PushShuffleStore`]: one entry per partition holds the
/// ordered list of IPC payloads pushed by the map tasks.
type PushShuffleMap = DashMap<PushShuffleKey, Vec<Vec<u8>>>;

/// Default memory cap: 2 GiB.  Override via [`PushShuffleStore::with_memory_limit`].
const DEFAULT_MEMORY_LIMIT_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// In-process store for push-based shuffle data.
///
/// Shared (via `Arc`) between the executor push path and the ESS HTTP handler.
/// A configurable memory limit (default 2 GiB) causes [`push`](Self::push) to
/// return an error rather than grow the heap without bound when producers are
/// faster than consumers.
#[derive(Clone)]
pub struct PushShuffleStore {
    /// (job_id, stage_id, partition_idx) → ordered list of IPC payloads
    inner: Arc<PushShuffleMap>,
    /// Running total of bytes held across all partitions.
    total_bytes: Arc<AtomicUsize>,
    /// Maximum bytes before push() returns an error.
    memory_limit: usize,
}

impl Default for PushShuffleStore {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            total_bytes: Arc::new(AtomicUsize::new(0)),
            memory_limit: DEFAULT_MEMORY_LIMIT_BYTES,
        }
    }
}

impl PushShuffleStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the store-wide memory limit (bytes).
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = bytes;
        self
    }

    /// Accept one map-task push: append `ipc_bytes` for `(job_id, stage_id, partition)`.
    ///
    /// Returns `Err` when the store-wide memory limit would be exceeded, so
    /// producers are back-pressured rather than growing the heap without bound.
    ///
    /// This is the hot path — called for every (partition, task) combination
    /// during map-stage execution.  The call is O(1) amortised.
    pub fn push(
        &self,
        job_id: &str,
        stage_id: &str,
        partition: u32,
        ipc_bytes: Vec<u8>,
    ) -> Result<(), String> {
        let incoming = ipc_bytes.len();
        let current = self.total_bytes.load(Ordering::Relaxed);
        if current.saturating_add(incoming) > self.memory_limit {
            return Err(format!(
                "push shuffle store memory limit ({} bytes) exceeded; \
                 {} bytes used + {} bytes incoming; \
                 reduce tasks may be lagging",
                self.memory_limit, current, incoming
            ));
        }
        self.inner
            .entry((job_id.to_owned(), stage_id.to_owned(), partition))
            .or_default()
            .push(ipc_bytes);
        self.total_bytes.fetch_add(incoming, Ordering::Relaxed);
        Ok(())
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
            return chunks.into_iter().next();
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
        let freed: usize = self
            .inner
            .iter()
            .filter(|e| e.key().0 == job_id)
            .map(|e| e.value().iter().map(|b| b.len()).sum::<usize>())
            .sum();
        self.inner.retain(|(jid, _, _), _| jid != job_id);
        self.total_bytes.fetch_sub(
            freed.min(self.total_bytes.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }

    /// Release data for a specific `(job_id, stage_id)` stage.
    pub fn gc_stage(&self, job_id: &str, stage_id: &str) {
        let freed: usize = self
            .inner
            .iter()
            .filter(|e| e.key().0 == job_id && e.key().1 == stage_id)
            .map(|e| e.value().iter().map(|b| b.len()).sum::<usize>())
            .sum();
        self.inner
            .retain(|(jid, sid, _), _| jid != job_id || sid != stage_id);
        self.total_bytes.fetch_sub(
            freed.min(self.total_bytes.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }

    /// Total number of bytes held in the store (all jobs, all partitions).
    pub fn total_bytes(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
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
        store.push("job-1", "stage-0", 0, ipc(0xAA, 10)).unwrap();
        store.push("job-1", "stage-0", 0, ipc(0xBB, 20)).unwrap();
        store.push("job-1", "stage-0", 0, ipc(0xCC, 5)).unwrap();

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
        store.push("j", "s", 0, ipc(1, 4)).unwrap();
        store.push("j", "s", 0, ipc(2, 4)).unwrap();
        store.push("j", "s", 1, ipc(3, 4)).unwrap();
        assert_eq!(store.push_count("j", "s", 0), 2);
        assert_eq!(store.push_count("j", "s", 1), 1);
        assert_eq!(store.push_count("j", "s", 2), 0);
    }

    #[test]
    fn gc_job_removes_all_job_data() {
        let store = PushShuffleStore::new();
        store.push("gc-job", "s0", 0, ipc(1, 10)).unwrap();
        store.push("gc-job", "s0", 1, ipc(2, 10)).unwrap();
        store.push("other-job", "s0", 0, ipc(3, 10)).unwrap();

        store.gc_job("gc-job");

        assert!(store.merge_read("gc-job", "s0", 0).is_none());
        assert!(store.merge_read("gc-job", "s0", 1).is_none());
        assert!(store.merge_read("other-job", "s0", 0).is_some());
    }

    #[test]
    fn gc_stage_removes_only_that_stage() {
        let store = PushShuffleStore::new();
        store.push("j", "s0", 0, ipc(1, 10)).unwrap();
        store.push("j", "s1", 0, ipc(2, 10)).unwrap();

        store.gc_stage("j", "s0");

        assert!(store.merge_read("j", "s0", 0).is_none());
        assert!(store.merge_read("j", "s1", 0).is_some());
    }

    #[test]
    fn total_bytes_sums_all_payloads() {
        let store = PushShuffleStore::new();
        store.push("j", "s", 0, ipc(1, 100)).unwrap();
        store.push("j", "s", 1, ipc(2, 200)).unwrap();
        assert_eq!(store.total_bytes(), 300);
    }

    #[test]
    fn push_errors_when_memory_limit_exceeded() {
        let store = PushShuffleStore::new().with_memory_limit(50);
        store.push("j", "s", 0, ipc(1, 30)).unwrap();
        let err = store.push("j", "s", 0, ipc(2, 30)).unwrap_err();
        assert!(
            err.contains("memory limit"),
            "expected memory-limit error, got: {err}"
        );
    }
}
