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

/// Fallback memory cap for a store built without a container budget: 2 GiB.
///
/// Production sizing does **not** come from here — `run_shuffle_svc` derives the
/// ceiling from `ExecutorCapacity`, so the store's claim and the query pool's
/// claim are shares of one container rather than two independent budgets. This
/// constant only applies off a cgroup (tests, embedded use), where there is no
/// container to divide and the previous unbounded-ish behaviour is correct.
///
/// Taking this default in a container is what OOM-killed every SF100 executor:
/// 2 GiB here plus a query pool sized at 0.6 of the same container exceeded the
/// container on both the 2500Mi and 4500Mi shapes.
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
    /// DIST-4: Expected map-task push count per partition keyed by
    /// (job_id, stage_id, partition). When set, merge_read returns None
    /// until all expected pushes arrive.
    expected_pushes: Arc<DashMap<(String, String, u32), usize>>,
}

impl Default for PushShuffleStore {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            total_bytes: Arc::new(AtomicUsize::new(0)),
            memory_limit: DEFAULT_MEMORY_LIMIT_BYTES,
            expected_pushes: Arc::default(),
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

    /// DIST-4: Set the expected number of map-task pushes for a partition.
    /// When set, merge_read returns None until all expected pushes arrive.
    pub fn set_expected_pushes(&self, job_id: &str, stage_id: &str, partition: u32, count: usize) {
        self.expected_pushes
            .insert((job_id.to_owned(), stage_id.to_owned(), partition), count);
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
        // DIST-5: Atomically reserve bytes first, then check and roll back if
        // over the limit. The old load-then-check-then-add pattern allowed N
        // concurrent pushers to all pass the check before any incremented.
        let new_total = self.total_bytes.fetch_add(incoming, Ordering::Relaxed) + incoming;
        if new_total > self.memory_limit {
            self.total_bytes.fetch_sub(incoming, Ordering::Relaxed);
            return Err(format!(
                "push shuffle store memory limit ({} bytes) exceeded; \
                 {} bytes after adding {} bytes incoming; \
                 reduce tasks may be lagging",
                self.memory_limit, new_total, incoming
            ));
        }
        self.inner
            .entry((job_id.to_owned(), stage_id.to_owned(), partition))
            .or_default()
            .push(ipc_bytes);
        Ok(())
    }

    /// Return the merged Arrow IPC stream for `(job_id, stage_id, partition)`.
    ///
    /// The stream is the **concatenation** of all pushed IPC payloads in the
    /// order they were pushed. Returns `None` if no data has been pushed, or —
    /// when [`set_expected_pushes`](Self::set_expected_pushes) has declared a
    /// count for this partition — if fewer than that many pushes have arrived.
    ///
    /// # The gate this restores
    ///
    /// `expected_pushes` was written by `set_expected_pushes` and read by
    /// nothing. Three doc comments (the field, the setter, and the
    /// `POST /ess/expect/…` route) all promised that a merged read waits for
    /// every declared push, and none of them was true: the map was pure
    /// write-only state.
    ///
    /// What that costs is not an error — it is a **wrong answer**. A reduce
    /// task fetching `/ess/merged/…` between two map pushes gets a partition
    /// containing some map tasks' rows and not others, which is a
    /// well-formed Arrow stream that simply has fewer rows in it. Nothing
    /// downstream can tell that apart from a partition that genuinely had
    /// those rows, so the query returns a smaller result and succeeds.
    ///
    /// Gating only applies where a count was declared, so a caller that never
    /// calls `set_expected_pushes` sees exactly the previous behaviour.
    pub fn merge_read(&self, job_id: &str, stage_id: &str, partition: u32) -> Option<Vec<u8>> {
        let key = (job_id.to_owned(), stage_id.to_owned(), partition);
        let expected = self.expected_pushes.get(&key).map(|e| *e.value());
        let entry = self.inner.get(&key)?;
        if entry.is_empty() {
            return None;
        }
        if let Some(expected) = expected
            && entry.len() < expected
        {
            tracing::debug!(
                job_id,
                stage_id,
                partition,
                have = entry.len(),
                expected,
                "merged read withheld: not every map task has pushed this partition yet"
            );
            return None;
        }
        // Build the result under the read guard. Cloning the chunk list first
        // and concatenating after held two full copies of the partition at
        // once, on a path whose whole reason to exist is avoiding N fetches of
        // it. There is no await here, so the guard is held for a memcpy.
        if let [only] = entry.as_slice() {
            return Some(only.clone());
        }
        let total: usize = entry.iter().map(|b| b.len()).sum();
        let mut merged = Vec::with_capacity(total);
        for chunk in entry.iter() {
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
        // The expected-push counts are keyed by job too, and nothing else ever
        // removes them. Leaving them behind leaks one small entry per partition
        // per job for the lifetime of the process, and — worse — a later job
        // that reuses the id would inherit a stale gate.
        self.expected_pushes.retain(|(jid, _, _), _| jid != job_id);
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
        self.expected_pushes
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

    /// The gate that did not exist: a merged read must withhold a partition
    /// until every declared map-task push has arrived.
    ///
    /// `expected_pushes` was written by `set_expected_pushes` and read by
    /// nothing, so a reduce task fetching between two map pushes received a
    /// well-formed Arrow stream containing only some of the map tasks' rows.
    /// Nothing downstream can distinguish that from a partition that genuinely
    /// had fewer rows, so the query returns a wrong answer and *succeeds* —
    /// which is why the failure never showed up as an error.
    #[test]
    fn a_merged_read_withholds_a_partition_until_every_expected_push_arrives() {
        let store = PushShuffleStore::new();
        store.set_expected_pushes("j", "s", 0, 3);

        store.push("j", "s", 0, ipc(0xAA, 10)).unwrap();
        assert!(
            store.merge_read("j", "s", 0).is_none(),
            "1 of 3 pushes must not be served"
        );
        store.push("j", "s", 0, ipc(0xBB, 10)).unwrap();
        assert!(
            store.merge_read("j", "s", 0).is_none(),
            "2 of 3 pushes must not be served"
        );

        store.push("j", "s", 0, ipc(0xCC, 10)).unwrap();
        let merged = store
            .merge_read("j", "s", 0)
            .expect("the partition must be served once every push has arrived");
        assert_eq!(merged.len(), 30);
    }

    /// A partition with no declared count behaves exactly as before, so wiring
    /// the gate cannot stall a caller that never sets one.
    #[test]
    fn a_partition_with_no_declared_count_is_served_immediately() {
        let store = PushShuffleStore::new();
        store.push("j", "s", 0, ipc(0xAA, 10)).unwrap();
        assert!(store.merge_read("j", "s", 0).is_some());
    }

    /// Counts are keyed by job, and only `gc_job`/`gc_stage` ever removed
    /// anything — from the *data* map. A job id reused after GC would otherwise
    /// inherit the previous job's gate and withhold a complete partition
    /// forever.
    #[test]
    fn gc_clears_the_expected_counts_as_well_as_the_data() {
        let store = PushShuffleStore::new();
        store.set_expected_pushes("j", "s", 0, 2);
        store.push("j", "s", 0, ipc(1, 10)).unwrap();
        store.gc_job("j");

        // Same ids again, one push, no new declaration.
        store.push("j", "s", 0, ipc(2, 10)).unwrap();
        assert!(
            store.merge_read("j", "s", 0).is_some(),
            "a stale expected-push count survived GC and withheld a complete partition"
        );

        store.set_expected_pushes("j2", "s", 0, 2);
        store.push("j2", "s", 0, ipc(1, 10)).unwrap();
        store.gc_stage("j2", "s");
        store.push("j2", "s", 0, ipc(2, 10)).unwrap();
        assert!(
            store.merge_read("j2", "s", 0).is_some(),
            "gc_stage must clear that stage's expected-push counts too"
        );
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
