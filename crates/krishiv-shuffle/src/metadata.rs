use crate::ShufflePath;
use std::collections::HashMap;

/// Lifecycle state of a single shuffle partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionState {
    /// Write has been started but not yet completed.
    Pending,
    /// Write completed and the partition is ready to be read.
    Available,
    /// Write failed; the error reason is captured.
    Failed {
        /// Human-readable failure reason.
        reason: String,
    },
}

/// In-memory registry tracking the state of shuffle partitions.
///
/// # There is deliberately no partition cap
///
/// This map used to carry a `max_partitions` bound (65536) that only
/// `mark_pending` enforced. Nothing ever called `mark_pending`: production
/// reaches this type exclusively through [`mark_available`](Self::mark_available)
/// and [`mark_failed`](Self::mark_failed), from `krishiv-scheduler`'s
/// `job/record.rs` and `store.rs`. The bound was therefore never once
/// evaluated, and `with_max_partitions` configured a limit that could not fire.
///
/// It was removed rather than extended to the two completion paths, because
/// refusing to record a partition that has *already been written to disk*
/// would make the consumer stage treat live data as missing and recompute its
/// producer — a strictly worse failure than the unbounded map it was meant to
/// guard. A cap belongs at admission, and there is no admission call site.
///
/// The real bound on this map is the shuffle partition count the physical plan
/// declares for the job, fixed by the planner before any task is dispatched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShuffleMetadata {
    states: HashMap<ShufflePath, PartitionState>,
}

impl ShuffleMetadata {
    /// Create an empty metadata store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a partition write has been started.
    ///
    /// # Not reached in production — check before relying on `Pending`
    ///
    /// No production caller invokes this, so [`state`](Self::state) never
    /// returns [`PartitionState::Pending`] in a running cluster. Treat a match
    /// arm for `Pending` as dead until this gains a caller.
    pub fn mark_pending(&mut self, path: &ShufflePath) {
        self.states.insert(path.clone(), PartitionState::Pending);
    }

    /// Record that a partition is fully written and available.
    pub fn mark_available(&mut self, path: &ShufflePath) {
        self.states.insert(path.clone(), PartitionState::Available);
    }

    /// Record that a partition write failed with the given reason.
    pub fn mark_failed(&mut self, path: &ShufflePath, reason: String) {
        self.states
            .insert(path.clone(), PartitionState::Failed { reason });
    }

    /// Return the current state for a partition, if known.
    pub fn state(&self, path: &ShufflePath) -> Option<&PartitionState> {
        self.states.get(path)
    }

    /// Return `true` only when every path in the slice is `Available`.
    pub fn all_available(&self, paths: &[ShufflePath]) -> bool {
        paths
            .iter()
            .all(|p| matches!(self.states.get(p), Some(PartitionState::Available)))
    }

    /// Number of partitions currently in the `Available` state.
    pub fn available_count(&self) -> usize {
        self.states
            .values()
            .filter(|s| **s == PartitionState::Available)
            .count()
    }

    /// Number of partitions currently tracked (any state).
    pub fn total_count(&self) -> usize {
        self.states.len()
    }

    /// Return all paths in the `Available` state.
    pub fn available_paths(&self) -> Vec<&ShufflePath> {
        self.states
            .iter()
            .filter(|(_, s)| **s == PartitionState::Available)
            .map(|(path, _)| path)
            .collect()
    }
}
