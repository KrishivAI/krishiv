use crate::{ShuffleError, ShufflePath, ShuffleResult};
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleMetadata {
    states: HashMap<ShufflePath, PartitionState>,
    max_partitions: usize,
}

impl Default for ShuffleMetadata {
    fn default() -> Self {
        Self {
            states: HashMap::new(),
            max_partitions: 65_536,
        }
    }
}

impl ShuffleMetadata {
    /// Create an empty metadata store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of tracked partitions (default 65536).
    #[must_use]
    pub fn with_max_partitions(mut self, n: usize) -> Self {
        self.max_partitions = n;
        self
    }

    /// Record that a partition write has been started.
    ///
    /// Returns `TooManyPartitions` when the cap is already reached.
    pub fn mark_pending(&mut self, path: &ShufflePath) -> ShuffleResult<()> {
        if self.states.len() >= self.max_partitions && !self.states.contains_key(path) {
            return Err(ShuffleError::TooManyPartitions {
                limit: self.max_partitions,
            });
        }
        self.states.insert(path.clone(), PartitionState::Pending);
        Ok(())
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
