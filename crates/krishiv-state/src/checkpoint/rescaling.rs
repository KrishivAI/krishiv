//! Key-group rescaling for checkpoint restore (R16 S4.2).

use crate::key_group::{KeyGroupRange, key_group_ranges_for_parallelism};

// ── RescaleChecksum ──────────────────────────────────────────────────────────

/// Lightweight integrity checkpoint for a partition split or merge operation.
///
/// Inspired by the Netflix Planner/Splitter pattern: the coordinator computes
/// a `RescaleChecksum` from the pre-split data and stores it. After the split,
/// the executor computes the post-split checksum and the two must match before
/// the operation is marked complete. This makes split operations idempotent and
/// verifiable without shipping full record batches back to the coordinator.
///
/// The checksum is deliberately simple (row count + column count) so it can be
/// computed in O(1) from metadata without reading batch payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RescaleChecksum {
    /// Total row count across all input batches / shards.
    pub total_rows: u64,
    /// Number of columns (schema width). Used to catch schema divergence.
    pub column_count: u32,
    /// Parallelism before the rescale.
    pub old_parallelism: u32,
    /// Parallelism after the rescale.
    pub new_parallelism: u32,
}

impl RescaleChecksum {
    /// Create a checksum from aggregate statistics collected by the planner
    /// before the split begins.
    pub fn new(
        total_rows: u64,
        column_count: u32,
        old_parallelism: u32,
        new_parallelism: u32,
    ) -> Self {
        Self {
            total_rows,
            column_count,
            old_parallelism,
            new_parallelism,
        }
    }

    /// Verify that the post-split shards are consistent with this pre-split
    /// checksum.
    ///
    /// `post_shard_row_counts` must contain one entry per post-split shard.
    /// Returns `true` when the split is valid (no rows lost or duplicated and
    /// schema width is unchanged).
    pub fn verify(&self, post_shard_row_counts: &[u64], post_column_count: u32) -> bool {
        let post_total: u64 = post_shard_row_counts.iter().sum();
        post_total == self.total_rows
            && post_column_count == self.column_count
            && post_shard_row_counts.len() == self.new_parallelism as usize
    }
}

/// Computes key-group → task slot mapping when restoring with new parallelism.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyGroupRescaler {
    /// Parallelism of the checkpoint being restored (stored for diagnostic
    /// logging; not used in the key-group → task mapping arithmetic).
    pub old_parallelism: u32,
    pub new_parallelism: u32,
    pub new_ranges: Vec<KeyGroupRange>,
}

impl KeyGroupRescaler {
    pub fn new(old_parallelism: u32, new_parallelism: u32) -> Self {
        Self {
            old_parallelism: old_parallelism.max(1),
            new_parallelism: new_parallelism.max(1),
            new_ranges: key_group_ranges_for_parallelism(new_parallelism.max(1)),
        }
    }

    /// Task slot index in the new deployment for `key_group`.
    ///
    /// Uses binary search over [`new_ranges`] to stay consistent with the
    /// potentially uneven range distribution produced by
    /// [`key_group_ranges_for_parallelism`].
    pub fn task_for_key_group(&self, key_group: u16) -> u32 {
        match self.new_ranges.partition_point(|r| r.end < key_group) as u32 {
            idx if (idx as usize) < self.new_ranges.len() => idx,
            _ => self.new_parallelism.saturating_sub(1),
        }
    }

    /// Key-group range assigned to `task_index` in the new deployment.
    /// Returns `None` if `task_index` is out of range.
    pub fn range_for_task(&self, task_index: u32) -> Option<KeyGroupRange> {
        self.new_ranges.get(task_index as usize).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_group::NUM_KEY_GROUPS;

    #[test]
    fn rescale_4_to_2_maps_all_key_groups() {
        let rescaler = KeyGroupRescaler::new(4, 2);
        assert_eq!(rescaler.new_ranges.len(), 2);
        for kg in 0..NUM_KEY_GROUPS {
            let task = rescaler.task_for_key_group(kg);
            assert!(task < 2);
            let range = rescaler.range_for_task(task).expect("task index in range");
            assert!(range.contains(kg));
        }
    }

    #[test]
    fn rescale_2_to_4_expands_ranges() {
        let rescaler = KeyGroupRescaler::new(2, 4);
        assert_eq!(rescaler.new_ranges.len(), 4);
    }

    /// C12 regression: range_for_task must return None for out-of-range indices
    /// (task_index >= new_parallelism) instead of panicking or returning garbage.
    #[test]
    fn range_for_task_out_of_range_returns_none() {
        let rescaler = KeyGroupRescaler::new(4, 2);
        assert!(rescaler.range_for_task(0).is_some());
        assert!(rescaler.range_for_task(1).is_some());
        assert!(
            rescaler.range_for_task(2).is_none(),
            "task_index >= parallelism must return None"
        );
        assert!(rescaler.range_for_task(999).is_none());
    }

    // ── new() clamping ──────────────────────────────────────────────────

    #[test]
    fn rescaler_new_clamps_zero_to_one() {
        let rescaler = KeyGroupRescaler::new(0, 0);
        assert_eq!(rescaler.old_parallelism, 1);
        assert_eq!(rescaler.new_parallelism, 1);
        assert_eq!(rescaler.new_ranges.len(), 1);
        let range = rescaler.range_for_task(0).unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, NUM_KEY_GROUPS - 1);
    }

    #[test]
    fn rescaler_new_clamps_large_to_one() {
        let rescaler = KeyGroupRescaler::new(100, 1);
        assert_eq!(rescaler.old_parallelism, 100);
        assert_eq!(rescaler.new_parallelism, 1);
        assert_eq!(rescaler.new_ranges.len(), 1);
    }

    // ── task_for_key_group boundaries ───────────────────────────────────

    #[test]
    fn task_for_key_group_first_key_group() {
        let rescaler = KeyGroupRescaler::new(4, 4);
        let task = rescaler.task_for_key_group(0);
        assert_eq!(task, 0);
    }

    #[test]
    fn task_for_key_group_last_key_group() {
        let rescaler = KeyGroupRescaler::new(4, 4);
        let task = rescaler.task_for_key_group(NUM_KEY_GROUPS - 1);
        assert!(task < 4);
    }

    #[test]
    fn task_for_key_group_single_parallelism() {
        let rescaler = KeyGroupRescaler::new(4, 1);
        for kg in 0..NUM_KEY_GROUPS {
            assert_eq!(rescaler.task_for_key_group(kg), 0);
        }
    }

    // ── range_for_task coverage ─────────────────────────────────────────

    #[test]
    fn range_for_task_covers_full_range() {
        for p in [1u32, 2, 3, 4, 8, 16, 32] {
            let rescaler = KeyGroupRescaler::new(p, p);
            let mut covered = 0u32;
            for task in 0..p {
                let range = rescaler.range_for_task(task).unwrap();
                covered += (range.end - range.start + 1) as u32;
            }
            assert_eq!(covered, NUM_KEY_GROUPS as u32, "p={p}");
        }
    }

    #[test]
    fn range_for_task_no_gaps() {
        let rescaler = KeyGroupRescaler::new(4, 4);
        for i in 0..(rescaler.new_ranges.len() - 1) {
            let r1 = rescaler.range_for_task(i as u32).unwrap();
            let r2 = rescaler.range_for_task(i as u32 + 1).unwrap();
            assert_eq!(r1.end + 1, r2.start, "gap between task {i} and {}", i + 1);
        }
    }

    // ── Consistency: task → range → key_group mapping ───────────────────

    #[test]
    fn key_group_consistency_all_parallelisms() {
        for new_p in [1u32, 2, 3, 4, 5, 7, 8, 16, 32, 64, 128, 256] {
            let rescaler = KeyGroupRescaler::new(4, new_p);
            for kg in 0..NUM_KEY_GROUPS {
                let task = rescaler.task_for_key_group(kg);
                let range = rescaler
                    .range_for_task(task)
                    .unwrap_or_else(|| panic!("task={task} not in range for new_p={new_p}"));
                assert!(
                    range.contains(kg),
                    "kg={kg} task={task} range={range:?} new_p={new_p}"
                );
            }
        }
    }

    // ── Display / Debug ─────────────────────────────────────────────────

    #[test]
    fn rescaler_debug_output() {
        let rescaler = KeyGroupRescaler::new(4, 2);
        let debug = format!("{:?}", rescaler);
        assert!(debug.contains("old_parallelism"));
        assert!(debug.contains("new_parallelism"));
        assert!(debug.contains("new_ranges"));
    }

    // ── Old parallelism is stored but not used in arithmetic ────────────

    #[test]
    fn old_parallelism_is_stored() {
        let rescaler = KeyGroupRescaler::new(8, 4);
        assert_eq!(rescaler.old_parallelism, 8);
        // The mapping only depends on new_parallelism
        let rescaler2 = KeyGroupRescaler::new(16, 4);
        for kg in 0..NUM_KEY_GROUPS {
            assert_eq!(
                rescaler.task_for_key_group(kg),
                rescaler2.task_for_key_group(kg)
            );
        }
    }

    // ── Single key group to single task ─────────────────────────────────

    #[test]
    fn rescale_1_to_1_single_range() {
        let rescaler = KeyGroupRescaler::new(1, 1);
        assert_eq!(rescaler.new_ranges.len(), 1);
        let range = rescaler.range_for_task(0).unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, NUM_KEY_GROUPS - 1);
        assert_eq!(rescaler.task_for_key_group(0), 0);
        assert_eq!(rescaler.task_for_key_group(NUM_KEY_GROUPS - 1), 0);
    }

    // ── KeyGroupRange::contains ─────────────────────────────────────────

    #[test]
    fn key_group_range_contains_boundaries() {
        let range = KeyGroupRange::new(10, 20);
        assert!(!range.contains(9));
        assert!(range.contains(10));
        assert!(range.contains(15));
        assert!(range.contains(20));
        assert!(!range.contains(21));
    }

    #[test]
    fn key_group_range_as_range() {
        let range = KeyGroupRange::new(5, 10);
        let r = range.as_range();
        assert_eq!(r, 5..=10);
    }

    #[test]
    fn key_group_range_equality() {
        let a = KeyGroupRange::new(0, 100);
        let b = KeyGroupRange::new(0, 100);
        let c = KeyGroupRange::new(0, 101);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ── Uneven division ─────────────────────────────────────────────────

    #[test]
    fn rescale_uneven_division_no_gaps() {
        let rescaler = KeyGroupRescaler::new(4, 3);
        assert_eq!(rescaler.new_ranges.len(), 3);
        // All key groups must map to a valid task with containing range
        for kg in 0..NUM_KEY_GROUPS {
            let task = rescaler.task_for_key_group(kg);
            let range = rescaler.range_for_task(task).unwrap();
            assert!(range.contains(kg), "kg={kg} task={task} range={range:?}");
        }
    }

    // ── RescaleChecksum ───────────────────────────────────────────────────────

    #[test]
    fn checksum_verifies_matching_split() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        // 4 shards totalling 1000 rows, 5 columns
        let counts = [250u64, 250, 250, 250];
        assert!(pre.verify(&counts, 5));
    }

    #[test]
    fn checksum_rejects_lost_rows() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        let counts = [200u64, 250, 250, 250]; // only 950 total
        assert!(!pre.verify(&counts, 5));
    }

    #[test]
    fn checksum_rejects_duplicated_rows() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        let counts = [300u64, 250, 250, 250]; // 1050 total
        assert!(!pre.verify(&counts, 5));
    }

    #[test]
    fn checksum_rejects_wrong_shard_count() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        let counts = [500u64, 500]; // only 2 shards, expected 4
        assert!(!pre.verify(&counts, 5));
    }

    #[test]
    fn checksum_rejects_schema_divergence() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        let counts = [250u64, 250, 250, 250];
        assert!(!pre.verify(&counts, 6)); // column count changed
    }

    #[test]
    fn checksum_zero_rows_splits_cleanly() {
        let pre = RescaleChecksum::new(0, 3, 1, 2);
        assert!(pre.verify(&[0, 0], 3));
        assert!(!pre.verify(&[1, 0], 3));
    }
}
