//! Key-group rescaling for checkpoint restore (R16 S4.2).

use krishiv_state::key_group::{KeyGroupRange, key_group_ranges_for_parallelism};

/// Computes key-group → task slot mapping when restoring with new parallelism.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyGroupRescaler {
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
    pub fn task_for_key_group(&self, key_group: u16) -> u32 {
        for (idx, range) in self.new_ranges.iter().enumerate() {
            if range.contains(key_group) {
                return idx as u32;
            }
        }
        self.new_parallelism.saturating_sub(1)
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
    use krishiv_state::key_group::NUM_KEY_GROUPS;

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
}
