//! Key-group hashing for state rescaling (ADR-R16.3).

use std::hash::{Hash, Hasher};
use std::ops::RangeInclusive;

/// Number of key groups (Flink-style fixed parallelism for rescaling).
pub const NUM_KEY_GROUPS: u16 = 32_768;

/// Inclusive key-group range owned by a task slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyGroupRange {
    pub start: u16,
    pub end: u16,
}

impl KeyGroupRange {
    pub fn new(start: u16, end: u16) -> Self {
        Self { start, end }
    }

    pub fn contains(&self, key_group: u16) -> bool {
        key_group >= self.start && key_group <= self.end
    }

    pub fn as_range(&self) -> RangeInclusive<u16> {
        self.start..=self.end
    }
}

/// Hash a record key into `[0, NUM_KEY_GROUPS)`.
pub fn key_group_for_key(key: &[u8]) -> u16 {
    let mut h = twox_hash::XxHash64::with_seed(0);
    key.hash(&mut h);
    (h.finish() % u64::from(NUM_KEY_GROUPS)) as u16
}

/// Assign key groups evenly across `parallelism` task slots.
pub fn key_group_ranges_for_parallelism(parallelism: u32) -> Vec<KeyGroupRange> {
    let p = parallelism.max(1);
    let groups = u32::from(NUM_KEY_GROUPS);
    let base = groups / p;
    let rem = groups % p;
    let mut ranges = Vec::with_capacity(parallelism as usize);
    let mut start = 0u32;
    for i in 0..p {
        let extra = if i < rem { 1 } else { 0 };
        let count = base + extra;
        let end = start + count - 1;
        ranges.push(KeyGroupRange::new(start as u16, end as u16));
        start = end + 1;
    }
    ranges
}

/// Map a key group to task index for `parallelism` slots.
///
/// Uses O(1) arithmetic: `task_idx = key_group * parallelism / NUM_KEY_GROUPS`.
pub fn task_index_for_key_group(key_group: u16, parallelism: u32) -> u32 {
    let p = parallelism.max(1);
    (key_group as u32) * p / u32::from(NUM_KEY_GROUPS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_groups_cover_full_range() {
        let ranges = key_group_ranges_for_parallelism(4);
        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, NUM_KEY_GROUPS - 1);
        for w in ranges.windows(2) {
            assert_eq!(w[0].end + 1, w[1].start);
        }
    }

    #[test]
    fn rescale_4_to_2_splits_evenly() {
        let four = key_group_ranges_for_parallelism(4);
        let two = key_group_ranges_for_parallelism(2);
        assert_eq!(four.len(), 4);
        assert_eq!(two.len(), 2);
        assert!(two[0].end < two[1].start);
    }
}
