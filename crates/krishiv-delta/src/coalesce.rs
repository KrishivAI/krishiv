#![forbid(unsafe_code)]

//! `CoalescingMap` — rapid-update debouncing for live sources.
//!
//! When a source emits multiple rapid updates to the same key (e.g., a CDC
//! row updated 10 times in one second), a `CoalescingMap` collapses them into
//! a single latest state before the incremental operators see them.
//!
//! This trades a bounded amount of recency for throughput: only the most
//! recent change per key is forwarded downstream on each tick.

use ahash::AHashMap;

/// A map where inserting a key always overwrites the previous value.
///
/// Drain produces the latest value per key in insertion order of *first* appearance.
#[derive(Debug)]
pub struct CoalescingMap<K, V> {
    map: AHashMap<K, V>,
    /// Preserves the order in which keys were first inserted for deterministic drain.
    insertion_order: Vec<K>,
}

impl<K, V> CoalescingMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
{
    pub fn new() -> Self {
        Self {
            map: AHashMap::new(),
            insertion_order: Vec::new(),
        }
    }

    /// Insert or overwrite the value for `key`.
    /// If the key is new, it is added to the insertion-order list.
    pub fn insert(&mut self, key: K, value: V) {
        if !self.map.contains_key(&key) {
            self.insertion_order.push(key.clone());
        }
        self.map.insert(key, value);
    }

    /// Drain all entries, returning `(key, latest_value)` in insertion order.
    /// The map is empty after this call.
    pub fn drain(&mut self) -> Vec<(K, V)> {
        let order = std::mem::take(&mut self.insertion_order);
        let mut out = Vec::with_capacity(order.len());
        for key in order {
            if let Some(val) = self.map.remove(&key) {
                out.push((key, val));
            }
        }
        self.map.clear();
        out
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<K, V> Default for CoalescingMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_value_wins() {
        let mut m: CoalescingMap<&str, i32> = CoalescingMap::new();
        m.insert("a", 1);
        m.insert("a", 2);
        m.insert("a", 3);
        let drained = m.drain();
        assert_eq!(drained, vec![("a", 3)]);
    }

    #[test]
    fn multiple_keys_preserve_first_insertion_order() {
        let mut m: CoalescingMap<&str, i32> = CoalescingMap::new();
        m.insert("b", 10);
        m.insert("a", 1);
        m.insert("b", 20);
        m.insert("c", 5);
        let drained = m.drain();
        assert_eq!(drained[0], ("b", 20));
        assert_eq!(drained[1], ("a", 1));
        assert_eq!(drained[2], ("c", 5));
    }

    #[test]
    fn drain_empties_map() {
        let mut m: CoalescingMap<i32, i32> = CoalescingMap::new();
        m.insert(1, 10);
        m.drain();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
    }
}
