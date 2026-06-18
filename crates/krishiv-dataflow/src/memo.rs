//! Content-hash memoization cache (R14 S2.2).

use std::collections::{HashMap, VecDeque};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use arrow::record_batch::RecordBatch;

use crate::ExecError;

struct MemoCacheInner {
    map: HashMap<[u8; 32], RecordBatch>,
    order: VecDeque<[u8; 32]>,
}

/// LRU memo cache keyed by SHA-256 content hash.
///
/// Uses a single `Mutex<MemoCacheInner>` to eliminate the TOCTOU race that
/// exists when a separate `DashMap` and order-`Mutex` are used together.
/// Stores `RecordBatch` values directly to avoid IPC serialization overhead.
/// Hit/miss counters use `AtomicU64` for lock-free reads.
#[derive(Debug)]
pub struct MemoCache {
    max_entries: usize,
    inner: Mutex<MemoCacheInner>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for MemoCacheInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoCacheInner")
            .field("len", &self.map.len())
            .finish()
    }
}

impl MemoCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            inner: Mutex::new(MemoCacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn lookup(&self, key: [u8; 32]) -> Option<RecordBatch> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ExecError::Arrow("memo cache lock poisoned".into()))
            .ok()?;
        let batch = inner.map.get(&key).cloned()?;
        inner.order.retain(|k| k != &key);
        inner.order.push_back(key);
        Some(batch)
    }

    pub fn store(&self, key: [u8; 32], batch: RecordBatch) -> Result<(), ExecError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ExecError::Arrow("memo cache lock poisoned".into()))?;

        if inner.map.contains_key(&key) {
            inner.order.retain(|k| k != &key);
        }
        inner.order.push_back(key);
        inner.map.insert(key, batch);

        while inner.order.len() > self.max_entries {
            if let Some(evicted) = inner.order.pop_front() {
                inner.map.remove(&evicted);
            }
        }
        Ok(())
    }

    pub fn cache_info(&self) -> (u64, u64, usize) {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let size = self.inner.lock().map(|g| g.map.len()).unwrap_or(0);
        (hits, misses, size)
    }

    pub fn lookup_or_miss(&self, key: [u8; 32]) -> Option<RecordBatch> {
        if let Some(batch) = self.lookup(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(batch);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    #[test]
    fn memo_hit_and_miss() {
        let cache = MemoCache::new(10);
        let key = [7u8; 32];
        assert!(cache.lookup_or_miss(key).is_none());
        cache.store(key, batch(1)).unwrap();
        let hit = cache.lookup_or_miss(key).unwrap();
        assert_eq!(
            hit.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        let (hits, misses, size) = cache.cache_info();
        assert!(hits >= 1);
        assert!(misses >= 1);
        assert_eq!(size, 1);
    }

    #[test]
    fn memo_lru_eviction() {
        let cache = MemoCache::new(2);
        let k1 = [1u8; 32];
        let k2 = [2u8; 32];
        let k3 = [3u8; 32];
        cache.store(k1, batch(1)).unwrap();
        cache.store(k2, batch(2)).unwrap();
        cache.store(k3, batch(3)).unwrap();
        assert!(cache.lookup_or_miss(k1).is_none());
        assert!(cache.lookup_or_miss(k3).is_some());
    }

    #[test]
    fn memo_lru_lookup_promotes_key() {
        let cache = MemoCache::new(2);
        let k1 = [1u8; 32];
        let k2 = [2u8; 32];
        let k3 = [3u8; 32];
        cache.store(k1, batch(1)).unwrap();
        cache.store(k2, batch(2)).unwrap();
        assert!(cache.lookup_or_miss(k1).is_some());
        cache.store(k3, batch(3)).unwrap();
        assert!(cache.lookup_or_miss(k1).is_some());
        assert!(cache.lookup_or_miss(k2).is_none());
    }

    #[test]
    fn memo_lru_re_insert_promotes_key() {
        let cache = MemoCache::new(2);
        let k1 = [1u8; 32];
        let k2 = [2u8; 32];
        cache.store(k1, batch(1)).unwrap();
        cache.store(k2, batch(2)).unwrap();
        // Re-store k1 — this should promote it to the back of the LRU.
        cache.store(k1, batch(10)).unwrap();
        // k2 should still be accessible (not evicted).
        let hit = cache.lookup_or_miss(k2).unwrap();
        assert_eq!(
            hit.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        // k1 should have the updated value.
        let hit1 = cache.lookup_or_miss(k1).unwrap();
        assert_eq!(
            hit1.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
    }

    #[test]
    fn memo_concurrent_store_no_race() {
        let cache = Arc::new(MemoCache::new(100));
        let mut handles = vec![];
        for i in 0u8..8 {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let mut key = [0u8; 32];
                key[0] = i;
                c.store(key, batch(i as i64)).unwrap();
                // Immediately look it up — must find the value we just stored.
                let hit = c.lookup(key);
                assert!(
                    hit.is_some(),
                    "concurrent store then lookup must succeed for thread {i}"
                );
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }
        let (_, _, size) = cache.cache_info();
        assert_eq!(
            size, 8,
            "all 8 unique keys must be present after concurrent stores"
        );
    }
}
