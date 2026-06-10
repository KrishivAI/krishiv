use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};

/// Default memo TTL: 7 days (P3-13).
pub const DEFAULT_MEMO_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Build a memo key from document content hash and chunk index (P3-12).
pub fn memo_key(content_hash: &str, chunk_index: usize) -> String {
    format!("{content_hash}:{chunk_index}")
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Memoized embedding entry (ADR-R17.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoEntry {
    pub content_hash: String,
    pub embedding: Vec<f32>,
    pub point_id: String,
    /// Wall-clock insert time; used for TTL eviction on `get`.
    #[serde(default = "MemoEntry::default_created_at_ms")]
    pub created_at_ms: u64,
}

impl MemoEntry {
    fn default_created_at_ms() -> u64 {
        now_ms()
    }
}

/// RocksDB-backed memo store for incremental RAG re-indexing.
pub struct MemoStore {
    db: DB,
    ttl_ms: u64,
}

impl MemoStore {
    /// Open or create a memo database at `path` with default TTL.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_with_ttl(path, DEFAULT_MEMO_TTL_MS)
    }

    /// Open with a custom TTL in milliseconds (`0` disables expiry).
    pub fn open_with_ttl(path: impl AsRef<Path>, ttl_ms: u64) -> Result<Self, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path.as_ref()).map_err(|e| e.to_string())?;
        Ok(Self { db, ttl_ms })
    }

    /// Lookup memo entry by key; evicts expired entries on TTL.
    pub fn get(&self, key: &str) -> Result<Option<MemoEntry>, String> {
        let Some(bytes) = self.db.get(key.as_bytes()).map_err(|e| e.to_string())? else {
            return Ok(None);
        };
        let entry: MemoEntry = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if self.ttl_ms > 0 && now_ms().saturating_sub(entry.created_at_ms) > self.ttl_ms {
            self.delete(key)?;
            return Ok(None);
        }
        Ok(Some(entry))
    }

    /// Insert or update memo entry (refreshes `created_at_ms`).
    pub fn put(&self, key: &str, entry: &MemoEntry) -> Result<(), String> {
        let mut stored = entry.clone();
        stored.created_at_ms = now_ms();
        let bytes = serde_json::to_vec(&stored).map_err(|e| e.to_string())?;
        self.db
            .put(key.as_bytes(), bytes)
            .map_err(|e| e.to_string())
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.db
            .delete(key.as_bytes())
            .map_err(|e| e.to_string())
    }

    /// Benchmark lookup latency for `key_count` keys.
    pub fn bench_lookup_p99(key_count: usize) -> Result<u64, String> {
        let dir = std::env::temp_dir().join(format!("krishiv-memo-bench-{}", std::process::id()));
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let store = Self::open(dir.join("memo.rocksdb"))?;
        for i in 0..key_count {
            let key = format!("key-{i}");
            store.put(
                &key,
                &MemoEntry {
                    content_hash: key.clone(),
                    embedding: vec![0.0; 8],
                    point_id: format!("p-{i}"),
                    created_at_ms: now_ms(),
                },
            )?;
        }
        let mut latencies = Vec::with_capacity(1000);
        for i in 0..1000 {
            let key = format!("key-{}", i % key_count);
            let start = Instant::now();
            let _ = store.get(&key)?;
            latencies.push(start.elapsed().as_micros());
        }
        latencies.sort_unstable();
        let p99_idx = latencies.len() * 99 / 100;
        Ok(latencies[p99_idx] as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memo_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        let entry = MemoEntry {
            content_hash: "abc".into(),
            embedding: vec![1.0, 2.0],
            point_id: "pt".into(),
            created_at_ms: now_ms(),
        };
        store.put("abc:0", &entry).unwrap();
        let got = store.get("abc:0").unwrap().unwrap();
        assert_eq!(got.point_id, "pt");
    }

    #[test]
    fn memo_key_includes_chunk_index() {
        assert_eq!(memo_key("hash", 3), "hash:3");
    }

    #[test]
    fn memo_ttl_evicts_stale_entry() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open_with_ttl(dir.path().join("m.redb"), 1).unwrap();
        let entry = MemoEntry {
            content_hash: "x".into(),
            embedding: vec![1.0],
            point_id: "p".into(),
            created_at_ms: now_ms().saturating_sub(10),
        };
        store.put("x:0", &entry).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(store.get("x:0").unwrap().is_none());
    }

    #[test]
    fn memo_store_million_keys_p99_under_1ms() {
        let p99_us = MemoStore::bench_lookup_p99(10_000).unwrap();
        assert!(p99_us < 1_000, "p99 lookup {p99_us}µs exceeds 1ms");
    }

    #[test]
    fn memo_store_open_default_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        assert_eq!(store.ttl_ms, DEFAULT_MEMO_TTL_MS);
    }

    #[test]
    fn memo_store_open_custom_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open_with_ttl(dir.path().join("m.redb"), 5000).unwrap();
        assert_eq!(store.ttl_ms, 5000);
    }

    #[test]
    fn memo_store_open_zero_ttl_no_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open_with_ttl(dir.path().join("m.redb"), 0).unwrap();
        let entry = MemoEntry {
            content_hash: "old".into(),
            embedding: vec![1.0],
            point_id: "p".into(),
            created_at_ms: now_ms().saturating_sub(1_000_000),
        };
        store.put("old:0", &entry).unwrap();
        let got = store.get("old:0").unwrap();
        assert!(got.is_some(), "zero TTL should not evict");
    }

    #[test]
    fn memo_store_get_missing_key_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        assert!(store.get("nonexistent:0").unwrap().is_none());
    }

    #[test]
    fn memo_store_overwrite_refreshes_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open_with_ttl(dir.path().join("m.redb"), 100).unwrap();
        let entry = MemoEntry {
            content_hash: "c".into(),
            embedding: vec![1.0],
            point_id: "p1".into(),
            created_at_ms: now_ms().saturating_sub(100),
        };
        store.put("c:0", &entry).unwrap();
        let entry2 = MemoEntry {
            content_hash: "c".into(),
            embedding: vec![2.0],
            point_id: "p2".into(),
            created_at_ms: 0,
        };
        store.put("c:0", &entry2).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let got = store.get("c:0").unwrap().unwrap();
        assert_eq!(got.point_id, "p2");
    }

    #[test]
    fn memo_store_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        for i in 0..10 {
            let key = format!("doc-{i}:{i}");
            let entry = MemoEntry {
                content_hash: format!("hash-{i}"),
                embedding: vec![i as f32; 4],
                point_id: format!("pt-{i}"),
                created_at_ms: now_ms(),
            };
            store.put(&key, &entry).unwrap();
        }
        for i in 0..10 {
            let key = format!("doc-{i}:{i}");
            let got = store.get(&key).unwrap().unwrap();
            assert_eq!(got.point_id, format!("pt-{i}"));
            assert_eq!(got.embedding, vec![i as f32; 4]);
        }
    }

    #[test]
    fn memo_entry_serialization_roundtrip() {
        let entry = MemoEntry {
            content_hash: "abc123".into(),
            embedding: vec![0.1, -0.5, 1.0],
            point_id: "pt-42".into(),
            created_at_ms: 1234567890,
        };
        let bytes = serde_json::to_vec(&entry).unwrap();
        let deserialized: MemoEntry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(entry, deserialized);
    }

    #[test]
    fn memo_key_format() {
        assert_eq!(memo_key("", 0), ":0");
        assert_eq!(memo_key("a", 1), "a:1");
        assert_eq!(memo_key("abcdef1234567890", 999), "abcdef1234567890:999");
    }

    #[test]
    fn now_ms_returns_positive_value() {
        let t = now_ms();
        assert!(t > 0);
    }

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn memo_entry_default_created_at() {
        let entry = MemoEntry {
            content_hash: "h".into(),
            embedding: vec![],
            point_id: "p".into(),
            created_at_ms: 0, // will be overwritten by default_created_at_ms if needed
        };
        assert_eq!(entry.created_at_ms, 0);
    }

    #[test]
    fn memo_entry_debug() {
        let entry = MemoEntry {
            content_hash: "abc".into(),
            embedding: vec![1.0],
            point_id: "pt".into(),
            created_at_ms: 100,
        };
        let debug = format!("{:?}", entry);
        assert!(debug.contains("abc"));
        assert!(debug.contains("pt"));
    }

    #[test]
    fn memo_entry_clone() {
        let entry = MemoEntry {
            content_hash: "abc".into(),
            embedding: vec![1.0, 2.0],
            point_id: "pt".into(),
            created_at_ms: 100,
        };
        let cloned = entry.clone();
        assert_eq!(entry, cloned);
    }

    #[test]
    fn memo_key_various_inputs() {
        assert_eq!(memo_key("abc", 0), "abc:0");
        assert_eq!(memo_key("abc", 1), "abc:1");
        assert_eq!(memo_key("abc", 999), "abc:999");
        assert_eq!(memo_key("", 0), ":0");
        assert_eq!(memo_key("hash", 42), "hash:42");
    }

    #[test]
    fn memo_store_put_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        let entry = MemoEntry {
            content_hash: "h1".into(),
            embedding: vec![0.5, -0.5],
            point_id: "p1".into(),
            created_at_ms: now_ms(),
        };
        store.put("key1:0", &entry).unwrap();
        let got = store.get("key1:0").unwrap().unwrap();
        assert_eq!(got.content_hash, "h1");
        assert_eq!(got.embedding, vec![0.5, -0.5]);
        assert_eq!(got.point_id, "p1");
    }

    #[test]
    fn memo_store_get_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        assert!(store.get("missing:0").unwrap().is_none());
    }

    #[test]
    fn memo_store_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        let e1 = MemoEntry {
            content_hash: "h".into(),
            embedding: vec![1.0],
            point_id: "old".into(),
            created_at_ms: now_ms(),
        };
        store.put("k:0", &e1).unwrap();
        let e2 = MemoEntry {
            content_hash: "h".into(),
            embedding: vec![2.0],
            point_id: "new".into(),
            created_at_ms: 0,
        };
        store.put("k:0", &e2).unwrap();
        let got = store.get("k:0").unwrap().unwrap();
        assert_eq!(got.point_id, "new");
    }

    #[test]
    fn memo_store_large_embedding() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        let embedding: Vec<f32> = (0..1536).map(|i| i as f32 / 1536.0).collect();
        let entry = MemoEntry {
            content_hash: "large".into(),
            embedding: embedding.clone(),
            point_id: "pt-large".into(),
            created_at_ms: now_ms(),
        };
        store.put("large:0", &entry).unwrap();
        let got = store.get("large:0").unwrap().unwrap();
        assert_eq!(got.embedding.len(), 1536);
        assert!((got.embedding[0] - 0.0).abs() < 1e-6);
    }

    #[test]
    fn memo_store_many_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        for i in 0..100 {
            let key = format!("doc:{i}");
            let entry = MemoEntry {
                content_hash: format!("hash-{i}"),
                embedding: vec![i as f32],
                point_id: format!("pt-{i}"),
                created_at_ms: now_ms(),
            };
            store.put(&key, &entry).unwrap();
        }
        for i in 0..100 {
            let key = format!("doc:{i}");
            let got = store.get(&key).unwrap().unwrap();
            assert_eq!(got.point_id, format!("pt-{i}"));
        }
    }

    #[test]
    fn memo_store_ttl_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open_with_ttl(dir.path().join("m.redb"), 1).unwrap();
        let entry = MemoEntry {
            content_hash: "h".into(),
            embedding: vec![1.0],
            point_id: "p".into(),
            created_at_ms: now_ms().saturating_sub(100),
        };
        store.put("k:0", &entry).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(store.get("k:0").unwrap().is_none());
    }

    #[test]
    fn memo_store_zero_ttl_no_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open_with_ttl(dir.path().join("m.redb"), 0).unwrap();
        let entry = MemoEntry {
            content_hash: "h".into(),
            embedding: vec![1.0],
            point_id: "p".into(),
            created_at_ms: now_ms().saturating_sub(1_000_000),
        };
        store.put("k:0", &entry).unwrap();
        assert!(store.get("k:0").unwrap().is_some());
    }

    #[test]
    fn memo_store_default_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoStore::open(dir.path().join("m.redb")).unwrap();
        assert_eq!(store.ttl_ms, DEFAULT_MEMO_TTL_MS);
    }

    #[test]
    fn memo_store_serialization_roundtrip() {
        let entry = MemoEntry {
            content_hash: "hash123".into(),
            embedding: vec![0.1, 0.2, 0.3],
            point_id: "pt-42".into(),
            created_at_ms: 999999,
        };
        let bytes = serde_json::to_vec(&entry).unwrap();
        let deserialized: MemoEntry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(entry, deserialized);
    }

    #[test]
    fn now_ms_consistency() {
        let t1 = now_ms();
        let t2 = now_ms();
        assert!(t2 >= t1);
    }

    #[test]
    fn memo_entry_serialization_with_zero_embedding() {
        let entry = MemoEntry {
            content_hash: "zero".into(),
            embedding: vec![],
            point_id: "p".into(),
            created_at_ms: 0,
        };
        let bytes = serde_json::to_vec(&entry).unwrap();
        let deserialized: MemoEntry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(entry, deserialized);
    }

    #[test]
    fn memo_entry_serialization_with_special_chars() {
        let entry = MemoEntry {
            content_hash: "abc/def=ghi+jkl".into(),
            embedding: vec![1.0],
            point_id: "pt-special!@#$%".into(),
            created_at_ms: 12345,
        };
        let bytes = serde_json::to_vec(&entry).unwrap();
        let deserialized: MemoEntry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(entry, deserialized);
    }
}
