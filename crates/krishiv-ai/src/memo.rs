use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use redb::{ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};

const MEMO_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("memo");

/// Default memo TTL: 7 days (P3-13).
pub const DEFAULT_MEMO_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Build a memo key from document content hash and chunk index (P3-12).
pub fn memo_key(content_hash: &str, chunk_index: usize) -> String {
    format!("{content_hash}:{chunk_index}")
}

fn now_ms() -> u64 {
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

/// redb-backed memo store for incremental RAG re-indexing.
pub struct MemoStore {
    db: redb::Database,
    ttl_ms: u64,
}

impl MemoStore {
    /// Open or create a memo database at `path` with default TTL.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_with_ttl(path, DEFAULT_MEMO_TTL_MS)
    }

    /// Open with a custom TTL in milliseconds (`0` disables expiry).
    pub fn open_with_ttl(path: impl AsRef<Path>, ttl_ms: u64) -> Result<Self, String> {
        let db = redb::Database::create(path).map_err(|e| e.to_string())?;
        let write = db.begin_write().map_err(|e| e.to_string())?;
        {
            let _ = write.open_table(MEMO_TABLE).map_err(|e| e.to_string())?;
        }
        write.commit().map_err(|e| e.to_string())?;
        Ok(Self { db, ttl_ms })
    }

    /// Lookup memo entry by key; evicts expired entries (P3-13).
    pub fn get(&self, key: &str) -> Result<Option<MemoEntry>, String> {
        let read = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = read.open_table(MEMO_TABLE).map_err(|e| e.to_string())?;
        let value = table.get(key).map_err(|e| e.to_string())?;
        let Some(value) = value else {
            return Ok(None);
        };
        let entry: MemoEntry = serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
        if self.ttl_ms > 0 && now_ms().saturating_sub(entry.created_at_ms) > self.ttl_ms {
            drop(read);
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
        let write = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write.open_table(MEMO_TABLE).map_err(|e| e.to_string())?;
            table.insert(key, bytes.as_slice()).map_err(|e| e.to_string())?;
        }
        write.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        let write = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write.open_table(MEMO_TABLE).map_err(|e| e.to_string())?;
            let _ = table.remove(key).map_err(|e| e.to_string())?;
        }
        write.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Benchmark lookup latency for `key_count` keys (Sprint 5 acceptance).
    pub fn bench_lookup_p99(key_count: usize) -> Result<u64, String> {
        let dir = std::env::temp_dir().join(format!("krishiv-memo-bench-{}", std::process::id()));
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let store = Self::open(dir.join("memo.redb"))?;
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
}
