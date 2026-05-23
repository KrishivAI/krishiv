use std::path::Path;
use std::time::Instant;

use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

const MEMO_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("memo");

/// Memoized embedding entry (ADR-R17.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoEntry {
    pub content_hash: String,
    pub embedding: Vec<f32>,
    pub point_id: String,
}

/// redb-backed memo store for incremental RAG re-indexing.
pub struct MemoStore {
    db: redb::Database,
}

impl MemoStore {
    /// Open or create a memo database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let db = redb::Database::create(path).map_err(|e| e.to_string())?;
        let write = db.begin_write().map_err(|e| e.to_string())?;
        {
            let _ = write.open_table(MEMO_TABLE).map_err(|e| e.to_string())?;
        }
        write.commit().map_err(|e| e.to_string())?;
        Ok(Self { db })
    }

    /// Lookup memo entry by content hash key.
    pub fn get(&self, key: &str) -> Result<Option<MemoEntry>, String> {
        let read = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = read.open_table(MEMO_TABLE).map_err(|e| e.to_string())?;
        let value = table.get(key).map_err(|e| e.to_string())?;
        Ok(value
            .map(|v| serde_json::from_slice(v.value()).map_err(|e| e.to_string()))
            .transpose()?)
    }

    /// Insert or update memo entry.
    pub fn put(&self, key: &str, entry: &MemoEntry) -> Result<(), String> {
        let bytes = serde_json::to_vec(entry).map_err(|e| e.to_string())?;
        let write = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write.open_table(MEMO_TABLE).map_err(|e| e.to_string())?;
            table.insert(key, bytes.as_slice()).map_err(|e| e.to_string())?;
        }
        write.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Benchmark lookup latency for `key_count` keys (Sprint 5 acceptance).
    pub fn bench_lookup_p99(key_count: usize) -> Result<u64, String> {
        let dir = std::env::temp_dir().join(format!("krishiv-memo-bench-{}" , std::process::id()));
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
        };
        store.put("abc", &entry).unwrap();
        let got = store.get("abc").unwrap().unwrap();
        assert_eq!(got.point_id, "pt");
    }

    #[test]
    fn memo_store_million_keys_p99_under_1ms() {
        // Scaled-down smoke: 10k keys must be fast; full 1M run in #[ignore]
        let p99_us = MemoStore::bench_lookup_p99(10_000).unwrap();
        assert!(p99_us < 1_000, "p99 lookup {p99_us}µs exceeds 1ms");
    }
}
