//! Content-hash memoization cache (R14 S2.2).

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::ExecError;

/// LRU memo cache keyed by SHA-256 content hash.
#[derive(Debug)]
pub struct MemoCache {
    max_entries: usize,
    map: Mutex<HashMap<[u8; 32], Vec<u8>>>,
    order: Mutex<VecDeque<[u8; 32]>>,
    hits: Mutex<u64>,
    misses: Mutex<u64>,
}

impl MemoCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            map: Mutex::new(HashMap::new()),
            order: Mutex::new(VecDeque::new()),
            hits: Mutex::new(0),
            misses: Mutex::new(0),
        }
    }

    pub fn lookup(&self, key: [u8; 32]) -> Option<RecordBatch> {
        let map = self.map.lock().ok()?;
        let bytes = map.get(&key)?;
        *self.hits.lock().ok()? += 1;
        decode_batch(bytes).ok()
    }

    pub fn store(&self, key: [u8; 32], batch: RecordBatch) -> Result<(), ExecError> {
        let encoded = encode_batch(&batch)?;
        let mut map = self
            .map
            .lock()
            .map_err(|_| ExecError::Arrow("memo cache lock poisoned".into()))?;
        let mut order = self
            .order
            .lock()
            .map_err(|_| ExecError::Arrow("memo cache lock poisoned".into()))?;

        if !map.contains_key(&key) {
            order.push_back(key);
        }
        map.insert(key, encoded);

        while order.len() > self.max_entries {
            if let Some(evicted) = order.pop_front() {
                map.remove(&evicted);
            }
        }
        Ok(())
    }

    pub fn cache_info(&self) -> (u64, u64, usize) {
        let hits = self.hits.lock().map(|h| *h).unwrap_or(0);
        let misses = self.misses.lock().map(|m| *m).unwrap_or(0);
        let size = self.map.lock().map(|m| m.len()).unwrap_or(0);
        (hits, misses, size)
    }

    pub fn lookup_or_miss(&self, key: [u8; 32]) -> Option<RecordBatch> {
        if let Some(batch) = self.lookup(key) {
            return Some(batch);
        }
        if let Ok(mut misses) = self.misses.lock() {
            *misses += 1;
        }
        None
    }
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, ExecError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| ExecError::Arrow(e.to_string()))?;
        writer
            .write(batch)
            .map_err(|e| ExecError::Arrow(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| ExecError::Arrow(e.to_string()))?;
    }
    Ok(buf)
}

fn decode_batch(bytes: &[u8]) -> Result<RecordBatch, ExecError> {
    let cursor = std::io::Cursor::new(bytes);
    let mut reader =
        StreamReader::try_new(cursor, None).map_err(|e| ExecError::Arrow(e.to_string()))?;
    reader
        .next()
        .transpose()
        .map_err(|e| ExecError::Arrow(e.to_string()))?
        .ok_or_else(|| ExecError::Arrow("empty memo ipc stream".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
        assert_eq!(hit.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 1);
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
}
