//! Incremental delta log for live tables (ADR-R14-01).

use std::sync::Mutex;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use rocksdb::{DB, IteratorMode, Options, WriteBatch};
use serde::{Deserialize, Serialize};

use super::LakehouseError;

/// Row-level change operation in a live table delta log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaOp {
    Insert,
    Update,
    Delete,
}

impl DeltaOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "insert" => Some(Self::Insert),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }
}

/// One delta log entry.
#[derive(Debug, Clone)]
pub struct DeltaEntry {
    pub op: DeltaOp,
    pub batch: RecordBatch,
}

/// Append-only store for live-table row deltas.
pub trait DeltaStore: Send + Sync {
    fn append(&self, batch: RecordBatch, op: DeltaOp) -> Result<(), LakehouseError>;
    fn scan(&self) -> Result<Vec<DeltaEntry>, LakehouseError>;
    fn truncate(&self) -> Result<(), LakehouseError>;
    fn len(&self) -> Result<usize, LakehouseError>;

    fn is_empty(&self) -> Result<bool, LakehouseError> {
        Ok(self.len()? == 0)
    }
}

fn encode_entry(op: DeltaOp, batch: &RecordBatch) -> Result<Vec<u8>, LakehouseError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        writer
            .write(batch)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
    }
    let payload = DeltaPayload {
        op: op.as_str().to_string(),
        ipc: buf,
    };
    serde_json::to_vec(&payload).map_err(|e| LakehouseError::Io(e.to_string()))
}

fn decode_entry(bytes: &[u8]) -> Result<DeltaEntry, LakehouseError> {
    let payload: DeltaPayload =
        serde_json::from_slice(bytes).map_err(|e| LakehouseError::Io(e.to_string()))?;
    let op = DeltaOp::from_str(&payload.op)
        .ok_or_else(|| LakehouseError::Io(format!("unknown delta op: {}", payload.op)))?;
    let cursor = std::io::Cursor::new(payload.ipc);
    let mut reader =
        StreamReader::try_new(cursor, None).map_err(|e| LakehouseError::Io(e.to_string()))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|e| LakehouseError::Io(e.to_string()))?
        .ok_or_else(|| LakehouseError::Io("empty delta ipc stream".to_string()))?;
    Ok(DeltaEntry { op, batch })
}

#[derive(Serialize, Deserialize)]
struct DeltaPayload {
    op: String,
    ipc: Vec<u8>,
}

/// In-memory delta store for unit tests and embedded mode.
#[derive(Debug, Default)]
pub struct MemoryDeltaStore {
    entries: Mutex<Vec<Vec<u8>>>,
}

impl MemoryDeltaStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DeltaStore for MemoryDeltaStore {
    fn append(&self, batch: RecordBatch, op: DeltaOp) -> Result<(), LakehouseError> {
        self.entries
            .lock()
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .push(encode_entry(op, &batch)?);
        Ok(())
    }

    fn scan(&self) -> Result<Vec<DeltaEntry>, LakehouseError> {
        let guard = self
            .entries
            .lock()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        guard.iter().map(|b| decode_entry(b)).collect()
    }

    fn truncate(&self) -> Result<(), LakehouseError> {
        self.entries
            .lock()
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .clear();
        Ok(())
    }

    fn len(&self) -> Result<usize, LakehouseError> {
        Ok(self
            .entries
            .lock()
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .len())
    }
}

/// RocksDB-backed durable delta store for embedded / single-node live tables.
pub struct RocksDbDeltaStore {
    db: DB,
    namespace: Vec<u8>,
    seq: Mutex<u64>,
    // Keep tempdir alive for ephemeral instances.
    _tempdir: Option<tempfile::TempDir>,
}

/// Legacy alias so existing callers continue to compile.
pub type RedbDeltaStore = RocksDbDeltaStore;

impl RocksDbDeltaStore {
    pub fn open(
        path: impl AsRef<std::path::Path>,
        namespace: impl AsRef<[u8]>,
    ) -> Result<Self, LakehouseError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let ns = namespace.as_ref().to_vec();
        let db = DB::open(&opts, path.as_ref()).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let seq = Self::load_max_seq(&db, &ns);
        Ok(Self {
            db,
            namespace: ns,
            seq: Mutex::new(seq),
            _tempdir: None,
        })
    }

    pub fn open_in_memory(namespace: impl AsRef<[u8]>) -> Result<Self, LakehouseError> {
        let dir = tempfile::tempdir().map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let ns = namespace.as_ref().to_vec();
        let db = DB::open(&opts, dir.path()).map_err(|e| LakehouseError::Io(e.to_string()))?;
        Ok(Self {
            db,
            namespace: ns,
            seq: Mutex::new(0),
            _tempdir: Some(dir),
        })
    }

    fn load_max_seq(db: &DB, prefix: &[u8]) -> u64 {
        let mut max = 0u64;
        for item in db.iterator(IteratorMode::Start) {
            let Ok((k, _)) = item else { continue };
            if k.starts_with(prefix) && k.len() == prefix.len() + 8 {
                let seq = k.get(prefix.len()..).and_then(|s| <[u8; 8]>::try_from(s).ok()).map(u64::from_le_bytes).unwrap_or(0);
                if seq >= max {
                    max = seq + 1;
                }
            }
        }
        max
    }

    fn next_key(&self) -> Vec<u8> {
        let mut seq = self.seq.lock().unwrap_or_else(|e| e.into_inner());
        let id = *seq;
        *seq += 1;
        let mut key = self.namespace.clone();
        key.extend_from_slice(&id.to_le_bytes());
        key
    }
}

impl DeltaStore for RocksDbDeltaStore {
    fn append(&self, batch: RecordBatch, op: DeltaOp) -> Result<(), LakehouseError> {
        let key = self.next_key();
        let value = encode_entry(op, &batch)?;
        self.db
            .put(key, value)
            .map_err(|e| LakehouseError::Io(e.to_string()))
    }

    fn scan(&self) -> Result<Vec<DeltaEntry>, LakehouseError> {
        let prefix = self.namespace.as_slice();
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(prefix, rocksdb::Direction::Forward))
        {
            let (k, v) = item.map_err(|e| LakehouseError::Io(e.to_string()))?;
            if !k.starts_with(prefix) {
                break;
            }
            out.push(decode_entry(&v)?);
        }
        Ok(out)
    }

    fn truncate(&self) -> Result<(), LakehouseError> {
        let prefix = self.namespace.as_slice();
        let mut batch = WriteBatch::default();
        for item in self
            .db
            .iterator(IteratorMode::From(prefix, rocksdb::Direction::Forward))
        {
            let (k, _) = item.map_err(|e| LakehouseError::Io(e.to_string()))?;
            if !k.starts_with(prefix) {
                break;
            }
            batch.delete(&*k);
        }
        self.db
            .write(batch)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut seq = self.seq.lock().unwrap_or_else(|e| e.into_inner());
        *seq = 0;
        Ok(())
    }

    fn len(&self) -> Result<usize, LakehouseError> {
        self.scan().map(|v| v.len())
    }
}

/// Distributed-mode delta store backed by a Kafka compacted topic (or in-memory log).
#[derive(Debug)]
pub struct KafkaDeltaStore {
    topic: String,
    inner: MemoryDeltaStore,
}

impl KafkaDeltaStore {
    /// Create an in-process delta log keyed by `topic` (used in tests and local mode).
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            inner: MemoryDeltaStore::new(),
        }
    }

    /// Topic name for this delta log.
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl DeltaStore for KafkaDeltaStore {
    fn append(&self, batch: RecordBatch, op: DeltaOp) -> Result<(), LakehouseError> {
        self.inner.append(batch, op)
    }

    fn scan(&self) -> Result<Vec<DeltaEntry>, LakehouseError> {
        self.inner.scan()
    }

    fn truncate(&self) -> Result<(), LakehouseError> {
        self.inner.truncate()
    }

    fn len(&self) -> Result<usize, LakehouseError> {
        self.inner.len()
    }
}

#[cfg(feature = "kafka")]
mod kafka_delta {
    use super::{DeltaEntry, DeltaOp, DeltaStore, LakehouseError, RecordBatch, encode_entry};
    use std::sync::Mutex;

    use rdkafka::ClientConfig;
    use rdkafka::producer::{FutureProducer, FutureRecord};

    /// Broker-backed compacted-topic delta store.
    pub struct RdkafkaDeltaStore {
        producer: FutureProducer,
        topic: String,
        seq: Mutex<u64>,
    }

    impl RdkafkaDeltaStore {
        pub fn new(
            bootstrap_servers: &str,
            topic: impl Into<String>,
        ) -> Result<Self, LakehouseError> {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", bootstrap_servers)
                .set("enable.idempotence", "true")
                .set(
                    "transactional.id",
                    format!("krishiv-delta-{}", std::process::id()),
                )
                .create()
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            Ok(Self {
                producer,
                topic: topic.into(),
                seq: Mutex::new(0),
            })
        }

        fn next_key(&self) -> Vec<u8> {
            let mut seq = self.seq.lock().unwrap_or_else(|e| e.into_inner());
            *seq += 1;
            seq.to_le_bytes().to_vec()
        }
    }

    impl DeltaStore for RdkafkaDeltaStore {
        fn append(&self, batch: RecordBatch, op: DeltaOp) -> Result<(), LakehouseError> {
            let payload = encode_entry(op, &batch)?;
            let key = self.next_key();
            let record = FutureRecord::to(&self.topic).key(&key).payload(&payload);
            let fut = self
                .producer
                .send(record, std::time::Duration::from_secs(5));
            krishiv_common::async_util::block_on(fut)
                .map_err(|(e, _)| LakehouseError::Io(e.to_string()))?;
            Ok(())
        }

        fn scan(&self) -> Result<Vec<DeltaEntry>, LakehouseError> {
            Err(LakehouseError::Io(
                "RdkafkaDeltaStore::scan requires a consumer; use KafkaDeltaStore for tests"
                    .to_string(),
            ))
        }

        fn truncate(&self) -> Result<(), LakehouseError> {
            Ok(())
        }

        fn len(&self) -> Result<usize, LakehouseError> {
            Ok(0)
        }
    }
}

#[cfg(feature = "kafka")]
pub use kafka_delta::RdkafkaDeltaStore;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn sample_batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    #[test]
    fn memory_delta_store_roundtrip() {
        let store = MemoryDeltaStore::new();
        store.append(sample_batch(1), DeltaOp::Insert).unwrap();
        store.append(sample_batch(2), DeltaOp::Update).unwrap();
        let entries = store.scan().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].op, DeltaOp::Insert);
        store.truncate().unwrap();
        assert_eq!(store.len().unwrap(), 0);
    }

    #[test]
    fn redb_delta_store_roundtrip() {
        let store = RedbDeltaStore::open_in_memory(b"orders").unwrap();
        store.append(sample_batch(10), DeltaOp::Insert).unwrap();
        store.append(sample_batch(11), DeltaOp::Delete).unwrap();
        assert_eq!(store.len().unwrap(), 2);
        store.truncate().unwrap();
        assert_eq!(store.len().unwrap(), 0);
    }
}
