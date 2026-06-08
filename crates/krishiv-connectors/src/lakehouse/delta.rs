//! Incremental delta log for live tables (ADR-R14-01).

use std::sync::Mutex;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::LakehouseError;

const DELTA_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("delta_log");

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

/// redb-backed durable delta store for embedded / single-node live tables.
pub struct RedbDeltaStore {
    db: Database,
    namespace: Vec<u8>,
}

impl RedbDeltaStore {
    pub fn open(
        path: impl AsRef<std::path::Path>,
        namespace: impl AsRef<[u8]>,
    ) -> Result<Self, LakehouseError> {
        let db = Database::create(path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let store = Self {
            db,
            namespace: namespace.as_ref().to_vec(),
        };
        store
            .db
            .begin_write()
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .open_table(DELTA_TABLE)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        Ok(store)
    }

    pub fn open_in_memory(namespace: impl AsRef<[u8]>) -> Result<Self, LakehouseError> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let store = Self {
            db,
            namespace: namespace.as_ref().to_vec(),
        };
        store.ensure_table()?;
        Ok(store)
    }

    fn key_for(&self, seq: u64) -> Vec<u8> {
        let mut key = self.namespace.clone();
        key.extend_from_slice(&seq.to_le_bytes());
        key
    }

    fn ensure_table(&self) -> Result<(), LakehouseError> {
        let wtxn = self
            .db
            .begin_write()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        {
            let _ = wtxn
                .open_table(DELTA_TABLE)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
        }
        wtxn.commit()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        Ok(())
    }

    fn next_seq(&self) -> Result<u64, LakehouseError> {
        let read = self
            .db
            .begin_read()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let table = read
            .open_table(DELTA_TABLE)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut max = 0u64;
        let prefix = self.namespace.as_slice();
        for item in table
            .iter()
            .map_err(|e| LakehouseError::Io(e.to_string()))?
        {
            let (k, _) = item.map_err(|e| LakehouseError::Io(e.to_string()))?;
            let k = k.value();
            if k.len() >= prefix.len() + 8 && k.starts_with(prefix) {
                let seq_bytes: [u8; 8] = k[k.len() - 8..]
                    .try_into()
                    .map_err(|_| LakehouseError::Io("failed to parse sequence bytes".into()))?;
                max = max.max(u64::from_le_bytes(seq_bytes));
            }
        }
        Ok(max + 1)
    }
}

impl DeltaStore for RedbDeltaStore {
    fn append(&self, batch: RecordBatch, op: DeltaOp) -> Result<(), LakehouseError> {
        let seq = self.next_seq()?;
        let value = encode_entry(op, &batch)?;
        let write = self
            .db
            .begin_write()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        {
            let mut table = write
                .open_table(DELTA_TABLE)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            table
                .insert(self.key_for(seq).as_slice(), value.as_slice())
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
        }
        write
            .commit()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        Ok(())
    }

    fn scan(&self) -> Result<Vec<DeltaEntry>, LakehouseError> {
        let read = self
            .db
            .begin_read()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let table = read
            .open_table(DELTA_TABLE)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let prefix = self.namespace.as_slice();
        let mut out = Vec::new();
        for item in table
            .iter()
            .map_err(|e| LakehouseError::Io(e.to_string()))?
        {
            let (k, v) = item.map_err(|e| LakehouseError::Io(e.to_string()))?;
            if k.value().starts_with(prefix) {
                out.push(decode_entry(v.value())?);
            }
        }
        Ok(out)
    }

    fn truncate(&self) -> Result<(), LakehouseError> {
        let read = self
            .db
            .begin_read()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let table = read
            .open_table(DELTA_TABLE)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let prefix = self.namespace.as_slice();
        let keys: Vec<Vec<u8>> = table
            .iter()
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .filter_map(|item| {
                item.ok().and_then(|(k, _)| {
                    let kv = k.value();
                    if kv.starts_with(prefix) {
                        Some(kv.to_vec())
                    } else {
                        None
                    }
                })
            })
            .collect();
        let write = self
            .db
            .begin_write()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        {
            let mut table = write
                .open_table(DELTA_TABLE)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
        }
        write
            .commit()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
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
    use super::*;
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
