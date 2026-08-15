//! Incremental delta log for live tables (ADR-R14-01).

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

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
///
/// Key layout (format v2): `b"v2:" ++ namespace ++ seq.to_be_bytes()`.
/// Big-endian sequence numbers keep RocksDB's lexicographic iteration order
/// identical to append order (little-endian keys misorder after seq 255).
/// Stores written with the unversioned v1 (little-endian) layout are detected
/// at open time and rejected with an explicit error: they must be rebuilt.
pub struct RocksDbDeltaStore {
    db: DB,
    namespace: Vec<u8>,
    seq: Mutex<u64>,
    // Keep tempdir alive for ephemeral instances.
    _tempdir: Option<tempfile::TempDir>,
}

/// Version prefix prepended to every key in the current format.
const KEY_FORMAT_PREFIX: &[u8] = b"v2:";

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
        Self::reject_legacy_format(&db, &ns)?;
        let prefix = Self::key_prefix(&ns);
        let seq = Self::load_max_seq(&db, &prefix);
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

    fn key_prefix(namespace: &[u8]) -> Vec<u8> {
        let mut prefix = KEY_FORMAT_PREFIX.to_vec();
        prefix.extend_from_slice(namespace);
        prefix
    }

    /// Detect keys written by the pre-versioned little-endian layout
    /// (`namespace ++ seq_le`, no `v2:` prefix). Reading them with the v2
    /// big-endian decoding would silently misorder entries, so refuse.
    fn reject_legacy_format(db: &DB, namespace: &[u8]) -> Result<(), LakehouseError> {
        for item in db.iterator(IteratorMode::Start) {
            let Ok((k, _)) = item else { continue };
            if k.starts_with(namespace)
                && k.len() == namespace.len() + 8
                && !k.starts_with(KEY_FORMAT_PREFIX)
            {
                return Err(LakehouseError::Io(format!(
                    "RocksDbDeltaStore: namespace {:?} contains legacy (v1 little-endian) keys; \
                     the key format changed to big-endian sequence ordering — rebuild the store \
                     from its source before reopening",
                    String::from_utf8_lossy(namespace)
                )));
            }
        }
        Ok(())
    }

    fn load_max_seq(db: &DB, prefix: &[u8]) -> u64 {
        let mut max = 0u64;
        for item in db.iterator(IteratorMode::Start) {
            let Ok((k, _)) = item else { continue };
            if k.starts_with(prefix) && k.len() == prefix.len() + 8 {
                let seq = k
                    .get(prefix.len()..)
                    .and_then(|s| <[u8; 8]>::try_from(s).ok())
                    .map(u64::from_be_bytes)
                    .unwrap_or(0);
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
        let mut key = Self::key_prefix(&self.namespace);
        key.extend_from_slice(&id.to_be_bytes());
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
        let prefix = Self::key_prefix(&self.namespace);
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, rocksdb::Direction::Forward))
        {
            let (k, v) = item.map_err(|e| LakehouseError::Io(e.to_string()))?;
            if !k.starts_with(&prefix) {
                break;
            }
            // Same length check as load_max_seq: a sibling namespace that
            // extends this one (e.g. "orders2" vs "orders") shares the byte
            // prefix but has a longer key.
            if k.len() != prefix.len() + 8 {
                continue;
            }
            out.push(decode_entry(&v)?);
        }
        Ok(out)
    }

    fn truncate(&self) -> Result<(), LakehouseError> {
        let prefix = Self::key_prefix(&self.namespace);
        let mut batch = WriteBatch::default();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, rocksdb::Direction::Forward))
        {
            let (k, _) = item.map_err(|e| LakehouseError::Io(e.to_string()))?;
            if !k.starts_with(&prefix) {
                break;
            }
            if k.len() != prefix.len() + 8 {
                continue;
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
    ///
    /// Write-only: this uses a plain idempotent producer (no Kafka
    /// transactions — a `transactional.id` without `init_transactions` /
    /// `begin_transaction` would be misleading). `scan`, `len`, and
    /// `truncate` require a consumer / admin client and return an explicit
    /// error instead of pretending the log is empty.
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
            Err(LakehouseError::Io(
                "RdkafkaDeltaStore::truncate is unsupported: deleting a compacted topic's \
                 records requires an admin client"
                    .to_string(),
            ))
        }

        fn len(&self) -> Result<usize, LakehouseError> {
            Err(LakehouseError::Io(
                "RdkafkaDeltaStore::len is unsupported: counting records requires a consumer"
                    .to_string(),
            ))
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

    fn entry_id(entry: &DeltaEntry) -> i64 {
        entry
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    /// Scan order must equal append order past sequence 255 — little-endian
    /// key encoding misorders there (0x00,0x01 sorts before 0xff,0x00), which
    /// can replay a delete before its insert.
    #[test]
    fn rocksdb_delta_store_scan_preserves_append_order_past_256() {
        let store = RocksDbDeltaStore::open_in_memory(b"seqorder").unwrap();
        let n = 300i64;
        for i in 0..n {
            store.append(sample_batch(i), DeltaOp::Insert).unwrap();
        }
        let entries = store.scan().unwrap();
        assert_eq!(entries.len(), n as usize);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry_id(entry),
                i as i64,
                "entry at position {i} out of append order"
            );
        }
    }

    /// A namespace must not leak entries from a sibling namespace that merely
    /// extends its byte prefix ("orders" vs "orders2").
    #[test]
    fn rocksdb_delta_store_prefix_namespace_does_not_leak() {
        let dir = tempfile::tempdir().unwrap();
        {
            let sibling = RocksDbDeltaStore::open(dir.path(), b"orders2").unwrap();
            sibling.append(sample_batch(99), DeltaOp::Insert).unwrap();
            sibling.append(sample_batch(98), DeltaOp::Insert).unwrap();
        }
        {
            let store = RocksDbDeltaStore::open(dir.path(), b"orders").unwrap();
            store.append(sample_batch(1), DeltaOp::Insert).unwrap();

            assert_eq!(store.len().unwrap(), 1, "scan must not see orders2 rows");
            let entries = store.scan().unwrap();
            assert_eq!(entry_id(&entries[0]), 1);

            // truncate must only delete this namespace's entries.
            store.truncate().unwrap();
            assert_eq!(store.len().unwrap(), 0);
        }
        let sibling = RocksDbDeltaStore::open(dir.path(), b"orders2").unwrap();
        assert_eq!(
            sibling.len().unwrap(),
            2,
            "truncate of 'orders' must not delete 'orders2' entries"
        );
    }

    /// Reopening a store containing v1 (unversioned little-endian) keys must
    /// fail loudly rather than silently misreading the sequence order.
    #[test]
    fn rocksdb_delta_store_rejects_legacy_key_format() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            let db = DB::open(&opts, dir.path()).unwrap();
            // Legacy v1 key layout: namespace ++ seq.to_le_bytes().
            let mut key = b"orders".to_vec();
            key.extend_from_slice(&0u64.to_le_bytes());
            db.put(key, b"legacy").unwrap();
        }
        let Err(err) = RocksDbDeltaStore::open(dir.path(), b"orders") else {
            panic!("legacy key format must be rejected");
        };
        assert!(
            err.to_string().contains("legacy"),
            "unexpected error: {err}"
        );
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

    /// Write-only Kafka store must refuse len/truncate instead of lying
    /// (len == 0 made is_empty() report an empty log for any topic).
    #[cfg(feature = "kafka")]
    #[test]
    fn rdkafka_delta_store_len_and_truncate_are_unsupported_errors() {
        // Producer creation is lazy — no broker connection is made here.
        let store = RdkafkaDeltaStore::new("localhost:1", "krishiv-test-topic").unwrap();
        assert!(store.len().is_err(), "len must not report a fake 0");
        assert!(
            store.is_empty().is_err(),
            "is_empty must propagate the error"
        );
        assert!(
            store.truncate().is_err(),
            "truncate must not silently no-op"
        );
    }
}
