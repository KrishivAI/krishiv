//! In-memory reference implementations of the runtime services.
//!
//! These back the **embedded** placement and serve as fixtures for engine
//! adapters and tests. They are intentionally simple (single process, no
//! durability); single-node and distributed placements provide their own
//! implementations behind the same traits.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use async_trait::async_trait;
use krishiv_proto::JobId;

use crate::changelog::{ChangelogBatch, RowKind};
use crate::error::{EngineError, EngineResult};
use crate::job::{SinkSpec, SourceSpec};
use crate::runtime::{
    CheckpointPayload, CheckpointService, EngineRuntime, KeyedState, Placement, ShuffleService,
    SinkProvider, SinkWriter, SourceProvider, SourceReader, StateBackendFactory, SystemClock,
};

// ── Sources ───────────────────────────────────────────────────────────────────

/// Serves preloaded record batches per source name.
#[derive(Clone, Default)]
pub struct InMemorySourceProvider {
    data: Arc<Mutex<HashMap<String, Vec<RecordBatch>>>>,
}

impl InMemorySourceProvider {
    /// Create an empty provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Preload `batches` for source `name`.
    pub fn insert(&self, name: impl Into<String>, batches: Vec<RecordBatch>) {
        #[allow(clippy::expect_used, reason = "poisoned mutex = impossible invariant")]
        let mut g = self
            .data
            .lock()
            .expect("InMemorySourceProvider mutex poisoned");
        g.insert(name.into(), batches);
    }
}

#[async_trait]
impl SourceProvider for InMemorySourceProvider {
    async fn open(&self, spec: &SourceSpec) -> EngineResult<Box<dyn SourceReader>> {
        let batches = self
            .data
            .lock()
            .map_err(|_| EngineError::Source("source mutex poisoned".into()))?
            .get(&spec.name)
            .cloned()
            .unwrap_or_default();
        Ok(Box::new(InMemorySourceReader { batches, cursor: 0 }))
    }
}

struct InMemorySourceReader {
    batches: Vec<RecordBatch>,
    cursor: usize,
}

#[async_trait]
impl SourceReader for InMemorySourceReader {
    async fn next(&mut self) -> EngineResult<Option<RecordBatch>> {
        let batch = self.batches.get(self.cursor).cloned();
        if batch.is_some() {
            self.cursor = self.cursor.saturating_add(1);
        }
        Ok(batch)
    }

    fn checkpoint_offset(&self) -> Option<Vec<u8>> {
        Some((self.cursor as u64).to_le_bytes().to_vec())
    }

    fn restore_offset(&mut self, encoded: &[u8]) -> EngineResult<()> {
        let arr: [u8; 8] = encoded
            .try_into()
            .map_err(|_| EngineError::Source("source offset must be 8 bytes".into()))?;
        self.cursor = usize::try_from(u64::from_le_bytes(arr))
            .map_err(|_| EngineError::Source("source offset exceeds usize".into()))?;
        Ok(())
    }
}

/// Serves preloaded **changelogs** per source name — the in-memory CDC source.
///
/// Where [`InMemorySourceProvider`] treats every row as an insertion, this
/// provider carries per-row [`RowKind`]s, so its reader surfaces true deletes
/// and updates through [`SourceReader::next_changelog`]. It is the embedded
/// fixture standing in for a real CDC connector (Debezium, logical decoding).
#[derive(Clone, Default)]
pub struct InMemoryCdcSourceProvider {
    data: Arc<Mutex<HashMap<String, Vec<ChangelogBatch>>>>,
}

impl InMemoryCdcSourceProvider {
    /// Create an empty CDC provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Preload `changes` for source `name`.
    pub fn insert(&self, name: impl Into<String>, changes: Vec<ChangelogBatch>) {
        if let Ok(mut g) = self.data.lock() {
            g.insert(name.into(), changes);
        }
    }
}

#[async_trait]
impl SourceProvider for InMemoryCdcSourceProvider {
    async fn open(&self, spec: &SourceSpec) -> EngineResult<Box<dyn SourceReader>> {
        let changes = self
            .data
            .lock()
            .map_err(|_| EngineError::Source("cdc source mutex poisoned".into()))?
            .get(&spec.name)
            .cloned()
            .unwrap_or_default();
        Ok(Box::new(InMemoryCdcSourceReader { changes, cursor: 0 }))
    }
}

struct InMemoryCdcSourceReader {
    changes: Vec<ChangelogBatch>,
    cursor: usize,
}

#[async_trait]
impl SourceReader for InMemoryCdcSourceReader {
    /// The data image of the next changelog, ignoring row kinds. Provided for
    /// trait completeness; CDC consumers read [`next_changelog`](Self::next_changelog).
    async fn next(&mut self) -> EngineResult<Option<RecordBatch>> {
        Ok(self
            .changes
            .get(self.cursor)
            .map(|cl| cl.batch().clone())
            .inspect(|_| self.cursor = self.cursor.saturating_add(1)))
    }

    async fn next_changelog(&mut self) -> EngineResult<Option<ChangelogBatch>> {
        let cl = self.changes.get(self.cursor).cloned();
        if cl.is_some() {
            self.cursor = self.cursor.saturating_add(1);
        }
        Ok(cl)
    }

    fn checkpoint_offset(&self) -> Option<Vec<u8>> {
        Some((self.cursor as u64).to_le_bytes().to_vec())
    }

    fn restore_offset(&mut self, encoded: &[u8]) -> EngineResult<()> {
        let arr: [u8; 8] = encoded
            .try_into()
            .map_err(|_| EngineError::Source("source offset must be 8 bytes".into()))?;
        self.cursor = usize::try_from(u64::from_le_bytes(arr))
            .map_err(|_| EngineError::Source("source offset exceeds usize".into()))?;
        Ok(())
    }
}

// ── Sinks ───────────────────────────────────────────────────────────────────

/// Collects written changelog batches per view for later inspection.
#[derive(Clone, Default)]
pub struct InMemorySinkProvider {
    collected: Arc<Mutex<HashMap<String, Vec<Arc<ChangelogBatch>>>>>,
}

impl InMemorySinkProvider {
    /// Create an empty sink provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Remove and return everything written for `view` so far.
    ///
    /// Unwraps the internal `Arc<ChangelogBatch>` storage back to owned
    /// `ChangelogBatch` so the public API is unchanged. The unwrap is free
    /// when this is the only strong reference (the common case after
    /// `take`), and falls back to a clone otherwise.
    pub fn take(&self, view: &str) -> Vec<ChangelogBatch> {
        let arcs: Vec<Arc<ChangelogBatch>> = self
            .collected
            .lock()
            .map(|mut m| m.remove(view).unwrap_or_default())
            .unwrap_or_default();
        arcs.into_iter()
            .map(|arc| Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone()))
            .collect()
    }
}

#[async_trait]
impl SinkProvider for InMemorySinkProvider {
    async fn open(&self, spec: &SinkSpec) -> EngineResult<Box<dyn SinkWriter>> {
        Ok(Box::new(InMemorySinkWriter {
            view: spec.view.clone(),
            collected: Arc::clone(&self.collected),
        }))
    }
}

struct InMemorySinkWriter {
    view: String,
    /// Stored as `Arc<ChangelogBatch>` so the streaming engine's fan-out
    /// (`write_arc` called once per sink with the same `Arc`) can share a
    /// single allocation across N sinks — each `Arc::clone` is two atomic
    /// adds, not a `RecordBatch::clone` + `Vec<RowKind>` allocation.
    collected: Arc<Mutex<HashMap<String, Vec<Arc<ChangelogBatch>>>>>,
}

#[async_trait]
impl SinkWriter for InMemorySinkWriter {
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()> {
        self.write_arc(Arc::new(changes)).await
    }

    async fn write_arc(&mut self, batch: Arc<ChangelogBatch>) -> EngineResult<()> {
        let mut g = self
            .collected
            .lock()
            .map_err(|_| EngineError::Sink("sink mutex poisoned".into()))?;
        g.entry(self.view.clone()).or_default().push(batch);
        Ok(())
    }
}

// ── Upsert sink ───────────────────────────────────────────────────────────────

/// A keyed, retraction-aware sink: it **applies** a changelog rather than
/// appending it, maintaining the current materialized table by key.
///
/// This is the reference upsert sink the stateful engines target — the same
/// contract real upsert connectors implement (Iceberg merge-on-read, upsert
/// Kafka, JDBC). Rows keyed by `key_columns`: an insert/`UpdateAfter` writes the
/// row; a delete/`UpdateBefore` removes it. [`table`](Self::table) reads the
/// current state back in deterministic key order.
#[derive(Clone)]
pub struct InMemoryUpsertSink {
    key_columns: Arc<Vec<usize>>,
    state: Arc<Mutex<BTreeMap<Vec<u8>, RecordBatch>>>,
}

impl InMemoryUpsertSink {
    /// Create an upsert sink keyed on the given column indices.
    pub fn new(key_columns: Vec<usize>) -> Self {
        Self {
            key_columns: Arc::new(key_columns),
            state: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// The current table after all applied changelogs, in ascending key order.
    pub fn table(&self, schema: &SchemaRef) -> EngineResult<RecordBatch> {
        let state = self
            .state
            .lock()
            .map_err(|_| EngineError::Sink("upsert sink mutex poisoned".into()))?;
        concat_batches(schema, state.values()).map_err(|e| EngineError::Sink(e.to_string()))
    }

    /// Number of live keys currently held.
    pub fn len(&self) -> usize {
        self.state.lock().map(|s| s.len()).unwrap_or(0)
    }

    /// Whether the maintained table is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Encode each row's key columns to a stable, comparable byte string.
fn encode_keys(batch: &RecordBatch, key_columns: &[usize]) -> EngineResult<Vec<Vec<u8>>> {
    let key_arrays: Vec<ArrayRef> = key_columns
        .iter()
        .map(|&i| {
            batch
                .columns()
                .get(i)
                .cloned()
                .ok_or_else(|| EngineError::Sink(format!("key column {i} out of range")))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let fields: Vec<SortField> = key_arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).map_err(|e| EngineError::Sink(e.to_string()))?;
    let rows = converter
        .convert_columns(&key_arrays)
        .map_err(|e| EngineError::Sink(e.to_string()))?;
    Ok((0..batch.num_rows())
        .map(|i| rows.row(i).as_ref().to_vec())
        .collect())
}

#[async_trait]
impl SinkProvider for InMemoryUpsertSink {
    async fn open(&self, _spec: &SinkSpec) -> EngineResult<Box<dyn SinkWriter>> {
        Ok(Box::new(InMemoryUpsertWriter {
            key_columns: Arc::clone(&self.key_columns),
            state: Arc::clone(&self.state),
        }))
    }
}

struct InMemoryUpsertWriter {
    key_columns: Arc<Vec<usize>>,
    state: Arc<Mutex<BTreeMap<Vec<u8>, RecordBatch>>>,
}

#[async_trait]
impl SinkWriter for InMemoryUpsertWriter {
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()> {
        // Single-owner case: the engine created a fresh `ChangelogBatch` and
        // the upsert sink takes it by value without a clone.
        self.write_inner(&changes)
    }

    async fn write_arc(&mut self, batch: Arc<ChangelogBatch>) -> EngineResult<()> {
        // Multi-owner fan-out case: borrow the changelog without unwrapping.
        // The upsert sink never needs to retain the batch past the call.
        self.write_inner(&batch)
    }
}

impl InMemoryUpsertWriter {
    fn write_inner(&self, changes: &ChangelogBatch) -> EngineResult<()> {
        let batch = changes.batch();
        let keys = encode_keys(batch, &self.key_columns)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| EngineError::Sink("upsert sink mutex poisoned".into()))?;
        // Apply retractions first, then additions, so an update encoded as
        // (retract old, insert new) on the same key resolves to the new row
        // regardless of the row order within the changelog.
        for (i, kind) in changes.row_kinds().iter().enumerate() {
            if matches!(kind, RowKind::Delete | RowKind::UpdateBefore)
                && let Some(key) = keys.get(i)
            {
                state.remove(key);
            }
        }
        for (i, kind) in changes.row_kinds().iter().enumerate() {
            if matches!(kind, RowKind::Insert | RowKind::UpdateAfter) {
                let key = keys
                    .get(i)
                    .cloned()
                    .ok_or_else(|| EngineError::Sink("key/row length mismatch".into()))?;
                state.insert(key, batch.slice(i, 1));
            }
        }
        Ok(())
    }
}

// ── State ───────────────────────────────────────────────────────────────────

/// In-memory keyed-state backend factory.
#[derive(Clone, Copy, Default)]
pub struct InMemoryStateBackend;

impl StateBackendFactory for InMemoryStateBackend {
    fn open_keyed(&self, _namespace: &str) -> EngineResult<Box<dyn KeyedState>> {
        Ok(Box::new(InMemoryKeyedState::default()))
    }
}

#[derive(Default)]
struct InMemoryKeyedState {
    map: HashMap<Vec<u8>, Vec<u8>>,
}

impl KeyedState for InMemoryKeyedState {
    fn get(&self, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        Ok(self.map.get(key).cloned())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> EngineResult<()> {
        self.map.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> EngineResult<()> {
        self.map.remove(key);
        Ok(())
    }

    fn snapshot(&self) -> EngineResult<Vec<u8>> {
        // B-6 fix: use a length-prefixed binary format shared with the
        // production `KeyedState` impls (RocksDB), not `serde_json`. The
        // previous JSON encoding was 2-3× larger and 2-5× slower on large
        // state; the binary format is also testable in one place.
        //
        // Wire format (little-endian, single-pass decodable):
        //   u32  : format version (current = 1)
        //   u64  : number of entries N
        //   then N × (u64 key_len | key_len bytes | u64 val_len | val_len bytes)
        let mut buf = Vec::with_capacity(12 + self.map.len() * 32);
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&(self.map.len() as u64).to_le_bytes());
        for (k, v) in &self.map {
            buf.extend_from_slice(&(k.len() as u64).to_le_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(v.len() as u64).to_le_bytes());
            buf.extend_from_slice(v);
        }
        Ok(buf)
    }

    fn restore(&mut self, bytes: &[u8]) -> EngineResult<()> {
        self.map.clear();
        if bytes.is_empty() {
            return Ok(());
        }
        let mut cursor = std::io::Cursor::new(bytes);
        // Read the version header (must be 1).
        let mut header = [0u8; 4];
        std::io::Read::read_exact(&mut cursor, &mut header)
            .map_err(|e| EngineError::State(format!("state snapshot header short read: {e}")))?;
        if u32::from_le_bytes(header) != 1 {
            return Err(EngineError::State(format!(
                "unsupported in-memory state snapshot version {}",
                u32::from_le_bytes(header)
            )));
        }
        // Read the entry count.
        let mut count_bytes = [0u8; 8];
        std::io::Read::read_exact(&mut cursor, &mut count_bytes)
            .map_err(|e| EngineError::State(format!("state snapshot count short read: {e}")))?;
        let count = u64::from_le_bytes(count_bytes) as usize;
        for _ in 0..count {
            // Each entry: u64 key_len, key, u64 val_len, value.
            let mut len_bytes = [0u8; 8];
            std::io::Read::read_exact(&mut cursor, &mut len_bytes).map_err(|e| {
                EngineError::State(format!("state snapshot key_len short read: {e}"))
            })?;
            let key_len = u64::from_le_bytes(len_bytes) as usize;
            let mut key = vec![0u8; key_len];
            std::io::Read::read_exact(&mut cursor, &mut key)
                .map_err(|e| EngineError::State(format!("state snapshot key short read: {e}")))?;
            std::io::Read::read_exact(&mut cursor, &mut len_bytes).map_err(|e| {
                EngineError::State(format!("state snapshot val_len short read: {e}"))
            })?;
            let val_len = u64::from_le_bytes(len_bytes) as usize;
            let mut val = vec![0u8; val_len];
            std::io::Read::read_exact(&mut cursor, &mut val)
                .map_err(|e| EngineError::State(format!("state snapshot value short read: {e}")))?;
            self.map.insert(key, val);
        }
        Ok(())
    }
}

// ── Checkpoint ──────────────────────────────────────────────────────────────

/// In-memory checkpoint store keyed by job id (latest epoch wins).
#[derive(Clone, Default)]
pub struct InMemoryCheckpointService {
    latest: Arc<Mutex<HashMap<String, CheckpointPayload>>>,
}

impl InMemoryCheckpointService {
    /// Create an empty checkpoint service.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CheckpointService for InMemoryCheckpointService {
    async fn persist(&self, job: &JobId, payload: &CheckpointPayload) -> EngineResult<()> {
        let mut g = self
            .latest
            .lock()
            .map_err(|_| EngineError::Checkpoint("checkpoint mutex poisoned".into()))?;
        g.insert(job.as_str().to_string(), payload.clone());
        Ok(())
    }

    async fn restore_latest(&self, job: &JobId) -> EngineResult<Option<CheckpointPayload>> {
        let g = self
            .latest
            .lock()
            .map_err(|_| EngineError::Checkpoint("checkpoint mutex poisoned".into()))?;
        Ok(g.get(job.as_str()).cloned())
    }
}

// ── Embedded runtime ────────────────────────────────────────────────────────

/// Build an embedded [`EngineRuntime`] backed by in-memory services.
///
/// The caller supplies the source and sink providers (so test data and
/// collected output stay reachable); state, checkpoint, and clock use the
/// in-memory defaults, and there is no shuffle (single task).
pub fn embedded_runtime(
    sources: Arc<dyn SourceProvider>,
    sinks: Arc<dyn SinkProvider>,
) -> EngineRuntime {
    EngineRuntime {
        placement: Placement::Embedded,
        sources,
        sinks,
        state: Arc::new(InMemoryStateBackend),
        checkpoint: Arc::new(InMemoryCheckpointService::new()),
        shuffle: None,
        query_executor: None,
        state_dir: None,
        clock: Arc::new(SystemClock),
    }
}

// ── Shuffle ───────────────────────────────────────────────────────────────────

/// In-memory [`ShuffleService`]: hash-partitions batches by key into a fixed
/// number of buckets. This is the local reference implementation of the shuffle
/// data-movement seam — the partitions stay in process — but the partitioning
/// contract (deterministic, value-based key hashing) is identical to a
/// distributed network shuffle, so a stateful operator's repartition step does
/// not change when the downstream task moves onto another node.
#[derive(Debug, Clone, Copy)]
pub struct InMemoryShuffle {
    partitions: usize,
}

impl InMemoryShuffle {
    /// Build an in-memory shuffle fanning out to `partitions` buckets (clamped
    /// to at least 1).
    pub fn new(partitions: usize) -> Self {
        Self {
            partitions: partitions.max(1),
        }
    }
}

impl ShuffleService for InMemoryShuffle {
    fn partitions(&self) -> usize {
        self.partitions
    }

    fn partition_by_key(
        &self,
        batch: &RecordBatch,
        key_indices: &[usize],
    ) -> EngineResult<Vec<RecordBatch>> {
        let schema = batch.schema();
        // Canonical, value-ordered row bytes for the key columns: equal values
        // produce identical bytes regardless of physical encoding, so the hash
        // is value-based rather than representation-based.
        let key_cols: Vec<ArrayRef> = key_indices
            .iter()
            .map(|&i| {
                batch.columns().get(i).cloned().ok_or_else(|| {
                    EngineError::Runtime(format!("shuffle key index {i} out of range"))
                })
            })
            .collect::<EngineResult<Vec<_>>>()?;
        let fields: Vec<SortField> = key_cols
            .iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect();
        let converter =
            RowConverter::new(fields).map_err(|e| EngineError::Runtime(e.to_string()))?;
        let rows = converter
            .convert_columns(&key_cols)
            .map_err(|e| EngineError::Runtime(e.to_string()))?;

        let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); self.partitions];
        for row in 0..batch.num_rows() {
            let part = (fnv1a_64(rows.row(row).as_ref()) % self.partitions as u64) as usize;
            let idx = u32::try_from(row)
                .map_err(|_| EngineError::Runtime("shuffle row index overflow".to_string()))?;
            // `part` is always < partitions (modulo), so the bucket exists.
            if let Some(bucket) = buckets.get_mut(part) {
                bucket.push(idx);
            }
        }

        buckets
            .into_iter()
            .map(|idxs| {
                if idxs.is_empty() {
                    return Ok(RecordBatch::new_empty(schema.clone()));
                }
                let index = arrow::array::UInt32Array::from(idxs);
                let cols = batch
                    .columns()
                    .iter()
                    .map(|c| arrow::compute::take(c.as_ref(), &index, None))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| EngineError::Runtime(e.to_string()))?;
                RecordBatch::try_new(schema.clone(), cols)
                    .map_err(|e| EngineError::Runtime(e.to_string()))
            })
            .collect()
    }
}

/// Deterministic FNV-1a 64-bit hash. A fixed algorithm (not a randomly-seeded
/// `Hasher`) is required so equal key bytes map to the same partition across
/// processes — the property a distributed shuffle depends on.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::changelog::ChangelogBatch;

    fn batch(v: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![v]))]).unwrap()
    }

    #[tokio::test]
    async fn source_reads_then_idles_and_checkpoints_offset() {
        let sp = InMemorySourceProvider::new();
        sp.insert("t", vec![batch(1), batch(2)]);
        let spec = SourceSpec::bounded("t", "memory", "");
        let mut reader = sp.open(&spec).await.unwrap();

        assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
        let off = reader.checkpoint_offset().unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
        assert!(reader.next().await.unwrap().is_none());

        // Restore to the saved offset and re-read the second batch.
        reader.restore_offset(&off).unwrap();
        let replay = reader.next().await.unwrap().unwrap();
        assert_eq!(
            replay
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[tokio::test]
    async fn sink_collects_changelog_per_view() {
        let sink = InMemorySinkProvider::new();
        let spec = SinkSpec::new("out", "memory", "");
        let mut w = sink.open(&spec).await.unwrap();
        w.write(ChangelogBatch::inserts(batch(7))).await.unwrap();
        w.flush().await.unwrap();
        let got = sink.take("out");
        assert_eq!(got.len(), 1);
        assert_eq!(got.first().map(ChangelogBatch::num_rows), Some(1));
    }

    fn kv(keys: &[&str], vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn in_memory_shuffle_partitions_keys_deterministically() {
        use std::collections::HashSet;

        use arrow::array::Array;

        let shuffle = InMemoryShuffle::new(3);
        assert_eq!(shuffle.partitions(), 3);
        let batch = kv(&["a", "b", "a", "c", "b", "a"], &[1, 2, 3, 4, 5, 6]);

        let parts = shuffle.partition_by_key(&batch, &[0]).unwrap();
        assert_eq!(parts.len(), 3, "one batch per partition");

        // No row is lost or duplicated across the partitions.
        let total: usize = parts.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, batch.num_rows());

        // Re-partitioning the same data is byte-for-byte stable (deterministic).
        let again = shuffle.partition_by_key(&batch, &[0]).unwrap();
        let sizes: Vec<usize> = parts.iter().map(RecordBatch::num_rows).collect();
        let sizes2: Vec<usize> = again.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(sizes, sizes2);

        // Every row of a given key lands in exactly one partition (co-location).
        let mut seen: HashSet<String> = HashSet::new();
        for part in &parts {
            if part.num_rows() == 0 {
                continue;
            }
            let col = part
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let here: HashSet<String> = (0..col.len()).map(|i| col.value(i).to_string()).collect();
            for k in &here {
                assert!(!seen.contains(k), "key {k} split across partitions");
            }
            seen.extend(here);
        }
        assert_eq!(seen.len(), 3, "keys a, b, c each routed to one partition");
    }

    #[tokio::test]
    async fn upsert_sink_applies_retractions_by_key() {
        let sink = InMemoryUpsertSink::new(vec![0]); // key on column "k"
        let schema = kv(&[], &[]).schema();
        let mut w = sink.open(&SinkSpec::new("t", "memory", "")).await.unwrap();

        // Initial inserts: a=1, b=2.
        w.write(ChangelogBatch::inserts(kv(&["a", "b"], &[1, 2])))
            .await
            .unwrap();
        assert_eq!(sink.len(), 2);

        // Update a: retract (a,1) then insert (a,10); b is untouched.
        let cl = ChangelogBatch::new(
            kv(&["a", "a"], &[1, 10]),
            vec![RowKind::Delete, RowKind::Insert],
        )
        .unwrap();
        w.write(cl).await.unwrap();

        let table = sink.table(&schema).unwrap();
        assert_eq!(table.num_rows(), 2);
        let vs = table
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Keys sort a < b: a was upserted to 10, b stayed 2.
        assert_eq!(vs.value(0), 10);
        assert_eq!(vs.value(1), 2);

        // Delete b → only a remains.
        let cl = ChangelogBatch::new(kv(&["b"], &[2]), vec![RowKind::Delete]).unwrap();
        w.write(cl).await.unwrap();
        assert_eq!(sink.len(), 1);
    }

    #[test]
    fn keyed_state_snapshot_restore_roundtrips() {
        let backend = InMemoryStateBackend;
        let mut s = backend.open_keyed("ns").unwrap();
        s.put(b"a", b"1").unwrap();
        s.put(b"b", b"2").unwrap();
        let snap = s.snapshot().unwrap();

        let mut s2 = backend.open_keyed("ns").unwrap();
        s2.restore(&snap).unwrap();
        assert_eq!(s2.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(s2.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
        s2.delete(b"a").unwrap();
        assert_eq!(s2.get(b"a").unwrap(), None);
    }

    #[tokio::test]
    async fn checkpoint_service_returns_latest() {
        let svc = InMemoryCheckpointService::new();
        let job = JobId::try_new("job-1").unwrap();
        assert!(svc.restore_latest(&job).await.unwrap().is_none());

        let payload = CheckpointPayload {
            epoch: 4,
            operator_state: vec![1, 2, 3],
            source_offsets: vec![("t".to_string(), vec![9])],
            in_flight: vec![],
            source_in_flight: vec![],
        };
        svc.persist(&job, &payload).await.unwrap();
        assert_eq!(svc.restore_latest(&job).await.unwrap(), Some(payload));
    }

    #[test]
    fn embedded_runtime_has_no_shuffle() {
        let rt = embedded_runtime(
            Arc::new(InMemorySourceProvider::new()),
            Arc::new(InMemorySinkProvider::new()),
        );
        assert_eq!(rt.placement, Placement::Embedded);
        assert!(rt.shuffle.is_none());
        assert!(!rt.is_distributed());
    }
}
