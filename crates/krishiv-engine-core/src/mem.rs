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
    CheckpointPayload, CheckpointService, EngineRuntime, KeyedState, Placement, SinkProvider,
    SinkWriter, SourceProvider, SourceReader, StateBackendFactory, SystemClock,
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
        if let Ok(mut g) = self.data.lock() {
            g.insert(name.into(), batches);
        }
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
    collected: Arc<Mutex<HashMap<String, Vec<ChangelogBatch>>>>,
}

impl InMemorySinkProvider {
    /// Create an empty sink provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Remove and return everything written for `view` so far.
    pub fn take(&self, view: &str) -> Vec<ChangelogBatch> {
        self.collected
            .lock()
            .map(|mut m| m.remove(view).unwrap_or_default())
            .unwrap_or_default()
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
    collected: Arc<Mutex<HashMap<String, Vec<ChangelogBatch>>>>,
}

#[async_trait]
impl SinkWriter for InMemorySinkWriter {
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()> {
        let mut g = self
            .collected
            .lock()
            .map_err(|_| EngineError::Sink("sink mutex poisoned".into()))?;
        g.entry(self.view.clone()).or_default().push(changes);
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
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = self
            .map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        serde_json::to_vec(&pairs).map_err(|e| EngineError::State(e.to_string()))
    }

    fn restore(&mut self, bytes: &[u8]) -> EngineResult<()> {
        if bytes.is_empty() {
            self.map.clear();
            return Ok(());
        }
        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
            serde_json::from_slice(bytes).map_err(|e| EngineError::State(e.to_string()))?;
        self.map = pairs.into_iter().collect();
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
