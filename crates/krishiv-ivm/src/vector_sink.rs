#![forbid(unsafe_code)]

//! Incremental vector index maintenance via a pluggable `IvmVectorSink`.
//!
//! Users pre-compute embedding vectors and store them as a column in their
//! source data.  Krishiv maintains the vector index incrementally:
//!
//! * `+1` weight rows in view output → `upsert_batch`
//! * `-1` weight rows in view output → `delete_batch`
//!
//! No ML model deployment, no embedding API calls — the caller owns the
//! embedding pipeline and passes pre-computed `Float32` vectors as an Arrow
//! column.
//!
//! # Maturity: HTTP-only preview (IVM-AUD-INT-F17)
//!
//! This module is reachable from exactly two places: this crate's
//! `spawn_vector_view` / `PartitionedIncrementalFlow::spawn_vector_views`, and
//! the coordinator's `POST` / `GET` / `DELETE
//! /api/v1/ivm/jobs/{job}/vector-views` endpoints, whose only supported
//! `sink_type` is `in_memory`. There is **no** CLI, Python, MCP
//! or SQL surface, `krishiv-ivm` is not part of the stable public Rust API
//! (`api/rust-public-api.txt` contains no `krishiv_ivm` entry), and the
//! `VectorSinkBridge` adapter in `krishiv-runtime` that would connect a real
//! Qdrant / pgvector store has no caller other than its own test. Treat this as
//! a preview: the maintenance loop is real and tested, the productised surface
//! around it is not there yet.
//!
//! # Usage
//!
//! ```rust,ignore
//! let sink = Arc::new(MyVectorStore::new(...));
//! let handle = spawn_vector_view(
//!     &flow,
//!     VectorViewSpec {
//!         view_name: "doc_embeddings".into(),
//!         id_column: "doc_id".into(),
//!         vector_column: "embedding".into(),  // FixedSizeList<Float32> column
//!         sink,
//!     },
//! )?;
//! // keep the handle alive; dropping it aborts the background task
//! let health = handle.health().status();
//! ```

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use ahash::AHashSet;
use arrow::array::{Array, FixedSizeListArray, Float32Array, StringArray};

use krishiv_delta::DeltaBatch;
use tokio::sync::broadcast;

use crate::error::{IvmError, IvmResult};
use crate::flow::IncrementalFlow;

// ── IvmVectorSink trait ───────────────────────────────────────────────────────

pub type VectorFuture<'a> = Pin<Box<dyn std::future::Future<Output = IvmResult<()>> + Send + 'a>>;

/// Pluggable vector index sink for incremental maintenance.
///
/// Implementors **must** map `ids` to their store's point IDs and perform
/// idempotent upserts / deletes: the maintenance loop retries a failed call
/// ([`SINK_RETRY_ATTEMPTS`] attempts), so a call that partially succeeded before
/// erroring will be issued again.
pub trait IvmVectorSink: Send + Sync + 'static {
    /// Upsert `ids[i]` → `vectors[i]` for all rows with positive weight.
    fn upsert_batch<'a>(&'a self, ids: &'a [String], vectors: &'a [Vec<f32>]) -> VectorFuture<'a>;
    /// Delete the points identified by `ids` (negative-weight rows).
    fn delete_batch<'a>(&'a self, ids: &'a [String]) -> VectorFuture<'a>;
}

// ── VectorViewSpec ────────────────────────────────────────────────────────────

/// Specification for an incremental vector view.
pub struct VectorViewSpec {
    /// Name of the IVM view whose output drives vector index updates.
    pub view_name: String,
    /// Column name containing the string point ID.
    pub id_column: String,
    /// Column name containing the embedding vector (`FixedSizeList<Float32>`).
    pub vector_column: String,
    /// The vector store sink.
    pub sink: Arc<dyn IvmVectorSink>,
}

// ── Health / status surface ───────────────────────────────────────────────────

/// Total attempts (first try + retries) for one sink call.
pub const SINK_RETRY_ATTEMPTS: u32 = 3;
/// Base backoff between sink retries; attempt *n* waits `n * BASE`.
const SINK_RETRY_BASE_DELAY: Duration = Duration::from_millis(50);

/// Operator-visible state of one running vector view.
///
/// IVM-AUD-PART-19 / PART-20: a sink error used to be a `tracing::warn!` and
/// nothing else — no retry, no counter, no way to ask whether the index still
/// matched the view — and a lagging broadcast receiver logged and carried on
/// updating an index that had already lost deltas. Both now land here.
///
/// `diverged` is the flag that matters: once it is set, the vector index is
/// known **not** to be a faithful image of the view and has to be rebuilt from
/// a full scan. It is never cleared.
#[derive(Debug, Default)]
pub struct VectorViewHealth {
    deltas_applied: AtomicU64,
    rows_upserted: AtomicU64,
    rows_deleted: AtomicU64,
    sink_errors: AtomicU64,
    deltas_missed: AtomicU64,
    diverged: AtomicBool,
    last_error: Mutex<Option<String>>,
    stopped_reason: Mutex<Option<String>>,
}

/// A point-in-time copy of [`VectorViewHealth`], for serialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct VectorViewStatus {
    pub deltas_applied: u64,
    pub rows_upserted: u64,
    pub rows_deleted: u64,
    pub sink_errors: u64,
    pub deltas_missed: u64,
    /// True when the index is known to no longer match the view.
    pub diverged: bool,
    pub last_error: Option<String>,
    /// `Some(reason)` once the maintenance task has terminated.
    pub stopped_reason: Option<String>,
}

impl VectorViewHealth {
    fn set_string(slot: &Mutex<Option<String>>, value: String) {
        *slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(value);
    }

    fn read_string(slot: &Mutex<Option<String>>) -> Option<String> {
        slot.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn record_applied(&self, upserted: usize, deleted: usize) {
        self.deltas_applied.fetch_add(1, Ordering::Relaxed);
        self.rows_upserted
            .fetch_add(upserted as u64, Ordering::Relaxed);
        self.rows_deleted
            .fetch_add(deleted as u64, Ordering::Relaxed);
    }

    /// A delta that could not be applied. The index has diverged from the view.
    fn record_failure(&self, message: String) {
        self.sink_errors.fetch_add(1, Ordering::Relaxed);
        self.diverged.store(true, Ordering::Relaxed);
        Self::set_string(&self.last_error, message);
    }

    /// `n` deltas were dropped by the broadcast channel before this task read
    /// them. They are unrecoverable: the index has diverged.
    fn record_missed(&self, n: u64) {
        self.deltas_missed.fetch_add(n, Ordering::Relaxed);
        self.diverged.store(true, Ordering::Relaxed);
    }

    fn record_stopped(&self, reason: impl Into<String>) {
        Self::set_string(&self.stopped_reason, reason.into());
    }

    /// Number of deltas successfully applied to the sink.
    pub fn deltas_applied(&self) -> u64 {
        self.deltas_applied.load(Ordering::Relaxed)
    }
    pub fn rows_upserted(&self) -> u64 {
        self.rows_upserted.load(Ordering::Relaxed)
    }
    pub fn rows_deleted(&self) -> u64 {
        self.rows_deleted.load(Ordering::Relaxed)
    }
    /// Deltas that exhausted [`SINK_RETRY_ATTEMPTS`] (or failed to decode) and
    /// were therefore never applied.
    pub fn sink_errors(&self) -> u64 {
        self.sink_errors.load(Ordering::Relaxed)
    }
    /// Deltas dropped by the broadcast channel before this task could read them.
    pub fn deltas_missed(&self) -> u64 {
        self.deltas_missed.load(Ordering::Relaxed)
    }
    /// True once the index is known not to match the view. Never cleared.
    pub fn is_diverged(&self) -> bool {
        self.diverged.load(Ordering::Relaxed)
    }
    pub fn last_error(&self) -> Option<String> {
        Self::read_string(&self.last_error)
    }
    /// `Some(reason)` once the maintenance task has terminated.
    pub fn stopped_reason(&self) -> Option<String> {
        Self::read_string(&self.stopped_reason)
    }

    /// Copy every counter into a serializable struct.
    pub fn status(&self) -> VectorViewStatus {
        VectorViewStatus {
            deltas_applied: self.deltas_applied(),
            rows_upserted: self.rows_upserted(),
            rows_deleted: self.rows_deleted(),
            sink_errors: self.sink_errors(),
            deltas_missed: self.deltas_missed(),
            diverged: self.is_diverged(),
            last_error: self.last_error(),
            stopped_reason: self.stopped_reason(),
        }
    }
}

/// Handle to a running vector-view maintenance task.
///
/// Dropping the handle aborts the task — the module doc has always said "drop
/// to stop the background task", which was false for a bare `tokio::JoinHandle`
/// (dropping one detaches the task and it runs forever). Holding the handle is
/// therefore what keeps the vector view alive, and whoever holds it can read
/// [`health`](Self::health).
#[derive(Debug)]
pub struct VectorViewHandle {
    task: tokio::task::JoinHandle<()>,
    health: Arc<VectorViewHealth>,
}

impl VectorViewHandle {
    /// Counters and divergence flag for this vector view.
    pub fn health(&self) -> &Arc<VectorViewHealth> {
        &self.health
    }

    /// Stop the maintenance task now.
    pub fn abort(&self) {
        self.task.abort();
    }

    /// True once the maintenance task has ended (stopped, aborted, or panicked).
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

impl Drop for VectorViewHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// ── spawn_vector_view ─────────────────────────────────────────────────────────

/// Spawn a background Tokio task that watches `spec.view_name` output and
/// forwards insertions / retractions to the configured vector sink.
///
/// Returns a [`VectorViewHandle`]; **keep it alive** — dropping it aborts the
/// task. Read `handle.health()` to see whether the index still matches the view.
pub fn spawn_vector_view(
    flow: &IncrementalFlow,
    spec: VectorViewSpec,
) -> IvmResult<VectorViewHandle> {
    let mut rx = flow.view_output_stream(&spec.view_name)?;
    let sink = spec.sink;
    let id_col = spec.id_column;
    let vec_col = spec.vector_column;

    let view_name = spec.view_name;
    let health = Arc::new(VectorViewHealth::default());
    let task_health = Arc::clone(&health);

    let task = tokio::spawn(async move {
        loop {
            // Audit: this used to read a `watch` receiver, which keeps only the
            // latest value — every delta published while this task was awaiting
            // the sink was skipped outright, so an upsert or a delete could
            // never reach the vector index and nothing reported it. The
            // broadcast stream delivers each delta; overflow is `Lagged`.
            let delta = match rx.recv().await {
                Ok(d) => d,
                Err(broadcast::error::RecvError::Closed) => {
                    task_health.record_stopped("view delta stream closed (flow dropped)");
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    // IVM-AUD-PART-20: this used to `continue`. The deltas the
                    // channel dropped are unrecoverable — we cannot know which
                    // ids they upserted or deleted — so carrying on produces an
                    // index that looks live and is permanently wrong. There is
                    // no backpressure to fall back on (the step engine never
                    // waits on this task), so the honest response is to stop:
                    // the divergence becomes terminal and visible instead of
                    // silent and growing. Recovery is a full rebuild of the
                    // index followed by a fresh `spawn_vector_view`.
                    task_health.record_missed(n);
                    task_health.record_stopped(format!(
                        "fell behind the delta stream and lost {n} deltas; the index must be \
                         rebuilt and the vector view re-registered"
                    ));
                    tracing::error!(
                        view = %view_name,
                        skipped = n,
                        "vector view fell behind the delta stream; the index is now \
                         missing {n} deltas — stopping so it cannot diverge further"
                    );
                    break;
                }
            };
            match apply_delta_to_sink(sink.as_ref(), &delta, &id_col, &vec_col, &view_name).await {
                Ok((upserted, deleted)) => task_health.record_applied(upserted, deleted),
                Err(e) => {
                    // IVM-AUD-PART-19: previously `tracing::warn!` and nothing
                    // else. The delta is lost after the retries above, so the
                    // index has diverged; record it where an operator can see
                    // it (`GET .../vector-views`) instead of in a log line
                    // nobody is watching. Unlike a lag, the loss is bounded and
                    // named — the failing delta is in the log — so the task
                    // keeps going and the rest of the index stays current.
                    task_health.record_failure(e.to_string());
                    tracing::error!(
                        view = %view_name,
                        error = %e,
                        "vector sink delta permanently failed; the index has diverged \
                         from the view"
                    );
                }
            }
        }
    });

    Ok(VectorViewHandle { task, health })
}

// ── Internal: apply one DeltaBatch to a sink ─────────────────────────────────

/// Retry a sink call up to [`SINK_RETRY_ATTEMPTS`] times with linear backoff.
///
/// Safe because `IvmVectorSink` requires idempotent upserts and deletes.
async fn with_retry<'a>(
    view_name: &str,
    op: &'static str,
    mut call: impl FnMut() -> VectorFuture<'a>,
) -> IvmResult<()> {
    let mut attempt: u32 = 1;
    loop {
        match call().await {
            Ok(()) => return Ok(()),
            Err(e) if attempt < SINK_RETRY_ATTEMPTS => {
                tracing::warn!(
                    view = %view_name,
                    op,
                    attempt,
                    error = %e,
                    "vector sink call failed; retrying"
                );
                tokio::time::sleep(SINK_RETRY_BASE_DELAY * attempt).await;
                attempt += 1;
            }
            Err(e) => {
                return Err(IvmError::execution(format!(
                    "vector view '{view_name}': {op} failed after {SINK_RETRY_ATTEMPTS} \
                     attempts: {e}"
                )));
            }
        }
    }
}

/// Apply one delta, returning `(rows_upserted, rows_deleted)`.
async fn apply_delta_to_sink(
    sink: &dyn IvmVectorSink,
    delta: &DeltaBatch,
    id_col: &str,
    vec_col: &str,
    view_name: &str,
) -> IvmResult<(usize, usize)> {
    let (upsert_ids, upsert_vecs, delete_ids) = extract_vector_rows(delta, id_col, vec_col)?;
    // IVM-AUD-PART-17: deletes run first. An update arrives as one delta
    // carrying `-1 old` and `+1 new` for the *same* id, so the id used to land
    // in both lists — and with upserts first the delete then removed the row the
    // upsert had just written, dropping every updated id out of the index with
    // nothing reported. `extract_vector_rows` now keeps the two id sets
    // disjoint (an id being upserted is never also deleted), which is what makes
    // an update non-lossy: the upsert overwrites the point in place, so the row
    // is never momentarily absent and a failed upsert leaves the stale vector
    // rather than no vector at all. With the sets disjoint the order below is no
    // longer load-bearing; deletes still go first so that a future change that
    // re-introduced an overlap would re-add a row rather than lose one.
    if !delete_ids.is_empty() {
        with_retry(view_name, "delete_batch", || sink.delete_batch(&delete_ids)).await?;
    }
    if !upsert_ids.is_empty() {
        with_retry(view_name, "upsert_batch", || {
            sink.upsert_batch(&upsert_ids, &upsert_vecs)
        })
        .await?;
    }
    Ok((upsert_ids.len(), delete_ids.len()))
}

/// Split a `DeltaBatch` into (upsert_ids, upsert_vecs, delete_ids).
///
/// Positive-weight rows → upsert; negative-weight rows → delete. An id that
/// appears on both sides (a same-id update: `-1 old`, `+1 new`) is upserted
/// only — see the note in [`apply_delta_to_sink`].
#[allow(clippy::type_complexity)]
fn extract_vector_rows(
    delta: &DeltaBatch,
    id_col: &str,
    vec_col: &str,
) -> IvmResult<(Vec<String>, Vec<Vec<f32>>, Vec<String>)> {
    let data = delta.data_batch();
    let weights = delta.weights();

    let id_idx = data.schema().index_of(id_col).map_err(|_| {
        IvmError::execution(format!(
            "vector view: id column '{id_col}' not found in view output"
        ))
    })?;
    let vec_idx = data.schema().index_of(vec_col).map_err(|_| {
        IvmError::execution(format!(
            "vector view: vector column '{vec_col}' not found in view output"
        ))
    })?;

    let id_arr = data.column(id_idx);
    let vec_arr = data.column(vec_idx);

    let mut upsert_ids: Vec<String> = Vec::new();
    let mut upsert_vecs: Vec<Vec<f32>> = Vec::new();
    let mut delete_ids: Vec<String> = Vec::new();

    for row in 0..data.num_rows() {
        let w = weights.value(row);
        if w == 0 {
            continue;
        }
        let id = extract_string_at(id_arr.as_ref(), row)?;
        if w > 0 {
            let vec = extract_f32_list_at(vec_arr.as_ref(), row)?;
            upsert_ids.push(id);
            upsert_vecs.push(vec);
        } else {
            delete_ids.push(id);
        }
    }

    // IVM-AUD-PART-17: keep the two id sets disjoint.
    if !delete_ids.is_empty() && !upsert_ids.is_empty() {
        let upserted: AHashSet<&str> = upsert_ids.iter().map(String::as_str).collect();
        delete_ids.retain(|id| !upserted.contains(id.as_str()));
    }

    Ok((upsert_ids, upsert_vecs, delete_ids))
}

fn extract_string_at(arr: &dyn Array, row: usize) -> IvmResult<String> {
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        if a.is_null(row) {
            return Err(IvmError::execution("vector view: null id value"));
        }
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        if a.is_null(row) {
            return Err(IvmError::execution("vector view: null id value"));
        }
        return Ok(a.value(row).to_string());
    }
    // IVM-AUD-PART-18: `Utf8View` is DataFusion's default string representation,
    // so without this arm *every* row of a string-id view errored — and the
    // caller only logged it. `krishiv-common`'s partition key hashing was fixed
    // for exactly this (`partition.rs::digest_for_key`); this is the same fix.
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringViewArray>() {
        if a.is_null(row) {
            return Err(IvmError::execution("vector view: null id value"));
        }
        return Ok(a.value(row).to_string());
    }
    // Fallback: coerce via Int64
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        if a.is_null(row) {
            return Err(IvmError::execution("vector view: null id value"));
        }
        return Ok(a.value(row).to_string());
    }
    Err(IvmError::execution(format!(
        "vector view: id column has unsupported type {:?}",
        arr.data_type()
    )))
}

fn extract_f32_list_at(arr: &dyn Array, row: usize) -> IvmResult<Vec<f32>> {
    if let Some(fsl) = arr.as_any().downcast_ref::<FixedSizeListArray>() {
        let value = fsl.value(row);
        let f32s = value
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                IvmError::execution("vector view: FixedSizeList element type must be Float32")
            })?;
        return Ok((0..f32s.len()).map(|i| f32s.value(i)).collect());
    }
    // ListArray<Float32>
    if let Some(la) = arr.as_any().downcast_ref::<arrow::array::ListArray>() {
        let value = la.value(row);
        let f32s = value
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| IvmError::execution("vector view: List element type must be Float32"))?;
        return Ok((0..f32s.len()).map(|i| f32s.value(i)).collect());
    }
    Err(IvmError::execution(format!(
        "vector view: vector column has unsupported type {:?}; expected FixedSizeList<Float32>",
        arr.data_type()
    )))
}

#[cfg(test)]
mod extract_tests {
    use super::*;
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema};

    /// Regression (crate-12 audit): a null Int64 id must error like the null
    /// string paths do — previously `a.value(row)` on a null slot silently
    /// produced the id "0", corrupting the vector index.
    #[test]
    fn null_int64_id_errors_instead_of_becoming_zero() {
        let arr = arrow::array::Int64Array::from(vec![Some(7), None]);
        assert_eq!(extract_string_at(&arr, 0).unwrap(), "7");
        assert!(extract_string_at(&arr, 1).is_err(), "null id must error");
    }

    /// IVM-AUD-PART-18: `Utf8View` is what DataFusion emits for string columns.
    /// Without an arm for it every row of a string-id vector view errored out of
    /// `extract_vector_rows`, and the caller only logged the error — so the
    /// index silently received nothing at all.
    #[test]
    fn utf8view_id_is_extracted_like_utf8() {
        let arr = arrow::array::StringViewArray::from(vec![Some("doc-1"), None]);
        assert_eq!(
            extract_string_at(&arr, 0).unwrap(),
            "doc-1",
            "Utf8View is DataFusion's default string type and must be supported"
        );
        assert!(
            extract_string_at(&arr, 1).is_err(),
            "a null Utf8View id must error like the other id types"
        );
    }

    fn one_row(id: &str, v: [f32; 2]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "v",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
        ]));
        let vectors = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(Float32Array::from(v.to_vec())),
            None,
        );
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![id])),
                Arc::new(vectors) as _,
            ],
        )
        .unwrap()
    }

    /// `-1` the old row, `+1` the new one — exactly what an update emits.
    fn update_delta() -> DeltaBatch {
        DeltaBatch::from_update(&one_row("a", [1.0, 2.0]), &one_row("a", [9.0, 9.0])).unwrap()
    }

    /// IVM-AUD-PART-17: an update delta carries `-1 old` and `+1 new` for the
    /// same id. Before the fix the id landed in both lists and the delete wiped
    /// the row the upsert had written.
    #[test]
    fn an_id_that_is_upserted_is_never_also_deleted() {
        let (upserts, vecs, deletes) = extract_vector_rows(&update_delta(), "id", "v").unwrap();
        assert_eq!(upserts, vec!["a".to_string()]);
        assert_eq!(vecs, vec![vec![9.0, 9.0]]);
        assert!(
            deletes.is_empty(),
            "an id being upserted must not also be handed to delete_batch, got {deletes:?}"
        );
    }

    /// The retraction-only case still deletes.
    #[test]
    fn a_retraction_only_delta_still_deletes() {
        let only_retract = DeltaBatch::from_deletes(one_row("a", [1.0, 2.0])).unwrap();
        let (upserts, _, deletes) = extract_vector_rows(&only_retract, "id", "v").unwrap();
        assert!(upserts.is_empty());
        assert_eq!(deletes, vec!["a".to_string()]);
    }
}

#[cfg(test)]
mod stream_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use arrow::array::{FixedSizeListArray, Float32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_delta::{DeltaBatch, IncrementalViewSpec};

    use super::testing::InMemoryVectorSink;
    use super::{IvmVectorSink, VectorFuture, VectorViewSpec, spawn_vector_view};
    use crate::error::IvmError;
    use crate::flow::IncrementalFlow;

    fn vec_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "v",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
        ]))
    }

    fn batch_of(ids: &[&str], vs: &[[f32; 2]]) -> RecordBatch {
        let flat: Vec<f32> = vs.iter().flat_map(|v| v.iter().copied()).collect();
        let vectors = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(Float32Array::from(flat)),
            None,
        );
        RecordBatch::try_new(
            vec_schema(),
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(vectors) as _,
            ],
        )
        .unwrap()
    }

    fn row(id: &str, v: [f32; 2]) -> DeltaBatch {
        DeltaBatch::from_inserts(batch_of(&[id], &[v])).unwrap()
    }

    fn docs_flow() -> IncrementalFlow {
        let flow = IncrementalFlow::new();
        flow.register_view(IncrementalViewSpec {
            name: "docs".into(),
            body_sql: "SELECT * FROM src".into(),
            output_schema: vec_schema(),
            is_materialized: false,
            is_recursive: false,
            lateness: Vec::new(),
        })
        .unwrap();
        flow
    }

    fn spec_for(sink: Arc<dyn IvmVectorSink>) -> VectorViewSpec {
        VectorViewSpec {
            view_name: "docs".into(),
            id_column: "id".into(),
            vector_column: "v".into(),
            sink,
        }
    }

    async fn wait_until(mut done: impl FnMut() -> bool) -> bool {
        for _ in 0..2000 {
            if done() {
                return true;
            }
            tokio::task::yield_now().await;
        }
        done()
    }

    /// Regression (post-register audit): every delta must reach the vector sink.
    ///
    /// Two ticks are applied back to back, so both are published before the sink
    /// task gets a chance to poll. The view's `watch` channel retains only the
    /// latest value, so the pre-fix loop saw only the second delta and `a` was
    /// never indexed — silently, with no error on any path. The lossless
    /// broadcast stream delivers both.
    #[tokio::test]
    async fn every_delta_reaches_the_vector_sink_even_when_ticks_outpace_it() {
        let flow = docs_flow();
        let sink = InMemoryVectorSink::new();
        let handle =
            spawn_vector_view(&flow, spec_for(Arc::clone(&sink) as Arc<dyn IvmVectorSink>))
                .unwrap();

        for (id, v) in [("a", [1.0, 2.0]), ("b", [3.0, 4.0])] {
            flow.apply_remote_tick(
                HashMap::new(),
                HashMap::from([("docs".to_string(), row(id, v))]),
            )
            .unwrap();
        }

        wait_until(|| sink.len() == 2).await;
        handle.abort();

        assert_eq!(
            sink.get("a"),
            Some(vec![1.0, 2.0]),
            "the first delta must not be coalesced away by the second"
        );
        assert_eq!(sink.get("b"), Some(vec![3.0, 4.0]));
    }

    /// IVM-AUD-PART-17 end to end: a same-id update (`-1 old`, `+1 new`) must
    /// leave the id in the index with the new vector. Pre-fix the delete ran
    /// after the upsert against the same id and removed it outright.
    #[tokio::test]
    async fn an_update_delta_leaves_the_id_in_the_index_with_the_new_vector() {
        let flow = docs_flow();
        let sink = InMemoryVectorSink::new();
        let handle =
            spawn_vector_view(&flow, spec_for(Arc::clone(&sink) as Arc<dyn IvmVectorSink>))
                .unwrap();

        flow.apply_remote_tick(
            HashMap::new(),
            HashMap::from([("docs".to_string(), row("a", [1.0, 2.0]))]),
        )
        .unwrap();
        wait_until(|| sink.get("a").is_some()).await;

        // The update: retract the old row, insert the new one, one delta.
        let update = DeltaBatch::from_update(
            &batch_of(&["a"], &[[1.0, 2.0]]),
            &batch_of(&["a"], &[[7.0, 8.0]]),
        )
        .unwrap();
        flow.apply_remote_tick(
            HashMap::new(),
            HashMap::from([("docs".to_string(), update)]),
        )
        .unwrap();
        wait_until(|| sink.get("a") == Some(vec![7.0, 8.0])).await;
        handle.abort();

        assert_eq!(
            sink.get("a"),
            Some(vec![7.0, 8.0]),
            "an update must leave the id indexed with the new vector, not delete it"
        );
    }

    /// A sink that fails its first `fail_first` calls, then succeeds.
    #[derive(Debug, Default)]
    struct FlakySink {
        fail_first: u32,
        calls: AtomicU32,
        inner: super::testing::InMemoryVectorSink,
    }

    impl FlakySink {
        fn should_fail(&self) -> bool {
            self.calls.fetch_add(1, Ordering::SeqCst) < self.fail_first
        }
    }

    impl IvmVectorSink for FlakySink {
        fn upsert_batch<'a>(
            &'a self,
            ids: &'a [String],
            vectors: &'a [Vec<f32>],
        ) -> VectorFuture<'a> {
            Box::pin(async move {
                if self.should_fail() {
                    return Err(IvmError::execution("transient store failure"));
                }
                self.inner.upsert_batch(ids, vectors).await
            })
        }

        fn delete_batch<'a>(&'a self, ids: &'a [String]) -> VectorFuture<'a> {
            Box::pin(async move {
                if self.should_fail() {
                    return Err(IvmError::execution("transient store failure"));
                }
                self.inner.delete_batch(ids).await
            })
        }
    }

    /// IVM-AUD-PART-19 (retry half): a transient sink failure must not cost the
    /// delta. Pre-fix there was no retry at all — one blip and the row was gone
    /// from the index for good.
    #[tokio::test]
    async fn a_transient_sink_failure_is_retried_and_the_row_still_lands() {
        let flow = docs_flow();
        let sink = Arc::new(FlakySink {
            fail_first: 2,
            ..Default::default()
        });
        let handle =
            spawn_vector_view(&flow, spec_for(Arc::clone(&sink) as Arc<dyn IvmVectorSink>))
                .unwrap();

        flow.apply_remote_tick(
            HashMap::new(),
            HashMap::from([("docs".to_string(), row("a", [1.0, 2.0]))]),
        )
        .unwrap();

        for _ in 0..200 {
            if sink.inner.get("a").is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let health = handle.health().status();
        handle.abort();

        assert_eq!(
            sink.inner.get("a"),
            Some(vec![1.0, 2.0]),
            "two transient failures must be retried, not dropped"
        );
        assert_eq!(health.sink_errors, 0, "a retried success is not an error");
        assert!(
            !health.diverged,
            "a retried success must not mark divergence"
        );
        assert_eq!(health.deltas_applied, 1);
    }

    /// A sink that always fails.
    #[derive(Debug, Default)]
    struct DeadSink;

    impl IvmVectorSink for DeadSink {
        fn upsert_batch<'a>(&'a self, _: &'a [String], _: &'a [Vec<f32>]) -> VectorFuture<'a> {
            Box::pin(async { Err(IvmError::execution("vector store is down")) })
        }
        fn delete_batch<'a>(&'a self, _: &'a [String]) -> VectorFuture<'a> {
            Box::pin(async { Err(IvmError::execution("vector store is down")) })
        }
    }

    /// IVM-AUD-PART-19 (visibility half): once the retries are exhausted the
    /// delta is lost and the index no longer matches the view. Pre-fix that was
    /// a `tracing::warn!` and nothing else — no counter, no flag, nothing an
    /// operator could query.
    #[tokio::test]
    async fn an_exhausted_sink_failure_is_counted_and_marks_the_index_diverged() {
        let flow = docs_flow();
        let handle = spawn_vector_view(
            &flow,
            spec_for(Arc::new(DeadSink) as Arc<dyn IvmVectorSink>),
        )
        .unwrap();

        flow.apply_remote_tick(
            HashMap::new(),
            HashMap::from([("docs".to_string(), row("a", [1.0, 2.0]))]),
        )
        .unwrap();

        for _ in 0..200 {
            if handle.health().sink_errors() > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let status = handle.health().status();
        handle.abort();

        assert_eq!(status.sink_errors, 1, "the lost delta must be counted");
        assert!(
            status.diverged,
            "a permanently failed delta means the index no longer matches the view"
        );
        assert!(
            status
                .last_error
                .as_deref()
                .unwrap_or_default()
                .contains("vector store is down"),
            "the operator must be able to read why: {:?}",
            status.last_error
        );
    }

    /// IVM-AUD-PART-20: overflowing the broadcast buffer drops deltas that can
    /// never be recovered. Pre-fix the task logged and `continue`d, so it went
    /// on maintaining an index it had already corrupted and reported nothing.
    /// It must stop, and say so.
    #[tokio::test]
    async fn a_lagging_vector_view_stops_instead_of_maintaining_a_corrupt_index() {
        let flow = docs_flow();
        let sink = InMemoryVectorSink::new();
        let handle =
            spawn_vector_view(&flow, spec_for(Arc::clone(&sink) as Arc<dyn IvmVectorSink>))
                .unwrap();

        // `apply_remote_tick` is synchronous, so on a current-thread runtime the
        // sink task never gets polled during this loop: it starts more than a
        // full buffer behind and its first `recv()` is `Lagged`.
        let overflow = krishiv_delta::view::VIEW_DELTA_STREAM_CAPACITY + 50;
        for i in 0..overflow {
            flow.apply_remote_tick(
                HashMap::new(),
                HashMap::from([("docs".to_string(), row(&format!("id-{i}"), [1.0, 2.0]))]),
            )
            .unwrap();
        }

        // The task must *end*. Asserting only on the recorded reason would not
        // distinguish the fix from the bug — the pre-fix arm logged and
        // `continue`d, and a counter set just before a `continue` looks exactly
        // like a counter set just before a `break`. What changed is that the
        // task stops, and stops before touching the sink again.
        let finished = wait_until(|| handle.is_finished()).await;
        let status = handle.health().status();
        let applied_after_lag = sink.len();
        handle.abort();

        assert!(
            finished,
            "a lagging vector view must stop, not go on maintaining an index it has \
             already lost deltas from; status {status:?}"
        );
        assert_eq!(
            applied_after_lag, 0,
            "nothing may be written to the sink after the lag: those writes would \
             build an index that looks live and is permanently wrong"
        );
        assert!(
            status
                .stopped_reason
                .as_deref()
                .unwrap_or_default()
                .contains("fell behind"),
            "the stop reason must name the lag: {:?}",
            status.stopped_reason
        );
        assert!(status.diverged, "lost deltas mean a diverged index");
        assert!(
            status.deltas_missed > 0,
            "the missed count must be recorded"
        );
    }

    /// The handle owns the task: dropping it stops maintenance. The module doc
    /// always claimed this; with a bare `tokio::JoinHandle` it was false
    /// (dropping one detaches the task).
    #[tokio::test]
    async fn dropping_the_handle_stops_the_maintenance_task() {
        let flow = docs_flow();
        let sink = InMemoryVectorSink::new();
        let handle =
            spawn_vector_view(&flow, spec_for(Arc::clone(&sink) as Arc<dyn IvmVectorSink>))
                .unwrap();
        drop(handle);
        tokio::task::yield_now().await;

        flow.apply_remote_tick(
            HashMap::new(),
            HashMap::from([("docs".to_string(), row("a", [1.0, 2.0]))]),
        )
        .unwrap();
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert!(
            sink.is_empty(),
            "the task must be gone once its handle is dropped"
        );
    }
}

// ── InMemoryVectorSink (for tests and in-process HTTP use) ───────────────────

pub mod testing {
    use super::{Arc, IvmVectorSink, VectorFuture};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Simple in-memory vector sink for unit tests.
    #[derive(Debug, Default)]
    pub struct InMemoryVectorSink {
        pub store: Mutex<HashMap<String, Vec<f32>>>,
    }

    impl InMemoryVectorSink {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        pub fn get(&self, id: &str) -> Option<Vec<f32>> {
            self.store
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(id)
                .cloned()
        }

        pub fn len(&self) -> usize {
            self.store.lock().unwrap_or_else(|p| p.into_inner()).len()
        }

        pub fn is_empty(&self) -> bool {
            self.store
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
        }
    }

    impl IvmVectorSink for InMemoryVectorSink {
        fn upsert_batch<'a>(
            &'a self,
            ids: &'a [String],
            vectors: &'a [Vec<f32>],
        ) -> VectorFuture<'a> {
            Box::pin(async move {
                let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
                for (id, vec) in ids.iter().zip(vectors.iter()) {
                    store.insert(id.clone(), vec.clone());
                }
                Ok(())
            })
        }

        fn delete_batch<'a>(&'a self, ids: &'a [String]) -> VectorFuture<'a> {
            Box::pin(async move {
                let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
                for id in ids {
                    store.remove(id);
                }
                Ok(())
            })
        }
    }
}
