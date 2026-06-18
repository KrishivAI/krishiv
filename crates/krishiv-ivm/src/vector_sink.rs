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
//! // keep handle alive; drop to stop the background task
//! ```

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeListArray, Float32Array, StringArray};

use krishiv_delta::DeltaBatch;

use crate::error::{IvmError, IvmResult};
use crate::flow::IncrementalFlow;

// ── IvmVectorSink trait ───────────────────────────────────────────────────────

pub type VectorFuture<'a> = Pin<Box<dyn std::future::Future<Output = IvmResult<()>> + Send + 'a>>;

/// Pluggable vector index sink for incremental maintenance.
///
/// Implementors should map `ids` to their store's point IDs and perform
/// idempotent upserts / deletes.
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

// ── spawn_vector_view ─────────────────────────────────────────────────────────

/// Spawn a background Tokio task that watches `spec.view_name` output and
/// forwards insertions / retractions to the configured vector sink.
///
/// Returns a `JoinHandle`; drop or abort it to stop the task.
pub fn spawn_vector_view(
    flow: &IncrementalFlow,
    spec: VectorViewSpec,
) -> IvmResult<tokio::task::JoinHandle<()>> {
    let mut rx = flow.view_output_stream(&spec.view_name)?;
    let sink = spec.sink;
    let id_col = spec.id_column;
    let vec_col = spec.vector_column;

    Ok(tokio::spawn(async move {
        loop {
            if rx.changed().await.is_err() {
                break;
            }
            let delta = match rx.borrow_and_update().clone() {
                Some(d) => d,
                None => continue,
            };
            if let Err(e) = apply_delta_to_sink(sink.as_ref(), &delta, &id_col, &vec_col).await {
                tracing::warn!("IvmVectorSink error: {e}");
            }
        }
    }))
}

// ── Internal: apply one DeltaBatch to a sink ─────────────────────────────────

async fn apply_delta_to_sink(
    sink: &dyn IvmVectorSink,
    delta: &DeltaBatch,
    id_col: &str,
    vec_col: &str,
) -> IvmResult<()> {
    let (upsert_ids, upsert_vecs, delete_ids) = extract_vector_rows(delta, id_col, vec_col)?;
    if !upsert_ids.is_empty() {
        sink.upsert_batch(&upsert_ids, &upsert_vecs).await?;
    }
    if !delete_ids.is_empty() {
        sink.delete_batch(&delete_ids).await?;
    }
    Ok(())
}

/// Split a `DeltaBatch` into (upsert_ids, upsert_vecs, delete_ids).
///
/// Positive-weight rows → upsert; negative-weight rows → delete.
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
    // Fallback: coerce via Int64
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
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

// ── InMemoryVectorSink (for tests and in-process HTTP use) ───────────────────

pub mod testing {
    use super::*;
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
            self.store.lock().unwrap().get(id).cloned()
        }

        pub fn len(&self) -> usize {
            self.store.lock().unwrap().len()
        }
    }

    impl IvmVectorSink for InMemoryVectorSink {
        fn upsert_batch<'a>(
            &'a self,
            ids: &'a [String],
            vectors: &'a [Vec<f32>],
        ) -> VectorFuture<'a> {
            Box::pin(async move {
                let mut store = self.store.lock().unwrap();
                for (id, vec) in ids.iter().zip(vectors.iter()) {
                    store.insert(id.clone(), vec.clone());
                }
                Ok(())
            })
        }

        fn delete_batch<'a>(&'a self, ids: &'a [String]) -> VectorFuture<'a> {
            Box::pin(async move {
                let mut store = self.store.lock().unwrap();
                for id in ids {
                    store.remove(id);
                }
                Ok(())
            })
        }
    }
}
