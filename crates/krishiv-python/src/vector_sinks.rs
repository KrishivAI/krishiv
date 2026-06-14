//! Python bindings for Krishiv vector sinks (`krishiv.ai` submodule).
//!
//! Exposes `InMemoryVectorSink`, `EmbeddingBatch`, and `ScoredChunk` so
//! Python code can upsert embeddings and run nearest-neighbor queries without
//! standing up an external vector store.

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use krishiv_connectors::vector::{EmbeddingBatch, InMemoryVectorSink, PayloadValue, VectorSink};

fn map_sink_err(e: krishiv_connectors::vector::VectorSinkError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// A single nearest-neighbor search result.
#[pyclass(name = "ScoredChunk")]
pub struct PyScoredChunk {
    #[pyo3(get)]
    pub doc_id: String,
    #[pyo3(get)]
    pub chunk_index: usize,
    #[pyo3(get)]
    pub text: String,
    #[pyo3(get)]
    pub score: f32,
    payload: HashMap<String, PayloadValue>,
}

#[pymethods]
impl PyScoredChunk {
    pub fn payload(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        for (k, v) in &self.payload {
            match v {
                PayloadValue::String(s) => d.set_item(k, s)?,
                PayloadValue::Int(i) => d.set_item(k, i)?,
                PayloadValue::Float(f) => d.set_item(k, f)?,
                PayloadValue::Bool(b) => d.set_item(k, b)?,
            }
        }
        Ok(d.unbind())
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ScoredChunk(doc_id={:?}, score={})",
            self.doc_id, self.score
        )
    }
}

/// In-memory vector sink for development and testing.
///
/// Supports upsert, delete, and cosine-similarity nearest-neighbor queries.
///
/// Example::
///
///     import krishiv.ai as ai
///     sink = ai.InMemoryVectorSink()
///     sink.upsert_batch(
///         doc_ids=["doc1", "doc2"],
///         vectors=[[1.0, 0.0], [0.0, 1.0]],
///         payloads=[{"text": "hello"}, {"text": "world"}],
///         epoch=1,
///     )
///     results = sink.query_nearest(vector=[1.0, 0.0], top_k=5)
///     for r in results:
///         print(r.doc_id, r.score)
#[pyclass(name = "InMemoryVectorSink")]
pub struct PyInMemoryVectorSink {
    inner: Arc<InMemoryVectorSink>,
}

#[pymethods]
impl PyInMemoryVectorSink {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryVectorSink::new()),
        }
    }

    /// Upsert a batch of embeddings.
    ///
    /// All three lists must have the same length. `payloads` is a list of
    /// dicts mapping string keys to ``str | int | float | bool`` values.
    #[pyo3(signature = (doc_ids, vectors, payloads=None, epoch=0))]
    pub fn upsert_batch(
        &self,
        py: Python<'_>,
        doc_ids: Vec<String>,
        vectors: Vec<Vec<f32>>,
        payloads: Option<Vec<Bound<'_, PyDict>>>,
        epoch: u64,
    ) -> PyResult<()> {
        let payloads: Vec<HashMap<String, PayloadValue>> = match payloads {
            None => vec![HashMap::new(); doc_ids.len()],
            Some(dicts) => dicts
                .iter()
                .map(|d| {
                    let mut m = HashMap::new();
                    for (k, v) in d.iter() {
                        let key: String = k.extract()?;
                        let val: PayloadValue = if let Ok(b) = v.extract::<bool>() {
                            PayloadValue::Bool(b)
                        } else if let Ok(i) = v.extract::<i64>() {
                            PayloadValue::Int(i)
                        } else if let Ok(f) = v.extract::<f64>() {
                            PayloadValue::Float(f)
                        } else if let Ok(s) = v.extract::<String>() {
                            PayloadValue::String(s)
                        } else {
                            return Err(PyRuntimeError::new_err(format!(
                                "unsupported payload value type for key {key:?}"
                            )));
                        };
                        m.insert(key, val);
                    }
                    Ok(m)
                })
                .collect::<PyResult<Vec<_>>>()?,
        };
        let batch = EmbeddingBatch::new(doc_ids, vectors, payloads, epoch);
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.upsert_batch(&batch))
                .map_err(map_sink_err)
        })
    }

    /// Delete embeddings by their Krishiv point IDs.
    pub fn delete_by_ids(&self, py: Python<'_>, ids: Vec<String>) -> PyResult<()> {
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.delete_by_ids(&ids))
                .map_err(map_sink_err)
        })
    }

    /// Run a nearest-neighbor query and return the top-k results.
    ///
    /// `filter` is an optional dict of metadata equality constraints.
    #[pyo3(signature = (vector, top_k=10, filter=None))]
    pub fn query_nearest(
        &self,
        py: Python<'_>,
        vector: Vec<f32>,
        top_k: usize,
        filter: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyScoredChunk>> {
        let payload_filter = filter
            .map(|d| {
                let mut equals = HashMap::new();
                for (k, v) in d.iter() {
                    let key: String = k.extract()?;
                    let val: PayloadValue = if let Ok(b) = v.extract::<bool>() {
                        PayloadValue::Bool(b)
                    } else if let Ok(i) = v.extract::<i64>() {
                        PayloadValue::Int(i)
                    } else if let Ok(f) = v.extract::<f64>() {
                        PayloadValue::Float(f)
                    } else if let Ok(s) = v.extract::<String>() {
                        PayloadValue::String(s)
                    } else {
                        return Err(PyRuntimeError::new_err(
                            "unsupported filter value type".to_string(),
                        ));
                    };
                    equals.insert(key, val);
                }
                Ok(krishiv_connectors::vector::PayloadFilter { equals })
            })
            .transpose()?;
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.query_nearest(&vector, top_k, payload_filter.as_ref()))
                .map_err(map_sink_err)
                .map(|chunks| {
                    chunks
                        .into_iter()
                        .map(|c| PyScoredChunk {
                            doc_id: c.doc_id,
                            chunk_index: c.chunk_index,
                            text: c.text,
                            score: c.score,
                            payload: c.payload,
                        })
                        .collect()
                })
        })
    }

    pub fn sink_name(&self) -> &str {
        self.inner.sink_name()
    }

    pub fn __repr__(&self) -> String {
        format!("InMemoryVectorSink(name={:?})", self.inner.sink_name())
    }
}

pub fn register_ai_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let ai = PyModule::new(py, "ai")?;
    ai.add_class::<PyInMemoryVectorSink>()?;
    ai.add_class::<PyScoredChunk>()?;
    parent.add_submodule(&ai)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("krishiv.ai", &ai)?;
    Ok(())
}
