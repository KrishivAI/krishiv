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

// ── Helper: parse payload dict from Python ────────────────────────────────────

fn parse_payload_dict(d: &Bound<'_, PyDict>) -> PyResult<HashMap<String, PayloadValue>> {
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
}

fn parse_payloads(
    payloads: Option<Vec<Bound<'_, PyDict>>>,
    n: usize,
) -> PyResult<Vec<HashMap<String, PayloadValue>>> {
    match payloads {
        None => Ok(vec![HashMap::new(); n]),
        Some(dicts) => dicts.iter().map(parse_payload_dict).collect(),
    }
}

fn parse_filter(
    filter: Option<Bound<'_, PyDict>>,
) -> PyResult<Option<krishiv_connectors::vector::PayloadFilter>> {
    filter
        .map(|d| {
            let equals = parse_payload_dict(&d)?;
            Ok(krishiv_connectors::vector::PayloadFilter { equals })
        })
        .transpose()
}

fn chunks_to_py(chunks: Vec<krishiv_connectors::vector::ScoredChunk>) -> Vec<PyScoredChunk> {
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
}

// ── LanceDB sink ──────────────────────────────────────────────────────────────

/// Local LanceDB-compatible vector sink (stores Parquet fragments under `uri`).
///
/// Combines a durable Parquet store with an in-memory cosine-similarity index.
/// Available whenever the `vector-sinks` (or `ai`) feature is enabled.
#[pyclass(name = "LanceDbSink")]
pub struct PyLanceDbSink {
    inner: Arc<krishiv_connectors::vector::LanceDbSink>,
}

#[pymethods]
impl PyLanceDbSink {
    /// Open or create a LanceDB table directory at `uri`.
    #[staticmethod]
    pub fn open(py: Python<'_>, uri: String, table: String, vector_dim: usize) -> PyResult<Self> {
        py.detach(move || {
            crate::RUNTIME
                .block_on(krishiv_connectors::vector::LanceDbSink::open(
                    &uri, &table, vector_dim,
                ))
                .map(|s| Self { inner: Arc::new(s) })
                .map_err(map_sink_err)
        })
    }

    #[pyo3(signature = (doc_ids, vectors, payloads=None, epoch=0))]
    pub fn upsert_batch(
        &self,
        py: Python<'_>,
        doc_ids: Vec<String>,
        vectors: Vec<Vec<f32>>,
        payloads: Option<Vec<Bound<'_, PyDict>>>,
        epoch: u64,
    ) -> PyResult<()> {
        let payloads = parse_payloads(payloads, doc_ids.len())?;
        let batch = EmbeddingBatch::new(doc_ids, vectors, payloads, epoch);
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.upsert_batch(&batch))
                .map_err(map_sink_err)
        })
    }

    pub fn delete_by_ids(&self, py: Python<'_>, ids: Vec<String>) -> PyResult<()> {
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.delete_by_ids(&ids))
                .map_err(map_sink_err)
        })
    }

    #[pyo3(signature = (vector, top_k=10, filter=None))]
    pub fn query_nearest(
        &self,
        py: Python<'_>,
        vector: Vec<f32>,
        top_k: usize,
        filter: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyScoredChunk>> {
        let payload_filter = parse_filter(filter)?;
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.query_nearest(&vector, top_k, payload_filter.as_ref()))
                .map(chunks_to_py)
                .map_err(map_sink_err)
        })
    }

    pub fn sink_name(&self) -> &str {
        self.inner.sink_name()
    }
    pub fn __repr__(&self) -> String {
        format!("LanceDbSink(name={:?})", self.inner.sink_name())
    }
}

// ── Weaviate sink ─────────────────────────────────────────────────────────────

/// Weaviate REST vector sink.
///
/// `class_name` must be a valid Weaviate class identifier (letters, digits, underscores).
#[pyclass(name = "WeaviateSink")]
pub struct PyWeaviateSink {
    inner: Arc<krishiv_connectors::vector::WeaviateSink>,
}

#[pymethods]
impl PyWeaviateSink {
    #[new]
    #[pyo3(signature = (base_url, class_name, api_key=None))]
    pub fn new(base_url: String, class_name: String, api_key: Option<String>) -> PyResult<Self> {
        krishiv_connectors::vector::WeaviateSink::new(base_url, class_name, api_key)
            .map(|s| Self { inner: Arc::new(s) })
            .map_err(map_sink_err)
    }

    #[pyo3(signature = (doc_ids, vectors, payloads=None, epoch=0))]
    pub fn upsert_batch(
        &self,
        py: Python<'_>,
        doc_ids: Vec<String>,
        vectors: Vec<Vec<f32>>,
        payloads: Option<Vec<Bound<'_, PyDict>>>,
        epoch: u64,
    ) -> PyResult<()> {
        let payloads = parse_payloads(payloads, doc_ids.len())?;
        let batch = EmbeddingBatch::new(doc_ids, vectors, payloads, epoch);
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.upsert_batch(&batch))
                .map_err(map_sink_err)
        })
    }

    pub fn delete_by_ids(&self, py: Python<'_>, ids: Vec<String>) -> PyResult<()> {
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.delete_by_ids(&ids))
                .map_err(map_sink_err)
        })
    }

    #[pyo3(signature = (vector, top_k=10, filter=None))]
    pub fn query_nearest(
        &self,
        py: Python<'_>,
        vector: Vec<f32>,
        top_k: usize,
        filter: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyScoredChunk>> {
        let payload_filter = parse_filter(filter)?;
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.query_nearest(&vector, top_k, payload_filter.as_ref()))
                .map(chunks_to_py)
                .map_err(map_sink_err)
        })
    }

    pub fn sink_name(&self) -> &str {
        self.inner.sink_name()
    }
    pub fn __repr__(&self) -> String {
        format!("WeaviateSink(name={:?})", self.inner.sink_name())
    }
}

// ── Pinecone sink ─────────────────────────────────────────────────────────────

/// Pinecone REST vector sink.
///
/// `host` is the index host URL (e.g. `"my-index.svc.pinecone.io"`).
#[pyclass(name = "PineconeSink")]
pub struct PyPineconeSink {
    inner: Arc<krishiv_connectors::vector::PineconeSink>,
}

#[pymethods]
impl PyPineconeSink {
    #[new]
    #[pyo3(signature = (host, api_key, namespace=None))]
    pub fn new(host: String, api_key: String, namespace: Option<String>) -> Self {
        Self {
            inner: Arc::new(krishiv_connectors::vector::PineconeSink::new(
                host, api_key, namespace,
            )),
        }
    }

    #[pyo3(signature = (doc_ids, vectors, payloads=None, epoch=0))]
    pub fn upsert_batch(
        &self,
        py: Python<'_>,
        doc_ids: Vec<String>,
        vectors: Vec<Vec<f32>>,
        payloads: Option<Vec<Bound<'_, PyDict>>>,
        epoch: u64,
    ) -> PyResult<()> {
        let payloads = parse_payloads(payloads, doc_ids.len())?;
        let batch = EmbeddingBatch::new(doc_ids, vectors, payloads, epoch);
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.upsert_batch(&batch))
                .map_err(map_sink_err)
        })
    }

    pub fn delete_by_ids(&self, py: Python<'_>, ids: Vec<String>) -> PyResult<()> {
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.delete_by_ids(&ids))
                .map_err(map_sink_err)
        })
    }

    #[pyo3(signature = (vector, top_k=10, filter=None))]
    pub fn query_nearest(
        &self,
        py: Python<'_>,
        vector: Vec<f32>,
        top_k: usize,
        filter: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyScoredChunk>> {
        let payload_filter = parse_filter(filter)?;
        let sink = Arc::clone(&self.inner);
        py.detach(move || {
            crate::RUNTIME
                .block_on(sink.query_nearest(&vector, top_k, payload_filter.as_ref()))
                .map(chunks_to_py)
                .map_err(map_sink_err)
        })
    }

    pub fn sink_name(&self) -> &str {
        self.inner.sink_name()
    }
    pub fn __repr__(&self) -> String {
        format!("PineconeSink(name={:?})", self.inner.sink_name())
    }
}

// ── Qdrant sink (feature-gated) ───────────────────────────────────────────────

/// Qdrant gRPC vector sink.
///
/// Requires the `qdrant` Cargo feature.
#[pyclass(name = "QdrantSink")]
pub struct PyQdrantSink {
    #[cfg(feature = "qdrant")]
    inner: Arc<krishiv_connectors::vector::QdrantSink>,
    #[cfg(not(feature = "qdrant"))]
    _phantom: (),
}

#[pymethods]
impl PyQdrantSink {
    /// Connect to Qdrant at `url`. Creates the collection if `create_if_missing` is True.
    #[staticmethod]
    #[pyo3(signature = (url, collection, vector_size, create_if_missing=true))]
    pub fn connect(
        py: Python<'_>,
        url: String,
        collection: String,
        vector_size: u64,
        create_if_missing: bool,
    ) -> PyResult<Self> {
        #[cfg(feature = "qdrant")]
        {
            py.detach(move || {
                crate::RUNTIME
                    .block_on(krishiv_connectors::vector::QdrantSink::connect(
                        &url,
                        collection,
                        vector_size,
                        create_if_missing,
                    ))
                    .map(|s| Self { inner: Arc::new(s) })
                    .map_err(map_sink_err)
            })
        }
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (py, url, collection, vector_size, create_if_missing);
            Err(PyRuntimeError::new_err(
                "QdrantSink requires the 'qdrant' feature; rebuild with: maturin develop --features qdrant",
            ))
        }
    }

    #[pyo3(signature = (doc_ids, vectors, payloads=None, epoch=0))]
    pub fn upsert_batch(
        &self,
        py: Python<'_>,
        doc_ids: Vec<String>,
        vectors: Vec<Vec<f32>>,
        payloads: Option<Vec<Bound<'_, PyDict>>>,
        epoch: u64,
    ) -> PyResult<()> {
        #[cfg(feature = "qdrant")]
        {
            let payloads = parse_payloads(payloads, doc_ids.len())?;
            let batch = EmbeddingBatch::new(doc_ids, vectors, payloads, epoch);
            let sink = Arc::clone(&self.inner);
            py.detach(move || {
                crate::RUNTIME
                    .block_on(sink.upsert_batch(&batch))
                    .map_err(map_sink_err)
            })
        }
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (py, doc_ids, vectors, payloads, epoch);
            Err(PyRuntimeError::new_err(
                "QdrantSink requires the 'qdrant' feature",
            ))
        }
    }

    pub fn delete_by_ids(&self, py: Python<'_>, ids: Vec<String>) -> PyResult<()> {
        #[cfg(feature = "qdrant")]
        {
            let sink = Arc::clone(&self.inner);
            py.detach(move || {
                crate::RUNTIME
                    .block_on(sink.delete_by_ids(&ids))
                    .map_err(map_sink_err)
            })
        }
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (py, ids);
            Err(PyRuntimeError::new_err(
                "QdrantSink requires the 'qdrant' feature",
            ))
        }
    }

    #[pyo3(signature = (vector, top_k=10, filter=None))]
    pub fn query_nearest(
        &self,
        py: Python<'_>,
        vector: Vec<f32>,
        top_k: usize,
        filter: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyScoredChunk>> {
        #[cfg(feature = "qdrant")]
        {
            let payload_filter = parse_filter(filter)?;
            let sink = Arc::clone(&self.inner);
            py.detach(move || {
                crate::RUNTIME
                    .block_on(sink.query_nearest(&vector, top_k, payload_filter.as_ref()))
                    .map(chunks_to_py)
                    .map_err(map_sink_err)
            })
        }
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (py, vector, top_k, filter);
            Err(PyRuntimeError::new_err(
                "QdrantSink requires the 'qdrant' feature",
            ))
        }
    }

    pub fn sink_name(&self) -> &str {
        #[cfg(feature = "qdrant")]
        {
            self.inner.sink_name()
        }
        #[cfg(not(feature = "qdrant"))]
        {
            "qdrant"
        }
    }
}

// ── pgvector sink (feature-gated) ─────────────────────────────────────────────

/// PostgreSQL pgvector sink.
///
/// Requires the `pgvector` Cargo feature.
#[pyclass(name = "PgvectorSink")]
pub struct PyPgvectorSink {
    #[cfg(feature = "pgvector")]
    inner: Arc<krishiv_connectors::vector::PgvectorSink>,
    #[cfg(not(feature = "pgvector"))]
    _phantom: (),
}

#[pymethods]
impl PyPgvectorSink {
    /// Connect using a PostgreSQL connection URL.
    #[staticmethod]
    pub fn connect(
        py: Python<'_>,
        database_url: String,
        table: String,
        vector_dim: usize,
    ) -> PyResult<Self> {
        #[cfg(feature = "pgvector")]
        {
            py.detach(move || {
                crate::RUNTIME
                    .block_on(krishiv_connectors::vector::PgvectorSink::connect(
                        &database_url,
                        table,
                        vector_dim,
                    ))
                    .map(|s| Self { inner: Arc::new(s) })
                    .map_err(map_sink_err)
            })
        }
        #[cfg(not(feature = "pgvector"))]
        {
            let _ = (py, database_url, table, vector_dim);
            Err(PyRuntimeError::new_err(
                "PgvectorSink requires the 'pgvector' feature; rebuild with: maturin develop --features pgvector",
            ))
        }
    }

    #[pyo3(signature = (doc_ids, vectors, payloads=None, epoch=0))]
    pub fn upsert_batch(
        &self,
        py: Python<'_>,
        doc_ids: Vec<String>,
        vectors: Vec<Vec<f32>>,
        payloads: Option<Vec<Bound<'_, PyDict>>>,
        epoch: u64,
    ) -> PyResult<()> {
        #[cfg(feature = "pgvector")]
        {
            let payloads = parse_payloads(payloads, doc_ids.len())?;
            let batch = EmbeddingBatch::new(doc_ids, vectors, payloads, epoch);
            let sink = Arc::clone(&self.inner);
            py.detach(move || {
                crate::RUNTIME
                    .block_on(sink.upsert_batch(&batch))
                    .map_err(map_sink_err)
            })
        }
        #[cfg(not(feature = "pgvector"))]
        {
            let _ = (py, doc_ids, vectors, payloads, epoch);
            Err(PyRuntimeError::new_err(
                "PgvectorSink requires the 'pgvector' feature",
            ))
        }
    }

    pub fn delete_by_ids(&self, py: Python<'_>, ids: Vec<String>) -> PyResult<()> {
        #[cfg(feature = "pgvector")]
        {
            let sink = Arc::clone(&self.inner);
            py.detach(move || {
                crate::RUNTIME
                    .block_on(sink.delete_by_ids(&ids))
                    .map_err(map_sink_err)
            })
        }
        #[cfg(not(feature = "pgvector"))]
        {
            let _ = (py, ids);
            Err(PyRuntimeError::new_err(
                "PgvectorSink requires the 'pgvector' feature",
            ))
        }
    }

    #[pyo3(signature = (vector, top_k=10, filter=None))]
    pub fn query_nearest(
        &self,
        py: Python<'_>,
        vector: Vec<f32>,
        top_k: usize,
        filter: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyScoredChunk>> {
        #[cfg(feature = "pgvector")]
        {
            let payload_filter = parse_filter(filter)?;
            let sink = Arc::clone(&self.inner);
            py.detach(move || {
                crate::RUNTIME
                    .block_on(sink.query_nearest(&vector, top_k, payload_filter.as_ref()))
                    .map(chunks_to_py)
                    .map_err(map_sink_err)
            })
        }
        #[cfg(not(feature = "pgvector"))]
        {
            let _ = (py, vector, top_k, filter);
            Err(PyRuntimeError::new_err(
                "PgvectorSink requires the 'pgvector' feature",
            ))
        }
    }

    pub fn sink_name(&self) -> &str {
        #[cfg(feature = "pgvector")]
        {
            self.inner.sink_name()
        }
        #[cfg(not(feature = "pgvector"))]
        {
            "pgvector"
        }
    }
}

pub fn register_ai_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let ai = PyModule::new(py, "ai")?;
    ai.add_class::<PyInMemoryVectorSink>()?;
    ai.add_class::<PyScoredChunk>()?;
    ai.add_class::<PyLanceDbSink>()?;
    ai.add_class::<PyWeaviateSink>()?;
    ai.add_class::<PyPineconeSink>()?;
    ai.add_class::<PyQdrantSink>()?;
    ai.add_class::<PyPgvectorSink>()?;
    parent.add_submodule(&ai)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("krishiv.ai", &ai)?;
    Ok(())
}
