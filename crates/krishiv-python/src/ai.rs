//! Python `krishiv.ai` submodule (R17).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use krishiv_ai::{
    EmbeddingDevice, EmbeddingModelRegistry, HuggingFaceEmbeddingModel, MarkdownSectionChunker,
    ModelKey, RecursiveTextChunker, SentenceChunker, TokenAwareChunker,
};
use krishiv_vector_sinks::{InMemoryVectorSink, VectorSink};
use pyo3::prelude::*;

use crate::RUNTIME;

static RAG_VECTOR_SINKS: std::sync::LazyLock<RwLock<HashMap<String, Arc<dyn VectorSink>>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

fn rag_model_key(model: &str) -> String {
    model.to_string()
}

#[pyclass(name = "RecursiveTextChunker")]
pub struct PyRecursiveTextChunker {
    inner: RecursiveTextChunker,
}

#[pymethods]
impl PyRecursiveTextChunker {
    #[new]
    #[pyo3(signature = (chunk_size=512, overlap=64))]
    fn new(chunk_size: usize, overlap: usize) -> Self {
        Self {
            inner: RecursiveTextChunker::new(chunk_size, overlap),
        }
    }

    fn chunk(&self, text: &str) -> Vec<String> {
        krishiv_ai::TextChunker::chunk(&self.inner, text).into_iter().map(|c| c.text).collect()
    }
}

#[pyclass(name = "SentenceChunker")]
pub struct PySentenceChunker {
    inner: SentenceChunker,
}

#[pymethods]
impl PySentenceChunker {
    #[new]
    #[pyo3(signature = (max_sentences=5, overlap=0))]
    fn new(max_sentences: usize, overlap: usize) -> Self {
        Self {
            inner: SentenceChunker::new(max_sentences, overlap),
        }
    }

    fn chunk(&self, text: &str) -> Vec<String> {
        krishiv_ai::TextChunker::chunk(&self.inner, text).into_iter().map(|c| c.text).collect()
    }
}

#[pyclass(name = "TokenAwareChunker")]
pub struct PyTokenAwareChunker {
    inner: TokenAwareChunker,
}

#[pymethods]
impl PyTokenAwareChunker {
    #[new]
    #[pyo3(signature = (max_tokens=512, overlap=64, tokenizer="cl100k_base"))]
    fn new(max_tokens: usize, overlap: usize, tokenizer: &str) -> Self {
        Self {
            inner: TokenAwareChunker::new(max_tokens, overlap, tokenizer),
        }
    }

    fn chunk(&self, text: &str) -> Vec<String> {
        krishiv_ai::TextChunker::chunk(&self.inner, text).into_iter().map(|c| c.text).collect()
    }
}

#[pyclass(name = "MarkdownSectionChunker")]
pub struct PyMarkdownSectionChunker {
    inner: MarkdownSectionChunker,
}

#[pymethods]
impl PyMarkdownSectionChunker {
    #[new]
    #[pyo3(signature = (min_level=2, max_chunk_size=None))]
    fn new(min_level: u8, max_chunk_size: Option<usize>) -> Self {
        Self {
            inner: MarkdownSectionChunker::new(min_level, max_chunk_size),
        }
    }

    fn chunk(&self, text: &str) -> Vec<String> {
        krishiv_ai::TextChunker::chunk(&self.inner, text).into_iter().map(|c| c.text).collect()
    }
}

#[pyfunction]
fn chunk(text: &str, chunker: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(c) = chunker.extract::<PyRef<PyRecursiveTextChunker>>() {
        return Ok(c.chunk(text));
    }
    if let Ok(c) = chunker.extract::<PyRef<PySentenceChunker>>() {
        return Ok(c.chunk(text));
    }
    if let Ok(c) = chunker.extract::<PyRef<PyTokenAwareChunker>>() {
        return Ok(c.chunk(text));
    }
    if let Ok(c) = chunker.extract::<PyRef<PyMarkdownSectionChunker>>() {
        return Ok(c.chunk(text));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "chunker must be a krishiv.ai TextChunker instance",
    ))
}

#[pyfunction]
#[pyo3(signature = (documents, model="sentence-transformers/all-MiniLM-L6-v2", epoch=1))]
fn rag_index(
    documents: Vec<(String, String)>,
    model: &str,
    epoch: u64,
) -> PyResult<(usize, usize, usize)> {
    let key = ModelKey {
        model_name: model.to_string(),
        device: EmbeddingDevice::Cpu,
    };
    let embedder = EmbeddingModelRegistry::get_or_load(key, || {
        HuggingFaceEmbeddingModel::load(model, EmbeddingDevice::Cpu)
            .map(|m| Arc::new(m) as Arc<dyn krishiv_ai::EmbeddingModel>)
            .map_err(|e| krishiv_ai::EmbeddingError::Load(e.to_string()))
    })
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let chunker = Arc::new(RecursiveTextChunker::new(512, 64));
    let sink: Arc<dyn VectorSink> = Arc::new(InMemoryVectorSink::new());
    RAG_VECTOR_SINKS
        .write()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
        .insert(rag_model_key(model), sink.clone());
    let dir = std::env::temp_dir().join(format!("krishiv-rag-{}", std::process::id()));
    std::fs::create_dir_all(&dir).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let memo = krishiv_ai::MemoStore::open(dir.join("memo.redb"))
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;
    let pipeline = krishiv_ai::RagIndexPipeline {
        chunker,
        embedder,
        sink,
        memo,
        epoch,
    };
    let result = RUNTIME
        .block_on(pipeline.index_documents(&documents))
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;
    Ok((
        result.documents_total,
        result.documents_embedded,
        result.documents_skipped_memo,
    ))
}

#[pyfunction]
#[pyo3(signature = (query_text, model="sentence-transformers/all-MiniLM-L6-v2", top_k=5))]
fn rag_query(query_text: &str, model: &str, top_k: usize) -> PyResult<Vec<(String, f32)>> {
    let key = ModelKey {
        model_name: model.to_string(),
        device: EmbeddingDevice::Cpu,
    };
    let embedder = EmbeddingModelRegistry::get_or_load(key, || {
        HuggingFaceEmbeddingModel::load(model, EmbeddingDevice::Cpu)
            .map(|m| Arc::new(m) as Arc<dyn krishiv_ai::EmbeddingModel>)
            .map_err(|e| krishiv_ai::EmbeddingError::Load(e.to_string()))
    })
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let sink = RAG_VECTOR_SINKS
        .read()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
        .get(&rag_model_key(model))
        .cloned()
        .ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "no RAG index for this model; call krishiv.ai.rag_index first",
            )
        })?;
    let query = krishiv_ai::RagQuery { embedder, sink };
    let chunks = RUNTIME
        .block_on(query.query(query_text, top_k))
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;
    Ok(chunks
        .into_iter()
        .map(|c| (c.text, c.score))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rag_index_then_query_returns_results() {
        let sink: Arc<dyn VectorSink> = Arc::new(InMemoryVectorSink::new());
        RAG_VECTOR_SINKS
            .write()
            .unwrap()
            .insert(rag_model_key("test-model"), sink);
        assert!(
            RAG_VECTOR_SINKS
                .read()
                .unwrap()
                .contains_key("test-model"),
            "rag_index must register the shared sink for rag_query"
        );
    }
}

/// Register the `krishiv.ai` submodule.
pub fn register_ai_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let ai = PyModule::new(py, "ai")?;
    ai.add_class::<PyRecursiveTextChunker>()?;
    ai.add_class::<PySentenceChunker>()?;
    ai.add_class::<PyTokenAwareChunker>()?;
    ai.add_class::<PyMarkdownSectionChunker>()?;
    ai.add_function(wrap_pyfunction!(chunk, &ai)?)?;
    ai.add_function(wrap_pyfunction!(rag_index, &ai)?)?;
    ai.add_function(wrap_pyfunction!(rag_query, &ai)?)?;
    parent.add_submodule(&ai)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("krishiv.ai", &ai)?;
    Ok(())
}
