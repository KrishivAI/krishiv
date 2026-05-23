//! `@ks.transform(memo=True)` support (R14 S2).

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use krishiv_exec::memo::MemoCache;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Global memo cache shared across transform calls in the session.
pub static MEMO_CACHE: std::sync::LazyLock<MemoCache> =
    std::sync::LazyLock::new(|| MemoCache::new(10_000));

#[pyclass]
pub struct MemoCacheInfo {
    #[pyo3(get)]
    pub hits: u64,
    #[pyo3(get)]
    pub misses: u64,
    #[pyo3(get)]
    pub size: usize,
}

#[pyfunction]
pub fn memo_cache_info() -> MemoCacheInfo {
    let (hits, misses, size) = MEMO_CACHE.cache_info();
    MemoCacheInfo { hits, misses, size }
}

pub fn compute_memo_key(source: &[u8], schema_json: &str, ipc: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(source);
    hasher.update(schema_json.as_bytes());
    hasher.update(ipc);
    hasher.finalize().into()
}

pub fn memo_lookup_or_store(
    key: [u8; 32],
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    if let Some(hit) = MEMO_CACHE.lookup_or_miss(key) {
        return Ok(hit);
    }
    MEMO_CACHE.store(key, batch.clone()).map_err(|e| e.to_string())?;
    Ok(batch)
}

#[pyfunction]
pub fn memo_transform_call(
    source_hash: Vec<u8>,
_schema_json: String,
    ipc_bytes: Vec<u8>,
) -> PyResult<Py<PyBytes>> {
    if source_hash.len() != 32 {
        return Err(PyRuntimeError::new_err("source_hash must be 32 bytes"));
    }

    let cursor = std::io::Cursor::new(ipc_bytes.clone());
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        .ok_or_else(|| PyRuntimeError::new_err("empty ipc batch"))?;
    let key = compute_memo_key(&source_hash, &_schema_json, &ipc_bytes);
    let out = memo_lookup_or_store(key, batch).map_err(PyRuntimeError::new_err)?;
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, out.schema().as_ref())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        writer
            .write(&out)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    }
    Python::attach(|py| -> PyResult<Py<PyBytes>> { Ok(PyBytes::new(py, &buf).into()) })
}
