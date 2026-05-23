//! Streaming handles: `Stream`, `KeyedStream`, `WindowedStream`.

use std::sync::Mutex;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;
use crate::batch::PyBatch;
use crate::ModeError;

/// Materialize SQL (or table) query results into record batches.
pub(crate) fn materialize_sql(
    session: &Arc<krishiv_api::Session>,
    query: &str,
) -> PyResult<Vec<RecordBatch>> {
    if query.starts_with("kafka:") {
        return Ok(Vec::new());
    }
    session
        .sql(query)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        .and_then(|df| {
            df.collect()
                .map(|r| r.batches().to_vec())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
}

/// A streaming source handle produced by `Session.stream()` or source readers.
#[pyclass(name = "Stream")]
pub struct PyStream {
    pub(crate) session: Arc<krishiv_api::Session>,
    pub(crate) query: String,
    pub(crate) watermark_column: String,
    pub(crate) max_lateness_ms: u64,
    pub(crate) key_columns: Vec<String>,
}

#[pymethods]
impl PyStream {
    /// Declare event-time watermark (alias: ``with_watermark``).
    pub fn watermark(&self, column: String, max_lateness_ms: u64) -> PyResult<PyStream> {
        self.with_watermark(column, max_lateness_ms)
    }

    pub fn with_watermark(&self, column: String, max_lateness_ms: u64) -> PyResult<PyStream> {
        Ok(PyStream {
            session: self.session.clone(),
            query: self.query.clone(),
            watermark_column: column,
            max_lateness_ms,
            key_columns: self.key_columns.clone(),
        })
    }

    #[pyo3(signature = (*columns))]
    pub fn key_by(&self, columns: Vec<String>) -> PyResult<PyKeyedStream> {
        let keys = columns;
        if keys.is_empty() {
            return Err(ModeError::new_err("key_by() requires at least one column name"));
        }
        Ok(PyKeyedStream {
            inner: PyStream {
                session: self.session.clone(),
                query: self.query.clone(),
                watermark_column: self.watermark_column.clone(),
                max_lateness_ms: self.max_lateness_ms,
                key_columns: keys,
            },
        })
    }

    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        Ok(PyWindowedStream::from_parts(
            self.session.clone(),
            self.query.clone(),
            self.watermark_column.clone(),
            self.max_lateness_ms,
            self.key_columns.clone(),
            Some(window_secs),
        ))
    }

    pub fn sink(&self, sink: &Bound<'_, PyAny>) -> PyResult<()> {
        let _ = sink;
        Ok(())
    }

    pub fn _repr_html_(&self) -> String {
        format!(
            "<p>Stream query={:?} watermark={}</p>",
            self.query, self.watermark_column
        )
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Stream(query={:?}, watermark={})",
            self.query, self.watermark_column
        )
    }
}

/// Stream partitioned by key columns.
#[pyclass(name = "KeyedStream")]
pub struct PyKeyedStream {
    pub(crate) inner: PyStream,
}

#[pymethods]
impl PyKeyedStream {
    pub fn with_watermark(&self, column: String, max_lateness_ms: u64) -> PyResult<PyKeyedStream> {
        Ok(PyKeyedStream {
            inner: self.inner.with_watermark(column, max_lateness_ms)?,
        })
    }

    pub fn window(&self, spec: &Bound<'_, PyAny>) -> PyResult<PyWindowedStream> {
        if self.inner.watermark_column.is_empty() {
            return Err(crate::SchemaError::new_err(
                "window() requires a watermark; call with_watermark() first",
            ));
        }
        let window_ms = window_ms_from_spec(spec)?;
        Ok(PyWindowedStream::from_parts(
            self.inner.session.clone(),
            self.inner.query.clone(),
            self.inner.watermark_column.clone(),
            self.inner.max_lateness_ms,
            self.inner.key_columns.clone(),
            Some(window_ms / 1000),
        ))
    }

    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        self.inner.tumbling_window(window_secs)
    }

    pub fn __repr__(&self) -> String {
        format!("KeyedStream(keys={:?})", self.inner.key_columns)
    }
}

/// Windowed, async-iterable stream.
#[pyclass(name = "WindowedStream")]
pub struct PyWindowedStream {
    session: Arc<krishiv_api::Session>,
    query: String,
    watermark_column: String,
    max_lateness_ms: u64,
    key_columns: Vec<String>,
    window_secs: Option<u64>,
    cached: Mutex<Option<Vec<RecordBatch>>>,
    next_index: Mutex<usize>,
}

impl PyWindowedStream {
    fn from_parts(
        session: Arc<krishiv_api::Session>,
        query: String,
        watermark_column: String,
        max_lateness_ms: u64,
        key_columns: Vec<String>,
        window_secs: Option<u64>,
    ) -> Self {
        Self {
            session,
            query,
            watermark_column,
            max_lateness_ms,
            key_columns,
            window_secs,
            cached: Mutex::new(None),
            next_index: Mutex::new(0),
        }
    }

    fn ensure_cached(&self) -> PyResult<()> {
        let mut cache = self.cached.lock().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        if cache.is_some() {
            return Ok(());
        }
        let sql = self.windowed_sql();
        *cache = Some(materialize_sql(&self.session, &sql)?);
        Ok(())
    }

    fn windowed_sql(&self) -> String {
        if self.query.starts_with("kafka:") {
            return self.query.clone();
        }
        if self.key_columns.is_empty() {
            return format!("SELECT COUNT(*) AS window_count FROM ({})", self.query);
        }
        let key = &self.key_columns[0];
        format!(
            "SELECT \"{key}\", COUNT(*) AS window_count FROM ({inner}) GROUP BY \"{key}\"",
            key = key,
            inner = self.query
        )
    }

    fn next_batch(&self) -> PyResult<Option<PyBatch>> {
        self.ensure_cached()?;
        let cache = self.cached.lock().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let batches = cache.as_ref().expect("cache populated");
        let mut idx = self.next_index.lock().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        if *idx >= batches.len() {
            return Ok(None);
        }
        let batch = batches[*idx].clone();
        *idx += 1;
        Ok(Some(PyBatch::from_record_batch(batch)))
    }
}

#[pymethods]
impl PyWindowedStream {
    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        Ok(PyWindowedStream::from_parts(
            self.session.clone(),
            self.query.clone(),
            self.watermark_column.clone(),
            self.max_lateness_ms,
            self.key_columns.clone(),
            Some(window_secs),
        ))
    }

    #[pyo3(signature = (**kwargs))]
    pub fn agg(&self, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<PyWindowedStream> {
        let _ = kwargs;
        Ok(PyWindowedStream::from_parts(
            self.session.clone(),
            self.query.clone(),
            self.watermark_column.clone(),
            self.max_lateness_ms,
            self.key_columns.clone(),
            self.window_secs,
        ))
    }

    pub fn collect(&self, py: Python<'_>) -> PyResult<Vec<PyBatch>> {
        py.detach(|| {
            self.ensure_cached()?;
            let cache = self.cached.lock().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(cache
                .as_ref()
                .unwrap_or(&Vec::new())
                .iter()
                .cloned()
                .map(PyBatch::from_record_batch)
                .collect())
        })
    }

    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __anext__(&self, py: Python<'_>) -> PyResult<Py<PyBatch>> {
        let batch = py.detach(|| self.next_batch())?;
        match batch {
            Some(b) => Ok(b.into_pyobject(py)?.unbind()),
            None => Err(PyStopAsyncIteration::new_err(())),
        }
    }

    pub fn sink(&self, _sink: &Bound<'_, PyAny>) -> PyResult<()> {
        Ok(())
    }

    pub fn __repr__(&self) -> String {
        format!(
            "WindowedStream(watermark={}, window={:?}s)",
            self.watermark_column, self.window_secs
        )
    }
}

fn window_ms_from_spec(spec: &Bound<'_, PyAny>) -> PyResult<u64> {
    if let Ok(tuple) = spec.extract::<(String, u64)>() {
        return match tuple.0.as_str() {
            "tumbling" | "sliding" | "session" => Ok(tuple.1),
            other => Err(PyRuntimeError::new_err(format!("unknown window kind: {other}"))),
        };
    }
    if let Ok(secs) = spec.extract::<u64>() {
        return Ok(secs * 1000);
    }
    Err(PyRuntimeError::new_err(
        "window spec must be ks.windows.tumbling(ms) or window size in seconds",
    ))
}

use pyo3::types::PyDict;
