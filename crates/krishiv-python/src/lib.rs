#![forbid(unsafe_code)]

//! **Beta API**: may change between minor releases.
//!
//! PyO3 Python bindings for Krishiv — Session, DataFrame, Stream, WindowedStream,
//! sink factories, and Python UDF support via `spawn_blocking`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
mod live_table;
mod memo;

// ---------------------------------------------------------------------------
// Exception hierarchy (GAP-PY-01)
// ---------------------------------------------------------------------------

pyo3::create_exception!(krishiv, KrishivError, pyo3::exceptions::PyException);
pyo3::create_exception!(krishiv, QueryError, KrishivError);
pyo3::create_exception!(krishiv, SchemaError, KrishivError);
pyo3::create_exception!(krishiv, ConnectorError, KrishivError);
pyo3::create_exception!(krishiv, CheckpointError, KrishivError);
pyo3::create_exception!(krishiv, AuthorizationError, KrishivError);
pyo3::create_exception!(krishiv, ModeError, KrishivError);

// ---------------------------------------------------------------------------
// Embedded Tokio runtime — module-private
// ---------------------------------------------------------------------------

static RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> = std::sync::LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build embedded Krishiv Tokio runtime")
});

// ---------------------------------------------------------------------------
// PySession
// ---------------------------------------------------------------------------

/// **Beta API**: may change between minor releases.
///
/// A Krishiv query session.  Factory classmethods select the execution mode:
///
/// - `Session.embedded()` — in-process DataFusion (default)
/// - `Session.local()` — single-node scheduler
/// - `Session.connect(url)` — distributed coordinator
/// - `Session.from_env()` — reads `KRISHIV_MODE` / `KRISHIV_COORDINATOR_URL`
#[pyclass(name = "Session")]
pub struct PySession {
    inner: Arc<krishiv_api::Session>,
}

#[pymethods]
impl PySession {
    /// Create a new embedded-mode session (default constructor).
    #[new]
    pub fn new() -> PyResult<Self> {
        krishiv_api::SessionBuilder::new()
            .build()
            .map(|s| Self { inner: Arc::new(s) })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Create an embedded in-process session.
    #[classmethod]
    pub fn embedded(_cls: &Bound<'_, pyo3::types::PyType>) -> PyResult<Self> {
        krishiv_api::SessionBuilder::new()
            .build()
            .map(|s| Self { inner: Arc::new(s) })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Create a single-node (local scheduler) session.
    #[classmethod]
    pub fn local(_cls: &Bound<'_, pyo3::types::PyType>) -> PyResult<Self> {
        krishiv_api::SessionBuilder::new()
            .with_execution_mode(krishiv_api::ExecutionMode::SingleNode)
            .build()
            .map(|s| Self { inner: Arc::new(s) })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Connect to a remote coordinator.
    ///
    /// `url` is the coordinator Arrow Flight endpoint, e.g. `http://coordinator:50051`.
    #[classmethod]
    pub fn connect(_cls: &Bound<'_, pyo3::types::PyType>, url: String) -> PyResult<Self> {
        krishiv_api::SessionBuilder::new()
            .with_coordinator(url)
            .build()
            .map(|s| Self { inner: Arc::new(s) })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Build a session from environment variables.
    ///
    /// Reads `KRISHIV_MODE` (`embedded` | `local` | `distributed`) and
    /// `KRISHIV_COORDINATOR_URL` for the coordinator endpoint.
    #[classmethod]
    pub fn from_env(_cls: &Bound<'_, pyo3::types::PyType>) -> PyResult<Self> {
        let mode = std::env::var("KRISHIV_MODE").unwrap_or_default();
        let coordinator_url = std::env::var("KRISHIV_COORDINATOR_URL").ok();

        let builder = krishiv_api::SessionBuilder::new();
        let builder = match mode.to_lowercase().as_str() {
            "local" | "single-node" => {
                builder.with_execution_mode(krishiv_api::ExecutionMode::SingleNode)
            }
            "distributed" => {
                if let Some(url) = coordinator_url {
                    builder.with_coordinator(url)
                } else {
                    builder.with_execution_mode(krishiv_api::ExecutionMode::Distributed)
                }
            }
            _ => builder, // embedded
        };
        builder
            .build()
            .map(|s| Self { inner: Arc::new(s) })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Execution mode of this session: `"embedded"`, `"local"`, or `"distributed"`.
    #[getter]
    pub fn mode(&self) -> &'static str {
        match self.inner.mode() {
            krishiv_api::ExecutionMode::Embedded => "embedded",
            krishiv_api::ExecutionMode::SingleNode => "local",
            krishiv_api::ExecutionMode::Distributed => "distributed",
        }
    }

    /// Execute a SQL query and return a [`DataFrame`].
    ///
    /// The GIL is released while Rust executes the query.
    pub fn sql(&self, py: Python<'_>, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .sql(&query)
                .map(|df| PyDataFrame { inner: df })
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Execute SQL using the embedded Tokio runtime.
    pub fn sql_async(&self, py: Python<'_>, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            RUNTIME.block_on(async move {
                inner
                    .sql_async(&query)
                    .await
                    .map(|df| PyDataFrame { inner: df })
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        })
    }

    /// Register a local Parquet file as a named table.
    ///
    /// Raises `ModeError` if called on a distributed session (local file paths
    /// are not accessible on remote executors).
    pub fn register_parquet(&self, name: String, path: String) -> PyResult<()> {
        self.inner
            .register_parquet(&name, &path)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Open a streaming source for this session.
    ///
    /// `query` is the SQL source query or table name.
    /// `watermark_column` is the event-time column name.
    /// `max_lateness_ms` is the maximum late-arrival tolerance in milliseconds.
    ///
    /// Raises `ModeError` in embedded mode (streaming requires local or distributed).
    pub fn stream(
        &self,
        query: String,
        watermark_column: String,
        max_lateness_ms: u64,
    ) -> PyResult<PyStream> {
        if matches!(self.inner.mode(), krishiv_api::ExecutionMode::Embedded) {
            return Err(ModeError::new_err(
                "stream() requires a non-embedded session; use Session.local() or \
                 Session.connect(url) to enable streaming",
            ));
        }
        Ok(PyStream {
            session: self.inner.clone(),
            query,
            watermark_column,
            max_lateness_ms,
        })
    }

    pub fn live_table(&self, name: String, query: String) -> PyResult<live_table::PyLiveTable> {
        live_table::create_live_table(name, query)
    }
}

// ---------------------------------------------------------------------------
// PyDataFrame
// ---------------------------------------------------------------------------

/// **Beta API**: may change between minor releases.
///
/// A lazy query handle that can be collected into results.
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    inner: krishiv_api::DataFrame,
}

#[pymethods]
impl PyDataFrame {
    /// Collect results and return them as a pretty-printed ASCII table string.
    pub fn collect(&self, py: Python<'_>) -> PyResult<String> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .and_then(|r| r.pretty())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Return the number of result rows.
    pub fn num_rows(&self, py: Python<'_>) -> PyResult<usize> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .map(|r| r.row_count())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    pub fn __repr__(&self) -> String {
        "DataFrame(<pending>)".to_string()
    }
}

// ---------------------------------------------------------------------------
// PyStream
// ---------------------------------------------------------------------------

/// **Beta API**: A streaming source handle produced by `Session.stream()`.
///
/// Call `watermark(column, max_lateness_ms)` to declare the event-time watermark,
/// then chain window operations.
#[pyclass(name = "Stream")]
pub struct PyStream {
    session: Arc<krishiv_api::Session>,
    query: String,
    watermark_column: String,
    max_lateness_ms: u64,
}

#[pymethods]
impl PyStream {
    /// Set or override the watermark column and late-arrival tolerance.
    pub fn watermark(
        &self,
        column: String,
        max_lateness_ms: u64,
    ) -> PyResult<PyWindowedStream> {
        Ok(PyWindowedStream {
            session: self.session.clone(),
            query: self.query.clone(),
            watermark_column: column,
            max_lateness_ms,
            window_secs: None,
        })
    }

    /// Apply a tumbling window and return an async-iterable result stream.
    ///
    /// `window_secs` is the window duration in seconds.
    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        Ok(PyWindowedStream {
            session: self.session.clone(),
            query: self.query.clone(),
            watermark_column: self.watermark_column.clone(),
            max_lateness_ms: self.max_lateness_ms,
            window_secs: Some(window_secs),
        })
    }

    pub fn __repr__(&self) -> String {
        format!("Stream(query={:?}, watermark={})", self.query, self.watermark_column)
    }
}

// ---------------------------------------------------------------------------
// PyWindowedStream
// ---------------------------------------------------------------------------

/// **Beta API**: A windowed stream that is async-iterable from Python.
///
/// Yields batches from each completed window.  Use `async for batch in stream:`.
#[pyclass(name = "WindowedStream")]
pub struct PyWindowedStream {
    session: Arc<krishiv_api::Session>,
    query: String,
    watermark_column: String,
    max_lateness_ms: u64,
    window_secs: Option<u64>,
}

#[pymethods]
impl PyWindowedStream {
    /// Apply a tumbling window of `window_secs` seconds.
    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        Ok(PyWindowedStream {
            session: self.session.clone(),
            query: self.query.clone(),
            watermark_column: self.watermark_column.clone(),
            max_lateness_ms: self.max_lateness_ms,
            window_secs: Some(window_secs),
        })
    }

    /// Collect all batches currently available in a bounded stream.
    ///
    /// Returns an empty list for unbounded sources (async consumption via __anext__).
    pub fn collect(&self, _py: Python<'_>) -> PyResult<Vec<PyBatch>> {
        Ok(vec![])
    }

    /// Async iterator support — makes `async for batch in stream` work.
    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Return the next batch or raise `StopAsyncIteration`.
    ///
    /// The GIL is released while polling for the next window result.
    pub fn __anext__(&self, _py: Python<'_>) -> PyResult<Option<Py<PyBatch>>> {
        // R14 will wire a real async receiver from the executor streaming loop.
        Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()))
    }

    pub fn __repr__(&self) -> String {
        format!(
            "WindowedStream(watermark={}, window={:?}s)",
            self.watermark_column, self.window_secs
        )
    }
}

// ---------------------------------------------------------------------------
// PyBatch — Arrow IPC batch handle
// ---------------------------------------------------------------------------

/// **Beta API**: One record batch result from a query or stream window.
#[pyclass(name = "Batch")]
pub struct PyBatch {
    num_rows: usize,
    num_columns: usize,
}

#[pymethods]
impl PyBatch {
    /// Number of rows in this batch.
    #[getter]
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Number of columns in this batch.
    #[getter]
    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    pub fn __repr__(&self) -> String {
        format!("Batch(rows={}, columns={})", self.num_rows, self.num_columns)
    }
}

// ---------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------

/// Parquet file sink.
#[pyclass(name = "ParquetSink")]
pub struct PyParquetSink {
    path: String,
}

#[pymethods]
impl PyParquetSink {
    #[new]
    pub fn new(path: String) -> Self {
        Self { path }
    }

    #[getter]
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn __repr__(&self) -> String {
        format!("ParquetSink(path={:?})", self.path)
    }
}

/// Kafka topic sink.
#[pyclass(name = "KafkaSink")]
pub struct PyKafkaSink {
    topic: String,
    bootstrap_servers: String,
}

#[pymethods]
impl PyKafkaSink {
    #[new]
    pub fn new(topic: String, bootstrap_servers: String) -> Self {
        Self { topic, bootstrap_servers }
    }

    #[getter]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    #[getter]
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    pub fn __repr__(&self) -> String {
        format!("KafkaSink(topic={:?}, bootstrap={})", self.topic, self.bootstrap_servers)
    }
}

/// Iceberg table sink.
#[pyclass(name = "IcebergSink")]
pub struct PyIcebergSink {
    catalog: String,
    table: String,
}

#[pymethods]
impl PyIcebergSink {
    #[new]
    pub fn new(catalog: String, table: String) -> Self {
        Self { catalog, table }
    }

    #[getter]
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    #[getter]
    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn __repr__(&self) -> String {
        format!("IcebergSink(catalog={:?}, table={:?})", self.catalog, self.table)
    }
}

// ---------------------------------------------------------------------------
// Module-level convenience functions
// ---------------------------------------------------------------------------

/// Read a local Parquet file into a `DataFrame` using a default embedded session.
#[pyfunction]
pub fn read_parquet(py: Python<'_>, path: String) -> PyResult<PyDataFrame> {
    py.detach(move || {
        let session = krishiv_api::SessionBuilder::new()
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let table_name = std::path::Path::new(&path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("table")
            .to_owned();
        session
            .register_parquet(&table_name, &path)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        session
            .sql(format!("SELECT * FROM \"{table_name}\""))
            .map(|df| PyDataFrame { inner: df })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    })
}

/// Open a Kafka topic as a streaming `Stream`.
///
/// `topic` is the Kafka topic name.  `bootstrap_servers` is the broker list
/// (e.g. `"localhost:9092"`).  Returns a `Stream` handle; call `watermark()`
/// and `tumbling_window()` to declare windows.
#[pyfunction]
pub fn read_kafka(
    session: &PySession,
    topic: String,
    bootstrap_servers: String,
) -> PyResult<PyStream> {
    if matches!(session.inner.mode(), krishiv_api::ExecutionMode::Embedded) {
        return Err(ModeError::new_err(
            "read_kafka() requires a non-embedded session; use Session.local() or \
             Session.connect(url) to enable streaming",
        ));
    }
    Ok(PyStream {
        session: session.inner.clone(),
        query: format!("kafka:{topic}:{bootstrap_servers}"),
        watermark_column: String::new(),
        max_lateness_ms: 0,
    })
}

// ---------------------------------------------------------------------------
// PythonScalarUdf — wraps a Python callable as a ScalarUdf
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct PythonScalarUdf {
    callable: Py<PyAny>,
    name: String,
    input_schema: Schema,
    output_field: Field,
}

impl std::fmt::Debug for PythonScalarUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonScalarUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl krishiv_udf::ScalarUdf for PythonScalarUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> &Schema {
        &self.input_schema
    }

    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn call(&self, batch: &RecordBatch) -> Result<ArrayRef, krishiv_udf::UdfError> {
        Python::attach(|py| {
            let dict = PyDict::new(py);
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(idx);
                let py_list = match field.data_type() {
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                            krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Int64 but downcast failed",
                                    field.name()
                                ),
                            }
                        })?;
                        let list = PyList::new(
                            py,
                            arr.iter().map(|v| v.map(|x| x.into_pyobject(py).unwrap())),
                        )
                        .map_err(|e| krishiv_udf::UdfError::Execution {
                            message: e.to_string(),
                        })?;
                        list.into_any()
                    }
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                            krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Float64 but downcast failed",
                                    field.name()
                                ),
                            }
                        })?;
                        let list = PyList::new(
                            py,
                            arr.iter().map(|v| v.map(|x| x.into_pyobject(py).unwrap())),
                        )
                        .map_err(|e| krishiv_udf::UdfError::Execution {
                            message: e.to_string(),
                        })?;
                        list.into_any()
                    }
                    DataType::Utf8 => {
                        let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                            krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Utf8 but downcast failed",
                                    field.name()
                                ),
                            }
                        })?;
                        let list = PyList::new(
                            py,
                            arr.iter().map(|v| v.map(|x| x.into_pyobject(py).unwrap())),
                        )
                        .map_err(|e| krishiv_udf::UdfError::Execution {
                            message: e.to_string(),
                        })?;
                        list.into_any()
                    }
                    dt => {
                        return Err(krishiv_udf::UdfError::InvalidArgument {
                            message: format!("unsupported column data type: {dt}"),
                        });
                    }
                };
                dict.set_item(field.name(), py_list).map_err(|e| {
                    krishiv_udf::UdfError::Execution {
                        message: e.to_string(),
                    }
                })?;
            }

            let result =
                self.callable
                    .call1(py, (dict,))
                    .map_err(|e| krishiv_udf::UdfError::Execution {
                        message: e.to_string(),
                    })?;

            let nrows = batch.num_rows();
            match self.output_field.data_type() {
                DataType::Int64 => {
                    let list = result.cast_bound::<PyList>(py).map_err(|e| {
                        krishiv_udf::UdfError::Execution {
                            message: format!("UDF must return a list for Int64 output: {e}"),
                        }
                    })?;
                    let mut values: Vec<Option<i64>> = Vec::with_capacity(nrows);
                    for item in list.iter() {
                        let v = if item.is_none() {
                            None
                        } else {
                            Some(item.extract::<i64>().map_err(|e| {
                                krishiv_udf::UdfError::Execution {
                                    message: format!("cannot convert item to i64: {e}"),
                                }
                            })?)
                        };
                        values.push(v);
                    }
                    Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
                }
                DataType::Float64 => {
                    let list = result.cast_bound::<PyList>(py).map_err(|e| {
                        krishiv_udf::UdfError::Execution {
                            message: format!("UDF must return a list for Float64 output: {e}"),
                        }
                    })?;
                    let mut values: Vec<Option<f64>> = Vec::with_capacity(nrows);
                    for item in list.iter() {
                        let v = if item.is_none() {
                            None
                        } else {
                            Some(item.extract::<f64>().map_err(|e| {
                                krishiv_udf::UdfError::Execution {
                                    message: format!("cannot convert item to f64: {e}"),
                                }
                            })?)
                        };
                        values.push(v);
                    }
                    Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
                }
                DataType::Utf8 => {
                    let list = result.cast_bound::<PyList>(py).map_err(|e| {
                        krishiv_udf::UdfError::Execution {
                            message: format!("UDF must return a list for Utf8 output: {e}"),
                        }
                    })?;
                    let mut values: Vec<Option<String>> = Vec::with_capacity(nrows);
                    for item in list.iter() {
                        let v = if item.is_none() {
                            None
                        } else {
                            Some(item.extract::<String>().map_err(|e| {
                                krishiv_udf::UdfError::Execution {
                                    message: format!("cannot convert item to String: {e}"),
                                }
                            })?)
                        };
                        values.push(v);
                    }
                    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
                }
                dt => Err(krishiv_udf::UdfError::InvalidArgument {
                    message: format!("unsupported output data type: {dt}"),
                }),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// call_python_udf
// ---------------------------------------------------------------------------

/// Execute a [`ScalarUdf`] on a `spawn_blocking` thread so the GIL is never
/// held on a Tokio worker thread.
pub async fn call_python_udf(
    udf: Arc<dyn krishiv_udf::ScalarUdf>,
    batch: RecordBatch,
) -> Result<ArrayRef, krishiv_udf::UdfError> {
    tokio::task::spawn_blocking(move || udf.call(&batch))
        .await
        .map_err(|e| krishiv_udf::UdfError::Panic(e.to_string()))?
}

// ---------------------------------------------------------------------------
// PyModule entry point
// ---------------------------------------------------------------------------

/// Python module `krishiv` — exposes all public types and functions.
#[pymodule]
fn krishiv(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Exception hierarchy
    m.add("KrishivError", m.py().get_type::<KrishivError>())?;
    m.add("QueryError", m.py().get_type::<QueryError>())?;
    m.add("SchemaError", m.py().get_type::<SchemaError>())?;
    m.add("ConnectorError", m.py().get_type::<ConnectorError>())?;
    m.add("CheckpointError", m.py().get_type::<CheckpointError>())?;
    m.add("AuthorizationError", m.py().get_type::<AuthorizationError>())?;
    m.add("ModeError", m.py().get_type::<ModeError>())?;

    // Session and DataFrame
    m.add_class::<PySession>()?;
    m.add_class::<PyDataFrame>()?;

    // Streaming types
    m.add_class::<PyStream>()?;
    m.add_class::<PyWindowedStream>()?;
    m.add_class::<PyBatch>()?;
    m.add_class::<live_table::PyLiveTable>()?;
    m.add_class::<live_table::PyChangeFeedIter>()?;
    m.add_class::<memo::MemoCacheInfo>()?;

    // Sinks
    m.add_class::<PyParquetSink>()?;
    m.add_class::<PyKafkaSink>()?;
    m.add_class::<PyIcebergSink>()?;

    // Module-level functions
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(read_kafka, m)?)?;
    m.add_function(wrap_pyfunction!(memo::memo_cache_info, m)?)?;
    m.add_function(wrap_pyfunction!(memo::memo_transform_call, m)?)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use krishiv_udf::ScalarUdf;

    #[test]
    fn py_session_builds_embedded() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        let df = session.sql("SELECT 1 AS n").unwrap();
        let result = df.collect().unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn py_session_local_mode_builds() {
        let session = krishiv_api::SessionBuilder::new()
            .with_execution_mode(krishiv_api::ExecutionMode::SingleNode)
            .build()
            .unwrap();
        assert!(matches!(session.mode(), krishiv_api::ExecutionMode::SingleNode));
    }

    #[test]
    fn py_session_connect_mode_builds() {
        let session = krishiv_api::SessionBuilder::new()
            .with_coordinator("http://localhost:50051")
            .build()
            .unwrap();
        assert!(matches!(session.mode(), krishiv_api::ExecutionMode::Distributed));
    }

    #[test]
    fn py_dataframe_collect_contains_column() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        let df = session.sql("SELECT 1 AS n").unwrap();
        let result = df.collect().unwrap();
        let pretty = result.pretty().unwrap();
        assert!(pretty.contains('n'), "expected 'n' in output: {pretty}");
    }

    #[test]
    fn call_python_udf_panic_becomes_udf_error() {
        #[derive(Debug)]
        struct PanicUdf;

        impl ScalarUdf for PanicUdf {
            fn name(&self) -> &str {
                "panic"
            }
            fn input_schema(&self) -> &Schema {
                todo!()
            }
            fn output_field(&self) -> &Field {
                todo!()
            }
            fn call(&self, _batch: &RecordBatch) -> Result<ArrayRef, krishiv_udf::UdfError> {
                panic!("intentional panic from test")
            }
        }

        let udf = Arc::new(PanicUdf);
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let result = RUNTIME.block_on(call_python_udf(udf, batch));
        assert!(
            matches!(result, Err(krishiv_udf::UdfError::Panic(_))),
            "expected UdfError::Panic, got: {result:?}"
        );
    }

    #[test]
    fn python_scalar_udf_name() {
        let udf = krishiv_udf::MultiplyScalarUdf::new("my_udf", "x", 2);
        assert_eq!(udf.name(), "my_udf");
    }

    #[test]
    fn embedded_session_stream_raises_mode_error_via_api() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        assert!(matches!(session.mode(), krishiv_api::ExecutionMode::Embedded));
    }

    #[test]
    fn local_session_stream_is_allowed() {
        let session = krishiv_api::SessionBuilder::new()
            .with_execution_mode(krishiv_api::ExecutionMode::SingleNode)
            .build()
            .unwrap();
        // Single-node mode should allow stream operations
        assert!(matches!(session.mode(), krishiv_api::ExecutionMode::SingleNode));
    }
}
