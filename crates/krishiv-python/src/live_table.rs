//! Python `live_table()` and change feed (R14).

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_exec::live_table::{ChangeFeed, CreateLiveTableExec, RefreshLiveTableExec};
use krishiv_lakehouse::{DeltaOp, MemoryDeltaStore};
use krishiv_sql::live_table::{LiveTableRegistry, execute_live_table_ddl};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;

use crate::PyBatch;

/// Live table backed by an in-process delta log.
#[pyclass(name = "LiveTable")]
pub struct PyLiveTable {
    name: String,
    store: Arc<MemoryDeltaStore>,
    exec: CreateLiveTableExec,
}

#[pymethods]
impl PyLiveTable {
    #[getter]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn refresh(&self) -> PyResult<usize> {
        RefreshLiveTableExec::new(self.name.clone(), self.store.clone())
            .compact()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    pub fn drop(&self) -> PyResult<()> {
        let sql = format!("DROP LIVE TABLE {}", self.name);
        execute_live_table_ddl(&LIVE_TABLE_REGISTRY, &sql)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn change_feed(&self) -> PyResult<PyChangeFeedIter> {
        let feed = ChangeFeed::from_store(self.store.as_ref())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyChangeFeedIter {
            entries: feed.into_iter(),
        })
    }

    /// Test helper: ingest one row with the given op label.
    pub fn ingest_row(&self, value: i64, op: &str) -> PyResult<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))])
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let delta_op = match op {
            "insert" => DeltaOp::Insert,
            "update" => DeltaOp::Update,
            "delete" => DeltaOp::Delete,
            other => return Err(PyRuntimeError::new_err(format!("unknown op: {other}"))),
        };
        self.exec
            .ingest(&batch, delta_op)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

#[pyclass(name = "ChangeFeedIter")]
pub struct PyChangeFeedIter {
    entries: std::vec::IntoIter<ChangeFeed>,
}

#[pymethods]
impl PyChangeFeedIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let Some(entry) = self.entries.next() else {
            return Ok(None);
        };
        let op = match entry.op {
            DeltaOp::Insert => "insert",
            DeltaOp::Update => "update",
            DeltaOp::Delete => "delete",
        };
        let batch = PyBatch::from_record_batch(entry.batch);
        Ok(Some(
            (op.to_string(), batch)
                .into_pyobject(py)?
                .into_any()
                .unbind(),
        ))
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let Some(entry) = self.entries.next() else {
            return Err(PyStopAsyncIteration::new_err(()));
        };
        let op = match entry.op {
            DeltaOp::Insert => "insert",
            DeltaOp::Update => "update",
            DeltaOp::Delete => "delete",
        };
        let batch = PyBatch::from_record_batch(entry.batch);
        Ok(Some(
            (op.to_string(), batch)
                .into_pyobject(py)?
                .into_any()
                .unbind(),
        ))
    }
}

static LIVE_TABLE_REGISTRY: std::sync::LazyLock<std::sync::Mutex<LiveTableRegistry>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(LiveTableRegistry::new()));

pub fn create_live_table(name: String, query: String) -> PyResult<PyLiveTable> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let store = Arc::new(MemoryDeltaStore::new());
    let exec = CreateLiveTableExec::new(name.clone(), query.clone(), schema, Some(store.clone()));
    let ddl = format!("CREATE LIVE TABLE {name} AS {query}");
    execute_live_table_ddl(&LIVE_TABLE_REGISTRY, &ddl)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyLiveTable { name, store, exec })
}
