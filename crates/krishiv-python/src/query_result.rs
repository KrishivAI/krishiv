//! `QueryResult` — collected Arrow batches from a DataFrame.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::batch::PyBatch;

/// Collected query output as one or more record batches.
#[pyclass(name = "QueryResult")]
pub struct PyQueryResult {
    inner: krishiv_api::QueryResult,
}

impl PyQueryResult {
    pub fn new(inner: krishiv_api::QueryResult) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyQueryResult {
    pub fn batches(&self) -> Vec<PyBatch> {
        self.inner
            .batches()
            .iter()
            .map(|b| PyBatch::from_record_batch(b.clone()))
            .collect()
    }

    #[getter]
    pub fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    pub fn pretty(&self) -> PyResult<String> {
        self.inner
            .pretty()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    pub fn __len__(&self) -> usize {
        self.inner.batches().len()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "QueryResult(batches={}, rows={})",
            self.inner.batches().len(),
            self.inner.row_count()
        )
    }
}
