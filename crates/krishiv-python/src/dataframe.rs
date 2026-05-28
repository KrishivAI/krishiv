//! `DataFrame` batch SQL results.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::errors::map_krishiv_error;
use crate::query_result::PyQueryResult;

#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    pub(crate) inner: krishiv_api::DataFrame,
}

#[pymethods]
impl PyDataFrame {
    /// Collect and return a [`QueryResult`] with Arrow batches.
    pub fn collect(&self, py: Python<'_>) -> PyResult<PyQueryResult> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .map(PyQueryResult::new)
                .map_err(map_krishiv_error)
        })
    }

    /// Collect and return a pretty-printed ASCII table.
    pub fn collect_pretty(&self, py: Python<'_>) -> PyResult<String> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .and_then(|r| r.pretty())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Alias for collect() — returns Arrow batches.
    pub fn collect_batches(&self, py: Python<'_>) -> PyResult<PyQueryResult> {
        self.collect(py)
    }

    pub fn collect_async(&self, py: Python<'_>) -> PyResult<PyQueryResult> {
        let inner = self.inner.clone();
        py.detach(move || {
            crate::session::block_on_async(inner.collect_async())
                .map(PyQueryResult::new)
                .map_err(map_krishiv_error)
        })
    }

    /// Print up to `n` rows as an ASCII table to stdout.
    #[pyo3(signature = (n=20))]
    pub fn show(&self, py: Python<'_>, n: usize) -> PyResult<()> {
        self.collect(py)?.show(n)
    }

    pub fn explain(&self, py: Python<'_>) -> PyResult<String> {
        let inner = self.inner.clone();
        py.detach(move || inner.explain().map_err(map_krishiv_error))
    }

    pub fn explain_logical(&self) -> String {
        self.inner.explain_logical()
    }

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
        format!("DataFrame(plan={})", self.inner.explain_logical())
    }
}
