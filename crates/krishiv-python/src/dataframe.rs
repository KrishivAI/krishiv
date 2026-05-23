//! `DataFrame` batch SQL results.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    pub(crate) inner: krishiv_api::DataFrame,
}

#[pymethods]
impl PyDataFrame {
    pub fn collect(&self, py: Python<'_>) -> PyResult<String> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .and_then(|r| r.pretty())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
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
        "DataFrame(<pending>)".to_string()
    }
}
