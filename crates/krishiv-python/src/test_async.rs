use pyo3::prelude::*;

#[pyfunction]
fn test_async(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    pyo3::future::into_py(py, async { Ok(()) })
}
