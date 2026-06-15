//! Python bindings for typed Rust scalar UDF registration.

use std::sync::Arc;

use krishiv_api::FunctionIdentifier;
use krishiv_plan::udf::MultiplyScalarUdf;
use pyo3::prelude::*;

use crate::errors::map_krishiv_error;

#[pyclass(name = "RustScalarUdf")]
pub struct PyRustScalarUdf {
    pub(crate) inner: Arc<dyn krishiv_plan::udf::ScalarUdf>,
}

#[pymethods]
impl PyRustScalarUdf {
    #[staticmethod]
    #[pyo3(signature = (name, column, factor))]
    fn multiply(name: String, column: String, factor: i64) -> Self {
        Self {
            inner: Arc::new(MultiplyScalarUdf::new(name, column, factor)),
        }
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

pub fn register_function(
    session: &krishiv_api::Session,
    name: String,
    udf: &PyRustScalarUdf,
) -> PyResult<()> {
    let identifier = FunctionIdentifier::new(name).map_err(map_krishiv_error)?;
    session
        .register_function(&identifier, udf.inner.clone())
        .map_err(map_krishiv_error)
}
