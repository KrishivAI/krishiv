use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::errors::map_krishiv_error;
use crate::expression::PyColumn;

#[pyclass(name = "PreparedStatement")]
pub struct PyPreparedStatement {
    pub(crate) inner: krishiv_api::PreparedStatement,
}

#[pymethods]
impl PyPreparedStatement {
    pub fn sql(&self) -> &str {
        self.inner.sql()
    }

    pub fn parameter_count(&self) -> usize {
        self.inner.parameter_count()
    }

    pub fn bind(&self, parameters: Vec<PyColumn>) -> PyResult<PyDataFrame> {
        let parameters = parameters
            .into_iter()
            .map(|column| match column.inner.into_node() {
                krishiv_plan::expression::Expr::Literal { value } => Ok(value),
                _ => Err(PyTypeError::new_err(
                    "prepared parameters must be created with lit()",
                )),
            })
            .collect::<PyResult<Vec<_>>>()?;
        self.inner
            .bind(&parameters)
            .map(|inner| PyDataFrame { inner })
            .map_err(map_krishiv_error)
    }
}
