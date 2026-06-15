//! Python bindings for [`krishiv_api::BlockingSession`].

use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::errors::map_krishiv_error;
use crate::query_result::PyQueryResult;

#[pyclass(name = "BlockingSession")]
pub struct PyBlockingSession {
    inner: krishiv_api::BlockingSession,
}

#[pymethods]
impl PyBlockingSession {
    #[staticmethod]
    fn embedded() -> PyResult<Self> {
        krishiv_api::BlockingSession::embedded()
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    #[staticmethod]
    fn from_env() -> PyResult<Self> {
        krishiv_api::BlockingSession::from_env()
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    #[staticmethod]
    fn connect(coordinator_url: String) -> PyResult<Self> {
        krishiv_api::BlockingSession::connect(coordinator_url)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn sql(&self, py: Python<'_>, query: String) -> PyResult<PyQueryResult> {
        py.detach(|| {
            self.inner
                .sql(&query)
                .map(PyQueryResult::new)
                .map_err(map_krishiv_error)
        })
    }

    pub fn collect(&self, py: Python<'_>, dataframe: &PyDataFrame) -> PyResult<PyQueryResult> {
        let df = dataframe.inner.clone();
        py.detach(move || {
            self.inner
                .collect(df)
                .map(PyQueryResult::new)
                .map_err(map_krishiv_error)
        })
    }
}
