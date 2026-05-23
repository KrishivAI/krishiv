//! Local job status surfaced by `Session.jobs()`.

use pyo3::prelude::*;

#[pyclass(name = "JobStatus")]
pub struct PyJobStatus {
    id: String,
    name: String,
    state: String,
}

impl PyJobStatus {
    pub fn from_status(status: krishiv_api::JobStatus) -> Self {
        Self {
            id: status.id().as_str().to_owned(),
            name: status.name().to_string(),
            state: status.state().to_string(),
        }
    }
}

#[pymethods]
impl PyJobStatus {
    #[getter]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn __repr__(&self) -> String {
        format!(
            "JobStatus(id={:?}, name={:?}, state={})",
            self.id, self.name, self.state
        )
    }
}
