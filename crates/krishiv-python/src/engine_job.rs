//! Python handle for a job submitted through the unified engine spine.
//!
//! `Session.submit_sql(...)` compiles a SQL pipeline script to a `CompiledJob`
//! and dispatches it through the same `run_job` path the Rust and SQL
//! front-ends use, returning this handle (job id + status).

use std::sync::Mutex;

use pyo3::prelude::*;

use crate::errors::map_krishiv_error;

/// Handle returned by [`Session.submit_sql`](crate::session::PySession::submit_sql):
/// the engine job's id and current status (`running` / `completed` / `failed`).
#[pyclass(name = "EngineJobHandle")]
pub struct PyEngineJobHandle {
    id: String,
    status: String,
}

impl PyEngineJobHandle {
    pub fn from_handle(handle: krishiv_api::JobHandle) -> Self {
        Self {
            id: handle.job_id().as_str().to_owned(),
            status: format!("{:?}", handle.status()).to_lowercase(),
        }
    }
}

#[pymethods]
impl PyEngineJobHandle {
    #[getter]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[getter]
    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn __repr__(&self) -> String {
        format!("EngineJobHandle(id={:?}, status={})", self.id, self.status)
    }
}

/// Handle returned by [`Session.submit_streaming_sql`](crate::session::PySession::submit_streaming_sql):
/// a continuously-running streaming job. Call [`stop`](Self::stop) to signal the
/// loop, flush, persist a final checkpoint, and read the terminal status.
#[pyclass(name = "RunningJob")]
pub struct PyRunningJob {
    id: String,
    job: Mutex<Option<krishiv_api::RunningJob>>,
}

impl PyRunningJob {
    pub fn from_running(job: krishiv_api::RunningJob) -> Self {
        Self {
            id: job.handle().job_id().as_str().to_owned(),
            job: Mutex::new(Some(job)),
        }
    }
}

#[pymethods]
impl PyRunningJob {
    #[getter]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Stop the streaming job, returning the terminal status (`completed`).
    /// Raises if called twice (the job is consumed on the first stop).
    pub fn stop(&self, py: Python<'_>) -> PyResult<String> {
        let job = self
            .job
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("running-job mutex poisoned"))?
            .take()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("streaming job already stopped")
            })?;
        py.detach(move || {
            crate::session::block_on_async(async move {
                job.stop().await.map_err(krishiv_api::KrishivError::from)
            })
            .map(|handle| format!("{:?}", handle.status()).to_lowercase())
            .map_err(map_krishiv_error)
        })
    }

    pub fn __repr__(&self) -> String {
        format!("RunningJob(id={:?})", self.id)
    }
}
