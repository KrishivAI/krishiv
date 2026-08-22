//! Python bindings for the converged streaming terminal (task #150 P5):
//! `StreamingDataFrame.write()` -> `StreamWriter` -> `start()` ->
//! `StreamingJob` — thin PyO3 over the Rust core in `krishiv_api`, so the
//! sink/mode/trigger semantics (and their refusals) exist exactly once.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::batch::PyBatch;
use crate::errors::map_krishiv_error;
use crate::session::PySession;

/// Builder for the write terminal. Mirrors the Rust `StreamWriter`.
#[pyclass(name = "StreamWriter")]
pub struct PyStreamWriter {
    inner: Option<krishiv_api::StreamWriter>,
}

impl PyStreamWriter {
    pub(crate) fn new(inner: krishiv_api::StreamWriter) -> Self {
        Self { inner: Some(inner) }
    }

    fn map(
        &mut self,
        f: impl FnOnce(krishiv_api::StreamWriter) -> krishiv_api::StreamWriter,
    ) -> PyResult<()> {
        let inner = self.inner.take().ok_or_else(|| {
            PyRuntimeError::new_err("StreamWriter.start() has already been called")
        })?;
        self.inner = Some(f(inner));
        Ok(())
    }
}

#[pymethods]
impl PyStreamWriter {
    /// Sink format: ``"iceberg"``, any registered connector kind, or omit
    /// for a drain-driven job. Returns ``self`` for chaining, mirroring the
    /// Rust builder: ``stream.write().format("iceberg").start(...)``.
    pub fn format<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.map(|w| w.format(name))?;
        Ok(slf)
    }

    /// Sink-specific option. Returns ``self`` for chaining.
    pub fn option<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: String,
        value: String,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.map(|w| w.option(key, value))?;
        Ok(slf)
    }

    /// Output mode: ``"append"`` (default), ``"update"``, or ``"complete"``.
    /// Returns ``self`` for chaining.
    pub fn output_mode<'py>(
        mut slf: PyRefMut<'py, Self>,
        mode: String,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.map(|w| w.output_mode(mode))?;
        Ok(slf)
    }

    /// Trigger policy: ``"continuous"``, ``"processing_time"``, ``"once"``,
    /// or ``"available_now"``. Returns ``self`` for chaining.
    #[pyo3(signature = (trigger, interval_ms=1000))]
    pub fn trigger<'py>(
        mut slf: PyRefMut<'py, Self>,
        trigger: String,
        interval_ms: u64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.map(|w| w.trigger(trigger, interval_ms))?;
        Ok(slf)
    }

    /// Run-loop subtask count (coordinator-backed session modes). Returns
    /// ``self`` for chaining.
    pub fn parallelism<'py>(
        mut slf: PyRefMut<'py, Self>,
        parallelism: u32,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.map(|w| w.parallelism(parallelism))?;
        Ok(slf)
    }

    /// Arm barrier checkpointing. Returns ``self`` for chaining.
    pub fn checkpoint<'py>(
        mut slf: PyRefMut<'py, Self>,
        interval_ms: u64,
        storage_path: String,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.map(|w| w.checkpoint(interval_ms, storage_path))?;
        Ok(slf)
    }

    /// Register the job on ``session`` and return a :class:`StreamingJob`.
    pub fn start(
        &mut self,
        session: &PySession,
        job_name: String,
    ) -> PyResult<PyUnifiedStreamingJob> {
        let inner = self.inner.take().ok_or_else(|| {
            PyRuntimeError::new_err("StreamWriter.start() has already been called")
        })?;
        let job = inner
            .start(session.engine(), job_name)
            .map_err(map_krishiv_error)?;
        Ok(PyUnifiedStreamingJob { inner: job })
    }
}

/// The ONE streaming lifecycle handle (task #150): id, push, drain, flush,
/// stop — served identically for embedded, single-node, and distributed
/// jobs, and attachable to a remote coordinator by id.
#[pyclass(name = "StreamingJob")]
pub struct PyUnifiedStreamingJob {
    inner: krishiv_api::StreamingJob,
}

#[pymethods]
impl PyUnifiedStreamingJob {
    /// Attach to a job on a remote coordinator by id.
    #[staticmethod]
    pub fn attach(coordinator_url: String, job_id: String) -> Self {
        Self {
            inner: krishiv_api::StreamingJob::attach(coordinator_url, job_id),
        }
    }

    /// The job id.
    #[getter]
    pub fn id(&self) -> String {
        self.inner.id()
    }

    /// Push input batches.
    pub fn push(&self, py: Python<'_>, batches: Vec<PyRef<'_, PyBatch>>) -> PyResult<()> {
        let rbs: Vec<_> = batches.iter().map(|b| b.record_batch().clone()).collect();
        py.detach(|| {
            crate::RUNTIME
                .block_on(self.inner.push(rbs))
                .map_err(map_krishiv_error)
        })
    }

    /// Drain output (complete mode returns the full result table).
    pub fn drain(&self, py: Python<'_>) -> PyResult<Vec<PyBatch>> {
        py.detach(|| {
            crate::RUNTIME
                .block_on(self.inner.drain())
                .map(|rbs| rbs.into_iter().map(PyBatch::from_record_batch).collect())
                .map_err(map_krishiv_error)
        })
    }

    /// Declare end-of-stream and return/stage the flushed windows.
    pub fn flush(&self, py: Python<'_>) -> PyResult<Vec<PyBatch>> {
        py.detach(|| {
            crate::RUNTIME
                .block_on(self.inner.flush())
                .map(|rbs| rbs.into_iter().map(PyBatch::from_record_batch).collect())
                .map_err(map_krishiv_error)
        })
    }

    /// Stop the job and free its state.
    pub fn stop(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            crate::RUNTIME
                .block_on(self.inner.stop())
                .map_err(map_krishiv_error)
        })
    }

    pub fn __repr__(&self) -> String {
        format!("StreamingJob(id='{}')", self.inner.id())
    }
}
