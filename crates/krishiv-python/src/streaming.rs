//! Python bindings for structured streaming — Phase F parity.
//!
//! Exposes [`PyDataStreamWriter`] and [`PyStreamingQuery`] so Python users
//! can write streaming queries using the same API as Rust.
//!
//! ## Example
//!
//! ```python
//! writer = session.sql("SELECT * FROM events").write_stream()
//! writer.output_mode("append").trigger("processing_time", 1000).query_name("my_job")
//! query = writer.start()
//! query.await_termination(timeout_ms=30_000)
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use krishiv_api::{
    DataFrame, ForeachBatchFn, StreamingOutputMode, StreamingQuery, StreamingTrigger,
};
use krishiv_runtime::RemoteStreamingJob;

use crate::RUNTIME;
use crate::batch::PyBatch;
use crate::errors::map_krishiv_error;

// ── PyStreamingQueryProgress ─────────────────────────────────────────────────

/// Progress snapshot from a running streaming query.
#[pyclass(name = "StreamingQueryProgress")]
pub struct PyStreamingQueryProgress {
    pub epoch: i64,
    pub input_rows: u64,
    pub output_rows: u64,
    pub trigger: Option<String>,
}

#[pymethods]
impl PyStreamingQueryProgress {
    #[getter]
    fn epoch(&self) -> i64 {
        self.epoch
    }

    #[getter]
    fn input_rows(&self) -> u64 {
        self.input_rows
    }

    #[getter]
    fn output_rows(&self) -> u64 {
        self.output_rows
    }

    #[getter]
    fn trigger(&self) -> Option<String> {
        self.trigger.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "StreamingQueryProgress(epoch={}, input_rows={}, output_rows={})",
            self.epoch, self.input_rows, self.output_rows
        )
    }
}

// ── PyStreamingQuery ──────────────────────────────────────────────────────────

/// Handle to a running or stopped streaming query.
///
/// Returned by :py:meth:`DataStreamWriter.start`.
#[pyclass(name = "StreamingQuery")]
pub struct PyStreamingQuery {
    inner: Arc<Mutex<StreamingQuery>>,
}

impl PyStreamingQuery {
    pub fn new(q: StreamingQuery) -> Self {
        Self {
            inner: Arc::new(Mutex::new(q)),
        }
    }
}

#[pymethods]
impl PyStreamingQuery {
    /// The query's unique ID string.
    fn id(&self) -> String {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .id()
            .to_string()
    }

    /// The query name if one was set.
    fn name(&self) -> Option<String> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .name()
            .map(str::to_string)
    }

    /// ``True`` if the query is still running.
    fn is_active(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_active()
    }

    /// Request the query to stop.
    ///
    /// Returns immediately; the background task may finish the current
    /// micro-batch before it actually stops.
    fn stop(&self) {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).stop();
    }

    /// Block until the query terminates.
    ///
    /// ``timeout_ms`` — optional maximum wait in milliseconds.
    /// Raises ``RuntimeError`` on query failure or timeout.
    #[pyo3(signature = (timeout_ms=None))]
    fn await_termination(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<()> {
        let q = Arc::clone(&self.inner);
        // Release the GIL for the (potentially long, up to timeout_ms) wait
        // so other Python threads are not frozen.
        py.detach(move || {
            RUNTIME.block_on(async move {
                // Poll the is_active flag until done, respecting timeout.
                let deadline =
                    timeout_ms.map(|ms| tokio::time::Instant::now() + Duration::from_millis(ms));
                loop {
                    {
                        let guard = q.lock().unwrap_or_else(|p| p.into_inner());
                        if !guard.is_active() {
                            return Ok(());
                        }
                    }
                    if let Some(d) = deadline {
                        if tokio::time::Instant::now() >= d {
                            return Err(krishiv_api::KrishivError::Runtime {
                                message: "streaming query timed out waiting for termination"
                                    .to_string(),
                            });
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        })
        .map_err(map_krishiv_error)
    }

    /// Return the latest progress snapshot, if any micro-batch has run.
    fn last_progress(&self) -> Option<PyStreamingQueryProgress> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_progress()
            .map(|p| PyStreamingQueryProgress {
                epoch: p.epoch,
                input_rows: p.input_rows,
                output_rows: p.output_rows,
                trigger: p.trigger,
            })
    }

    fn __repr__(&self) -> String {
        let q = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        format!(
            "StreamingQuery(id={}, name={:?}, active={})",
            q.id(),
            q.name(),
            q.is_active()
        )
    }
}

// ── PyDataStreamWriter ────────────────────────────────────────────────────────

/// Fluent builder for writing a streaming pipeline to a sink.
///
/// Obtain one via :py:meth:`DataFrame.write_stream`.
///
/// ## Example
///
/// ```python
/// writer = df.write_stream()
/// writer.output_mode("append")
/// writer.trigger("once")
/// writer.query_name("etl-job")
/// query = writer.start()
/// ```
#[pyclass(name = "DataStreamWriter")]
pub struct PyDataStreamWriter {
    df: Option<DataFrame>,
    output_mode: String,
    trigger_type: String,
    trigger_interval_ms: u64,
    query_name: Option<String>,
    options: HashMap<String, String>,
    foreach_batch_fn: Option<Py<PyAny>>,
}

impl PyDataStreamWriter {
    pub fn new(df: DataFrame) -> Self {
        Self {
            df: Some(df),
            output_mode: "append".to_string(),
            trigger_type: "available_now".to_string(),
            trigger_interval_ms: 1000,
            query_name: None,
            options: HashMap::new(),
            foreach_batch_fn: None,
        }
    }
}

#[pymethods]
impl PyDataStreamWriter {
    /// Set the output mode: ``"append"`` (default), ``"update"``, or ``"complete"``.
    #[pyo3(signature = (mode))]
    fn output_mode(&mut self, mode: String) -> PyResult<()> {
        match mode.to_lowercase().as_str() {
            "append" | "update" | "complete" => {
                self.output_mode = mode.to_lowercase();
                Ok(())
            }
            other => Err(PyRuntimeError::new_err(format!(
                "unknown output mode '{other}'; expected append, update, or complete"
            ))),
        }
    }

    /// Set the trigger policy.
    ///
    /// ``trigger_type`` — one of ``"once"``, ``"available_now"``,
    /// ``"processing_time"``, or ``"continuous"``.
    ///
    /// ``interval_ms`` — interval in milliseconds for ``processing_time`` and
    /// ``continuous`` triggers (ignored for ``once``/``available_now``).
    #[pyo3(signature = (trigger_type, interval_ms=1000))]
    fn trigger(&mut self, trigger_type: String, interval_ms: u64) -> PyResult<()> {
        match trigger_type.to_lowercase().as_str() {
            "once" | "available_now" | "processing_time" | "continuous" => {
                self.trigger_type = trigger_type.to_lowercase();
                self.trigger_interval_ms = interval_ms;
                Ok(())
            }
            other => Err(PyRuntimeError::new_err(format!(
                "unknown trigger '{other}'; expected once, available_now, processing_time, or continuous"
            ))),
        }
    }

    /// Set a human-readable query name (optional).
    #[pyo3(signature = (name))]
    fn query_name(&mut self, name: String) {
        self.query_name = Some(name);
    }

    /// Set an arbitrary sink option (e.g. ``checkpoint.location``).
    #[pyo3(signature = (key, value))]
    fn option(&mut self, key: String, value: String) {
        self.options.insert(key, value);
    }

    /// Register a per-micro-batch callback ``func(batches: list[Batch], epoch: int) -> None``.
    ///
    /// ``batches`` is a list of :class:`Batch` objects; ``epoch`` is a monotonically
    /// increasing integer starting at 0.
    #[pyo3(signature = (func))]
    fn foreach_batch(&mut self, func: Py<PyAny>) {
        self.foreach_batch_fn = Some(func);
    }

    /// Execute the streaming query and return a :class:`StreamingQuery` handle.
    ///
    /// This method is synchronous — it starts the background query task and returns
    /// immediately. Use :py:meth:`StreamingQuery.await_termination` to block.
    fn start(&mut self, py: Python<'_>) -> PyResult<PyStreamingQuery> {
        let df = self.df.take().ok_or_else(|| {
            PyRuntimeError::new_err("DataStreamWriter.start() has already been called")
        })?;

        let output_mode = match self.output_mode.as_str() {
            "update" => StreamingOutputMode::Update,
            "complete" => StreamingOutputMode::Complete,
            _ => StreamingOutputMode::Append,
        };

        let interval = Duration::from_millis(self.trigger_interval_ms);
        let trigger = match self.trigger_type.as_str() {
            "once" => StreamingTrigger::Once,
            "processing_time" => StreamingTrigger::ProcessingTime(interval),
            "continuous" => StreamingTrigger::Continuous(interval),
            _ => StreamingTrigger::AvailableNow,
        };

        let query_name = self.query_name.clone();
        let options = self.options.clone();
        let foreach_fn_opt = self.foreach_batch_fn.take();

        let query = py.detach(move || {
            // Build the writer.
            let mut writer = krishiv_api::DataStreamWriter::new(df)
                .output_mode(output_mode)
                .trigger(trigger);

            if let Some(name) = query_name {
                writer = writer.query_name(name);
            }
            for (k, v) in &options {
                writer = writer.option(k, v.clone());
            }

            if let Some(py_fn) = foreach_fn_opt {
                let foreach: ForeachBatchFn = Arc::new(move |batches, epoch| {
                    Python::attach(|py| {
                        let py_batches: Vec<PyBatch> = batches
                            .into_iter()
                            .map(PyBatch::from_record_batch)
                            .collect();
                        py_fn
                            .call1(py, (py_batches, epoch))
                            .map(|_| ())
                            .map_err(|e| krishiv_api::KrishivError::Runtime {
                                message: e.to_string(),
                            })
                    })
                });
                writer = writer.foreach_batch(foreach);
            }

            RUNTIME.block_on(writer.start())
        });

        query.map(PyStreamingQuery::new).map_err(map_krishiv_error)
    }

    fn __repr__(&self) -> String {
        format!(
            "DataStreamWriter(mode={}, trigger={}, query_name={:?})",
            self.output_mode, self.trigger_type, self.query_name
        )
    }
}

// ── PyRemoteStreamingJob ──────────────────────────────────────────────────────

/// Handle to a continuous streaming job managed by a remote coordinator.
///
/// Obtain via :func:`krishiv.connect_streaming` or
/// :meth:`Session.connect_streaming`.
///
/// All methods are synchronous wrappers that run on the shared Tokio runtime.
///
/// Example:
///
/// ```python
/// job = krishiv.connect_streaming("http://coordinator:8080", "etl-job")
/// job.push([batch1, batch2])
/// results = job.drain()
/// ```
#[pyclass(name = "RemoteStreamingJob")]
pub struct PyRemoteStreamingJob {
    inner: RemoteStreamingJob,
}

#[pymethods]
impl PyRemoteStreamingJob {
    /// Connect to an existing streaming job on the coordinator.
    ///
    /// ``coordinator_url`` — base URL of the coordinator HTTP API.
    /// ``job_id`` — the job ID assigned when the job was registered.
    #[new]
    pub fn py_new(coordinator_url: String, job_id: String) -> Self {
        Self {
            inner: RemoteStreamingJob::from_job_id(coordinator_url, job_id),
        }
    }

    /// The job ID.
    #[getter]
    pub fn job_id(&self) -> &str {
        self.inner.job_id()
    }

    /// Push a list of :class:`Batch` objects as input to the streaming job.
    pub fn push(&self, py: Python<'_>, batches: Vec<PyRef<'_, PyBatch>>) -> PyResult<()> {
        let rbs: Vec<_> = batches.iter().map(|b| b.record_batch().clone()).collect();
        let inner = self.inner.clone();
        py.detach(move || {
            RUNTIME
                .block_on(inner.push(&rbs))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Drain accumulated output batches from the job.
    ///
    /// Returns a list of :class:`Batch` objects.
    pub fn drain(&self, py: Python<'_>) -> PyResult<Vec<PyBatch>> {
        let inner = self.inner.clone();
        py.detach(move || {
            RUNTIME
                .block_on(inner.drain())
                .map(|rbs| rbs.into_iter().map(PyBatch::from_record_batch).collect())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    pub fn __repr__(&self) -> String {
        format!("RemoteStreamingJob(job_id='{}')", self.inner.job_id())
    }
}
