//! Python bindings for incremental view maintenance.
//!
//! Exposes `IncrementalFlow`, `DeltaBatch`, and `StepSummary` to Python.
//!
//! # Typical usage
//!
//! ```python
//! import krishiv
//! import pyarrow as pa
//!
//! flow = krishiv.IncrementalFlow()
//! schema = pa.schema([pa.field("id", pa.int32()), pa.field("amount", pa.float64())])
//! flow.register_view("revenue", "SELECT sum(amount) AS total FROM orders",
//!                    schema, is_materialized=True)
//!
//! batch = make_example_batch()   # or collect from a query result
//! cb = krishiv.DeltaBatch.from_inserts(batch)
//! flow.feed_source("orders", cb)
//!
//! summary = flow.step()
//! print(summary)
//! snap = flow.snapshot("revenue")   # -> Batch or None
//! ```

use krishiv_api::{IncrementalFlow, StepSummary};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec, LatenessSpec};
use krishiv_runtime::RemoteIvmJob;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::batch::PyBatch;
use crate::schema::PySchema;

// ── PyDeltaBatch ─────────────────────────────────────────────────────────────

/// A record batch annotated with per-row integer weights.
///
/// A weight of ``+1`` represents an insertion; ``-1`` represents a retraction.
/// The weight column is stored as the last column, named ``_weight``.
#[pyclass(name = "DeltaBatch")]
#[derive(Clone)]
pub struct PyDeltaBatch {
    pub(crate) inner: DeltaBatch,
}

#[pymethods]
impl PyDeltaBatch {
    /// Construct a `DeltaBatch` from a :class:`Batch`, treating all rows as
    /// insertions (weight = +1).
    ///
    /// The ``batch`` must be a :class:`krishiv.Batch` (e.g., from a query
    /// result or :func:`make_example_batch`).
    #[staticmethod]
    pub fn from_inserts(batch: PyRef<'_, PyBatch>) -> PyResult<Self> {
        let rb = batch.record_batch().clone();
        let inner =
            DeltaBatch::from_inserts(rb).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Construct a `DeltaBatch` from a :class:`Batch`, treating all rows as
    /// deletions / retractions (weight = -1).
    #[staticmethod]
    pub fn from_deletes(batch: PyRef<'_, PyBatch>) -> PyResult<Self> {
        let rb = batch.record_batch().clone();
        let inner =
            DeltaBatch::from_deletes(rb).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Return the underlying batch (includes the ``_weight`` column) as a
    /// :class:`Batch`.
    pub fn to_batch(&self) -> PyBatch {
        PyBatch::from_record_batch(self.inner.inner().clone())
    }

    /// Number of logical rows (before weight consolidation).
    #[getter]
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Return ``True`` if the batch contains no rows.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn __repr__(&self) -> String {
        format!("DeltaBatch(rows={})", self.inner.num_rows())
    }
}

// ── PyStepSummary ─────────────────────────────────────────────────────────────

/// Summary returned by :meth:`IncrementalFlow.step`.
#[pyclass(name = "StepSummary")]
pub struct PyStepSummary {
    /// Total number of output rows emitted across all views this tick.
    #[pyo3(get)]
    pub total_output_rows: usize,
    /// Number of views that produced non-empty output this tick.
    #[pyo3(get)]
    pub active_views: usize,
}

#[pymethods]
impl PyStepSummary {
    pub fn __repr__(&self) -> String {
        format!(
            "StepSummary(total_output_rows={}, active_views={})",
            self.total_output_rows, self.active_views
        )
    }
}

impl From<StepSummary> for PyStepSummary {
    fn from(s: StepSummary) -> Self {
        Self {
            total_output_rows: s.total_output_rows,
            active_views: s.active_views,
        }
    }
}

// ── PyIncrementalFlow ─────────────────────────────────────────────────────────

/// Driver for an incremental view maintenance pipeline.
///
/// Create via :meth:`Session.incremental` or directly with
/// :class:`IncrementalFlow()`.
///
/// Thread-safe: the flow can be shared across threads — all state is protected
/// internally by a Mutex.
#[pyclass(name = "IncrementalFlow")]
#[derive(Clone)]
pub struct PyIncrementalFlow {
    pub(crate) inner: IncrementalFlow,
}

#[pymethods]
impl PyIncrementalFlow {
    /// Create a new, empty incremental flow.
    #[new]
    pub fn py_new() -> Self {
        Self {
            inner: IncrementalFlow::new(),
        }
    }

    /// Register an incremental view programmatically.
    ///
    /// Parameters
    /// ----------
    /// name : str
    ///     Logical view name.
    /// body_sql : str
    ///     SQL body (for documentation; DataFusion-driven execution wired in a
    ///     later phase).
    /// schema : type
    ///     A :class:`krishiv.Schema` subclass declaring the output columns.
    /// is_materialized : bool, optional
    ///     If ``True``, the view maintains a full snapshot (default ``False``).
    /// is_recursive : bool, optional
    ///     If ``True``, the view participates in fixed-point iteration
    ///     (default ``False``).
    ///
    /// Example::
    ///
    ///     class Revenue(krishiv.Schema):
    ///         total: float
    ///
    ///     flow.register_view("revenue", "SELECT sum(amount) AS total FROM orders", Revenue)
    #[pyo3(signature = (name, body_sql, schema, is_materialized=false, is_recursive=false))]
    pub fn register_view(
        &self,
        name: String,
        body_sql: String,
        schema: &Bound<'_, PyType>,
        is_materialized: bool,
        is_recursive: bool,
    ) -> PyResult<()> {
        let output_schema = PySchema::arrow_schema_from_class(schema)?;
        let spec = IncrementalViewSpec {
            name,
            body_sql,
            output_schema,
            is_materialized,
            is_recursive,
            lateness: vec![],
        };
        self.inner
            .register_view(spec)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Register an incremental view with LATENESS annotations.
    ///
    /// ``schema`` is a :class:`krishiv.Schema` subclass. ``lateness_ms`` maps
    /// column name → lateness in milliseconds.
    #[pyo3(signature = (name, body_sql, schema, lateness_ms, is_materialized=false, is_recursive=false))]
    pub fn register_view_with_lateness(
        &self,
        name: String,
        body_sql: String,
        schema: &Bound<'_, PyType>,
        lateness_ms: std::collections::HashMap<String, i64>,
        is_materialized: bool,
        is_recursive: bool,
    ) -> PyResult<()> {
        let output_schema = PySchema::arrow_schema_from_class(schema)?;
        let lateness = lateness_ms
            .into_iter()
            .map(|(col, ms)| LatenessSpec {
                column: col,
                lateness_ms: ms,
            })
            .collect();
        let spec = IncrementalViewSpec {
            name,
            body_sql,
            output_schema,
            is_materialized,
            is_recursive,
            lateness,
        };
        self.inner
            .register_view(spec)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Push a :class:`DeltaBatch` as input delta for a named source.
    ///
    /// The delta is buffered until the next :meth:`step` call.
    pub fn feed_source(&self, source_name: String, batch: PyRef<'_, PyDeltaBatch>) -> PyResult<()> {
        self.inner
            .feed_source(source_name, batch.inner.clone())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Advance one clock tick (synchronous).
    ///
    /// Drains all pending input deltas, publishes output deltas to all
    /// registered views, and increments the tick counter.
    ///
    /// Returns a :class:`StepSummary` with aggregate statistics for this tick.
    pub fn step(&self) -> PyResult<PyStepSummary> {
        self.inner
            .step()
            .map(PyStepSummary::from)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Advance one clock tick asynchronously.
    ///
    /// Equivalent to ``await asyncio.to_thread(flow.step)`` — runs the tick
    /// on the shared Krishiv Tokio runtime so the asyncio event loop is not
    /// blocked. Returns a :class:`StepSummary`.
    pub async fn step_async(&self) -> PyResult<PyStepSummary> {
        self.step()
    }

    /// Advance one clock tick with a Python computation callback.
    ///
    /// ``compute`` is called with a ``dict[str, DeltaBatch]`` (source deltas)
    /// and should return a ``dict[str, DeltaBatch]`` (output deltas per view).
    /// The returned deltas are published to each view's watch channel.
    ///
    /// Example::
    ///
    ///     summary = flow.step_with(lambda inputs: {
    ///         "revenue": compute_revenue_delta(inputs.get("orders"))
    ///     })
    pub fn step_with(&self, py: Python<'_>, compute: &Bound<'_, PyAny>) -> PyResult<PyStepSummary> {
        use std::collections::HashMap;

        let mut callback_err: Option<PyErr> = None;

        let result = self.inner.step_with(|inputs| {
            // Build Python dict: { source_name: PyDeltaBatch }
            let py_inputs = pyo3::types::PyDict::new(py);
            for (name, cb) in inputs {
                let py_cb = PyDeltaBatch { inner: cb };
                if let Err(e) = py_inputs.set_item(&name, Py::new(py, py_cb).expect("Py::new")) {
                    callback_err = Some(e);
                    return Err(krishiv_api::KrishivError::Runtime {
                        message: "step_with: failed to build input dict".into(),
                    });
                }
            }
            // Call the Python function
            let py_result = match compute.call1((py_inputs,)) {
                Err(e) => {
                    callback_err = Some(e);
                    return Err(krishiv_api::KrishivError::Runtime {
                        message: "step_with: callback raised an exception".into(),
                    });
                }
                Ok(v) => v,
            };
            // Expect a dict[str, DeltaBatch] return value
            let py_dict = match py_result.downcast::<pyo3::types::PyDict>() {
                Err(_) => {
                    callback_err = Some(PyRuntimeError::new_err(
                        "step_with callback must return a dict[str, DeltaBatch]",
                    ));
                    return Err(krishiv_api::KrishivError::Runtime {
                        message: "step_with: callback did not return a dict".into(),
                    });
                }
                Ok(d) => d,
            };
            let mut out: HashMap<String, DeltaBatch> = HashMap::new();
            for (k, v) in py_dict.iter() {
                let view_name: String = match k.extract() {
                    Err(e) => {
                        callback_err = Some(e);
                        return Err(krishiv_api::KrishivError::Runtime {
                            message: "step_with: dict key must be str".into(),
                        });
                    }
                    Ok(s) => s,
                };
                let py_cb: PyRef<'_, PyDeltaBatch> = match v.extract() {
                    Err(e) => {
                        callback_err = Some(e);
                        return Err(krishiv_api::KrishivError::Runtime {
                            message: "step_with: dict value must be DeltaBatch".into(),
                        });
                    }
                    Ok(c) => c,
                };
                out.insert(view_name, py_cb.inner.clone());
            }
            Ok(out)
        });

        // Re-raise any Python exception captured inside the closure.
        if let Some(e) = callback_err {
            return Err(e);
        }
        result
            .map(PyStepSummary::from)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Return the latest materialized snapshot for a view as a
    /// :class:`Batch`, or ``None`` if no snapshot is available.
    ///
    /// Only valid for views registered with ``is_materialized=True``.
    pub fn snapshot(&self, name: String) -> PyResult<Option<PyBatch>> {
        self.inner
            .snapshot(&name)
            .map(|opt| opt.map(PyBatch::from_record_batch))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Return the names of all registered views.
    pub fn view_names(&self) -> PyResult<Vec<String>> {
        self.inner
            .view_names()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Current tick count (incremented on each :meth:`step` call).
    #[getter]
    pub fn tick(&self) -> PyResult<u64> {
        self.inner
            .tick()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Drop a registered view and stop emitting output for it.
    /// Drop a registered view. Returns ``True`` if the view existed.
    ///
    /// Note: full removal from the operator graph will be wired in a later
    /// phase. This currently only checks existence.
    pub fn drop_view(&self, name: String) -> PyResult<bool> {
        let names = self
            .inner
            .view_names()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(names.contains(&name))
    }

    pub fn __repr__(&self) -> PyResult<String> {
        let tick = self
            .inner
            .tick()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(format!("IncrementalFlow(tick={tick})"))
    }
}

// ── PyRemoteIvmJob ────────────────────────────────────────────────────────────

/// Handle to an IVM job running on a remote coordinator.
///
/// Obtain via :func:`krishiv.connect_ivm` or
/// :meth:`Session.connect_ivm`.
///
/// All methods are synchronous wrappers around async coordinator HTTP calls
/// that run on the shared Krishiv Tokio runtime.
///
/// Example::
///
///     job = krishiv.connect_ivm("http://coordinator:8080", "revenue")
///     job.feed_source("orders", delta)
///     summary = job.step()
///     print(summary)
#[pyclass(name = "RemoteIvmJob")]
pub struct PyRemoteIvmJob {
    pub(crate) inner: RemoteIvmJob,
}

#[pymethods]
impl PyRemoteIvmJob {
    /// Connect to an existing IVM job on the coordinator.
    ///
    /// ``coordinator_url`` — base URL of the coordinator HTTP API,
    ///   e.g. ``"http://localhost:8080"``.
    /// ``job_name`` — job name / ID. Creates the job if it doesn't exist.
    #[new]
    pub fn py_new(coordinator_url: String, job_name: String) -> PyResult<Self> {
        use crate::RUNTIME;
        let inner = RUNTIME
            .block_on(RemoteIvmJob::create(&coordinator_url, Some(&job_name)))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    /// The coordinator-assigned job ID.
    #[getter]
    pub fn job_id(&self) -> &str {
        self.inner.job_id()
    }

    /// Register or update an incremental view on this job.
    ///
    /// Parameters
    /// ----------
    /// name : str
    ///     Logical view name.
    /// body_sql : str
    ///     SQL body for the view.
    /// schema : type
    ///     A :class:`krishiv.Schema` subclass declaring output columns.
    /// is_materialized : bool, optional
    ///     Maintain a full snapshot (default ``False``).
    /// is_recursive : bool, optional
    ///     Participate in fixed-point iteration (default ``False``).
    #[pyo3(signature = (name, body_sql, schema, is_materialized=false, is_recursive=false))]
    pub fn register_view(
        &self,
        name: String,
        body_sql: String,
        schema: &Bound<'_, PyType>,
        is_materialized: bool,
        is_recursive: bool,
    ) -> PyResult<()> {
        use crate::RUNTIME;
        let output_schema = PySchema::arrow_schema_from_class(schema)?;
        let spec = IncrementalViewSpec {
            name,
            body_sql,
            output_schema,
            is_materialized,
            is_recursive,
            lateness: vec![],
        };
        RUNTIME
            .block_on(self.inner.register_view(&spec))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Push a :class:`DeltaBatch` as input delta for a named source.
    pub fn feed_source(&self, source_name: String, batch: PyRef<'_, PyDeltaBatch>) -> PyResult<()> {
        use crate::RUNTIME;
        RUNTIME
            .block_on(self.inner.feed_source(&source_name, &batch.inner))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Advance one clock tick on the coordinator.
    ///
    /// Returns a :class:`StepSummary`-like tuple ``(active_views, total_output_rows, tick)``.
    pub fn step(&self) -> PyResult<(usize, usize, u64)> {
        use crate::RUNTIME;
        RUNTIME
            .block_on(self.inner.step())
            .map(|s| (s.active_views, s.total_output_rows, s.tick))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Retrieve a serialized checkpoint from the coordinator as bytes.
    pub fn checkpoint(&self) -> PyResult<Vec<u8>> {
        use crate::RUNTIME;
        RUNTIME
            .block_on(self.inner.checkpoint())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Restore this job from previously captured checkpoint bytes.
    pub fn restore(&self, bytes: Vec<u8>) -> PyResult<()> {
        use crate::RUNTIME;
        RUNTIME
            .block_on(self.inner.restore(&bytes))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    pub fn __repr__(&self) -> String {
        format!("RemoteIvmJob(job_id='{}')", self.inner.job_id())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
            .expect("valid batch")
    }

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
    }

    #[test]
    fn delta_batch_from_inserts_num_rows() {
        let batch = make_batch();
        let cb = DeltaBatch::from_inserts(batch).expect("from_inserts");
        let py_cb = PyDeltaBatch { inner: cb };
        assert_eq!(py_cb.num_rows(), 3);
        assert!(!py_cb.is_empty());
    }

    #[test]
    fn delta_batch_from_deletes_num_rows() {
        let batch = make_batch();
        let cb = DeltaBatch::from_deletes(batch).expect("from_deletes");
        let py_cb = PyDeltaBatch { inner: cb };
        assert_eq!(py_cb.num_rows(), 3);
    }

    #[test]
    fn incremental_flow_register_and_list_views() {
        let flow = IncrementalFlow::new();
        let schema = make_schema();
        let spec = IncrementalViewSpec {
            name: "v1".to_string(),
            body_sql: "SELECT x FROM t".to_string(),
            output_schema: schema,
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        };
        flow.register_view(spec).expect("register");
        let py_flow = PyIncrementalFlow { inner: flow };
        let names = py_flow.view_names().expect("view_names");
        assert!(names.contains(&"v1".to_string()));
    }

    #[test]
    fn incremental_flow_step_increments_tick() {
        let flow = PyIncrementalFlow::py_new();
        assert_eq!(flow.tick().unwrap(), 0);
        flow.step().expect("step");
        assert_eq!(flow.tick().unwrap(), 1);
    }

    #[test]
    fn incremental_flow_feed_and_step() {
        let flow = PyIncrementalFlow::py_new();
        let schema = make_schema();
        let spec = IncrementalViewSpec {
            name: "v1".to_string(),
            body_sql: "SELECT x FROM t".to_string(),
            output_schema: schema,
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        };
        flow.inner.register_view(spec).expect("register");
        let cb = DeltaBatch::from_inserts(make_batch()).expect("cb");
        let py_cb = PyDeltaBatch { inner: cb };
        flow.inner
            .feed_source("t", py_cb.inner.clone())
            .expect("feed");
        let summary = flow.step().expect("step");
        assert_eq!(flow.tick().unwrap(), 1);
        // No DataFusion computation wired yet — output rows = 0.
        let _ = summary.total_output_rows;
    }

    #[test]
    fn incremental_flow_snapshot_none_before_step() {
        let flow = PyIncrementalFlow::py_new();
        let schema = make_schema();
        let spec = IncrementalViewSpec {
            name: "v1".to_string(),
            body_sql: "SELECT x FROM t".to_string(),
            output_schema: schema,
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        };
        flow.inner.register_view(spec).expect("register");
        let snap = flow.snapshot("v1".to_string()).expect("snapshot");
        assert!(snap.is_none());
    }

    #[test]
    fn delta_batch_is_empty_for_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let empty = RecordBatch::new_empty(schema);
        let cb = DeltaBatch::from_inserts(empty).expect("from_inserts");
        let py_cb = PyDeltaBatch { inner: cb };
        assert!(py_cb.is_empty());
    }
}
