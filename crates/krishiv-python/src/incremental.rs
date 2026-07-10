//! Python bindings for incremental view maintenance (IVM).
//!
//! Exposes [`PyDeltaBatch`], [`PyStepSummary`], and the mode-aware
//! [`PyIvmJob`] handle. Obtain a job from a session — the session picks the
//! embedded or remote backend automatically:
//!
//! ```python
//! import krishiv
//! import pyarrow as pa
//!
//! session = krishiv.Session()                 # embedded
//! job = session.ivm("revenue")
//!
//! class Revenue(krishiv.Schema):
//!     total: float
//! job.register_view("revenue", "SELECT sum(amount) AS total FROM orders",
//!                   Revenue, is_materialized=True)
//!
//! batch = make_example_batch()
//! job.feed("orders", krishiv.DeltaBatch.from_inserts(batch))
//! summary = job.step()
//! snap = job.snapshot("revenue")              # -> Batch or None
//! ```

use krishiv_api::{Checkpointable, FeedableJob, IvmJob, Job, StepReport};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec, LatenessSpec};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::batch::PyBatch;
use crate::schema::PySchema;

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn map_delta_error(e: krishiv_delta::DeltaError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

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
    #[staticmethod]
    pub fn from_inserts(batch: PyRef<'_, PyBatch>) -> PyResult<Self> {
        let rb = batch.record_batch().clone();
        let inner = DeltaBatch::from_inserts(rb).map_err(rt_err)?;
        Ok(Self { inner })
    }

    /// Construct a `DeltaBatch` from a :class:`Batch`, treating all rows as
    /// deletions / retractions (weight = -1).
    #[staticmethod]
    pub fn from_deletes(batch: PyRef<'_, PyBatch>) -> PyResult<Self> {
        let rb = batch.record_batch().clone();
        let inner = DeltaBatch::from_deletes(rb).map_err(rt_err)?;
        Ok(Self { inner })
    }

    /// Construct a `DeltaBatch` from a CDC change event.
    ///
    /// - INSERT: ``before=None, after=batch``
    /// - DELETE: ``before=batch, after=None``
    /// - UPDATE: ``before=old, after=new`` (retract old, insert new)
    /// - no-op:  ``before=None, after=None`` → returns ``None``
    #[staticmethod]
    #[pyo3(signature = (before=None, after=None))]
    pub fn from_cdc(
        before: Option<PyRef<'_, PyBatch>>,
        after: Option<PyRef<'_, PyBatch>>,
    ) -> PyResult<Option<Self>> {
        let before = before.map(|b| b.record_batch().clone());
        let after = after.map(|b| b.record_batch().clone());
        let opt = DeltaBatch::from_cdc(before, after).map_err(rt_err)?;
        Ok(opt.map(|inner| Self { inner }))
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

    /// Return ``True`` if all weights are >= 0 (no retractions).
    pub fn is_insert_only(&self) -> bool {
        self.inner.is_insert_only()
    }

    /// Return the data columns only (without the ``_weight`` column).
    pub fn data_batch(&self) -> PyBatch {
        PyBatch::from_record_batch(self.inner.data_batch())
    }

    /// Return rows with positive weight (insertions only, weight column stripped).
    pub fn filter_positive(&self) -> PyResult<PyBatch> {
        let batch = self.inner.filter_positive().map_err(map_delta_error)?;
        Ok(PyBatch::from_record_batch(batch))
    }

    /// Return rows with negative weight (retractions only, weight column stripped).
    pub fn filter_negative(&self) -> PyResult<PyBatch> {
        let batch = self.inner.filter_negative().map_err(map_delta_error)?;
        Ok(PyBatch::from_record_batch(batch))
    }

    /// Negate all weights (insert ↔ retract).
    pub fn negate(&self) -> PyResult<Self> {
        let negated = self.inner.negate().map_err(map_delta_error)?;
        Ok(Self { inner: negated })
    }

    /// Drop rows with net-zero weight after consolidation.
    pub fn drop_zeros(&self) -> PyResult<Self> {
        let dropped = self.inner.drop_zeros().map_err(map_delta_error)?;
        Ok(Self { inner: dropped })
    }

    /// Serialize to Arrow IPC bytes with ``DLT1`` magic prefix.
    /// Can be deserialized via :meth:`DeltaBatch.deserialize`.
    pub fn serialize(&self) -> PyResult<Vec<u8>> {
        krishiv_delta::serialize_delta_batch(&self.inner).map_err(map_delta_error)
    }

    /// Deserialize from Arrow IPC bytes (with or without DLT1 prefix).
    #[staticmethod]
    pub fn deserialize(bytes: Vec<u8>) -> PyResult<Self> {
        let db = krishiv_delta::deserialize_delta_batch(&bytes).map_err(map_delta_error)?;
        Ok(Self { inner: db })
    }

    /// Build a ``DeltaBatch`` encoding an update: ``before`` rows get weight
    /// ``-1``, ``after`` rows get weight ``+1``. Schemas must match.
    #[staticmethod]
    pub fn from_update(before: &PyBatch, after: &PyBatch) -> PyResult<Self> {
        let db = DeltaBatch::from_update(before.record_batch(), after.record_batch())
            .map_err(map_delta_error)?;
        Ok(Self { inner: db })
    }

    /// Build a ``DeltaBatch`` from an already-weighted ``RecordBatch`` (last
    /// column must be ``_weight: Int64``).
    #[staticmethod]
    pub fn from_weighted(batch: &PyBatch) -> PyResult<Self> {
        let db =
            DeltaBatch::from_weighted(batch.record_batch().clone()).map_err(map_delta_error)?;
        Ok(Self { inner: db })
    }

    pub fn __repr__(&self) -> String {
        format!("DeltaBatch(rows={})", self.inner.num_rows())
    }
}

// ── PyStepSummary ─────────────────────────────────────────────────────────────

/// Summary returned by :meth:`IvmJob.step`.
#[pyclass(name = "StepSummary")]
pub struct PyStepSummary {
    /// Total number of output rows emitted across all views this tick.
    #[pyo3(get)]
    pub total_output_rows: usize,
    /// Number of views that produced non-empty output this tick.
    #[pyo3(get)]
    pub active_views: usize,
    /// The tick counter after this step.
    #[pyo3(get)]
    pub tick: u64,
    /// View names that ran on the O(state) DiffBased path during this step
    /// (forced or because no incremental plan was built). Empty for
    /// streaming-only jobs and when every view has a working incremental plan.
    #[pyo3(get)]
    pub degraded_views: Vec<String>,
    /// Per-view errors that caused a view to be skipped during this step.
    /// Each entry is a `(view_name, kind, message)` triple; the step did
    /// not panic; subsequent ticks re-evaluate.
    #[pyo3(get)]
    pub errored_views: Vec<PyViewError>,
}

#[pymethods]
impl PyStepSummary {
    pub fn __repr__(&self) -> String {
        format!(
            "StepSummary(total_output_rows={}, active_views={}, tick={}, \
             degraded_views={}, errored_views={})",
            self.total_output_rows,
            self.active_views,
            self.tick,
            self.degraded_views.len(),
            self.errored_views.len()
        )
    }
}

impl From<StepReport> for PyStepSummary {
    fn from(s: StepReport) -> Self {
        Self {
            total_output_rows: s.total_output_rows,
            active_views: s.active_views,
            tick: s.tick,
            degraded_views: s.degraded_views,
            errored_views: s.errored_views.into_iter().map(PyViewError::from).collect(),
        }
    }
}

/// One view's failure during a step, surfaced via `StepSummary.errored_views`.
#[pyclass(name = "ViewError")]
#[derive(Clone)]
pub struct PyViewError {
    #[pyo3(get)]
    pub view: String,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub message: String,
}

#[pymethods]
impl PyViewError {
    pub fn __repr__(&self) -> String {
        format!(
            "ViewError(view={}, kind={}, message={})",
            self.view, self.kind, self.message
        )
    }
}

impl From<krishiv_api::compute::job::ViewError> for PyViewError {
    fn from(e: krishiv_api::compute::job::ViewError) -> Self {
        use krishiv_api::compute::job::ViewErrorKind;
        let kind = match e.kind {
            ViewErrorKind::OperatorApply => "operator_apply",
            ViewErrorKind::ViewSql => "view_sql",
            ViewErrorKind::Publish => "publish",
        };
        Self {
            view: e.view,
            kind: kind.to_owned(),
            message: e.message,
        }
    }
}

// ── PyIvmJob ──────────────────────────────────────────────────────────────────

/// Mode-aware handle to an incremental-view-maintenance job.
///
/// Obtain via :meth:`Session.ivm`. The same handle works whether the session is
/// embedded (in-process) or distributed (remote coordinator) — the session
/// chooses the backend, and every method behaves identically.
///
/// All methods are synchronous wrappers that drive the shared Krishiv Tokio
/// runtime; in distributed mode they issue coordinator HTTP calls.
#[pyclass(name = "IvmJob")]
#[derive(Clone)]
pub struct PyIvmJob {
    pub(crate) inner: IvmJob,
}

#[pymethods]
impl PyIvmJob {
    /// The job's stable identifier.
    #[getter]
    pub fn job_id(&self) -> &str {
        self.inner.job_id()
    }

    /// Register or update an incremental view on this job.
    ///
    /// ``schema`` is a :class:`krishiv.Schema` subclass declaring output columns.
    #[pyo3(signature = (name, body_sql, schema, is_materialized=false, is_recursive=false))]
    pub fn register_view(
        &self,
        py: Python<'_>,
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
        py.detach(move || crate::RUNTIME.block_on(self.inner.register_view(spec)))
            .map_err(rt_err)
    }

    /// Register an incremental view with LATENESS annotations
    /// (``lateness_ms`` maps column name → lateness in milliseconds).
    #[pyo3(signature = (name, body_sql, schema, lateness_ms, is_materialized=false, is_recursive=false))]
    pub fn register_view_with_lateness(
        &self,
        py: Python<'_>,
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
            .map(|(column, lateness_ms)| LatenessSpec {
                column,
                lateness_ms,
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
        py.detach(move || crate::RUNTIME.block_on(self.inner.register_view(spec)))
            .map_err(rt_err)
    }

    /// Feed a :class:`DeltaBatch` as input for a named source.
    ///
    /// Build the delta with :meth:`DeltaBatch.from_inserts`,
    /// :meth:`DeltaBatch.from_deletes`, or :meth:`DeltaBatch.from_cdc`.
    /// Buffered until the next :meth:`step`.
    pub fn feed(
        &self,
        py: Python<'_>,
        source: String,
        delta: PyRef<'_, PyDeltaBatch>,
    ) -> PyResult<()> {
        let delta = delta.inner.clone();
        py.detach(move || {
            crate::RUNTIME
                .block_on(async { self.inner.feed(&source, &delta).await })
                .map_err(rt_err)
        })
    }

    /// Feed a full streaming snapshot; differentiated against the previous
    /// snapshot for this source.
    pub fn feed_snapshot(
        &self,
        py: Python<'_>,
        source: String,
        batches: Vec<PyRef<'_, PyBatch>>,
    ) -> PyResult<()> {
        let rbs: Vec<_> = batches.iter().map(|b| b.record_batch().clone()).collect();
        py.detach(move || {
            crate::RUNTIME
                .block_on(async { self.inner.feed_snapshot(&source, &rbs).await })
                .map_err(rt_err)
        })
    }

    /// Advance one tick. Returns a :class:`StepSummary`.
    ///
    /// The GIL is released while waiting for the async tick to complete so that
    /// other Python threads (e.g. feed producers) can run concurrently.
    pub fn step(&self, py: Python<'_>) -> PyResult<PyStepSummary> {
        py.detach(|| {
            crate::RUNTIME
                .block_on(self.inner.step())
                .map(PyStepSummary::from)
                .map_err(rt_err)
        })
    }

    /// Feed a ``DeltaBatch`` and step in one call. Equivalent to ``feed`` +
    /// ``step``. Returns a ``StepSummary``.
    pub fn feed_and_step(
        &self,
        py: Python<'_>,
        source: String,
        delta: PyRef<'_, PyDeltaBatch>,
    ) -> PyResult<PyStepSummary> {
        self.feed(py, source, delta)?;
        self.step(py)
    }

    /// Feed a plain ``Batch`` as insertions and step in one call. Creates a
    /// ``DeltaBatch`` automatically — no need to call ``DeltaBatch.from_inserts``.
    pub fn feed_inserts_and_step(
        &self,
        py: Python<'_>,
        source: String,
        batch: PyRef<'_, PyBatch>,
    ) -> PyResult<PyStepSummary> {
        let delta = krishiv_delta::DeltaBatch::from_inserts(batch.record_batch().clone())
            .map_err(map_delta_error)?;
        let delta_batch = PyDeltaBatch { inner: delta };
        let inner = self.inner.clone();
        py.detach(move || {
            crate::RUNTIME
                .block_on(inner.feed_and_step(&source, &delta_batch.inner))
                .map(PyStepSummary::from)
                .map_err(rt_err)
        })
    }

    /// Read the current materialized snapshot of a view, or ``None``.
    pub fn snapshot(&self, py: Python<'_>, view: String) -> PyResult<Option<PyBatch>> {
        py.detach(move || {
            crate::RUNTIME
                .block_on(async { self.inner.snapshot(&view).await })
                .map(|opt| opt.map(PyBatch::from_record_batch))
                .map_err(rt_err)
        })
    }

    /// Enable delta-checkpoint accumulation (embedded only; remote always on).
    pub fn enable_delta_checkpoints(&self) -> PyResult<()> {
        self.inner.enable_delta_checkpoints().map_err(rt_err)
    }

    /// Enable content-addressed input dedup (embedded only).
    pub fn enable_input_dedup(&self) -> PyResult<()> {
        self.inner.enable_input_dedup().map_err(rt_err)
    }

    /// Serialize a full checkpoint to bytes.
    pub fn checkpoint(&self, py: Python<'_>) -> PyResult<Vec<u8>> {
        py.detach(|| {
            crate::RUNTIME
                .block_on(self.inner.checkpoint())
                .map_err(rt_err)
        })
    }

    /// Restore from a full checkpoint.
    pub fn restore(&self, py: Python<'_>, bytes: Vec<u8>) -> PyResult<()> {
        py.detach(move || {
            crate::RUNTIME
                .block_on(async { self.inner.restore(&bytes).await })
                .map_err(rt_err)
        })
    }

    /// Serialize only the deltas accumulated since the last call.
    pub fn checkpoint_delta(&self, py: Python<'_>) -> PyResult<Vec<u8>> {
        py.detach(|| {
            crate::RUNTIME
                .block_on(self.inner.checkpoint_delta())
                .map_err(rt_err)
        })
    }

    /// Apply delta-checkpoint bytes on top of restored state.
    pub fn restore_delta(&self, py: Python<'_>, bytes: Vec<u8>) -> PyResult<()> {
        py.detach(move || {
            crate::RUNTIME
                .block_on(async { self.inner.restore_delta(&bytes).await })
                .map_err(rt_err)
        })
    }

    pub fn __repr__(&self) -> String {
        format!("IvmJob(job_id='{}')", self.inner.job_id())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
            .expect("valid batch")
    }

    #[test]
    fn delta_batch_from_inserts_num_rows() {
        let cb = DeltaBatch::from_inserts(make_batch()).expect("from_inserts");
        let py_cb = PyDeltaBatch { inner: cb };
        assert_eq!(py_cb.num_rows(), 3);
        assert!(!py_cb.is_empty());
    }

    #[test]
    fn delta_batch_from_cdc_insert_and_noop() {
        // INSERT
        let inner = DeltaBatch::from_cdc(None, Some(make_batch()))
            .expect("from_cdc")
            .expect("insert produces a delta");
        let py_cb = PyDeltaBatch { inner };
        assert_eq!(py_cb.num_rows(), 3);
        // no-op
        assert!(DeltaBatch::from_cdc(None, None).unwrap().is_none());
    }

    #[test]
    fn step_summary_carries_tick() {
        let summary = PyStepSummary::from(StepReport {
            active_views: 2,
            total_output_rows: 5,
            tick: 7,
            ..Default::default()
        });
        assert_eq!(summary.tick, 7);
        assert_eq!(summary.active_views, 2);
        assert_eq!(summary.total_output_rows, 5);
    }
}
