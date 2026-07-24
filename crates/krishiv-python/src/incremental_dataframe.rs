//! pyo3 bindings for [`IncrementalDataFrame`] — the delta/IVM mode of the unified
//! DataFrame surface. Built by `DataFrame.to_incremental(name)`.
//!
//! The Rust surface is intentionally the minimal core (feed / step / read); the
//! ergonomic sugar (`insert`/`delete`/`upsert`/`transaction()`/`changes()`) is
//! grafted on in pure Python (`_pyspark.py`) so it needs no rebuild to evolve.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use krishiv_api::IncrementalDataFrame;

use crate::batch::PyBatch;
use crate::incremental::{PyDeltaBatch, PyStepSummary};

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Process-wide counter for auto-generated view names.
static VIEW_SEQ: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_view_name() -> String {
    format!("ivm_view_{}", VIEW_SEQ.fetch_add(1, Ordering::Relaxed))
}

/// The delta/IVM mode of the unified DataFrame surface.
///
/// Feed `DeltaBatch` changes with :meth:`apply` (auto-advances one tick unless
/// inside a :meth:`transaction`), then read the full :meth:`snapshot` or the
/// output change-feed via :meth:`last_output`.
#[pyclass(name = "IncrementalDataFrame", module = "krishiv")]
pub struct PyIncrementalDataFrame {
    pub(crate) inner: IncrementalDataFrame,
    defer_step: AtomicBool,
}

impl PyIncrementalDataFrame {
    pub(crate) fn new(inner: IncrementalDataFrame) -> Self {
        Self {
            inner,
            defer_step: AtomicBool::new(false),
        }
    }
}

#[pymethods]
impl PyIncrementalDataFrame {
    /// The view's identifier.
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// The source names this view reads (feedable via :meth:`apply`).
    #[getter]
    fn source_names(&self) -> Vec<String> {
        self.inner.source_names().to_vec()
    }

    /// An empty batch carrying the view's output schema — used by
    /// ``Session.view()`` to register this view as a client-side planning source
    /// for a downstream (view-DAG) query.
    fn schema_batch(&self) -> PyBatch {
        let empty = arrow::record_batch::RecordBatch::new_empty(self.inner.output_schema());
        PyBatch::from_record_batch(empty)
    }

    /// Feed a change to a source and (unless inside a :meth:`transaction`)
    /// advance one tick. `source` may be omitted only when the view has exactly
    /// one source.
    #[pyo3(signature = (delta, source=None))]
    fn apply(
        &self,
        py: Python<'_>,
        delta: PyRef<'_, PyDeltaBatch>,
        source: Option<String>,
    ) -> PyResult<()> {
        let delta = delta.inner.clone();
        let defer = self.defer_step.load(Ordering::SeqCst);
        py.detach(move || {
            crate::RUNTIME.block_on(async {
                self.inner.apply(source.as_deref(), &delta).await?;
                if !defer {
                    self.inner.step().await?;
                }
                Ok::<(), krishiv_api::KrishivError>(())
            })
        })
        .map_err(rt_err)
    }

    /// Advance one IVM tick, returning per-view output counts.
    fn step(&self, py: Python<'_>) -> PyResult<PyStepSummary> {
        py.detach(|| crate::RUNTIME.block_on(self.inner.step()))
            .map(PyStepSummary::from)
            .map_err(rt_err)
    }

    /// The current full materialized snapshot of the view (`None` if the view
    /// has not produced output yet). "Complete" output mode.
    fn snapshot(&self, py: Python<'_>) -> PyResult<Option<PyBatch>> {
        py.detach(|| crate::RUNTIME.block_on(self.inner.snapshot()))
            .map(|opt| opt.map(PyBatch::from_record_batch))
            .map_err(rt_err)
    }

    /// The output change-feed delta from the most recent tick (`None` if none).
    /// Together with repeated :meth:`apply`/:meth:`step` this is the "update"
    /// output mode. Embedded jobs only; a distributed job returns `None` here.
    fn last_output(&self) -> PyResult<Option<PyDeltaBatch>> {
        self.inner
            .last_output()
            .map(|opt| opt.map(|inner| PyDeltaBatch { inner }))
            .map_err(rt_err)
    }

    /// Internal: toggle tick-deferral used by the pure-Python `transaction()`
    /// context manager (feeds buffer; one tick fires on exit).
    fn _set_defer_step(&self, defer: bool) {
        self.defer_step.store(defer, Ordering::SeqCst);
    }

    fn __repr__(&self) -> String {
        format!(
            "IncrementalDataFrame(view='{}', sources={:?})",
            self.inner.name(),
            self.inner.source_names()
        )
    }
}
