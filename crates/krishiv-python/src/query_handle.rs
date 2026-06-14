//! Python `QueryHandle` — Phase E async query lifecycle.
//!
//! `PyQueryHandle` wraps the Rust [`krishiv_api::QueryHandle`] and exposes
//! status polling, cancellation, and a genuine asyncio-compatible coroutine
//! for awaiting the query result.

use std::sync::{Arc, Mutex};

use krishiv_api::query::{QueryHandle, QueryStatus};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::query_result::PyQueryResult;

/// A handle to a running or completed Krishiv query.
///
/// Obtain one via ``Session.submit_async()`` or ``DataFrame.submit_async()``.
/// The handle can be ``await``-ed in Python asyncio code::
///
///     handle = session.submit_async("SELECT 1 AS n")
///     result = await handle.collect_async()
///     print(result.pretty())
///
/// Or cancelled::
///
///     handle.cancel()
#[pyclass(name = "QueryHandle")]
pub struct PyQueryHandle {
    // Arc<Mutex<Option<...>>> lets us take the handle out in `&self` async methods
    // without holding a `&mut self` borrow across an await point.
    inner: Arc<Mutex<Option<QueryHandle>>>,
}

impl PyQueryHandle {
    pub fn new(handle: QueryHandle) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(handle))),
        }
    }
}

#[pymethods]
impl PyQueryHandle {
    /// Current status string: ``"pending"``, ``"running"``, ``"completed"``,
    /// ``"cancelled"``, or ``"failed"``.
    pub fn status(&self) -> &'static str {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            None => "consumed",
            Some(h) => match h.status() {
                QueryStatus::Pending => "pending",
                QueryStatus::Running { .. } => "running",
                QueryStatus::Completed(_) => "completed",
                QueryStatus::Cancelled => "cancelled",
                QueryStatus::Failed(_) => "failed",
            },
        }
    }

    /// ``True`` if the query has reached a terminal state.
    pub fn is_done(&self) -> bool {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            None => true,
            Some(h) => h.status().is_terminal(),
        }
    }

    /// Request cancellation. Returns immediately.
    pub fn cancel(&self) {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(h) = guard.as_ref() {
            h.cancel();
        }
    }

    /// Current progress as ``(rows_scanned, rows_emitted)``.
    pub fn progress(&self) -> (u64, u64) {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            Some(h) => {
                let p = h.progress();
                (p.rows_scanned, p.rows_emitted)
            }
            None => (0, 0),
        }
    }

    /// The query's unique numeric ID.
    pub fn query_id(&self) -> Option<u64> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.as_ref().map(|h| h.id().as_u64())
    }

    /// Await completion and return a ``QueryResult``.
    ///
    /// Returns a Python coroutine that resolves to ``QueryResult`` on success
    /// or raises ``RuntimeError`` on failure or cancellation::
    ///
    ///     result = await handle.collect_async()
    pub async fn collect_async(&self) -> PyResult<PyQueryResult> {
        // Take the handle out of the Option before the first await point.
        let handle = {
            let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            guard
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("QueryHandle has already been consumed"))?
        };
        // Spawn actual collection on the embedded Tokio runtime so the Tokio
        // futures (watch::Receiver::changed) run in the right executor context.
        // JoinHandle::poll does not require a Tokio runtime, so the asyncio
        // event loop can drive it directly via PyO3's coroutine protocol.
        let join = crate::RUNTIME.spawn(async move { handle.wait().await });
        join.await
            .map_err(|e| PyRuntimeError::new_err(format!("query task panicked: {e}")))?
            .map(PyQueryResult::new)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Block the current thread until the query completes and return the result.
    ///
    /// For use in non-async contexts only. Prefer ``collect_async()`` inside asyncio.
    pub fn collect(&self, py: Python<'_>) -> PyResult<PyQueryResult> {
        let handle = {
            let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            guard
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("QueryHandle has already been consumed"))?
        };
        py.detach(move || {
            crate::session::block_on_async(handle.wait())
                .map(PyQueryResult::new)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    pub fn __repr__(&self) -> String {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            Some(h) => format!(
                "QueryHandle(id=q{}, status={})",
                h.id().as_u64(),
                match h.status() {
                    QueryStatus::Pending => "pending",
                    QueryStatus::Running { .. } => "running",
                    QueryStatus::Completed(_) => "completed",
                    QueryStatus::Cancelled => "cancelled",
                    QueryStatus::Failed(_) => "failed",
                }
            ),
            None => "QueryHandle(consumed)".to_owned(),
        }
    }
}
