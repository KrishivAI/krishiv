//! Python bindings for the stateful process function and state API — Phase G parity.
//!
//! Exposes:
//! - [`PyProcessContext`] — context passed to user process callbacks for emitting batches
//!   and registering timers.
//! - [`apply_process_function`] — attaches a Python process object to a DataFrame pipeline.
//! - [`PyValueState`] / [`PyListState`] / [`PyMapState`] — JSON-backed state descriptors.
//!
//! ## Example
//!
//! ```python
//! import krishiv
//!
//! class WordCount:
//!     def on_event(self, key, batch, row, ctx):
//!         ctx.emit(batch)
//!     def on_timer(self, key, fire_time_ms, ctx):
//!         pass
//!
//! stream = krishiv.apply_process_function(df, "word", WordCount())
//! ```

use arrow::record_batch::RecordBatch;
use krishiv_api::{OperatorConfig, ProcessContext, ProcessFunction};
use krishiv_dataflow::{ExecError, ExecResult};
use pyo3::prelude::*;

use crate::batch::PyBatch;

// ── PyProcessContext ──────────────────────────────────────────────────────────

/// Context passed to Python process-function callbacks.
///
/// After the Python callback returns, all accumulated emits and timer
/// registrations are flushed to the Rust execution context.
#[pyclass(name = "ProcessContext")]
pub struct PyProcessContext {
    pub(crate) emitted: Vec<RecordBatch>,
    pub(crate) event_timers: Vec<(String, i64)>,
    pub(crate) processing_timers: Vec<(String, i64)>,
}

#[pymethods]
impl PyProcessContext {
    /// Emit an output :class:`Batch` to the downstream pipeline.
    fn emit(&mut self, batch: PyBatch) {
        self.emitted.push(batch.record_batch().clone());
    }

    /// Register an event-time timer to fire at ``fire_time_ms`` (epoch milliseconds).
    fn register_event_time_timer(&mut self, key: String, fire_time_ms: i64) {
        self.event_timers.push((key, fire_time_ms));
    }

    /// Register a processing-time timer to fire at ``fire_time_ms`` (epoch milliseconds).
    fn register_processing_time_timer(&mut self, key: String, fire_time_ms: i64) {
        self.processing_timers.push((key, fire_time_ms));
    }
}

// Helper: call Python function and collect the context outputs.
// Returns (emitted_batches, event_timers, processing_timers).
fn call_python_with_ctx<F>(
    callable: &Py<PyAny>,
    args_fn: F,
) -> ExecResult<(Vec<RecordBatch>, Vec<(String, i64)>, Vec<(String, i64)>)>
where
    F: for<'py> FnOnce(Python<'py>, Py<PyProcessContext>) -> PyResult<()>,
{
    Python::attach(|py| {
        let bridge_ctx = Py::new(
            py,
            PyProcessContext {
                emitted: Vec::new(),
                event_timers: Vec::new(),
                processing_timers: Vec::new(),
            },
        )
        .map_err(|e| ExecError::InvalidInput(e.to_string()))?;

        args_fn(py, bridge_ctx.clone_ref(py))
            .map_err(|e| ExecError::InvalidInput(e.to_string()))?;

        let inner = bridge_ctx.borrow(py);
        Ok((
            inner.emitted.clone(),
            inner.event_timers.clone(),
            inner.processing_timers.clone(),
        ))
    })
}

// ── Bridge: Python object → Rust ProcessFunction ─────────────────────────────

struct PyProcessFunctionBridge {
    on_event_callable: Py<PyAny>,
    on_timer_callable: Py<PyAny>,
}

impl PyProcessFunctionBridge {
    fn new(on_event: Py<PyAny>, on_timer: Py<PyAny>) -> Self {
        Self {
            on_event_callable: on_event,
            on_timer_callable: on_timer,
        }
    }
}

impl ProcessFunction for PyProcessFunctionBridge {
    fn on_event(
        &mut self,
        key: &str,
        batch: &RecordBatch,
        row: usize,
        ctx: &mut ProcessContext<'_>,
    ) -> ExecResult<()> {
        let key_owned = key.to_owned();
        let batch_clone = batch.clone();

        let (emitted, event_timers, processing_timers) =
            Python::attach(|py| -> ExecResult<_> {
                let on_event = self.on_event_callable.clone_ref(py);
                let bridge_ctx = Py::new(
                    py,
                    PyProcessContext {
                        emitted: Vec::new(),
                        event_timers: Vec::new(),
                        processing_timers: Vec::new(),
                    },
                )
                .map_err(|e| ExecError::InvalidInput(e.to_string()))?;

                let py_batch = PyBatch::from_record_batch(batch_clone);
                on_event
                    .call1(
                        py,
                        (key_owned.as_str(), py_batch, row, bridge_ctx.clone_ref(py)),
                    )
                    .map_err(|e| ExecError::InvalidInput(e.to_string()))?;

                let inner = bridge_ctx.borrow(py);
                Ok((
                    inner.emitted.clone(),
                    inner.event_timers.clone(),
                    inner.processing_timers.clone(),
                ))
            })?;

        for b in emitted {
            ctx.emit(b);
        }
        for (k, t) in event_timers {
            ctx.register_event_time_timer(&k, t);
        }
        for (k, t) in processing_timers {
            ctx.register_processing_time_timer(&k, t);
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        key: &str,
        fire_time_ms: i64,
        ctx: &mut ProcessContext<'_>,
    ) -> ExecResult<()> {
        let key_owned = key.to_owned();

        let (emitted, event_timers, processing_timers) =
            Python::attach(|py| -> ExecResult<_> {
                let on_timer = self.on_timer_callable.clone_ref(py);
                let bridge_ctx = Py::new(
                    py,
                    PyProcessContext {
                        emitted: Vec::new(),
                        event_timers: Vec::new(),
                        processing_timers: Vec::new(),
                    },
                )
                .map_err(|e| ExecError::InvalidInput(e.to_string()))?;

                on_timer
                    .call1(
                        py,
                        (key_owned.as_str(), fire_time_ms, bridge_ctx.clone_ref(py)),
                    )
                    .map_err(|e| ExecError::InvalidInput(e.to_string()))?;

                let inner = bridge_ctx.borrow(py);
                Ok((
                    inner.emitted.clone(),
                    inner.event_timers.clone(),
                    inner.processing_timers.clone(),
                ))
            })?;

        for b in emitted {
            ctx.emit(b);
        }
        for (k, t) in event_timers {
            ctx.register_event_time_timer(&k, t);
        }
        for (k, t) in processing_timers {
            ctx.register_processing_time_timer(&k, t);
        }
        Ok(())
    }
}

// ── apply_process_function ────────────────────────────────────────────────────

/// Attach a Python process function to a :class:`DataFrame` streaming pipeline.
///
/// ``df`` is a :class:`DataFrame` (bounded or unbounded).
/// ``key_column`` is the column used to partition state per key.
/// ``func`` must be an object with ``on_event(key, batch, row, ctx)`` and
/// ``on_timer(key, fire_time_ms, ctx)`` methods.
///
/// Returns a new :class:`DataFrameStream` emitting the batches produced by ``ctx.emit()``.
#[pyfunction]
#[pyo3(signature = (df, key_column, func))]
pub fn apply_process_function(
    py: Python<'_>,
    df: &crate::dataframe::PyDataFrame,
    key_column: String,
    func: Py<PyAny>,
) -> PyResult<crate::dataframe::PyDataFrameStream> {
    let on_event: Py<PyAny> = func.getattr(py, "on_event").map_err(|_| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "process function must have an 'on_event' method",
        )
    })?;
    let on_timer: Py<PyAny> = func.getattr(py, "on_timer").map_err(|_| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "process function must have an 'on_timer' method",
        )
    })?;

    let inner_df = df.inner.clone();
    let bridge = PyProcessFunctionBridge::new(on_event, on_timer);

    let out_stream = py
        .detach(move || {
            crate::session::block_on_async(async move {
                let input_stream = inner_df.execute_stream_async().await.map_err(|e| {
                    krishiv_api::KrishivError::Runtime {
                        message: e.to_string(),
                    }
                })?;
                Ok::<_, krishiv_api::KrishivError>(krishiv_api::apply_process_function(
                    input_stream,
                    key_column,
                    Box::new(bridge),
                    OperatorConfig::new("py-process-fn"),
                ))
            })
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    Ok(crate::dataframe::PyDataFrameStream::from_stream(out_stream))
}

// ── State descriptors ─────────────────────────────────────────────────────────

/// A JSON-backed value-state descriptor for use inside process functions.
///
/// ## Example
///
/// ```python
/// state = ValueState("count")
/// raw = b""
/// count = state.get_json(raw) or 0
/// count += 1
/// raw = state.set_json(raw, count)
/// ```
#[pyclass(name = "ValueState")]
pub struct PyValueState {
    key: String,
}

#[pymethods]
impl PyValueState {
    #[new]
    pub fn new(key: String) -> Self {
        Self { key }
    }

    #[getter]
    fn key(&self) -> &str {
        &self.key
    }

    /// Decode state from ``raw`` bytes; returns ``None`` if bytes are empty.
    fn get_json<'py>(&self, py: Python<'py>, raw: &[u8]) -> PyResult<Option<Bound<'py, PyAny>>> {
        if raw.is_empty() {
            return Ok(None);
        }
        let s = std::str::from_utf8(raw).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "state contains invalid UTF-8: {e}"
            ))
        })?;
        let val = py.import("json")?.getattr("loads")?.call1((s,))?;
        Ok(Some(val))
    }

    /// Encode ``value`` to bytes (JSON) and return the new raw bytes.
    fn set_json(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<Vec<u8>> {
        let s: String = py
            .import("json")?
            .getattr("dumps")?
            .call1((value,))?
            .extract()?;
        Ok(s.into_bytes())
    }

    /// Return empty bytes (clear the state).
    fn clear(&self) -> Vec<u8> {
        Vec::new()
    }
}

/// A JSON-backed list-state descriptor for use inside process functions.
#[pyclass(name = "ListState")]
pub struct PyListState {
    key: String,
}

#[pymethods]
impl PyListState {
    #[new]
    pub fn new(key: String) -> Self {
        Self { key }
    }

    #[getter]
    fn key(&self) -> &str {
        &self.key
    }

    /// Return the current list; returns ``[]`` for empty state.
    fn get_json<'py>(&self, py: Python<'py>, raw: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        if raw.is_empty() {
            return Ok(py.eval(c"[]", None, None)?);
        }
        let s = std::str::from_utf8(raw).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "state contains invalid UTF-8: {e}"
            ))
        })?;
        Ok(py.import("json")?.getattr("loads")?.call1((s,))?)
    }

    /// Append ``item`` to the list and return new raw bytes.
    fn add_json(&self, py: Python<'_>, raw: Vec<u8>, item: Py<PyAny>) -> PyResult<Vec<u8>> {
        let json = py.import("json")?;
        let items: Bound<'_, PyAny> = if raw.is_empty() {
            py.eval(c"[]", None, None)?
        } else {
            let s = std::str::from_utf8(&raw).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "state contains invalid UTF-8: {e}"
                ))
            })?;
            json.getattr("loads")?.call1((s,))?
        };
        items.call_method1("append", (item,))?;
        let s: String = json.getattr("dumps")?.call1((&items,))?.extract()?;
        Ok(s.into_bytes())
    }

    /// Return empty bytes (clear the list).
    fn clear(&self) -> Vec<u8> {
        Vec::new()
    }
}

/// A JSON-backed map-state descriptor for use inside process functions.
#[pyclass(name = "MapState")]
pub struct PyMapState {
    key: String,
}

#[pymethods]
impl PyMapState {
    #[new]
    pub fn new(key: String) -> Self {
        Self { key }
    }

    #[getter]
    fn key(&self) -> &str {
        &self.key
    }

    /// Return the map; returns ``{}`` for empty state.
    fn get_map_json<'py>(&self, py: Python<'py>, raw: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        if raw.is_empty() {
            return Ok(py.eval(c"{}", None, None)?);
        }
        let s = std::str::from_utf8(raw).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "state contains invalid UTF-8: {e}"
            ))
        })?;
        Ok(py.import("json")?.getattr("loads")?.call1((s,))?)
    }

    /// Put ``k → v`` and return new raw bytes.
    fn put_json(
        &self,
        py: Python<'_>,
        raw: Vec<u8>,
        k: Py<PyAny>,
        v: Py<PyAny>,
    ) -> PyResult<Vec<u8>> {
        let json = py.import("json")?;
        let d: Bound<'_, PyAny> = if raw.is_empty() {
            py.eval(c"{}", None, None)?
        } else {
            let s = std::str::from_utf8(&raw).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "state contains invalid UTF-8: {e}"
                ))
            })?;
            json.getattr("loads")?.call1((s,))?
        };
        d.set_item(k, v)?;
        let s: String = json.getattr("dumps")?.call1((&d,))?.extract()?;
        Ok(s.into_bytes())
    }

    /// Return empty bytes (clear the map).
    fn clear(&self) -> Vec<u8> {
        Vec::new()
    }
}
