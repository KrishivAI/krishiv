//! Python callback bridges for the stateful stream-to-stream operators
//! (`StreamingDataFrame.co_process` / `broadcast_process`). Extracted from the
//! retired DataStream `Stream` classes; these adapt a Python handler object to
//! the engine's `CoProcessFunction` / `BroadcastProcessFunction` traits.

use pyo3::prelude::*;

use crate::batch::PyBatch;

pub(crate) struct PyCoProcessBridge {
    on_stream1: pyo3::Py<pyo3::PyAny>,
    on_stream2: pyo3::Py<pyo3::PyAny>,
    on_timer: pyo3::Py<pyo3::PyAny>,
}

/// Build a co-process bridge from a Python handler (`on_stream1`/`on_stream2`/
/// `on_timer`). Shared by the DataStream `connect` and `SDF.co_process`.
pub(crate) fn co_bridge_from_func(
    py: pyo3::Python<'_>,
    func: &pyo3::Py<pyo3::PyAny>,
) -> PyResult<PyCoProcessBridge> {
    let getattr = |name: &str| {
        func.getattr(py, name).map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "co-process function must have an '{name}' method"
            ))
        })
    };
    Ok(PyCoProcessBridge {
        on_stream1: getattr("on_stream1")?,
        on_stream2: getattr("on_stream2")?,
        on_timer: getattr("on_timer")?,
    })
}

impl krishiv_api::CoProcessFunction for PyCoProcessBridge {
    fn on_stream1(
        &mut self,
        key: &str,
        batch: &arrow::record_batch::RecordBatch,
        row: usize,
        ctx: &mut krishiv_api::ProcessContext<'_>,
    ) -> krishiv_dataflow::ExecResult<()> {
        dispatch_co_event(&self.on_stream1, key, batch, row, ctx)
    }

    fn on_stream2(
        &mut self,
        key: &str,
        batch: &arrow::record_batch::RecordBatch,
        row: usize,
        ctx: &mut krishiv_api::ProcessContext<'_>,
    ) -> krishiv_dataflow::ExecResult<()> {
        dispatch_co_event(&self.on_stream2, key, batch, row, ctx)
    }

    fn on_timer(
        &mut self,
        key: &str,
        fire_time_ms: i64,
        ctx: &mut krishiv_api::ProcessContext<'_>,
    ) -> krishiv_dataflow::ExecResult<()> {
        let key_owned = key.to_owned();
        let seed_state = ctx.state.clone();
        let (emitted, event_timers, processing_timers, new_state) =
            pyo3::Python::attach(|py| -> krishiv_dataflow::ExecResult<_> {
                let bridge_ctx = pyo3::Py::new(
                    py,
                    crate::process_api::PyProcessContext {
                        state: seed_state.clone(),
                        emitted: Vec::new(),
                        event_timers: Vec::new(),
                        processing_timers: Vec::new(),
                    },
                )
                .map_err(|e| krishiv_dataflow::ExecError::InvalidInput(e.to_string()))?;
                self.on_timer
                    .call1(py, (&key_owned, fire_time_ms, bridge_ctx.clone_ref(py)))
                    .map_err(|e| krishiv_dataflow::ExecError::InvalidInput(e.to_string()))?;
                let inner = bridge_ctx.borrow(py);
                Ok((
                    inner.emitted.clone(),
                    inner.event_timers.clone(),
                    inner.processing_timers.clone(),
                    inner.state.clone(),
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
        *ctx.state = new_state;
        Ok(())
    }
}

pub(crate) fn dispatch_co_event(
    callable: &pyo3::Py<pyo3::PyAny>,
    key: &str,
    batch: &arrow::record_batch::RecordBatch,
    row: usize,
    ctx: &mut krishiv_api::ProcessContext<'_>,
) -> krishiv_dataflow::ExecResult<()> {
    let key_owned = key.to_owned();
    let batch_clone = batch.clone();
    let seed_state = ctx.state.clone();
    let (emitted, event_timers, processing_timers, new_state) =
        pyo3::Python::attach(|py| -> krishiv_dataflow::ExecResult<_> {
            let bridge_ctx = pyo3::Py::new(
                py,
                crate::process_api::PyProcessContext {
                    state: seed_state.clone(),
                    emitted: Vec::new(),
                    event_timers: Vec::new(),
                    processing_timers: Vec::new(),
                },
            )
            .map_err(|e| krishiv_dataflow::ExecError::InvalidInput(e.to_string()))?;
            let py_batch = crate::batch::PyBatch::from_record_batch(batch_clone);
            callable
                .call1(py, (&key_owned, py_batch, row, bridge_ctx.clone_ref(py)))
                .map_err(|e| krishiv_dataflow::ExecError::InvalidInput(e.to_string()))?;
            let inner = bridge_ctx.borrow(py);
            Ok((
                inner.emitted.clone(),
                inner.event_timers.clone(),
                inner.processing_timers.clone(),
                inner.state.clone(),
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
    *ctx.state = new_state;
    Ok(())
}

/// Python `BroadcastContext` — collects emits from a broadcast process callback.
#[pyclass(name = "BroadcastContext")]
pub struct PyBroadcastContext {
    pub(crate) emitted: Vec<arrow::record_batch::RecordBatch>,
}

#[pymethods]
impl PyBroadcastContext {
    /// Emit an output batch to the downstream pipeline.
    fn emit(&mut self, batch: PyBatch) {
        self.emitted.push(batch.record_batch().clone());
    }
}

pub(crate) struct PyBroadcastBridge {
    on_keyed: pyo3::Py<pyo3::PyAny>,
    on_broadcast: pyo3::Py<pyo3::PyAny>,
}

/// Build a broadcast bridge from a Python handler (`on_keyed_event`/
/// `on_broadcast_event`). Shared by the DataStream broadcast and
/// `SDF.broadcast_process`.
pub(crate) fn broadcast_bridge_from_func(
    py: pyo3::Python<'_>,
    func: &pyo3::Py<pyo3::PyAny>,
) -> PyResult<PyBroadcastBridge> {
    let getattr = |name: &str| {
        func.getattr(py, name).map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "broadcast function must have an '{name}' method"
            ))
        })
    };
    Ok(PyBroadcastBridge {
        on_keyed: getattr("on_keyed_event")?,
        on_broadcast: getattr("on_broadcast_event")?,
    })
}

impl krishiv_api::BroadcastProcessFunction for PyBroadcastBridge {
    fn on_keyed_event(
        &mut self,
        key: &str,
        batch: &arrow::record_batch::RecordBatch,
        row: usize,
        ctx: &mut krishiv_api::BroadcastContext<'_>,
    ) -> krishiv_dataflow::ExecResult<()> {
        let key_owned = key.to_owned();
        let batch_clone = batch.clone();
        let emitted = pyo3::Python::attach(|py| -> krishiv_dataflow::ExecResult<_> {
            let bridge_ctx = pyo3::Py::new(
                py,
                PyBroadcastContext {
                    emitted: Vec::new(),
                },
            )
            .map_err(|e| krishiv_dataflow::ExecError::InvalidInput(e.to_string()))?;
            let py_batch = PyBatch::from_record_batch(batch_clone);
            self.on_keyed
                .call1(py, (&key_owned, py_batch, row, bridge_ctx.clone_ref(py)))
                .map_err(|e| krishiv_dataflow::ExecError::InvalidInput(e.to_string()))?;
            Ok(bridge_ctx.borrow(py).emitted.clone())
        })?;
        for b in emitted {
            ctx.emit(b);
        }
        Ok(())
    }

    fn on_broadcast_event(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
        row: usize,
        ctx: &mut krishiv_api::BroadcastContext<'_>,
    ) -> krishiv_dataflow::ExecResult<()> {
        let batch_clone = batch.clone();
        let emitted = pyo3::Python::attach(|py| -> krishiv_dataflow::ExecResult<_> {
            let bridge_ctx = pyo3::Py::new(
                py,
                PyBroadcastContext {
                    emitted: Vec::new(),
                },
            )
            .map_err(|e| krishiv_dataflow::ExecError::InvalidInput(e.to_string()))?;
            let py_batch = PyBatch::from_record_batch(batch_clone);
            self.on_broadcast
                .call1(py, (py_batch, row, bridge_ctx.clone_ref(py)))
                .map_err(|e| krishiv_dataflow::ExecError::InvalidInput(e.to_string()))?;
            Ok(bridge_ctx.borrow(py).emitted.clone())
        })?;
        for b in emitted {
            ctx.emit(b);
        }
        Ok(())
    }
}
