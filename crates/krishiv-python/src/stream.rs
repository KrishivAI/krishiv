//! `Stream`, `KeyedStream`, `WindowedStream`, `ConnectedStreams`, and `BroadcastStream`.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::agg::descriptors_from_kwargs;
use crate::batch::PyBatch;
use crate::errors::SchemaError;
use crate::pipeline::{StreamPipeline, WindowDescriptor, WindowKind};
use crate::stream_exec::execute_pipeline;
use crate::windows::{PyWindowSpec, ensure_watermark_before_window};

fn new_windowed_stream(pipeline: StreamPipeline) -> PyWindowedStream {
    PyWindowedStream {
        pipeline,
        cached: Mutex::new(None),
        stream_rx: Mutex::new(None),
    }
}

fn stream_repr(pipeline: &StreamPipeline) -> String {
    format!(
        "Stream(source={:?}, watermark={})",
        pipeline.source_id, pipeline.watermark_column
    )
}

/// Streaming source handle.
#[pyclass(name = "Stream")]
pub struct PyStream {
    pub(crate) pipeline: StreamPipeline,
}

impl PyStream {
    pub fn from_pipeline(
        session: std::sync::Arc<krishiv_api::Session>,
        source_id: String,
        watermark_column: String,
        max_lateness_ms: u64,
    ) -> Self {
        Self {
            pipeline: StreamPipeline::new(session, source_id, watermark_column, max_lateness_ms),
        }
    }

    /// Build a stream from session-registered memory batches (`memory:<name>` source).
    pub fn from_memory(
        session: std::sync::Arc<krishiv_api::Session>,
        name: String,
        watermark_column: String,
        max_lateness_ms: u64,
        batches: Vec<crate::batch::PyBatch>,
    ) -> PyResult<Self> {
        let record_batches: Vec<arrow::record_batch::RecordBatch> =
            batches.iter().map(|b| b.record_batch().clone()).collect();
        session
            .register_memory_stream(&name, record_batches)
            .map_err(crate::errors::map_krishiv_error)?;
        Ok(Self {
            pipeline: StreamPipeline::new(
                session,
                format!("memory:{name}"),
                watermark_column,
                max_lateness_ms,
            ),
        })
    }
}

#[pymethods]
impl PyStream {
    pub fn with_watermark(&self, column: String, max_lateness_ms: u64) -> PyStream {
        PyStream {
            pipeline: self.pipeline.with_watermark(column, max_lateness_ms),
        }
    }

    /// Set state TTL for stateful window operators on this stream (milliseconds).
    pub fn with_state_ttl(&self, ttl_ms: u64) -> PyStream {
        PyStream {
            pipeline: self.pipeline.with_state_ttl(ttl_ms),
        }
    }

    /// Alias for `with_watermark` (R13 name).
    pub fn watermark(&self, column: String, max_lateness_ms: u64) -> PyStream {
        self.with_watermark(column, max_lateness_ms)
    }

    #[pyo3(signature = (*columns))]
    pub fn key_by(&self, columns: Vec<String>) -> PyResult<PyKeyedStream> {
        if columns.is_empty() {
            return Err(SchemaError::new_err(
                "key_by() requires at least one column",
            ));
        }
        Ok(PyKeyedStream {
            pipeline: self.pipeline.with_keys(columns),
        })
    }

    /// Tumbling window duration in milliseconds (preferred).
    pub fn tumbling_window_ms(&self, window_ms: u64) -> PyResult<PyWindowedStream> {
        ensure_watermark_before_window(
            &self.pipeline.watermark_column,
            self.pipeline.max_lateness_ms,
        )?;
        let pipeline = self
            .pipeline
            .with_window(crate::pipeline::WindowDescriptor {
                kind: WindowKind::Tumbling,
                size_ms: window_ms,
                slide_ms: None,
                gap_ms: None,
            });
        Ok(new_windowed_stream(pipeline))
    }

    /// Tumbling window duration in seconds (multiplied by 1000 for the engine).
    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        self.tumbling_window_ms(window_secs.saturating_mul(1000))
    }

    /// Sliding (hopping) window: `size_ms` is the window length, `slide_ms` is the hop interval.
    pub fn sliding_window_ms(&self, size_ms: u64, slide_ms: u64) -> PyResult<PyWindowedStream> {
        ensure_watermark_before_window(
            &self.pipeline.watermark_column,
            self.pipeline.max_lateness_ms,
        )?;
        let pipeline = self.pipeline.with_window(WindowDescriptor {
            kind: WindowKind::Sliding,
            size_ms,
            slide_ms: Some(slide_ms),
            gap_ms: None,
        });
        Ok(new_windowed_stream(pipeline))
    }

    /// Session window: windows close after `gap_ms` of inactivity per key.
    pub fn session_window_ms(&self, gap_ms: u64) -> PyResult<PyWindowedStream> {
        ensure_watermark_before_window(
            &self.pipeline.watermark_column,
            self.pipeline.max_lateness_ms,
        )?;
        let pipeline = self.pipeline.with_window(WindowDescriptor {
            kind: WindowKind::Session,
            size_ms: gap_ms,
            slide_ms: None,
            gap_ms: Some(gap_ms),
        });
        Ok(new_windowed_stream(pipeline))
    }

    /// Session window alias: `session_window(gap_ms)` — same as `session_window_ms`.
    pub fn session_window(&self, gap_ms: u64) -> PyResult<PyWindowedStream> {
        self.session_window_ms(gap_ms)
    }

    /// Connect this stream with another for dual-stream `CoProcessFunction` processing.
    pub fn connect(&self, other: &PyStream) -> PyConnectedStreams {
        PyConnectedStreams {
            left: self.pipeline.clone(),
            right: other.pipeline.clone(),
        }
    }

    /// Treat this stream as the broadcast side of a broadcast–keyed join.
    ///
    /// Returns a `BroadcastStream` handle. Call `.apply_broadcast_process(keyed, key_col, fn)`
    /// on it to drive a `BroadcastProcessFunction`.
    pub fn broadcast(&self) -> PyBroadcastStream {
        PyBroadcastStream {
            pipeline: self.pipeline.clone(),
        }
    }

    /// Apply a per-source watermark spec to this multi-source stream.
    ///
    /// `spec` is a :class:`MultiSourceWatermarkSpec` built with
    /// ``MultiSourceWatermarkSpec().add_source("src", lag_ms).with_source_id_column("col")``.
    pub fn with_multi_source_watermark(&self, spec: &PyMultiSourceWatermarkSpec) -> PyStream {
        let mut pipeline = self.pipeline.clone();
        for (source_id, lag_ms) in &spec.source_lags {
            pipeline = pipeline.with_source_watermark(source_id.clone(), *lag_ms);
        }
        if let Some(col) = &spec.source_id_column {
            pipeline = pipeline.with_source_id_column(col.clone());
        }
        PyStream { pipeline }
    }

    pub fn __repr__(&self) -> String {
        stream_repr(&self.pipeline)
    }

    pub fn _repr_html_(&self) -> String {
        self.pipeline.repr_html()
    }
}

/// Stream partitioned by key columns.
#[pyclass(name = "KeyedStream")]
pub struct PyKeyedStream {
    pub(crate) pipeline: StreamPipeline,
}

#[pymethods]
impl PyKeyedStream {
    pub fn window(&self, spec: PyWindowSpec) -> PyResult<PyWindowedStream> {
        ensure_watermark_before_window(
            &self.pipeline.watermark_column,
            self.pipeline.max_lateness_ms,
        )?;
        Ok(new_windowed_stream(
            self.pipeline.with_window(spec.into_descriptor()),
        ))
    }

    /// Tumbling window in seconds. Prefer `tumbling_window_ms` for explicit milliseconds.
    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        let stream = PyStream {
            pipeline: self.pipeline.clone(),
        };
        stream.tumbling_window(window_secs)
    }

    /// Tumbling window in milliseconds — consistent with `DataFrame.tumbling_window(ms)`.
    pub fn tumbling_window_ms(&self, window_ms: u64) -> PyResult<PyWindowedStream> {
        let stream = PyStream {
            pipeline: self.pipeline.clone(),
        };
        stream.tumbling_window_ms(window_ms)
    }

    /// Sliding (hopping) window in milliseconds.
    pub fn sliding_window_ms(&self, size_ms: u64, slide_ms: u64) -> PyResult<PyWindowedStream> {
        let stream = PyStream {
            pipeline: self.pipeline.clone(),
        };
        stream.sliding_window_ms(size_ms, slide_ms)
    }

    /// Session window in milliseconds.
    pub fn session_window_ms(&self, gap_ms: u64) -> PyResult<PyWindowedStream> {
        let stream = PyStream {
            pipeline: self.pipeline.clone(),
        };
        stream.session_window_ms(gap_ms)
    }

    /// Session window alias: `session_window(gap_ms)` — same as `session_window_ms`.
    pub fn session_window(&self, gap_ms: u64) -> PyResult<PyWindowedStream> {
        self.session_window_ms(gap_ms)
    }

    /// Connect this keyed stream with another for dual-stream co-processing.
    pub fn connect(&self, other: &PyKeyedStream) -> PyConnectedStreams {
        PyConnectedStreams {
            left: self.pipeline.clone(),
            right: other.pipeline.clone(),
        }
    }

    /// Apply per-source watermark lags to this keyed multi-source stream.
    pub fn with_multi_source_watermark(&self, spec: &PyMultiSourceWatermarkSpec) -> PyKeyedStream {
        let stream = PyStream {
            pipeline: self.pipeline.clone(),
        };
        let updated = stream.with_multi_source_watermark(spec);
        PyKeyedStream {
            pipeline: updated.pipeline,
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "KeyedStream(keys={:?}, source={:?})",
            self.pipeline.key_columns, self.pipeline.source_id
        )
    }

    pub fn _repr_html_(&self) -> String {
        self.pipeline.repr_html()
    }
}

/// Windowed, keyed stream — async iterable and aggregations.
#[pyclass(unsendable, name = "WindowedStream")]
pub struct PyWindowedStream {
    pub(crate) pipeline: StreamPipeline,
    cached: Mutex<Option<Vec<PyBatch>>>,
    stream_rx: Mutex<Option<tokio::sync::mpsc::Receiver<PyResult<PyBatch>>>>,
}

impl PyWindowedStream {
    fn ensure_collected(&self) -> PyResult<()> {
        let mut cached = self.cached.lock().unwrap_or_else(|e| e.into_inner());
        if cached.is_none() {
            let batches = execute_pipeline(&self.pipeline)?;
            *cached = Some(batches);
        }
        Ok(())
    }
}

#[pymethods]
impl PyWindowedStream {
    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        let stream = PyStream {
            pipeline: self.pipeline.clone(),
        };
        stream.tumbling_window(window_secs)
    }

    #[pyo3(signature = (**kwargs))]
    pub fn agg(&self, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<PyWindowedStream> {
        let aggs = descriptors_from_kwargs(kwargs)?;
        if aggs.is_empty() {
            return Err(SchemaError::new_err(
                "agg() requires at least one named aggregation expression",
            ));
        }
        Ok(new_windowed_stream(self.pipeline.with_aggregations(aggs)))
    }

    pub fn collect(&self, py: Python<'_>) -> PyResult<Vec<PyBatch>> {
        // G5: Release the GIL during the blocking windowed computation so other
        // Python threads and async tasks can run without stalling.
        // pyo3 0.28 uses `detach` instead of the older `allow_threads`.
        py.detach(|| self.ensure_collected())?;
        Ok(self
            .cached
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or_default())
    }

    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        let mut rx_guard = slf.stream_rx.lock().unwrap_or_else(|e| e.into_inner());
        if rx_guard.is_none() {
            match crate::stream_exec::spawn_pipeline_stream(slf.pipeline.clone()) {
                Ok(rx) => *rx_guard = Some(rx),
                Err(e) => {
                    tracing::error!(error = %e, "failed to spawn pipeline stream in __aiter__; __anext__ will raise StopAsyncIteration");
                }
            }
        }
        drop(rx_guard);
        slf
    }

    pub fn __anext__(&self, py: Python<'_>) -> PyResult<Option<Py<PyBatch>>> {
        let mut rx_guard = self.stream_rx.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(rx) = rx_guard.as_mut() {
            // Use block_in_place when running inside a tokio context so the
            // worker thread is properly yielded during the recv rather than
            // being permanently blocked. When no tokio runtime is active, fall
            // back to blocking_recv directly. Release the GIL for the wait
            // (py.detach) so other Python threads / the event loop thread
            // (when this call is offloaded to an executor) are not frozen
            // for the whole wait duration.
            let res = py.detach(|| match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    tokio::task::block_in_place(|| handle.block_on(async { rx.recv().await }))
                }
                Err(_) => rx.blocking_recv(),
            });
            match res {
                Some(Ok(batch)) => Ok(Some(Py::new(py, batch)?)),
                Some(Err(e)) => Err(e),
                None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
            }
        } else {
            Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()))
        }
    }

    /// Non-blocking poll — returns `None` immediately if no batch is ready.
    /// Useful for polling patterns in async Python without blocking the event loop.
    pub fn try_next(&self, py: Python<'_>) -> PyResult<Option<Py<PyBatch>>> {
        let mut rx_guard = self.stream_rx.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(rx) = rx_guard.as_mut() {
            match rx.try_recv() {
                Ok(Ok(batch)) => Ok(Some(Py::new(py, batch)?)),
                Ok(Err(e)) => Err(e),
                Err(_) => Ok(None), // channel empty or closed — not ready yet
            }
        } else {
            Ok(None)
        }
    }

    /// Slide interval in milliseconds; `None` for tumbling or session windows.
    #[getter]
    pub fn slide_ms(&self) -> Option<u64> {
        self.pipeline.window.as_ref().and_then(|w| w.slide_ms)
    }

    /// Session gap in milliseconds; `None` for tumbling or sliding windows.
    #[getter]
    pub fn session_gap_ms(&self) -> Option<u64> {
        self.pipeline.window.as_ref().and_then(|w| w.gap_ms)
    }

    /// Window size in milliseconds.
    #[getter]
    pub fn window_size_ms(&self) -> Option<u64> {
        self.pipeline.window.as_ref().map(|w| w.size_ms)
    }

    /// Window kind: `"tumbling"`, `"sliding"`, or `"session"`.
    #[getter]
    pub fn window_kind(&self) -> Option<&'static str> {
        self.pipeline.window.as_ref().map(|w| match w.kind {
            WindowKind::Tumbling => "tumbling",
            WindowKind::Sliding => "sliding",
            WindowKind::Session => "session",
        })
    }

    pub fn __repr__(&self) -> String {
        format!(
            "WindowedStream(watermark={}, window={:?})",
            self.pipeline.watermark_column, self.pipeline.window
        )
    }

    pub fn _repr_html_(&self) -> String {
        self.pipeline.repr_html()
    }
}

// ── G10: ConnectedStreams / CoProcessFunction ─────────────────────────────────

/// Bridge: Python object with `on_stream1`, `on_stream2`, `on_timer` → Rust `CoProcessFunction`.
struct PyCoProcessBridge {
    on_stream1: pyo3::Py<pyo3::PyAny>,
    on_stream2: pyo3::Py<pyo3::PyAny>,
    on_timer: pyo3::Py<pyo3::PyAny>,
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
        let (emitted, event_timers, processing_timers) =
            pyo3::Python::attach(|py| -> krishiv_dataflow::ExecResult<_> {
                let bridge_ctx = pyo3::Py::new(
                    py,
                    crate::process_api::PyProcessContext {
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

fn dispatch_co_event(
    callable: &pyo3::Py<pyo3::PyAny>,
    key: &str,
    batch: &arrow::record_batch::RecordBatch,
    row: usize,
    ctx: &mut krishiv_api::ProcessContext<'_>,
) -> krishiv_dataflow::ExecResult<()> {
    let key_owned = key.to_owned();
    let batch_clone = batch.clone();
    let (emitted, event_timers, processing_timers) =
        pyo3::Python::attach(|py| -> krishiv_dataflow::ExecResult<_> {
            let bridge_ctx = pyo3::Py::new(
                py,
                crate::process_api::PyProcessContext {
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

/// A pair of streams to process together with a [`CoProcessFunction`].
///
/// Created by `stream.connect(other)` or `keyed_stream.connect(other)`.
#[pyclass(name = "ConnectedStreams")]
pub struct PyConnectedStreams {
    left: StreamPipeline,
    right: StreamPipeline,
}

#[pymethods]
impl PyConnectedStreams {
    /// Apply a Python co-process function to this pair of connected streams.
    ///
    /// `key_column` — the column used to shard per-key state across both streams.
    /// `func` — object with `on_stream1`, `on_stream2`, and `on_timer` methods.
    ///
    /// Returns a :class:`DataFrameStream` emitting batches produced by `ctx.emit()`.
    pub fn apply_co_process(
        &self,
        py: pyo3::Python<'_>,
        key_column: String,
        func: pyo3::Py<pyo3::PyAny>,
    ) -> PyResult<crate::dataframe::PyDataFrameStream> {
        let on_stream1 = func.getattr(py, "on_stream1").map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "co-process function must have an 'on_stream1' method",
            )
        })?;
        let on_stream2 = func.getattr(py, "on_stream2").map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "co-process function must have an 'on_stream2' method",
            )
        })?;
        let on_timer = func.getattr(py, "on_timer").map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "co-process function must have an 'on_timer' method",
            )
        })?;

        let left_pipeline = self.left.clone();
        let right_pipeline = self.right.clone();

        let out_batches =
            py.detach(move || -> PyResult<Vec<arrow::record_batch::RecordBatch>> {
                let left_batches = crate::session::block_on_async(
                    crate::stream_exec::resolve_input_batches(&left_pipeline),
                )
                .map_err(crate::errors::map_krishiv_error)?;

                let right_batches = crate::session::block_on_async(
                    crate::stream_exec::resolve_input_batches(&right_pipeline),
                )
                .map_err(crate::errors::map_krishiv_error)?;

                let bridge = PyCoProcessBridge {
                    on_stream1,
                    on_stream2,
                    on_timer,
                };
                let mut executor =
                    krishiv_api::CoProcessExecutor::new(&key_column, Box::new(bridge));

                let mut emitted = Vec::new();
                for batch in &left_batches {
                    let out = executor
                        .process_stream1(batch, 0)
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    emitted.extend(out);
                }
                for batch in &right_batches {
                    let out = executor
                        .process_stream2(batch, 0)
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    emitted.extend(out);
                }
                let timer_out = executor
                    .fire_timers(i64::MAX)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                emitted.extend(timer_out);
                Ok(emitted)
            })?;

        let stream = futures::stream::iter(out_batches.into_iter().map(Ok::<_, String>));
        Ok(crate::dataframe::PyDataFrameStream::from_stream(Box::pin(
            stream,
        )))
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ConnectedStreams(left={:?}, right={:?})",
            self.left.source_id, self.right.source_id
        )
    }
}

// ── G11: MultiSourceWatermarkSpec ────────────────────────────────────────────

/// Per-source watermark configuration for multi-source streaming joins.
///
/// ## Example
///
/// ```python
/// spec = (
///     MultiSourceWatermarkSpec()
///     .add_source("clicks", lag_ms=5_000)
///     .add_source("impressions", lag_ms=10_000)
///     .with_source_id_column("source_id")
/// )
/// stream = stream.with_multi_source_watermark(spec)
/// ```
#[pyclass(name = "MultiSourceWatermarkSpec")]
pub struct PyMultiSourceWatermarkSpec {
    pub(crate) source_lags: std::collections::HashMap<String, u64>,
    pub(crate) source_id_column: Option<String>,
}

#[pymethods]
impl PyMultiSourceWatermarkSpec {
    #[new]
    pub fn new() -> Self {
        Self {
            source_lags: std::collections::HashMap::new(),
            source_id_column: None,
        }
    }

    /// Register a fixed-lag watermark for `source_id`.
    pub fn add_source(&self, source_id: String, lag_ms: u64) -> Self {
        let mut next = Self {
            source_lags: self.source_lags.clone(),
            source_id_column: self.source_id_column.clone(),
        };
        next.source_lags.insert(source_id, lag_ms);
        next
    }

    /// Set the row column that identifies the source in each batch.
    pub fn with_source_id_column(&self, column: String) -> Self {
        Self {
            source_lags: self.source_lags.clone(),
            source_id_column: Some(column),
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "MultiSourceWatermarkSpec(sources={:?}, id_col={:?})",
            self.source_lags, self.source_id_column
        )
    }
}

// ── G12: BroadcastStream / BroadcastProcessFunction ──────────────────────────

/// A stream designated as the broadcast side of a broadcast–keyed join.
///
/// Created by `stream.broadcast()`. Drive processing with `apply_broadcast_process`.
#[pyclass(name = "BroadcastStream")]
pub struct PyBroadcastStream {
    pipeline: StreamPipeline,
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

struct PyBroadcastBridge {
    on_keyed: pyo3::Py<pyo3::PyAny>,
    on_broadcast: pyo3::Py<pyo3::PyAny>,
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

#[pymethods]
impl PyBroadcastStream {
    /// Apply a broadcast process function, joining this broadcast stream with `keyed`.
    ///
    /// `key_column` — column used to shard per-key state in the keyed stream.
    /// `func` — object with `on_keyed_event(key, batch, row, ctx)` and
    ///   `on_broadcast_event(batch, row, ctx)` methods.
    ///
    /// Returns a :class:`DataFrameStream` of emitted batches.
    pub fn apply_broadcast_process(
        &self,
        py: pyo3::Python<'_>,
        keyed: &PyStream,
        key_column: String,
        func: pyo3::Py<pyo3::PyAny>,
    ) -> PyResult<crate::dataframe::PyDataFrameStream> {
        let on_keyed = func.getattr(py, "on_keyed_event").map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "broadcast function must have an 'on_keyed_event' method",
            )
        })?;
        let on_broadcast = func.getattr(py, "on_broadcast_event").map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "broadcast function must have an 'on_broadcast_event' method",
            )
        })?;

        let broadcast_pipeline = self.pipeline.clone();
        let keyed_pipeline = keyed.pipeline.clone();

        let out_batches =
            py.detach(move || -> PyResult<Vec<arrow::record_batch::RecordBatch>> {
                let broadcast_batches = crate::session::block_on_async(
                    crate::stream_exec::resolve_input_batches(&broadcast_pipeline),
                )
                .map_err(crate::errors::map_krishiv_error)?;

                let keyed_batches = crate::session::block_on_async(
                    crate::stream_exec::resolve_input_batches(&keyed_pipeline),
                )
                .map_err(crate::errors::map_krishiv_error)?;

                let bridge = PyBroadcastBridge {
                    on_keyed,
                    on_broadcast,
                };
                let mut executor =
                    krishiv_api::BroadcastProcessExecutor::new(&key_column, Box::new(bridge));

                let mut emitted = Vec::new();
                for batch in &broadcast_batches {
                    let out = executor
                        .process_broadcast_batch(batch, 0)
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    emitted.extend(out);
                }
                for batch in &keyed_batches {
                    let out = executor
                        .process_keyed_batch(batch, 0)
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    emitted.extend(out);
                }
                Ok(emitted)
            })?;

        let stream = futures::stream::iter(out_batches.into_iter().map(Ok::<_, String>));
        Ok(crate::dataframe::PyDataFrameStream::from_stream(Box::pin(
            stream,
        )))
    }

    pub fn __repr__(&self) -> String {
        format!("BroadcastStream(source={:?})", self.pipeline.source_id)
    }
}

#[cfg(test)]
mod stream_gap_tests {
    use super::*;
    use std::sync::Arc;

    fn make_session() -> Arc<krishiv_api::Session> {
        Arc::new(krishiv_api::SessionBuilder::new().build().unwrap())
    }

    #[test]
    fn sliding_window_ms_sets_slide_ms_getter() {
        let session = make_session();
        let stream = PyStream::from_pipeline(session, "test".into(), "ts".into(), 0);
        let ws = stream.sliding_window_ms(10_000, 5_000).unwrap();
        assert_eq!(ws.window_kind(), Some("sliding"));
        assert_eq!(ws.window_size_ms(), Some(10_000));
        assert_eq!(ws.slide_ms(), Some(5_000));
        assert_eq!(ws.session_gap_ms(), None);
    }

    #[test]
    fn session_window_ms_sets_gap_ms_getter() {
        let session = make_session();
        let stream = PyStream::from_pipeline(session, "test".into(), "ts".into(), 0);
        let ws = stream.session_window_ms(30_000).unwrap();
        assert_eq!(ws.window_kind(), Some("session"));
        assert_eq!(ws.session_gap_ms(), Some(30_000));
        assert_eq!(ws.slide_ms(), None);
    }

    #[test]
    fn connect_builds_connected_streams() {
        let session = make_session();
        let s1 = PyStream::from_pipeline(Arc::clone(&session), "src1".into(), "ts".into(), 0);
        let s2 = PyStream::from_pipeline(session, "src2".into(), "ts".into(), 0);
        let connected = s1.connect(&s2);
        assert!(connected.__repr__().contains("src1"));
        assert!(connected.__repr__().contains("src2"));
    }

    #[test]
    fn multi_source_watermark_spec_builder() {
        let spec = PyMultiSourceWatermarkSpec::new()
            .add_source("clicks".into(), 5_000)
            .add_source("impressions".into(), 10_000)
            .with_source_id_column("source_id".into());
        assert_eq!(spec.source_lags.get("clicks"), Some(&5_000));
        assert_eq!(spec.source_lags.get("impressions"), Some(&10_000));
        assert_eq!(spec.source_id_column.as_deref(), Some("source_id"));
    }

    #[test]
    fn with_multi_source_watermark_sets_pipeline_fields() {
        let session = make_session();
        let stream = PyStream::from_pipeline(session, "test".into(), "ts".into(), 0);
        let spec = PyMultiSourceWatermarkSpec::new()
            .add_source("s1".into(), 3_000)
            .with_source_id_column("sid".into());
        let updated = stream.with_multi_source_watermark(&spec);
        assert_eq!(updated.pipeline.source_watermarks.get("s1"), Some(&3_000));
        assert_eq!(updated.pipeline.source_id_column.as_deref(), Some("sid"));
    }

    #[test]
    fn broadcast_stream_repr_contains_source() {
        let session = make_session();
        let stream = PyStream::from_pipeline(session, "broadcast_src".into(), "ts".into(), 0);
        let bcast = stream.broadcast();
        assert!(bcast.__repr__().contains("broadcast_src"));
    }
}
