//! `Stream`, `KeyedStream`, and `WindowedStream` transformation chain.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::agg::descriptors_from_kwargs;
use crate::batch::PyBatch;
use crate::errors::SchemaError;
use crate::pipeline::{StreamPipeline, WindowKind};
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

    pub fn from_pipeline_struct(pipeline: StreamPipeline) -> Self {
        Self { pipeline }
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

    #[allow(dead_code)]
    fn _tumbling_window_secs_body(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        ensure_watermark_before_window(
            &self.pipeline.watermark_column,
            self.pipeline.max_lateness_ms,
        )?;
        let window_ms = window_secs.saturating_mul(1000);
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
        Ok(self.cached.lock().unwrap_or_else(|e| e.into_inner()).clone().unwrap_or_default())
    }

    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        let mut rx_guard = slf.stream_rx.lock().unwrap_or_else(|e| e.into_inner());
        if rx_guard.is_none() {
            if let Ok(rx) = crate::stream_exec::spawn_pipeline_stream(slf.pipeline.clone()) {
                *rx_guard = Some(rx);
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
            // back to blocking_recv directly.
            let res = match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    tokio::task::block_in_place(|| handle.block_on(async { rx.recv().await }))
                }
                Err(_) => rx.blocking_recv(),
            };
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
