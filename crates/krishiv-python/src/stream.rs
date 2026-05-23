//! `Stream`, `KeyedStream`, and `WindowedStream` transformation chain.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::agg::descriptors_from_kwargs;
use crate::errors::SchemaError;
use crate::pipeline::{StreamPipeline, WindowKind};
use crate::session::PySession;
use crate::windows::{ensure_watermark_before_window, PyWindowSpec};
use crate::batch::PyBatch;

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
}

#[pymethods]
impl PyStream {
    pub fn with_watermark(&self, column: String, max_lateness_ms: u64) -> PyStream {
        PyStream {
            pipeline: self.pipeline.with_watermark(column, max_lateness_ms),
        }
    }

    /// Alias for `with_watermark` (R13 name).
    pub fn watermark(&self, column: String, max_lateness_ms: u64) -> PyStream {
        self.with_watermark(column, max_lateness_ms)
    }

    #[pyo3(signature = (*columns))]
    pub fn key_by(&self, columns: Vec<String>) -> PyResult<PyKeyedStream> {
        if columns.is_empty() {
            return Err(SchemaError::new_err("key_by() requires at least one column"));
        }
        Ok(PyKeyedStream {
            pipeline: self.pipeline.with_keys(columns),
        })
    }

    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        ensure_watermark_before_window(
            &self.pipeline.watermark_column,
            self.pipeline.max_lateness_ms,
        )?;
        let window_ms = window_secs.saturating_mul(1000);
        let pipeline = self.pipeline.with_window(crate::pipeline::WindowDescriptor {
            kind: WindowKind::Tumbling,
            size_ms: window_ms,
            slide_ms: None,
            gap_ms: None,
        });
        Ok(PyWindowedStream { pipeline })
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
        Ok(PyWindowedStream {
            pipeline: self.pipeline.with_window(spec.into_descriptor()),
        })
    }

    pub fn tumbling_window(&self, window_secs: u64) -> PyResult<PyWindowedStream> {
        let stream = PyStream {
            pipeline: self.pipeline.clone(),
        };
        stream.tumbling_window(window_secs)
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
#[pyclass(name = "WindowedStream")]
pub struct PyWindowedStream {
    pub(crate) pipeline: StreamPipeline,
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
    pub fn agg(&self, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<PyStream> {
        let aggs = descriptors_from_kwargs(kwargs)?;
        if aggs.is_empty() {
            return Err(SchemaError::new_err(
                "agg() requires at least one named aggregation expression",
            ));
        }
        Ok(PyStream {
            pipeline: self.pipeline.with_aggregations(aggs),
        })
    }

    pub fn collect(&self, _py: Python<'_>) -> PyResult<Vec<PyBatch>> {
        Ok(vec![])
    }

    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __anext__(&self, _py: Python<'_>) -> PyResult<Option<Py<PyBatch>>> {
        Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()))
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
