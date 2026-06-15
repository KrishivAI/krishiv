//! Python bindings for [`krishiv_api::streaming_dataframe::StreamingDataFrame`].

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::dataframe::PyDataFrameStream;
use crate::errors::map_krishiv_error;

#[pyclass(name = "StreamingDataFrame")]
pub struct PyStreamingDataFrame {
    inner: krishiv_api::streaming_dataframe::StreamingDataFrame,
}

impl PyStreamingDataFrame {
    pub fn new(df: krishiv_api::DataFrame) -> Self {
        Self {
            inner: df.stream(),
        }
    }
}

#[pymethods]
impl PyStreamingDataFrame {
    pub fn with_event_time(&self, column: String) -> Self {
        Self {
            inner: self.inner.clone().with_event_time(column),
        }
    }

    pub fn key_by(&self, column: String) -> Self {
        Self {
            inner: self.inner.clone().key_by(column),
        }
    }

    pub fn tumbling_window(&self, window_size_ms: u64) -> Self {
        Self {
            inner: self.inner.clone().tumbling_window(window_size_ms),
        }
    }

    pub fn sliding_window(&self, window_size_ms: u64, slide_ms: u64) -> Self {
        Self {
            inner: self
                .inner
                .clone()
                .sliding_window(window_size_ms, slide_ms),
        }
    }

    pub fn session_window(&self, gap_ms: u64) -> Self {
        Self {
            inner: self.inner.clone().session_window(gap_ms),
        }
    }

    pub fn with_watermark_lag(&self, lag_ms: u64) -> Self {
        Self {
            inner: self.inner.clone().with_watermark_lag(lag_ms),
        }
    }

    pub fn with_side_output(&self, name: String, lateness_threshold_ms: u64) -> Self {
        Self {
            inner: self
                .inner
                .clone()
                .with_side_output(name, lateness_threshold_ms),
        }
    }

    #[pyo3(signature = (*, subset))]
    pub fn drop_duplicates(&self, subset: Vec<String>) -> Self {
        Self {
            inner: self.inner.clone().drop_duplicates(subset),
        }
    }

    pub async fn execute_stream_async(&self) -> PyResult<PyDataFrameStream> {
        let inner = self.inner.clone();
        let stream = inner
            .execute_stream_async()
            .await
            .map_err(map_krishiv_error)?;
        Ok(PyDataFrameStream::from_stream(stream))
    }

    pub fn write_stream(&self) -> PyResult<crate::streaming::PyDataStreamWriter> {
        // Rebuild from the streaming builder's underlying DataFrame via a fresh stream().
        Err(PyRuntimeError::new_err(
            "use DataFrame.write_stream() for structured streaming sinks; \
             StreamingDataFrame.execute_stream_async() runs the pipeline",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (left, right, left_time_col, right_time_col, lower_bound_ms, upper_bound_ms))]
pub fn interval_join(
    left: Vec<crate::batch::PyBatch>,
    right: Vec<crate::batch::PyBatch>,
    left_time_col: String,
    right_time_col: String,
    lower_bound_ms: i64,
    upper_bound_ms: i64,
) -> PyResult<Vec<(crate::batch::PyBatch, crate::batch::PyBatch)>> {
    let left_batches: Vec<_> = left
        .into_iter()
        .map(|b| b.record_batch().clone())
        .collect();
    let right_batches: Vec<_> = right
        .into_iter()
        .map(|b| b.record_batch().clone())
        .collect();
    let pairs = krishiv_api::streaming_dataframe::StreamingDataFrame::stream_stream_join(
        &left_batches,
        &right_batches,
        &left_time_col,
        &right_time_col,
        lower_bound_ms,
        upper_bound_ms,
    )
    .map_err(map_krishiv_error)?;
    Ok(pairs
        .into_iter()
        .map(|(l, r)| {
            (
                crate::batch::PyBatch::from_record_batch(l.as_ref().clone()),
                crate::batch::PyBatch::from_record_batch(r.as_ref().clone()),
            )
        })
        .collect())
}

#[pyclass(name = "DataStreamReader")]
pub struct PyDataStreamReader {
    session: krishiv_api::Session,
}

impl PyDataStreamReader {
    pub fn new(session: krishiv_api::Session) -> Self {
        Self { session }
    }
}

#[pymethods]
impl PyDataStreamReader {
    pub fn file_stream(&self, path: String) -> PyResult<crate::dataframe::PyDataFrame> {
        self.session
            .read_stream()
            .file_stream(path)
            .map(|df| crate::dataframe::PyDataFrame { inner: df })
            .map_err(map_krishiv_error)
    }
}
