//! Unified batch + streaming `DataFrame` for the Python API.
//!
//! `PyRelation` is exposed to Python as `DataFrame` — replacing the older split
//! between `PyDataFrame` (batch only) and `PyStream`/`PyWindowedStream` (streaming only).
//!
//! # Batch usage
//! ```python
//! session = Session()
//! df = session.sql("SELECT 1 AS n")        # returns DataFrame (PyRelation)
//! result = df.collect()                     # QueryResult
//! df.show()                                 # ASCII table
//! pdf = df.to_pandas()                      # pandas DataFrame
//! ```
//!
//! # Streaming usage
//! ```python
//! df = session.from_source("events")
//! result = (
//!     df.watermark("ts", 5000)
//!       .key_by("user_id")
//!       .window("tumbling", 60_000)
//!       .collect()
//! )
//! ```

use std::sync::Mutex;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::errors::map_krishiv_error;
use crate::pipeline::{StreamPipeline, WindowDescriptor, WindowKind};
use crate::query_result::PyQueryResult;
use crate::stream_exec::execute_pipeline;

// ── Internal kind ─────────────────────────────────────────────────────────────

enum RelationKind {
    Batch(krishiv_api::DataFrame),
    Stream(StreamPipeline),
}

// ── PyRelation ────────────────────────────────────────────────────────────────

/// Compatibility wrapper for the legacy unified batch and streaming relation API.
///
/// Exposed to Python as `Relation`. New relational features are implemented on
/// the canonical `DataFrame` class. Construct this compatibility type via:
///   - `Session.sql(query)` — batch SQL
///   - `Session.read_parquet(path)` — batch from file
///   - `Session.from_source(name)` — unbounded streaming
///   - `Session.from_bounded_stream(name, batches)` — bounded streaming
#[pyclass(name = "Relation", unsendable)]
pub struct PyRelation {
    kind: RelationKind,
    _cached: Mutex<Option<PyQueryResult>>,
}

impl PyRelation {
    pub fn from_dataframe(df: krishiv_api::DataFrame) -> Self {
        Self {
            kind: RelationKind::Batch(df),
            _cached: Mutex::new(None),
        }
    }

    pub fn from_pipeline(pipeline: StreamPipeline) -> Self {
        Self {
            kind: RelationKind::Stream(pipeline),
            _cached: Mutex::new(None),
        }
    }

    fn collect_internal(&self) -> PyResult<krishiv_api::QueryResult> {
        match &self.kind {
            RelationKind::Batch(df) => df.collect().map_err(map_krishiv_error),
            RelationKind::Stream(pipeline) => {
                let py_batches = execute_pipeline(pipeline)?;
                let batches: Vec<arrow::record_batch::RecordBatch> = py_batches
                    .into_iter()
                    .map(|b| b.record_batch().clone())
                    .collect();
                Ok(krishiv_api::QueryResult::new(batches))
            }
        }
    }

    fn ensure_stream(&self, method: &str) -> PyResult<&StreamPipeline> {
        match &self.kind {
            RelationKind::Stream(p) => Ok(p),
            RelationKind::Batch(_) => Err(PyRuntimeError::new_err(format!(
                ".{method}() is only valid on streaming DataFrames. \
                 Use Session.from_source() or Session.from_bounded_stream() to create a streaming relation."
            ))),
        }
    }
}

#[pymethods]
impl PyRelation {
    /// Returns `True` if this is a batch or bounded-stream relation.
    #[getter]
    pub fn is_bounded(&self) -> bool {
        match &self.kind {
            RelationKind::Batch(_) => true,
            RelationKind::Stream(p) => p.bounded,
        }
    }

    /// Return a human-readable execution plan description.
    pub fn explain(&self, py: Python<'_>) -> PyResult<String> {
        match &self.kind {
            RelationKind::Batch(df) => {
                let df = df.clone();
                py.detach(move || df.explain().map_err(map_krishiv_error))
            }
            RelationKind::Stream(p) => Ok(format!(
                "Stream[source={}, key={:?}, watermark={}ms, window={:?}]",
                p.source_id,
                p.key_columns,
                p.max_lateness_ms,
                p.window
                    .as_ref()
                    .map(|w| format!("{:?}({} ms)", w.kind, w.size_ms)),
            )),
        }
    }

    // ── Streaming builder methods ─────────────────────────────────────────────

    /// Set the watermark column and allowed lateness in milliseconds.
    /// Returns a new streaming DataFrame.
    pub fn watermark(&self, column: String, max_lateness_ms: u64) -> PyResult<PyRelation> {
        let pipeline = self
            .ensure_stream("watermark")?
            .with_watermark(column, max_lateness_ms);
        Ok(PyRelation::from_pipeline(pipeline))
    }

    /// Key the stream by one or more columns.
    pub fn key_by(&self, columns: Bound<'_, PyAny>) -> PyResult<PyRelation> {
        let cols = string_or_list(&columns)?;
        let p = self.ensure_stream("key_by")?;
        let mut pipeline = p.clone();
        pipeline.key_columns = cols;
        Ok(PyRelation::from_pipeline(pipeline))
    }

    /// Set the event-time column (used instead of watermark column for window assignment).
    pub fn with_event_time(&self, column: String) -> PyResult<PyRelation> {
        let p = self.ensure_stream("with_event_time")?;
        let mut pipeline = p.clone();
        pipeline.event_time_column = Some(column);
        Ok(PyRelation::from_pipeline(pipeline))
    }

    /// Apply a window to the stream.
    ///
    /// `kind` is one of `"tumbling"`, `"sliding"`, or `"session"`.
    /// `size_ms` is the window duration in milliseconds.
    /// `slide_ms` (optional) applies to sliding windows.
    /// `gap_ms` (optional) applies to session windows.
    #[pyo3(signature = (kind, size_ms, slide_ms=None, gap_ms=None))]
    pub fn window(
        &self,
        kind: &str,
        size_ms: u64,
        slide_ms: Option<u64>,
        gap_ms: Option<u64>,
    ) -> PyResult<PyRelation> {
        let p = self.ensure_stream("window")?;
        let window_kind = match kind.to_lowercase().as_str() {
            "tumbling" => WindowKind::Tumbling,
            "sliding" => WindowKind::Sliding,
            "session" => WindowKind::Session,
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "Unknown window kind '{other}'. Use 'tumbling', 'sliding', or 'session'."
                )));
            }
        };
        let mut pipeline = p.clone();
        pipeline.window = Some(WindowDescriptor {
            kind: window_kind,
            size_ms,
            slide_ms,
            gap_ms,
        });
        Ok(PyRelation::from_pipeline(pipeline))
    }

    /// Convenience shorthand for `window("tumbling", size_ms)`.
    pub fn tumbling_window(&self, size_ms: u64) -> PyResult<PyRelation> {
        self.window("tumbling", size_ms, None, None)
    }

    /// Convenience shorthand for `window("sliding", size_ms, slide_ms)`.
    pub fn sliding_window(&self, size_ms: u64, slide_ms: u64) -> PyResult<PyRelation> {
        self.window("sliding", size_ms, Some(slide_ms), None)
    }

    /// Convenience shorthand for a session window with the given inactivity gap.
    pub fn session_window(&self, gap_ms: u64) -> PyResult<PyRelation> {
        let p = self.ensure_stream("session_window")?;
        let mut pipeline = p.clone();
        pipeline.window = Some(WindowDescriptor {
            kind: WindowKind::Session,
            size_ms: 0,
            slide_ms: None,
            gap_ms: Some(gap_ms),
        });
        Ok(PyRelation::from_pipeline(pipeline))
    }

    /// Register a per-source watermark lag for multi-source streaming joins.
    ///
    /// Call once per source before `.collect()`. Each source uses its own lag;
    /// the effective watermark is the minimum across all registered sources.
    pub fn with_source_watermark(&self, source_id: String, lag_ms: u64) -> PyResult<PyRelation> {
        let p = self.ensure_stream("with_source_watermark")?;
        let pipeline = p.with_source_watermark(source_id, lag_ms);
        Ok(PyRelation::from_pipeline(pipeline))
    }

    /// Set the column that identifies each source for multi-source watermark reconciliation.
    pub fn with_source_id_column(&self, column: String) -> PyResult<PyRelation> {
        let p = self.ensure_stream("with_source_id_column")?;
        let pipeline = p.with_source_id_column(column);
        Ok(PyRelation::from_pipeline(pipeline))
    }

    // ── Terminal operations ───────────────────────────────────────────────────

    /// Collect results into a `QueryResult`.
    ///
    /// **Warning**: Materializes the entire result in process memory. For large or
    /// unbounded streaming relations this may cause out-of-memory (OOM) errors.
    /// Use `sink_to()` or a streaming sink for continuous output.
    pub fn collect(&self, py: Python<'_>) -> PyResult<PyQueryResult> {
        py.detach(|| self.collect_internal().map(PyQueryResult::new))
    }

    /// Print up to `n` rows as an ASCII table.
    #[pyo3(signature = (n=20))]
    pub fn show(&self, py: Python<'_>, n: usize) -> PyResult<()> {
        self.collect(py)?.show(n)
    }

    /// Convert results to a PyArrow Table.
    pub fn to_arrow(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.collect(py)?.to_arrow(py)
    }

    /// Convert results to a pandas DataFrame.
    pub fn to_pandas(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.collect(py)?.to_pandas(py)
    }

    /// Write all results to a Parquet file at `path`.
    ///
    /// Works for both batch and bounded streaming relations. Raises `RuntimeError`
    /// for unbounded streams (use `sink_to` instead).
    pub fn write_parquet(&self, py: Python<'_>, path: String) -> PyResult<()> {
        let result = py.detach(|| self.collect_internal())?;
        py.detach(move || {
            let batches = result.into_batches();
            if batches.is_empty() {
                return Ok(());
            }
            let schema = batches[0].schema();
            let file = std::fs::File::create(&path)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            for batch in &batches {
                writer
                    .write(batch)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            }
            writer
                .close()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    pub fn __repr__(&self) -> String {
        match &self.kind {
            RelationKind::Batch(df) => format!("Relation(plan={})", df.explain_logical()),
            RelationKind::Stream(p) => format!(
                "Relation[streaming](source={}, key={:?})",
                p.source_id, p.key_columns
            ),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn string_or_list(obj: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = obj.extract::<String>() {
        return Ok(vec![s]);
    }
    obj.extract::<Vec<String>>()
}
