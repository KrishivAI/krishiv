//! Python bindings for the declarative pipeline layer (`source → transform → sink`).
//!
//! ```python
//! pl = session.pipeline("revenue")
//! pl.source_cdc("orders", [(None, insert_batch)])     # (before, after) tuples
//! pl.view("revenue", "SELECT SUM(amount) AS total FROM orders", materialized=True)
//! sink = pl.sink_memory("revenue")
//! pl.run(advance="once")
//! print(sink.collect())                                # list[Batch]
//! ```
//!
//! There is no trigger argument: boundedness / watermark / change-events drive
//! each mode; `advance` only coalesces input.

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_api::{CdcChange, Egress, Ingest, PipelineMode, RunPolicy, Session};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::batch::PyBatch;

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Sync-safe source representation held by the pyclass.
///
/// We do not store `krishiv_api::Ingest` directly because its `Connector`
/// variant carries a `Box<dyn DynSource>` which is not `Sync` (and pyo3 requires
/// the pyclass to be `Sync`). The Python surface only needs Memory/Cdc; the
/// conversion to `Ingest` happens at `run` time.
enum PyIngest {
    Memory(Vec<RecordBatch>),
    Cdc(Vec<CdcChange>),
}

impl From<PyIngest> for Ingest {
    fn from(v: PyIngest) -> Self {
        match v {
            PyIngest::Memory(b) => Ingest::Memory(b),
            PyIngest::Cdc(c) => Ingest::Cdc(c),
        }
    }
}

/// A handle to an in-memory pipeline sink; read collected batches after `run`.
#[pyclass(name = "MemorySink")]
#[derive(Clone)]
pub struct PyMemorySink {
    inner: Arc<Mutex<Vec<RecordBatch>>>,
}

#[pymethods]
impl PyMemorySink {
    /// Return the output batches written to this sink.
    pub fn collect(&self) -> PyResult<Vec<PyBatch>> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| rt_err("memory sink mutex poisoned"))?;
        Ok(guard
            .iter()
            .cloned()
            .map(PyBatch::from_record_batch)
            .collect())
    }

    /// Number of batches collected so far.
    #[getter]
    pub fn len(&self) -> PyResult<usize> {
        Ok(self.inner.lock().map_err(|_| rt_err("poisoned"))?.len())
    }

    pub fn __repr__(&self) -> String {
        let n = self.inner.lock().map(|g| g.len()).unwrap_or(0);
        format!("MemorySink(batches={n})")
    }
}

/// A declarative pipeline builder.
///
/// Obtained from :meth:`Session.pipeline`. Add sources, views, and sinks, then
/// call :meth:`run`. State accumulates here and compiles to the Rust pipeline
/// (and thence to the imperative `feed`/`step` core) on `run`.
#[pyclass(name = "Pipeline")]
pub struct PyPipeline {
    session: Session,
    name: String,
    mode: Option<PipelineMode>,
    sources: Vec<(String, PyIngest)>,
    views: Vec<(String, String, bool)>,
    sinks: Vec<(String, Arc<Mutex<Vec<RecordBatch>>>)>,
}

impl PyPipeline {
    pub(crate) fn new(session: Session, name: String) -> Self {
        Self {
            session,
            name,
            mode: None,
            sources: Vec::new(),
            views: Vec::new(),
            sinks: Vec::new(),
        }
    }
}

#[pymethods]
impl PyPipeline {
    /// Add an in-memory record source (fed as insertions).
    pub fn source_memory(&mut self, name: String, batches: Vec<PyBatch>) {
        let rbs = batches.iter().map(|b| b.record_batch().clone()).collect();
        self.sources.push((name, PyIngest::Memory(rbs)));
    }

    /// Add an in-memory CDC source from `(before, after)` tuples.
    ///
    /// `before=None, after=batch` → INSERT; `before=batch, after=None` → DELETE;
    /// both present → UPDATE.
    pub fn source_cdc(&mut self, name: String, changes: Vec<(Option<PyBatch>, Option<PyBatch>)>) {
        let cdc = changes
            .into_iter()
            .map(|(before, after)| CdcChange {
                before: before.map(|b| b.record_batch().clone()),
                after: after.map(|b| b.record_batch().clone()),
            })
            .collect();
        self.sources.push((name, PyIngest::Cdc(cdc)));
    }

    /// Declare a transformation view by SQL.
    #[pyo3(signature = (name, sql, materialized=false))]
    pub fn view(&mut self, name: String, sql: String, materialized: bool) {
        self.views.push((name, sql, materialized));
    }

    /// Attach an in-memory sink to a view; returns a handle to read results.
    pub fn sink_memory(&mut self, view: String) -> PyMemorySink {
        let handle: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
        self.sinks.push((view, handle.clone()));
        PyMemorySink { inner: handle }
    }

    /// Force the execution mode ("batch" | "stream" | "ivm") instead of inferring it.
    pub fn mode(&mut self, mode: String) -> PyResult<()> {
        self.mode = Some(match mode.to_lowercase().as_str() {
            "batch" => PipelineMode::Batch,
            "stream" => PipelineMode::Stream,
            "ivm" => PipelineMode::Ivm,
            other => return Err(rt_err(format!("unknown pipeline mode '{other}'"))),
        });
        Ok(())
    }

    /// Build and run the pipeline.
    ///
    /// `advance` ∈ {"once", "on_change"}; `every_rows` coalesces input by row
    /// count. There is no trigger argument.
    #[pyo3(signature = (advance="once".to_string(), every_rows=None))]
    pub fn run(&mut self, advance: String, every_rows: Option<usize>) -> PyResult<()> {
        let policy = if let Some(n) = every_rows {
            RunPolicy::EveryRows(n)
        } else {
            match advance.to_lowercase().as_str() {
                "once" => RunPolicy::Once,
                "on_change" => RunPolicy::OnChange,
                other => return Err(rt_err(format!("unknown advance policy '{other}'"))),
            }
        };

        // Move accumulated state out and compile to the Rust builder.
        let sources = std::mem::take(&mut self.sources);
        let views = std::mem::take(&mut self.views);
        let sinks = std::mem::take(&mut self.sinks);

        let mut builder = self.session.pipeline(&self.name);
        if let Some(m) = self.mode {
            builder = builder.mode(m);
        }
        for (name, ingest) in sources {
            builder = builder.source(name, ingest.into());
        }
        for (name, sql, materialized) in views {
            builder = builder.view(name, sql, materialized);
        }
        for (view, handle) in sinks {
            builder = builder.sink(view, Egress::Memory(handle));
        }

        crate::RUNTIME.block_on(builder.run(policy)).map_err(rt_err)
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Pipeline(name='{}', sources={}, views={}, sinks={})",
            self.name,
            self.sources.len(),
            self.views.len(),
            self.sinks.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn order(id: i64, amount: i64) -> PyBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(Int64Array::from(vec![amount])),
            ],
        )
        .unwrap();
        PyBatch::from_record_batch(rb)
    }

    /// Drives the Python pipeline builder at the Rust level (no interpreter):
    /// CDC source → incremental SUM view → in-memory sink.
    #[test]
    fn py_pipeline_ivm_cdc_to_memory_sink() {
        let session = krishiv_api::Session::builder().build().unwrap();
        let mut pl = PyPipeline::new(session, "revenue".to_string());
        pl.source_cdc(
            "orders".to_string(),
            vec![(None, Some(order(1, 100))), (None, Some(order(2, 50)))],
        );
        pl.view(
            "revenue".to_string(),
            "SELECT SUM(amount) AS total FROM orders".to_string(),
            true,
        );
        let sink = pl.sink_memory("revenue".to_string());
        pl.run("once".to_string(), None).unwrap();

        let out = sink.collect().unwrap();
        assert_eq!(out.len(), 1, "sink should collect one snapshot batch");
        let total = out[0]
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("SUM is Float64")
            .value(0);
        assert_eq!(total, 150.0);
    }
}
