//! `QueryResult` — collected Arrow batches from a DataFrame.

use pyo3::exceptions::{PyImportError, PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::batch::PyBatch;

/// Collected query output as one or more record batches.
#[pyclass(name = "QueryResult")]
pub struct PyQueryResult {
    inner: krishiv_api::QueryResult,
}

impl PyQueryResult {
    pub fn new(inner: krishiv_api::QueryResult) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyQueryResult {
    pub fn batches(&self) -> Vec<PyBatch> {
        self.inner
            .batches()
            .iter()
            .map(|b| PyBatch::from_record_batch(b.clone()))
            .collect()
    }

    #[getter]
    pub fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    pub fn pretty(&self) -> PyResult<String> {
        self.inner
            .pretty()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Print up to `n` rows as an ASCII table to stdout.
    #[pyo3(signature = (n=20))]
    pub fn show(&self, n: usize) -> PyResult<()> {
        let text = self.pretty()?;
        let lines: Vec<&str> = text.lines().collect();
        let printed: Vec<&str> = lines.iter().take(n + 3).copied().collect();
        // Write through the process stdout handle rather than `println!` so a
        // broken pipe surfaces as a Python exception instead of a panic.
        use std::io::Write;
        writeln!(std::io::stdout(), "{}", printed.join("\n"))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Convert all batches to a single PyArrow Table.
    pub fn to_arrow(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let pyarrow = py.import("pyarrow").map_err(|_| {
            PyImportError::new_err(
                "pyarrow required for QueryResult.to_arrow(). Install with: pip install krishiv[arrow]",
            )
        })?;
        let record_batch_fn = pyarrow.getattr("record_batch")?;
        let py_batches: Vec<Py<PyAny>> = self
            .inner
            .batches()
            .iter()
            .map(|b| {
                let arrow_obj = PyBatch::from_record_batch(b.clone()).to_arrow(py)?;
                let pa_batch = record_batch_fn.call1((arrow_obj,))?;
                Ok(pa_batch.unbind())
            })
            .collect::<PyResult<_>>()?;
        let table_cls = pyarrow.getattr("Table")?;
        let py_list = PyList::new(py, py_batches)?;
        table_cls
            .call_method1("from_batches", (py_list,))
            .map(|o| o.unbind())
    }

    /// Arrow PyCapsule stream interface (`__arrow_c_stream__`).
    ///
    /// This is how pyarrow, Polars, DuckDB and pandas 2.x negotiate a zero-copy
    /// handoff: any consumer that understands the capsule protocol can read the
    /// result directly, without going through `to_arrow()` and its intermediate
    /// Python objects. `pa.table(result)`, `pl.DataFrame(result)` and
    /// `duckdb.sql("SELECT * FROM result")` all work off this one method.
    ///
    /// `requested_schema` is part of the protocol and is accepted for
    /// conformance; a cast is not attempted, and the consumer is free to cast
    /// what it receives. That is the behaviour the protocol permits when a
    /// producer cannot honour the request, and it is preferable to silently
    /// returning data in a schema the caller did not ask for.
    #[pyo3(signature = (requested_schema=None))]
    pub fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, pyo3::types::PyCapsule>> {
        use arrow::ffi_stream::FFI_ArrowArrayStream;
        use arrow::record_batch::RecordBatchIterator;
        use std::ffi::CStr;

        // The capsule name must outlive the capsule, so it is a 'static CStr
        // rather than a local CString. "arrow_array_stream" is the name the
        // PyCapsule protocol requires — consumers match on it exactly.
        const CAPSULE_NAME: &CStr = c"arrow_array_stream";

        let _ = requested_schema;
        let batches = self.inner.batches().to_vec();
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        pyo3::types::PyCapsule::new_with_value(py, stream, CAPSULE_NAME)
    }

    /// Convert to a pandas DataFrame.
    pub fn to_pandas(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.to_arrow(py)?
            .bind(py)
            .call_method0("to_pandas")
            .map(|o| o.unbind())
            .map_err(|e| {
                PyImportError::new_err(format!(
                    "pandas and pyarrow required for QueryResult.to_pandas(): {e}. \
                     Install with: pip install krishiv[arrow]"
                ))
            })
    }

    /// Iterate over batches.
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyQueryResultIter {
        PyQueryResultIter {
            batches: slf
                .inner
                .batches()
                .iter()
                .map(|b| PyBatch::from_record_batch(b.clone()))
                .collect(),
            pos: 0,
        }
    }

    pub fn __len__(&self) -> usize {
        self.inner.batches().len()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "QueryResult(batches={}, rows={})",
            self.inner.batches().len(),
            self.inner.row_count()
        )
    }
}

#[pyclass]
pub struct PyQueryResultIter {
    batches: Vec<PyBatch>,
    pos: usize,
}

#[pymethods]
impl PyQueryResultIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<PyBatch> {
        if let Some(batch) = self.batches.get(self.pos).cloned() {
            self.pos += 1;
            Ok(batch)
        } else {
            Err(PyStopIteration::new_err(()))
        }
    }
}
