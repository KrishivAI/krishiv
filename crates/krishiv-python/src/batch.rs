//! `Batch` — Arrow record batch exposed to Python.

use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyImportError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// One record batch from a query or stream window.
#[pyclass(name = "Batch")]
pub struct PyBatch {
    pub(crate) batch: Arc<RecordBatch>,
}

impl PyBatch {
    pub fn from_record_batch(batch: RecordBatch) -> Self {
        Self {
            batch: Arc::new(batch),
        }
    }
}

#[pymethods]
impl PyBatch {
    #[getter]
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    #[getter]
    pub fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    /// Export this batch as a ``pyarrow.RecordBatch``.
    pub fn to_arrow(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let pa = py.import("pyarrow").map_err(|e| {
            PyImportError::new_err(format!(
                "pyarrow is required for to_arrow(); install with pip install krishiv[pyarrow]: {e}"
            ))
        })?;
        let mut buf = Vec::new();
        {
            let schema = self.batch.schema();
            let mut writer =
                StreamWriter::try_new(&mut buf, schema.as_ref()).map_err(|e| {
                    PyRuntimeError::new_err(format!("failed to serialize batch: {e}"))
                })?;
            writer
                .write(self.batch.as_ref())
                .map_err(|e| PyRuntimeError::new_err(format!("failed to write batch: {e}")))?;
            writer
                .finish()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to finish stream: {e}")))?;
        }
        let ipc = pa.getattr("ipc")?;
        let reader = ipc.call_method1("open_stream", (PyBytes::new(py, &buf),))?;
        let batch = reader.call_method0("read_next_batch")?;
        if batch.is_none() {
            return Err(PyRuntimeError::new_err("empty IPC stream"));
        }
        Ok(batch.unbind())
    }

    /// Export as ``pandas.DataFrame`` via PyArrow.
    pub fn to_pandas(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = self.to_arrow(py)?;
        record.call_method0(py, "to_pandas")
    }

    pub fn _repr_html_(&self) -> PyResult<String> {
        Ok(format!(
            "<p>Batch: {} rows × {} columns</p>",
            self.num_rows(),
            self.num_columns()
        ))
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Batch(rows={}, columns={})",
            self.num_rows(),
            self.num_columns()
        )
    }
}
