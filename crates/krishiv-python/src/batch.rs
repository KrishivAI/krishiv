//! `Batch` — Arrow record batch with Pandas / PyArrow bridges.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyImportError;
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;

/// One record batch from a query or stream window.
#[pyclass(name = "Batch", from_py_object)]
#[derive(Clone)]
pub struct PyBatch {
    batch: Arc<RecordBatch>,
}

impl PyBatch {
    pub fn from_record_batch(batch: RecordBatch) -> Self {
        Self {
            batch: Arc::new(batch),
        }
    }

    pub fn empty() -> Self {
        Self::from_record_batch(RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )))
    }

    pub fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }
}

#[pymethods]
impl PyBatch {
    #[new]
    fn py_new(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let py_batch: PyRecordBatch = obj.extract()?;
        Ok(Self::from_record_batch(py_batch.into_inner()))
    }
    #[getter]
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    #[getter]
    pub fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    pub fn to_arrow(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let batch = (*self.batch).clone();
        let py_batch = PyRecordBatch::new(batch);
        Ok(py_batch.into_pyobject(py)?.into_any().unbind())
    }

    pub fn to_pandas(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let arrow_obj = self.to_arrow(py)?;
        let pyarrow = py.import("pyarrow").map_err(|_| {
            PyImportError::new_err(
                "pyarrow required for Batch.to_pandas(). Install with: pip install krishiv[arrow]",
            )
        })?;
        let record_batch_fn = pyarrow.getattr("record_batch")?;
        let pa_batch = record_batch_fn.call1((arrow_obj,))?;

        let table_cls = pyarrow.getattr("Table")?;
        let py_list = pyo3::types::PyList::new(py, vec![pa_batch])?;
        let table = table_cls.call_method1("from_batches", (py_list,))?;
        table
            .call_method0("to_pandas")
            .map(|o| o.unbind())
            .map_err(|e| {
                PyImportError::new_err(format!(
                    "pandas/pyarrow required for Batch.to_pandas(): {e}. \
                     Install with: pip install krishiv[arrow]"
                ))
            })
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Batch(rows={}, columns={})",
            self.batch.num_rows(),
            self.batch.num_columns()
        )
    }

    pub fn _repr_html_(&self) -> PyResult<String> {
        if self.batch.num_rows() == 0 {
            return Ok(format!(
                "<p><b>Batch</b> — {} columns, 0 rows</p>",
                self.batch.num_columns()
            ));
        }
        let preview_rows = self.batch.num_rows().min(20);
        let slice = self.batch.slice(0, preview_rows);
        let mut html = String::from("<table><thead><tr>");
        for field in slice.schema().fields() {
            html.push_str(&format!(
                "<th>{} <small>({})</small></th>",
                field.name(),
                field.data_type()
            ));
        }
        html.push_str("</tr></thead><tbody>");
        for row in 0..slice.num_rows() {
            html.push_str("<tr>");
            for col in 0..slice.num_columns() {
                let value = array_value_at(slice.column(col).as_ref(), row);
                html.push_str(&format!("<td>{value}</td>"));
            }
            html.push_str("</tr>");
        }
        html.push_str("</tbody></table>");
        if self.batch.num_rows() > preview_rows {
            html.push_str(&format!(
                "<p><i>Showing {preview_rows} of {} rows</i></p>",
                self.batch.num_rows()
            ));
        }
        Ok(html)
    }
}

#[pyfunction]
pub fn make_example_batch() -> PyBatch {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
        .expect("example batch");
    PyBatch::from_record_batch(batch)
}

fn array_value_at(array: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(row) {
        return "null".to_string();
    }
    match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_else(|| "?".into()),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_else(|| "?".into()),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_else(|| "?".into()),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_else(|| "?".into()),
        other => format!("<{other:?}>"),
    }
}
