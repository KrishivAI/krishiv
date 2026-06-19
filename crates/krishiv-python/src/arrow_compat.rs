//! Arrow ↔ Python interop without the `pyo3-arrow` crate.
//!
//! Uses Arrow IPC (for RecordBatch) and field-by-field reflection (for Schema)
//! to pass data to and from PyArrow, avoiding the pyo3-arrow version conflict.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

// ── RecordBatch wrappers ──────────────────────────────────────────────────────

/// Wrapper around `RecordBatch` for Python interop via Arrow IPC.
pub struct PyArrowBatch(pub RecordBatch);

impl PyArrowBatch {
    pub fn new(batch: RecordBatch) -> Self {
        Self(batch)
    }

    pub fn into_inner(self) -> RecordBatch {
        self.0
    }
}

impl<'py> IntoPyObject<'py> for PyArrowBatch {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        record_batch_to_py(py, &self.0)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for PyArrowBatch {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        record_batch_from_py(&ob).map(PyArrowBatch)
    }
}

// ── Schema wrappers ───────────────────────────────────────────────────────────

/// Wrapper around `SchemaRef` for Python interop via field reflection.
pub struct PyArrowSchema(pub SchemaRef);

impl PyArrowSchema {
    pub fn new(schema: SchemaRef) -> Self {
        Self(schema)
    }

    pub fn into_inner(self) -> SchemaRef {
        self.0
    }
}

impl<'py> IntoPyObject<'py> for PyArrowSchema {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        schema_to_py(py, &self.0)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for PyArrowSchema {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        schema_from_py(&ob).map(PyArrowSchema)
    }
}

// ── RecordBatch ↔ Python conversion ──────────────────────────────────────────

/// Serialize a `RecordBatch` to a PyArrow RecordBatch via Arrow IPC stream.
pub fn record_batch_to_py<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
) -> PyResult<Bound<'py, PyAny>> {
    use arrow::ipc::writer::StreamWriter;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| PyRuntimeError::new_err(format!("IPC writer init: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| PyRuntimeError::new_err(format!("IPC write batch: {e}")))?;
        writer
            .finish()
            .map_err(|e| PyRuntimeError::new_err(format!("IPC finish: {e}")))?;
    }

    let pa = py.import("pyarrow")?;
    let ipc = pa.getattr("ipc")?;
    let py_bytes = pyo3::types::PyBytes::new(py, &buf);
    let reader = ipc.call_method1("open_stream", (py_bytes,))?;
    reader.call_method0("read_next_batch")
}

/// Deserialize a PyArrow RecordBatch from Python into a Rust `RecordBatch`.
pub fn record_batch_from_py(ob: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    use arrow::ipc::reader::StreamReader;

    let py = ob.py();
    let pa = py.import("pyarrow")?;
    let ipc = pa.getattr("ipc")?;

    let table_cls = pa.getattr("Table")?;
    let batch_list = pyo3::types::PyList::new(py, [ob])?;
    let table = table_cls.call_method1("from_batches", (batch_list,))?;

    let sink_cls = pa.getattr("BufferOutputStream")?;
    let sink = sink_cls.call0()?;
    let writer_cls = ipc.getattr("new_stream")?;
    let writer = writer_cls.call1((&sink, table.getattr("schema")?))?;
    writer.call_method1("write_table", (&table,))?;
    writer.call_method0("close")?;
    let bytes: Vec<u8> = sink.call_method0("getvalue")?.extract()?;

    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| PyRuntimeError::new_err(format!("IPC reader: {e}")))?;
    reader
        .next()
        .ok_or_else(|| PyRuntimeError::new_err("IPC stream was empty"))?
        .map_err(|e| PyRuntimeError::new_err(format!("IPC read: {e}")))
}

// ── Schema ↔ Python conversion ────────────────────────────────────────────────

/// Build a PyArrow Schema from a Rust `SchemaRef` by reflecting each field.
pub fn schema_to_py<'py>(py: Python<'py>, schema: &Schema) -> PyResult<Bound<'py, PyAny>> {
    let pa = py.import("pyarrow")?;

    let py_fields: Vec<Py<PyAny>> = schema
        .fields()
        .iter()
        .map(|f| {
            let pa_type = arrow_dt_to_pa_type(py, &pa, f.data_type())?;
            pa.call_method("field", (f.name(), pa_type), None)
                .map(|v| v.unbind())
        })
        .collect::<PyResult<_>>()?;

    let fields_list = pyo3::types::PyList::new(py, py_fields)?;
    pa.call_method1("schema", (fields_list,))
}

/// Extract a Rust `SchemaRef` from a Python PyArrow Schema by iterating fields.
pub fn schema_from_py(ob: &Bound<'_, PyAny>) -> PyResult<SchemaRef> {
    let num_fields: usize = ob.len()?;
    let mut fields = Vec::with_capacity(num_fields);

    for i in 0..num_fields {
        let field = ob.call_method1("field", (i,))?;
        let name: String = field.getattr("name")?.extract()?;
        let nullable: bool = field.getattr("nullable")?.extract()?;
        let pa_type = field.getattr("type")?;
        let dt = pa_type_to_arrow(&pa_type)?;
        fields.push(Field::new(name, dt, nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}

// ── Type mapping helpers ──────────────────────────────────────────────────────

fn arrow_dt_to_pa_type<'py>(
    py: Python<'py>,
    pa: &Bound<'py, PyAny>,
    dt: &DataType,
) -> PyResult<Bound<'py, PyAny>> {
    match dt {
        DataType::Int8 => pa.call_method0("int8"),
        DataType::Int16 => pa.call_method0("int16"),
        DataType::Int32 => pa.call_method0("int32"),
        DataType::Int64 => pa.call_method0("int64"),
        DataType::UInt8 => pa.call_method0("uint8"),
        DataType::UInt16 => pa.call_method0("uint16"),
        DataType::UInt32 => pa.call_method0("uint32"),
        DataType::UInt64 => pa.call_method0("uint64"),
        DataType::Float32 => pa.call_method0("float32"),
        DataType::Float64 => pa.call_method0("float64"),
        DataType::Utf8 => pa.call_method0("utf8"),
        DataType::LargeUtf8 => pa.call_method0("large_utf8"),
        DataType::Boolean => pa.call_method0("bool_"),
        DataType::Binary => pa.call_method0("binary"),
        DataType::LargeBinary => pa.call_method0("large_binary"),
        DataType::Date32 => pa.call_method0("date32"),
        DataType::Date64 => pa.call_method0("date64"),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let kw = pyo3::types::PyDict::new(py);
            kw.set_item("tz", tz.as_ref().map(|s| s.as_ref()))?;
            pa.call_method("timestamp", ("us",), Some(&kw))
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let kw = pyo3::types::PyDict::new(py);
            kw.set_item("tz", tz.as_ref().map(|s| s.as_ref()))?;
            pa.call_method("timestamp", ("ms",), Some(&kw))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let kw = pyo3::types::PyDict::new(py);
            kw.set_item("tz", tz.as_ref().map(|s| s.as_ref()))?;
            pa.call_method("timestamp", ("ns",), Some(&kw))
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let kw = pyo3::types::PyDict::new(py);
            kw.set_item("tz", tz.as_ref().map(|s| s.as_ref()))?;
            pa.call_method("timestamp", ("s",), Some(&kw))
        }
        DataType::Float16 => pa.call_method0("float16"),
        DataType::Null => pa.call_method0("null"),
        other => Err(PyRuntimeError::new_err(format!(
            "unsupported Arrow DataType for Python export: {other:?}"
        ))),
    }
}

fn pa_type_to_arrow(pa_type: &Bound<'_, PyAny>) -> PyResult<DataType> {
    let type_str: String = pa_type.str()?.extract()?;
    match type_str.as_str() {
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float" | "float32" | "halffloat" => Ok(DataType::Float32),
        "double" | "float64" => Ok(DataType::Float64),
        "string" | "utf8" => Ok(DataType::Utf8),
        "large_string" | "large_utf8" => Ok(DataType::LargeUtf8),
        "bool" | "boolean" => Ok(DataType::Boolean),
        "binary" => Ok(DataType::Binary),
        "large_binary" => Ok(DataType::LargeBinary),
        "date32" => Ok(DataType::Date32),
        "date64" => Ok(DataType::Date64),
        "null" => Ok(DataType::Null),
        s if s.starts_with("timestamp") => {
            if s.contains("ns") {
                Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
            } else if s.contains("us") {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            } else if s.contains("ms") {
                Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
            } else {
                Ok(DataType::Timestamp(TimeUnit::Second, None))
            }
        }
        other => Err(PyRuntimeError::new_err(format!(
            "unsupported PyArrow type for Rust extraction: {other}"
        ))),
    }
}
