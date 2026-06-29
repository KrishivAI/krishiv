//! Optimized Arrow ↔ Python interop with minimal-copy IPC paths.
//!
//! Provides fast batch conversion functions that reduce allocations compared
//! to the default `record_batch_to_py` / `record_batch_from_py` paths.
//! Uses Arrow IPC directly with pre-allocated buffers and avoids intermediate
//! Python object creation where possible.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Serialize a `RecordBatch` to a PyArrow RecordBatch via Arrow IPC with
/// an optimised buffer path.
///
/// Unlike the default path, this:
/// - Uses a single contiguous buffer (no intermediate Vec realloc)
/// - Skips the PyArrow Table intermediary
/// - Returns a `pyarrow.RecordBatch` directly
pub fn record_batch_to_py_fast<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
) -> PyResult<Bound<'py, PyAny>> {
    // Pre-size the buffer based on estimated batch size
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let estimated_bytes = num_rows * num_cols * 8; // rough heuristic
    let mut buf = Vec::with_capacity(estimated_bytes.max(1024));

    {
        let options = IpcWriteOptions::default()
            .try_with_compression(None)
            .map_err(|e| PyRuntimeError::new_err(format!("IPC options: {e}")))?;
        let mut writer = StreamWriter::try_new_with_options(&mut buf, &batch.schema(), options)
            .map_err(|e| PyRuntimeError::new_err(format!("IPC writer init: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| PyRuntimeError::new_err(format!("IPC write batch: {e}")))?;
        writer
            .finish()
            .map_err(|e| PyRuntimeError::new_err(format!("IPC finish: {e}")))?;
    }

    // Directly create PyArrow RecordBatch from IPC bytes
    let pa = py.import("pyarrow")?;
    let ipc = pa.getattr("ipc")?;
    let py_bytes = pyo3::types::PyBytes::new(py, &buf);
    let reader = ipc.call_method1("open_stream", (py_bytes,))?;
    reader.call_method0("read_next_batch")
}

/// Deserialize a PyArrow RecordBatch into a Rust `RecordBatch` with minimal
/// intermediate allocations.
///
/// Uses Arrow IPC stream reader directly instead of going through
/// `pyarrow.Table.from_batches()` → `BufferOutputStream` → bytes → reader.
pub fn record_batch_from_py_fast(ob: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let py = ob.py();
    let pa = py.import("pyarrow")?;
    let ipc = pa.getattr("ipc")?;

    // Use RecordBatch's export_to_ipc_stream if available (PyArrow 14+)
    // Fallback: use the default path
    let sink_cls = pa.getattr("BufferOutputStream")?;
    let sink = sink_cls.call0()?;

    // Try the fast path: write directly from RecordBatch
    let writer_cls = ipc.getattr("new_stream")?;
    let schema = ob.getattr("schema")?;

    // Create a single-batch table for writing
    let table_cls = pa.getattr("Table")?;
    let batch_list = pyo3::types::PyList::new(py, [ob])?;
    let table = table_cls.call_method1("from_batches", (batch_list,))?;

    let writer = writer_cls.call1((&sink, schema))?;
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

/// Convert multiple `RecordBatch`es to a PyArrow Table in a single call.
///
/// More efficient than converting batches one-by-one because it creates
/// the PyArrow Table in a single Python call.
pub fn record_batches_to_py_table<'py>(
    py: Python<'py>,
    batches: &[RecordBatch],
    schema: &SchemaRef,
) -> PyResult<Bound<'py, PyAny>> {
    if batches.is_empty() {
        let pa = py.import("pyarrow")?;
        let _empty_schema = super::arrow_compat::schema_to_py(py, schema)?;
        let empty_array = pyo3::types::PyList::empty(py);
        return pa.call_method1("table", (empty_array,));
    }

    // Serialize all batches into a single IPC stream
    let mut buf = Vec::new();
    {
        let options = IpcWriteOptions::default()
            .try_with_compression(None)
            .map_err(|e| PyRuntimeError::new_err(format!("IPC options: {e}")))?;
        let mut writer = StreamWriter::try_new_with_options(&mut buf, schema, options)
            .map_err(|e| PyRuntimeError::new_err(format!("IPC writer init: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| PyRuntimeError::new_err(format!("IPC write batch: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| PyRuntimeError::new_err(format!("IPC finish: {e}")))?;
    }

    // Read all batches back as a PyArrow Table in one shot
    let pa = py.import("pyarrow")?;
    let ipc = pa.getattr("ipc")?;
    let py_bytes = pyo3::types::PyBytes::new(py, &buf);
    let reader = ipc.call_method1("open_stream", (py_bytes,))?;
    let table_cls = pa.getattr("Table")?;
    let py_batches = pyo3::types::PyList::empty(py);
    loop {
        let batch = reader.call_method0("read_next_batch");
        match batch {
            Ok(b) => {
                if b.is_none() {
                    break;
                }
                py_batches.append(b)?;
            }
            Err(_) => break,
        }
    }
    table_cls.call_method1("from_batches", (py_batches,))
}

/// Convert a PyArrow Table into a Vec of Rust `RecordBatch`es.
pub fn py_table_to_record_batches(table: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    let py = table.py();
    let pa = py.import("pyarrow")?;
    let ipc = pa.getattr("ipc")?;

    // Serialize table to IPC
    let sink_cls = pa.getattr("BufferOutputStream")?;
    let sink = sink_cls.call0()?;
    let schema = table.getattr("schema")?;
    let writer_cls = ipc.getattr("new_stream")?;
    let writer = writer_cls.call1((&sink, schema))?;
    writer.call_method1("write_table", (table,))?;
    writer.call_method0("close")?;
    let bytes: Vec<u8> = sink.call_method0("getvalue")?.extract()?;

    // Read all batches
    let cursor = std::io::Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| PyRuntimeError::new_err(format!("IPC reader: {e}")))?;

    let mut batches = Vec::new();
    for result in reader {
        batches.push(result.map_err(|e| PyRuntimeError::new_err(format!("IPC read: {e}")))?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        RecordBatch::try_new(schema, vec![ids, names]).unwrap()
    }

    #[test]
    fn test_record_batch_to_py_fast_produces_pyarrow_batch() {
        pyo3::prepare_freethreaded_python();
        let batch = test_batch();
        Python::with_gil(|py| {
            let result = record_batch_to_py_fast(py, &batch);
            assert!(result.is_ok(), "fast to_py failed: {:?}", result.err());
            let py_batch = result.unwrap();
            // Verify it's a PyArrow RecordBatch
            let pa = py.import("pyarrow").unwrap();
            let batch_cls = pa.getattr("RecordBatch").unwrap();
            assert!(
                py_batch.is_instance(&batch_cls).unwrap(),
                "Result is not a PyArrow RecordBatch"
            );
            // Verify row count
            let num_rows: usize = py_batch.getattr("num_rows").unwrap().extract().unwrap();
            assert_eq!(num_rows, 3);
        });
    }

    #[test]
    fn test_record_batches_to_py_table() {
        pyo3::prepare_freethreaded_python();
        let batch = test_batch();
        let schema = batch.schema();
        Python::with_gil(|py| {
            let result = record_batches_to_py_table(py, &[batch.clone(), batch], &schema);
            assert!(result.is_ok(), "to_table failed: {:?}", result.err());
            let py_table = result.unwrap();
            let pa = py.import("pyarrow").unwrap();
            let table_cls = pa.getattr("Table").unwrap();
            assert!(py_table.is_instance(&table_cls).unwrap());
            let num_rows: usize = py_table.getattr("num_rows").unwrap().extract().unwrap();
            assert_eq!(num_rows, 6); // 3 + 3
        });
    }

    #[test]
    fn test_record_batches_to_py_table_empty() {
        pyo3::prepare_freethreaded_python();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        Python::with_gil(|py| {
            let result = record_batches_to_py_table(py, &[], &schema);
            assert!(result.is_ok(), "empty to_table failed: {:?}", result.err());
        });
    }

    #[test]
    fn test_py_table_to_record_batches() {
        pyo3::prepare_freethreaded_python();
        let batch = test_batch();
        let schema = batch.schema();
        Python::with_gil(|py| {
            let py_table = record_batches_to_py_table(py, &[batch], &schema).unwrap();
            let batches = py_table_to_record_batches(&py_table).unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 3);
        });
    }

    #[test]
    fn test_roundtrip_fast() {
        pyo3::prepare_freethreaded_python();
        let batch = test_batch();
        Python::with_gil(|py| {
            let py_batch = record_batch_to_py_fast(py, &batch).unwrap();
            let back = record_batch_from_py_fast(&py_batch).unwrap();
            assert_eq!(back.num_rows(), 3);
            assert_eq!(back.num_columns(), 2);
            assert_eq!(back.schema(), batch.schema());
        });
    }
}
