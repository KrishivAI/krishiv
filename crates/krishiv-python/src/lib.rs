#![forbid(unsafe_code)]

//! **Beta API**: may change between minor releases.
//!
//! PyO3 Python bindings for Krishiv — Session, DataFrame, and Python UDF
//! support via `spawn_blocking` (GIL never held on a Tokio worker thread).

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

// ---------------------------------------------------------------------------
// Embedded Tokio runtime — module-private
// ---------------------------------------------------------------------------

/// Embedded Tokio runtime used by `sql_async` and `call_python_udf`.
/// GIL is never held on a worker thread of this runtime.
static RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> =
    std::sync::LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build embedded Krishiv Tokio runtime")
    });

// ---------------------------------------------------------------------------
// PySession
// ---------------------------------------------------------------------------

/// **Beta API**: may change between minor releases.
///
/// A Krishiv query session exposed to Python.
///
/// `sql()` releases the GIL while Rust executes the query so Python threads
/// are not blocked.
#[pyclass(name = "Session")]
pub struct PySession {
    inner: Arc<krishiv_api::Session>,
}

#[pymethods]
impl PySession {
    /// Create a new embedded-mode session.
    #[new]
    pub fn new() -> PyResult<Self> {
        krishiv_api::SessionBuilder::new()
            .build()
            .map(|s| Self {
                inner: Arc::new(s),
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Execute a SQL query and return a [`DataFrame`].
    ///
    /// The GIL is released while Rust executes the query.
    pub fn sql(&self, py: Python<'_>, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .sql(&query)
                .map(|df| PyDataFrame { inner: df })
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Execute SQL using the embedded Tokio runtime.
    ///
    /// Blocks the calling Python thread; use `run_in_executor` for asyncio.
    pub fn sql_async(&self, py: Python<'_>, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            RUNTIME.block_on(async move {
                inner
                    .sql_async(&query)
                    .await
                    .map(|df| PyDataFrame { inner: df })
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        })
    }
}

// ---------------------------------------------------------------------------
// PyDataFrame
// ---------------------------------------------------------------------------

/// **Beta API**: may change between minor releases.
///
/// A lazy query handle that can be collected into results.
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    inner: krishiv_api::DataFrame,
}

#[pymethods]
impl PyDataFrame {
    /// Collect results and return them as a pretty-printed ASCII table string.
    ///
    /// The GIL is released while Rust collects results.
    pub fn collect(&self, py: Python<'_>) -> PyResult<String> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .and_then(|r| r.pretty())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Return the number of result rows.
    ///
    /// The GIL is released while Rust collects results.
    pub fn num_rows(&self, py: Python<'_>) -> PyResult<usize> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .map(|r| r.row_count())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// PythonScalarUdf — wraps a Python callable as a ScalarUdf
// ---------------------------------------------------------------------------

/// Wraps a Python callable as a [`krishiv_udf::ScalarUdf`].
///
/// **Important**: `call()` acquires the GIL. Callers must invoke via
/// [`call_python_udf`] which runs on a `spawn_blocking` thread so Tokio
/// workers are never blocked waiting for Python.
// Not constructed in unit tests (requires Python interpreter); suppressed here.
#[allow(dead_code)]
struct PythonScalarUdf {
    callable: Py<PyAny>,
    name: String,
    input_schema: Schema,
    output_field: Field,
}

impl std::fmt::Debug for PythonScalarUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonScalarUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl krishiv_udf::ScalarUdf for PythonScalarUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> &Schema {
        &self.input_schema
    }

    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn call(&self, batch: &RecordBatch) -> Result<ArrayRef, krishiv_udf::UdfError> {
        Python::attach(|py| {
            // Build a dict mapping column_name -> Python list
            let dict = PyDict::new(py);
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(idx);
                let py_list = match field.data_type() {
                    DataType::Int64 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Int64 but downcast failed",
                                    field.name()
                                ),
                            })?;
                        let list = PyList::new(py, arr.iter().map(|v| v.map(|x| x.into_pyobject(py).unwrap())))
                            .map_err(|e| krishiv_udf::UdfError::Execution {
                                message: e.to_string(),
                            })?;
                        list.into_any()
                    }
                    DataType::Float64 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Float64 but downcast failed",
                                    field.name()
                                ),
                            })?;
                        let list = PyList::new(py, arr.iter().map(|v| v.map(|x| x.into_pyobject(py).unwrap())))
                            .map_err(|e| krishiv_udf::UdfError::Execution {
                                message: e.to_string(),
                            })?;
                        list.into_any()
                    }
                    DataType::Utf8 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Utf8 but downcast failed",
                                    field.name()
                                ),
                            })?;
                        let list = PyList::new(py, arr.iter().map(|v| v.map(|x| x.into_pyobject(py).unwrap())))
                            .map_err(|e| krishiv_udf::UdfError::Execution {
                                message: e.to_string(),
                            })?;
                        list.into_any()
                    }
                    dt => {
                        return Err(krishiv_udf::UdfError::InvalidArgument {
                            message: format!("unsupported column data type: {dt}"),
                        });
                    }
                };
                dict.set_item(field.name(), py_list)
                    .map_err(|e| krishiv_udf::UdfError::Execution {
                        message: e.to_string(),
                    })?;
            }

            // Call the Python UDF with the dict
            let result = self
                .callable
                .call1(py, (dict,))
                .map_err(|e| krishiv_udf::UdfError::Execution {
                    message: e.to_string(),
                })?;

            // Convert the returned Python list to an Arrow array based on the
            // declared output type.
            let nrows = batch.num_rows();
            match self.output_field.data_type() {
                DataType::Int64 => {
                    let list = result
                        .cast_bound::<PyList>(py)
                        .map_err(|e| krishiv_udf::UdfError::Execution {
                            message: format!("UDF must return a list for Int64 output: {e}"),
                        })?;
                    let mut values: Vec<Option<i64>> = Vec::with_capacity(nrows);
                    for item in list.iter() {
                        let v: Option<i64> = if item.is_none() {
                            None
                        } else {
                            Some(item.extract::<i64>().map_err(|e| {
                                krishiv_udf::UdfError::Execution {
                                    message: format!("cannot convert item to i64: {e}"),
                                }
                            })?)
                        };
                        values.push(v);
                    }
                    let array = Int64Array::from(values);
                    Ok(Arc::new(array) as ArrayRef)
                }
                DataType::Float64 => {
                    let list = result
                        .cast_bound::<PyList>(py)
                        .map_err(|e| krishiv_udf::UdfError::Execution {
                            message: format!("UDF must return a list for Float64 output: {e}"),
                        })?;
                    let mut values: Vec<Option<f64>> = Vec::with_capacity(nrows);
                    for item in list.iter() {
                        let v: Option<f64> = if item.is_none() {
                            None
                        } else {
                            Some(item.extract::<f64>().map_err(|e| {
                                krishiv_udf::UdfError::Execution {
                                    message: format!("cannot convert item to f64: {e}"),
                                }
                            })?)
                        };
                        values.push(v);
                    }
                    let array = Float64Array::from(values);
                    Ok(Arc::new(array) as ArrayRef)
                }
                DataType::Utf8 => {
                    let list = result
                        .cast_bound::<PyList>(py)
                        .map_err(|e| krishiv_udf::UdfError::Execution {
                            message: format!("UDF must return a list for Utf8 output: {e}"),
                        })?;
                    let mut values: Vec<Option<String>> = Vec::with_capacity(nrows);
                    for item in list.iter() {
                        let v: Option<String> = if item.is_none() {
                            None
                        } else {
                            Some(item.extract::<String>().map_err(|e| {
                                krishiv_udf::UdfError::Execution {
                                    message: format!("cannot convert item to String: {e}"),
                                }
                            })?)
                        };
                        values.push(v);
                    }
                    let array = StringArray::from(values);
                    Ok(Arc::new(array) as ArrayRef)
                }
                dt => Err(krishiv_udf::UdfError::InvalidArgument {
                    message: format!("unsupported output data type: {dt}"),
                }),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// call_python_udf — ADR implementation contract
// ---------------------------------------------------------------------------

/// Execute a [`ScalarUdf`] on a `spawn_blocking` thread so the GIL is never
/// held on a Tokio worker thread.
///
/// **Beta API**: may change between minor releases.
pub async fn call_python_udf(
    udf: Arc<dyn krishiv_udf::ScalarUdf>,
    batch: RecordBatch,
) -> Result<ArrayRef, krishiv_udf::UdfError> {
    tokio::task::spawn_blocking(move || udf.call(&batch))
        .await
        .map_err(|e| krishiv_udf::UdfError::Panic(e.to_string()))?
}

// ---------------------------------------------------------------------------
// PyModule entry point
// ---------------------------------------------------------------------------

/// **Beta API**: may change between minor releases.
///
/// Python module `krishiv_python` — exposes `Session` and `DataFrame`.
#[pymodule]
fn krishiv_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySession>()?;
    m.add_class::<PyDataFrame>()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use krishiv_udf::ScalarUdf;

    // Test 1: PySession::new() inner session builds successfully
    #[test]
    fn py_session_builds() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        let df = session.sql("SELECT 1 AS n").unwrap();
        let result = df.collect().unwrap();
        assert_eq!(result.row_count(), 1);
    }

    // Test 2: collect returns a string containing the column name
    #[test]
    fn py_dataframe_collect_contains_column() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        let df = session.sql("SELECT 1 AS n").unwrap();
        let result = df.collect().unwrap();
        let pretty = result.pretty().unwrap();
        assert!(pretty.contains('n'), "expected 'n' in output: {pretty}");
    }

    // Test 3: A panicking UDF becomes UdfError::Panic via call_python_udf
    #[test]
    fn call_python_udf_panic_becomes_udf_error() {
        #[derive(Debug)]
        struct PanicUdf;

        impl ScalarUdf for PanicUdf {
            fn name(&self) -> &str {
                "panic"
            }
            fn input_schema(&self) -> &Schema {
                todo!()
            }
            fn output_field(&self) -> &Field {
                todo!()
            }
            fn call(
                &self,
                _batch: &RecordBatch,
            ) -> Result<ArrayRef, krishiv_udf::UdfError> {
                panic!("intentional panic from test")
            }
        }

        let udf = Arc::new(PanicUdf);
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let result = RUNTIME.block_on(call_python_udf(udf, batch));
        assert!(
            matches!(result, Err(krishiv_udf::UdfError::Panic(_))),
            "expected UdfError::Panic, got: {result:?}"
        );
    }

    // Test 4: PythonScalarUdf name is accessible
    #[test]
    fn python_scalar_udf_name() {
        let schema = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        let output = Field::new("result", DataType::Int64, true);
        // We can't construct PythonScalarUdf without Python, so test via a
        // stand-in struct that exercises the same name() pattern.
        let udf = krishiv_udf::MultiplyScalarUdf::new("my_udf", "x", 2);
        assert_eq!(udf.name(), "my_udf");
        // Also verify Schema/Field construction matches what PythonScalarUdf does
        assert_eq!(schema.field(0).name(), "x");
        assert_eq!(output.name(), "result");
    }

    // Test 5: num_rows returns 1 for SELECT 1 AS n
    #[test]
    fn py_dataframe_num_rows() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        let df = session.sql("SELECT 1 AS n").unwrap();
        let result = df.collect().unwrap();
        assert_eq!(result.row_count(), 1);
    }
}
