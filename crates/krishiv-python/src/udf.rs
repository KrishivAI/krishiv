//! Python scalar UDF bridge.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

#[allow(dead_code)]
pub struct PythonScalarUdf {
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
            let dict = PyDict::new(py);
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(idx);
                let py_list = match field.data_type() {
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                            krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Int64 but downcast failed",
                                    field.name()
                                ),
                            }
                        })?;
                        let list = PyList::new(
                            py,
                            arr.iter().map(|v| v.map(|x| x.into_pyobject(py).unwrap())),
                        )
                        .map_err(|e| krishiv_udf::UdfError::Execution {
                            message: e.to_string(),
                        })?;
                        list.into_any()
                    }
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                            krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Float64 but downcast failed",
                                    field.name()
                                ),
                            }
                        })?;
                        let list = PyList::new(
                            py,
                            arr.iter().map(|v| v.map(|x| x.into_pyobject(py).unwrap())),
                        )
                        .map_err(|e| krishiv_udf::UdfError::Execution {
                            message: e.to_string(),
                        })?;
                        list.into_any()
                    }
                    DataType::Utf8 => {
                        let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                            krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Utf8 but downcast failed",
                                    field.name()
                                ),
                            }
                        })?;
                        let list = PyList::new(
                            py,
                            arr.iter().map(|v| v.map(|x| x.into_pyobject(py).unwrap())),
                        )
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
                dict.set_item(field.name(), py_list).map_err(|e| {
                    krishiv_udf::UdfError::Execution {
                        message: e.to_string(),
                    }
                })?;
            }

            let result =
                self.callable
                    .call1(py, (dict,))
                    .map_err(|e| krishiv_udf::UdfError::Execution {
                        message: e.to_string(),
                    })?;

            let nrows = batch.num_rows();
            match self.output_field.data_type() {
                DataType::Int64 => {
                    let list = result.cast_bound::<PyList>(py).map_err(|e| {
                        krishiv_udf::UdfError::Execution {
                            message: format!("UDF must return a list for Int64 output: {e}"),
                        }
                    })?;
                    let mut values: Vec<Option<i64>> = Vec::with_capacity(nrows);
                    for item in list.iter() {
                        let v = if item.is_none() {
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
                    Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
                }
                DataType::Float64 => {
                    let list = result.cast_bound::<PyList>(py).map_err(|e| {
                        krishiv_udf::UdfError::Execution {
                            message: format!("UDF must return a list for Float64 output: {e}"),
                        }
                    })?;
                    let mut values: Vec<Option<f64>> = Vec::with_capacity(nrows);
                    for item in list.iter() {
                        let v = if item.is_none() {
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
                    Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
                }
                DataType::Utf8 => {
                    let list = result.cast_bound::<PyList>(py).map_err(|e| {
                        krishiv_udf::UdfError::Execution {
                            message: format!("UDF must return a list for Utf8 output: {e}"),
                        }
                    })?;
                    let mut values: Vec<Option<String>> = Vec::with_capacity(nrows);
                    for item in list.iter() {
                        let v = if item.is_none() {
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
                    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
                }
                dt => Err(krishiv_udf::UdfError::InvalidArgument {
                    message: format!("unsupported output data type: {dt}"),
                }),
            }
        })
    }
}

pub async fn call_python_udf(
    udf: Arc<dyn krishiv_udf::ScalarUdf>,
    batch: RecordBatch,
) -> Result<ArrayRef, krishiv_udf::UdfError> {
    tokio::task::spawn_blocking(move || udf.call(&batch))
        .await
        .map_err(|e| krishiv_udf::UdfError::Panic(e.to_string()))?
}
