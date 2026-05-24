//! Python scalar UDF bridge.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::errors::SchemaError;

pub(crate) struct PythonScalarUdf {
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
                        let mut cells = Vec::with_capacity(arr.len());
                        for v in arr.iter() {
                            cells.push(match v {
                                Some(x) => Some(x.into_pyobject(py).map_err(|e| {
                                    krishiv_udf::UdfError::Execution {
                                        message: e.to_string(),
                                    }
                                })?),
                                None => None,
                            });
                        }
                        let list = PyList::new(py, cells).map_err(|e| {
                            krishiv_udf::UdfError::Execution {
                                message: e.to_string(),
                            }
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
                        let mut cells = Vec::with_capacity(arr.len());
                        for v in arr.iter() {
                            cells.push(match v {
                                Some(x) => Some(x.into_pyobject(py).map_err(|e| {
                                    krishiv_udf::UdfError::Execution {
                                        message: e.to_string(),
                                    }
                                })?),
                                None => None,
                            });
                        }
                        let list = PyList::new(py, cells).map_err(|e| {
                            krishiv_udf::UdfError::Execution {
                                message: e.to_string(),
                            }
                        })?;
                        list.into_any()
                    }
                    DataType::Utf8 => {
                        use arrow::array::Array;
                        let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                            krishiv_udf::UdfError::InvalidArgument {
                                message: format!(
                                    "column '{}' declared Utf8 but downcast failed",
                                    field.name()
                                ),
                            }
                        })?;
                        let mut cells = Vec::with_capacity(arr.len());
                        for v in arr.iter() {
                            cells.push(match v {
                                Some(x) => Some(x.into_pyobject(py).map_err(|e| {
                                    krishiv_udf::UdfError::Execution {
                                        message: e.to_string(),
                                    }
                                })?),
                                None => None,
                            });
                        }
                        let list = PyList::new(py, cells).map_err(|e| {
                            krishiv_udf::UdfError::Execution {
                                message: e.to_string(),
                            }
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

pub const UDF_META_ATTR: &str = "__krishiv_udf__";

pub(crate) fn parse_arrow_type(name: &str) -> PyResult<DataType> {
    match name.to_ascii_lowercase().as_str() {
        "int64" | "long" | "int" => Ok(DataType::Int64),
        "float64" | "double" | "float" => Ok(DataType::Float64),
        "utf8" | "string" | "str" => Ok(DataType::Utf8),
        other => Err(SchemaError::new_err(format!(
            "unsupported Arrow type '{other}'; use int64, float64, or utf8"
        ))),
    }
}

pub(crate) fn schema_from_input_types(input_types: &Bound<'_, PyDict>) -> PyResult<Schema> {
    let mut fields = Vec::new();
    for (col_name, ty_obj) in input_types.iter() {
        let col_name: String = col_name.extract()?;
        let ty_name: String = ty_obj.extract()?;
        fields.push(Field::new(col_name, parse_arrow_type(&ty_name)?, true));
    }
    if fields.is_empty() {
        return Err(SchemaError::new_err(
            "input_types must contain at least one column",
        ));
    }
    Ok(Schema::new(fields))
}

pub(crate) fn build_python_scalar_udf(
    py: Python<'_>,
    name: String,
    callable: Py<PyAny>,
    input_types: &Bound<'_, PyDict>,
    output_type: &str,
    output_name: Option<String>,
) -> PyResult<Arc<dyn krishiv_udf::ScalarUdf>> {
    let input_schema = schema_from_input_types(input_types)?;
    let output_field = Field::new(
        output_name.unwrap_or_else(|| name.clone()),
        parse_arrow_type(output_type)?,
        true,
    );
    Ok(Arc::new(PythonScalarUdf {
        callable: callable.clone_ref(py),
        name,
        input_schema,
        output_field,
    }))
}

pub(crate) fn resolve_register_udf_args(
    name_or_callable: Bound<'_, PyAny>,
    callable: Option<Bound<'_, PyAny>>,
    input_types: Option<Bound<'_, PyDict>>,
    output_type: Option<String>,
    output_name: Option<String>,
) -> PyResult<(String, Py<PyAny>, Py<PyDict>, String, Option<String>)> {
    if let Some(fn_obj) = callable {
        let input_types = input_types.ok_or_else(|| {
            SchemaError::new_err("register_udf() requires input_types= when name is given")
        })?;
        let output_type = output_type.ok_or_else(|| {
            SchemaError::new_err("register_udf() requires output_type= when name is given")
        })?;
        Ok((
            name_or_callable.extract::<String>()?,
            fn_obj.unbind(),
            input_types.unbind(),
            output_type,
            output_name,
        ))
    } else {
        let meta = name_or_callable.getattr(UDF_META_ATTR).map_err(|_| {
            SchemaError::new_err(
                "register_udf(fn) requires @udf(...) decoration, or pass name and callable",
            )
        })?;
        let meta = meta.cast::<PyDict>()?;
        let name: String = meta
            .get_item("name")?
            .ok_or_else(|| SchemaError::new_err("udf metadata missing 'name'"))?
            .extract()?;
        let input_types_obj = meta
            .get_item("input_types")?
            .ok_or_else(|| SchemaError::new_err("udf metadata missing 'input_types'"))?;
        let input_types = input_types_obj
            .downcast_into::<PyDict>()
            .map_err(|_| SchemaError::new_err("udf metadata 'input_types' must be a dict"))?
            .unbind();
        let output_type: String = meta
            .get_item("output_type")?
            .ok_or_else(|| SchemaError::new_err("udf metadata missing 'output_type'"))?
            .extract()?;
        let output_name: Option<String> = meta
            .get_item("output_name")?
            .map(|v| v.extract())
            .transpose()?;
        Ok((
            name,
            name_or_callable.unbind(),
            input_types,
            output_type,
            output_name,
        ))
    }
}

#[pyclass]
struct UdfDecorator {
    name: Option<String>,
    input_types: Py<PyDict>,
    output_type: String,
    output_name: Option<String>,
}

#[pymethods]
impl UdfDecorator {
    #[pyo3(signature = (callable))]
    fn __call__(&self, py: Python<'_>, callable: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let name = if let Some(ref n) = self.name {
            n.clone()
        } else {
            callable
                .getattr("__name__")?
                .extract::<String>()
                .unwrap_or_else(|_| "udf".to_string())
        };
        let meta = PyDict::new(py);
        meta.set_item("name", &name)?;
        meta.set_item("input_types", self.input_types.bind(py))?;
        meta.set_item("output_type", &self.output_type)?;
        if let Some(ref out_name) = self.output_name {
            meta.set_item("output_name", out_name)?;
        }
        callable.setattr(UDF_META_ATTR, meta)?;
        Ok(callable.unbind())
    }
}

#[pyfunction]
#[pyo3(signature = (callable=None, *, name=None, input_types, output_type, output_name=None))]
pub fn udf(
    py: Python<'_>,
    callable: Option<Bound<'_, PyAny>>,
    name: Option<String>,
    input_types: Bound<'_, PyDict>,
    output_type: String,
    output_name: Option<String>,
) -> PyResult<Py<PyAny>> {
    let decorator = Py::new(
        py,
        UdfDecorator {
            name,
            input_types: input_types.unbind(),
            output_type,
            output_name,
        },
    )?;
    if let Some(fn_obj) = callable {
        Ok(decorator.bind(py).call1((fn_obj,))?.unbind())
    } else {
        Ok(decorator.into_any())
    }
}
