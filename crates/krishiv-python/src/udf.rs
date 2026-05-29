//! Python scalar UDF bridge.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, TimestampNanosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
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
            macro_rules! to_py_list {
                ($col:expr, $arr_ty:ty, $native:ty, $field:expr) => {{
                    let arr = $col.as_any().downcast_ref::<$arr_ty>().ok_or_else(|| {
                        krishiv_udf::UdfError::InvalidArgument {
                            message: format!(
                                "column '{}' declared {} but downcast failed",
                                $field.name(),
                                stringify!($arr_ty)
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
                    let list =
                        PyList::new(py, cells).map_err(|e| krishiv_udf::UdfError::Execution {
                            message: e.to_string(),
                        })?;
                    list.into_any()
                }};
            }

            macro_rules! from_py_list {
                ($result:expr, $native:ty, $arr_ty:ty, $nrows:expr) => {{
                    let list = $result.cast_bound::<PyList>(py).map_err(|e| {
                        krishiv_udf::UdfError::Execution {
                            message: format!(
                                "UDF must return a list for {} output: {e}",
                                stringify!($arr_ty)
                            ),
                        }
                    })?;
                    let mut values: Vec<Option<$native>> = Vec::with_capacity($nrows);
                    for item in list.iter() {
                        let v = if item.is_none() {
                            None
                        } else {
                            Some(item.extract::<$native>().map_err(|e| {
                                krishiv_udf::UdfError::Execution {
                                    message: format!(
                                        "cannot convert item to {}: {e}",
                                        stringify!($native)
                                    ),
                                }
                            })?)
                        };
                        values.push(v);
                    }
                    Ok(Arc::new(<$arr_ty>::from(values)) as ArrayRef)
                }};
            }

            let dict = PyDict::new(py);
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(idx);
                let py_list = match field.data_type() {
                    DataType::Int8 => to_py_list!(col, Int8Array, i8, field),
                    DataType::Int16 => to_py_list!(col, Int16Array, i16, field),
                    DataType::Int32 => to_py_list!(col, Int32Array, i32, field),
                    DataType::Int64 => to_py_list!(col, Int64Array, i64, field),
                    DataType::UInt8 => to_py_list!(col, UInt8Array, u8, field),
                    DataType::UInt16 => to_py_list!(col, UInt16Array, u16, field),
                    DataType::UInt32 => to_py_list!(col, UInt32Array, u32, field),
                    DataType::UInt64 => to_py_list!(col, UInt64Array, u64, field),
                    DataType::Float32 => to_py_list!(col, Float32Array, f32, field),
                    DataType::Float64 => to_py_list!(col, Float64Array, f64, field),
                    DataType::Boolean => to_py_list!(col, BooleanArray, bool, field),
                    DataType::Utf8 => to_py_list!(col, StringArray, &str, field),
                    DataType::LargeUtf8 => to_py_list!(col, LargeStringArray, &str, field),
                    DataType::Date32 => to_py_list!(col, Date32Array, i32, field),
                    DataType::Date64 => to_py_list!(col, Date64Array, i64, field),
                    DataType::Timestamp(_, _) => {
                        to_py_list!(col, TimestampNanosecondArray, i64, field)
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
                DataType::Int8 => from_py_list!(result, i8, Int8Array, nrows),
                DataType::Int16 => from_py_list!(result, i16, Int16Array, nrows),
                DataType::Int32 => from_py_list!(result, i32, Int32Array, nrows),
                DataType::Int64 => from_py_list!(result, i64, Int64Array, nrows),
                DataType::UInt8 => from_py_list!(result, u8, UInt8Array, nrows),
                DataType::UInt16 => from_py_list!(result, u16, UInt16Array, nrows),
                DataType::UInt32 => from_py_list!(result, u32, UInt32Array, nrows),
                DataType::UInt64 => from_py_list!(result, u64, UInt64Array, nrows),
                DataType::Float32 => from_py_list!(result, f32, Float32Array, nrows),
                DataType::Float64 => from_py_list!(result, f64, Float64Array, nrows),
                DataType::Boolean => from_py_list!(result, bool, BooleanArray, nrows),
                DataType::Utf8 => from_py_list!(result, String, StringArray, nrows),
                DataType::LargeUtf8 => from_py_list!(result, String, LargeStringArray, nrows),
                DataType::Date32 => from_py_list!(result, i32, Date32Array, nrows),
                DataType::Date64 => from_py_list!(result, i64, Date64Array, nrows),
                DataType::Timestamp(_, _) => {
                    from_py_list!(result, i64, TimestampNanosecondArray, nrows)
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
        "int8" | "tinyint" => Ok(DataType::Int8),
        "int16" | "smallint" => Ok(DataType::Int16),
        "int32" | "int" | "integer" => Ok(DataType::Int32),
        "int64" | "long" | "bigint" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float32" | "float" | "real" => Ok(DataType::Float32),
        "float64" | "double" => Ok(DataType::Float64),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "utf8" | "string" | "str" | "varchar" => Ok(DataType::Utf8),
        "largeutf8" | "large_string" => Ok(DataType::LargeUtf8),
        "date32" | "date" => Ok(DataType::Date32),
        "date64" => Ok(DataType::Date64),
        "timestamp" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        )),
        other => Err(SchemaError::new_err(format!(
            "unsupported Arrow type '{other}'"
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
            .cast::<PyDict>()
            .map_err(|_| SchemaError::new_err("udf metadata 'input_types' must be a dict"))?
            .clone()
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
