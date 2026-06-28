//! Python scalar, aggregate, and table UDF bridges.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, TimestampNanosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

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

impl krishiv_plan::udf::ScalarUdf for PythonScalarUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> &Schema {
        &self.input_schema
    }

    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn call(&self, batch: &RecordBatch) -> Result<ArrayRef, krishiv_plan::udf::UdfError> {
        Python::attach(|py| {
            // ── Arrow-native fast path ────────────────────────────────────────
            // If the Python callable has `_krishiv_arrow_udf = True`, pass the
            // whole RecordBatch as a pyo3_arrow::PyRecordBatch and expect back a
            // PyRecordBatch (the first column is used as the output array).
            // This avoids the per-column Vec<Option<T>> → PyList → Arrow conversion
            // that the dict-based path requires.
            let is_arrow_native = self
                .callable
                .getattr(py, "_krishiv_arrow_udf")
                .ok()
                .and_then(|v| v.is_truthy(py).ok())
                .unwrap_or(false);

            if is_arrow_native {
                let py_batch = crate::arrow_compat::PyArrowBatch::new(batch.clone());
                let result = self.callable.call1(py, (py_batch,)).map_err(|e| {
                    krishiv_plan::udf::UdfError::Execution {
                        message: format!("arrow-native UDF call failed: {e}"),
                    }
                })?;
                let out_batch: RecordBatch = result
                    .extract::<crate::arrow_compat::PyArrowBatch>(py)
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: format!("arrow-native UDF must return a pyarrow.RecordBatch: {e}"),
                    })?
                    .into_inner();
                if out_batch.num_columns() == 0 {
                    return Err(krishiv_plan::udf::UdfError::Execution {
                        message: "arrow-native UDF returned a RecordBatch with 0 columns; \
                                  the first column is used as the output array"
                            .into(),
                    });
                }
                return Ok(Arc::clone(out_batch.column(0)));
            }

            macro_rules! to_py_list {
                ($col:expr, $arr_ty:ty, $native:ty, $field:expr) => {{
                    let arr = $col.as_any().downcast_ref::<$arr_ty>().ok_or_else(|| {
                        krishiv_plan::udf::UdfError::InvalidArgument {
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
                                krishiv_plan::udf::UdfError::Execution {
                                    message: e.to_string(),
                                }
                            })?),
                            None => None,
                        });
                    }
                    let list = PyList::new(py, cells).map_err(|e| {
                        krishiv_plan::udf::UdfError::Execution {
                            message: e.to_string(),
                        }
                    })?;
                    list.into_any()
                }};
            }

            macro_rules! from_py_list {
                ($result:expr, $native:ty, $arr_ty:ty, $nrows:expr) => {{
                    let list = $result.cast_bound::<PyList>(py).map_err(|e| {
                        krishiv_plan::udf::UdfError::Execution {
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
                                krishiv_plan::udf::UdfError::Execution {
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
                        return Err(krishiv_plan::udf::UdfError::InvalidArgument {
                            message: format!("unsupported column data type: {dt}"),
                        });
                    }
                };
                dict.set_item(field.name(), py_list).map_err(|e| {
                    krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    }
                })?;
            }

            let result = self.callable.call1(py, (dict,)).map_err(|e| {
                krishiv_plan::udf::UdfError::Execution {
                    message: e.to_string(),
                }
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
                dt => Err(krishiv_plan::udf::UdfError::InvalidArgument {
                    message: format!("unsupported output data type: {dt}"),
                }),
            }
        })
    }
}

/// Default Python UDF execution timeout (30 seconds).
const PYTHON_UDF_DEFAULT_TIMEOUT_MS: u64 = 30_000;

pub async fn call_python_udf(
    udf: Arc<dyn krishiv_plan::udf::ScalarUdf>,
    batch: RecordBatch,
) -> Result<ArrayRef, krishiv_plan::udf::UdfError> {
    call_python_udf_with_timeout(udf, batch, PYTHON_UDF_DEFAULT_TIMEOUT_MS).await
}

/// Execute a Python scalar UDF with an explicit millisecond timeout.
///
/// If the UDF does not return within `timeout_ms`, the blocking thread is
/// left running (it cannot be cancelled), but the caller receives a
/// `UdfError::Execution` with a timeout message. Callers should not rely on
/// subsequent calls completing quickly on the same thread.
pub async fn call_python_udf_with_timeout(
    udf: Arc<dyn krishiv_plan::udf::ScalarUdf>,
    batch: RecordBatch,
    timeout_ms: u64,
) -> Result<ArrayRef, krishiv_plan::udf::UdfError> {
    let handle = tokio::task::spawn_blocking(move || udf.call(&batch));
    tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), handle)
        .await
        .map_err(|_| krishiv_plan::udf::UdfError::Execution {
            message: format!(
                "Python UDF timed out after {timeout_ms} ms; \
                 set KRISHIV_PYTHON_UDF_TIMEOUT_MS to override"
            ),
        })?
        .map_err(|e| krishiv_plan::udf::UdfError::Panic(e.to_string()))?
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
) -> PyResult<Arc<dyn krishiv_plan::udf::ScalarUdf>> {
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

/// Return type for [`resolve_register_udf_args`].
type UdfArgs = (
    String,
    pyo3::Py<pyo3::PyAny>,
    pyo3::Py<pyo3::types::PyDict>,
    String,
    Option<String>,
);

pub(crate) fn resolve_register_udf_args(
    name_or_callable: Bound<'_, PyAny>,
    callable: Option<Bound<'_, PyAny>>,
    input_types: Option<Bound<'_, PyDict>>,
    output_type: Option<String>,
    output_name: Option<String>,
) -> PyResult<UdfArgs> {
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

// ---------------------------------------------------------------------------
// Python aggregate UDF (UDAF) bridge
// ---------------------------------------------------------------------------

/// Bridge between Python callables and the Rust [`AggregateUdf`] trait.
///
/// Python callers provide three functions:
/// - `accumulate(state: bytes, batch: dict[str, list]) -> bytes`
/// - `finalize(state: bytes) -> int | float | str | bool | bytes | None`
/// - `merge(state_a: bytes, state_b: bytes) -> bytes`
pub(crate) struct PythonAggregateUdf {
    accumulate_fn: Py<PyAny>,
    finalize_fn: Py<PyAny>,
    merge_fn: Py<PyAny>,
    name: String,
    input_schema: Schema,
    output_field: Field,
}

impl std::fmt::Debug for PythonAggregateUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonAggregateUdf")
            .field("name", &self.name)
            .finish()
    }
}

fn batch_to_py_dict(
    py: Python<'_>,
    batch: &RecordBatch,
) -> Result<Py<PyDict>, krishiv_plan::udf::UdfError> {
    let dict = PyDict::new(py);
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(idx);
        let py_list = match field.data_type() {
            DataType::Int8 => {
                let arr = col.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                    krishiv_plan::udf::UdfError::InvalidArgument {
                        message: format!(
                            "column '{}' declared Int8 but downcast failed (got {})",
                            field.name(),
                            col.data_type()
                        ),
                    }
                })?;
                PyList::new(py, arr.iter().map(|v| v.map(|x| x as i64)))
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    })?
                    .into_any()
            }
            DataType::Int16 => {
                let arr = col.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                    krishiv_plan::udf::UdfError::InvalidArgument {
                        message: format!(
                            "column '{}' declared Int16 but downcast failed (got {})",
                            field.name(),
                            col.data_type()
                        ),
                    }
                })?;
                PyList::new(py, arr.iter().map(|v| v.map(|x| x as i64)))
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    })?
                    .into_any()
            }
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    krishiv_plan::udf::UdfError::InvalidArgument {
                        message: format!(
                            "column '{}' declared Int32 but downcast failed (got {})",
                            field.name(),
                            col.data_type()
                        ),
                    }
                })?;
                PyList::new(py, arr.iter().map(|v| v.map(|x| x as i64)))
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    })?
                    .into_any()
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    krishiv_plan::udf::UdfError::InvalidArgument {
                        message: format!(
                            "column '{}' declared Int64 but downcast failed (got {})",
                            field.name(),
                            col.data_type()
                        ),
                    }
                })?;
                PyList::new(py, arr.iter())
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    })?
                    .into_any()
            }
            DataType::Float32 => {
                let arr = col.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                    krishiv_plan::udf::UdfError::InvalidArgument {
                        message: format!(
                            "column '{}' declared Float32 but downcast failed (got {})",
                            field.name(),
                            col.data_type()
                        ),
                    }
                })?;
                PyList::new(py, arr.iter().map(|v| v.map(|x| x as f64)))
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    })?
                    .into_any()
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    krishiv_plan::udf::UdfError::InvalidArgument {
                        message: format!(
                            "column '{}' declared Float64 but downcast failed (got {})",
                            field.name(),
                            col.data_type()
                        ),
                    }
                })?;
                PyList::new(py, arr.iter())
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    })?
                    .into_any()
            }
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                    krishiv_plan::udf::UdfError::InvalidArgument {
                        message: format!(
                            "column '{}' declared Boolean but downcast failed (got {})",
                            field.name(),
                            col.data_type()
                        ),
                    }
                })?;
                PyList::new(py, arr.iter())
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    })?
                    .into_any()
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    krishiv_plan::udf::UdfError::InvalidArgument {
                        message: format!(
                            "column '{}' declared Utf8 but downcast failed (got {})",
                            field.name(),
                            col.data_type()
                        ),
                    }
                })?;
                PyList::new(py, arr.iter())
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    })?
                    .into_any()
            }
            dt => {
                return Err(krishiv_plan::udf::UdfError::InvalidArgument {
                    message: format!("unsupported column type in UDAF accumulate: {dt}"),
                });
            }
        };
        dict.set_item(field.name(), py_list).map_err(|e| {
            krishiv_plan::udf::UdfError::Execution {
                message: e.to_string(),
            }
        })?;
    }
    Ok(dict.unbind())
}

fn py_to_scalar(
    py: Python<'_>,
    obj: &Py<PyAny>,
) -> Result<krishiv_plan::udf::ScalarValue, krishiv_plan::udf::UdfError> {
    let bound = obj.bind(py);
    if bound.is_none() {
        return Ok(krishiv_plan::udf::ScalarValue::Null);
    }
    if let Ok(v) = bound.extract::<bool>() {
        return Ok(krishiv_plan::udf::ScalarValue::Boolean(v));
    }
    if let Ok(v) = bound.extract::<i64>() {
        return Ok(krishiv_plan::udf::ScalarValue::Int64(v));
    }
    if let Ok(v) = bound.extract::<f64>() {
        return Ok(krishiv_plan::udf::ScalarValue::Float64(v));
    }
    if let Ok(v) = bound.extract::<String>() {
        return Ok(krishiv_plan::udf::ScalarValue::Utf8(v));
    }
    if let Ok(v) = bound.extract::<Vec<u8>>() {
        return Ok(krishiv_plan::udf::ScalarValue::Bytes(v));
    }
    Err(krishiv_plan::udf::UdfError::Execution {
        message: format!(
            "unsupported Python scalar type returned from UDAF finalize: {}",
            bound
                .get_type()
                .name()
                .map(|s| s.to_string())
                .unwrap_or_else(|_| "unknown".into())
        ),
    })
}

impl krishiv_plan::udf::AggregateUdf for PythonAggregateUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> &Schema {
        &self.input_schema
    }

    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn accumulate(
        &self,
        state: &mut krishiv_plan::udf::AggState,
        batch: &RecordBatch,
    ) -> Result<(), krishiv_plan::udf::UdfError> {
        Python::attach(|py| {
            let py_state = PyBytes::new(py, &state.data);
            let py_batch = batch_to_py_dict(py, batch)?;
            let result = self
                .accumulate_fn
                .call1(py, (py_state, py_batch.bind(py)))
                .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                    message: format!("UDAF accumulate failed: {e}"),
                })?;
            state.data = result.extract::<Vec<u8>>(py).map_err(|e| {
                krishiv_plan::udf::UdfError::Execution {
                    message: format!("UDAF accumulate must return bytes: {e}"),
                }
            })?;
            Ok(())
        })
    }

    fn finalize(
        &self,
        state: krishiv_plan::udf::AggState,
    ) -> Result<krishiv_plan::udf::ScalarValue, krishiv_plan::udf::UdfError> {
        Python::attach(|py| {
            let py_state = PyBytes::new(py, &state.data);
            let result = self.finalize_fn.call1(py, (py_state,)).map_err(|e| {
                krishiv_plan::udf::UdfError::Execution {
                    message: format!("UDAF finalize failed: {e}"),
                }
            })?;
            py_to_scalar(py, &result)
        })
    }

    fn merge(
        &self,
        a: krishiv_plan::udf::AggState,
        b: krishiv_plan::udf::AggState,
    ) -> Result<krishiv_plan::udf::AggState, krishiv_plan::udf::UdfError> {
        Python::attach(|py| {
            let py_a = PyBytes::new(py, &a.data);
            let py_b = PyBytes::new(py, &b.data);
            let result = self.merge_fn.call1(py, (py_a, py_b)).map_err(|e| {
                krishiv_plan::udf::UdfError::Execution {
                    message: format!("UDAF merge failed: {e}"),
                }
            })?;
            let data = result.extract::<Vec<u8>>(py).map_err(|e| {
                krishiv_plan::udf::UdfError::Execution {
                    message: format!("UDAF merge must return bytes: {e}"),
                }
            })?;
            Ok(krishiv_plan::udf::AggState { data })
        })
    }
}

/// Build a Python UDAF from three Python callables and type metadata.
pub(crate) fn build_python_aggregate_udf(
    py: Python<'_>,
    name: String,
    accumulate_fn: Py<PyAny>,
    finalize_fn: Py<PyAny>,
    merge_fn: Py<PyAny>,
    input_types: &Bound<'_, PyDict>,
    output_type: &str,
    output_name: Option<String>,
) -> PyResult<Arc<dyn krishiv_plan::udf::AggregateUdf>> {
    let input_schema = schema_from_input_types(input_types)?;
    let output_field = Field::new(
        output_name.unwrap_or_else(|| name.clone()),
        parse_arrow_type(output_type)?,
        true,
    );
    Ok(Arc::new(PythonAggregateUdf {
        accumulate_fn: accumulate_fn.clone_ref(py),
        finalize_fn: finalize_fn.clone_ref(py),
        merge_fn: merge_fn.clone_ref(py),
        name,
        input_schema,
        output_field,
    }))
}

// ---------------------------------------------------------------------------
// Python table UDF (UDTF) bridge
// ---------------------------------------------------------------------------

/// Bridge between a Python callable and the Rust [`TableUdf`] trait.
///
/// The Python callable has signature:
/// `fn(args: list[int | float | str | bool | bytes | None]) -> pyarrow.RecordBatch`
pub(crate) struct PythonTableUdf {
    callable: Py<PyAny>,
    name: String,
    output_schema: Schema,
}

impl std::fmt::Debug for PythonTableUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonTableUdf")
            .field("name", &self.name)
            .finish()
    }
}

fn scalar_to_py(py: Python<'_>, v: &krishiv_plan::udf::ScalarValue) -> PyResult<Py<PyAny>> {
    use krishiv_plan::udf::ScalarValue;
    match v {
        ScalarValue::Null => Ok(py.None().into_bound(py).unbind()),
        ScalarValue::Int64(n) => Ok(n.into_pyobject(py)?.into_any().unbind()),
        ScalarValue::Float64(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        ScalarValue::Utf8(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        ScalarValue::Boolean(b) => Ok((*b).into_pyobject(py)?.to_owned().into_any().unbind()),
        ScalarValue::Bytes(b) => Ok(PyBytes::new(py, b).into_any().unbind()),
    }
}

impl krishiv_plan::udf::TableUdf for PythonTableUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn call(
        &self,
        args: &[krishiv_plan::udf::ScalarValue],
    ) -> Result<RecordBatch, krishiv_plan::udf::UdfError> {
        Python::attach(|py| {
            let py_args: Vec<Py<PyAny>> = args
                .iter()
                .map(|v| {
                    scalar_to_py(py, v).map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: e.to_string(),
                    })
                })
                .collect::<Result<_, _>>()?;
            let py_list =
                PyList::new(py, &py_args).map_err(|e| krishiv_plan::udf::UdfError::Execution {
                    message: e.to_string(),
                })?;
            let result = self.callable.call1(py, (py_list,)).map_err(|e| {
                krishiv_plan::udf::UdfError::Execution {
                    message: format!("UDTF call failed: {e}"),
                }
            })?;
            let batch: RecordBatch = result
                .extract::<crate::arrow_compat::PyArrowBatch>(py)
                .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                    message: format!("UDTF must return a pyarrow.RecordBatch: {e}"),
                })?
                .into_inner();
            Ok(batch)
        })
    }
}

/// Build a Python UDTF from a Python callable and output schema metadata.
pub(crate) fn build_python_table_udf(
    py: Python<'_>,
    name: String,
    callable: Py<PyAny>,
    output_types: &Bound<'_, PyDict>,
) -> PyResult<Arc<dyn krishiv_plan::udf::TableUdf>> {
    let output_schema = schema_from_input_types(output_types)?;
    Ok(Arc::new(PythonTableUdf {
        callable: callable.clone_ref(py),
        name,
        output_schema,
    }))
}

// ---------------------------------------------------------------------------
// CoGroupMap UDF
// ---------------------------------------------------------------------------

/// Python-backed co-group map UDF.
///
/// The Python callable receives:
/// - `key: str`
/// - `left: list[pyarrow.RecordBatch]`
/// - `right: list[pyarrow.RecordBatch]`
///
/// It must return a `list[pyarrow.RecordBatch]` (may be empty).
pub(crate) struct PythonCoGroupMapUdf {
    callable: Py<PyAny>,
    name: String,
    left_schema: Schema,
    right_schema: Schema,
    output_schema: Schema,
}

impl std::fmt::Debug for PythonCoGroupMapUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonCoGroupMapUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl krishiv_plan::udf::CoGroupUdf for PythonCoGroupMapUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn left_schema(&self) -> &Schema {
        &self.left_schema
    }

    fn right_schema(&self) -> &Schema {
        &self.right_schema
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn call(
        &self,
        key: &str,
        left: &[RecordBatch],
        right: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>, krishiv_plan::udf::UdfError> {
        Python::attach(|py| {
            // Convert left batches to Python list.
            let left_list = PyList::new(
                py,
                left.iter()
                    .map(|b| crate::arrow_compat::PyArrowBatch::new(b.clone()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                message: format!("co_group: left list build failed: {e}"),
            })?;

            // Convert right batches to Python list.
            let right_list = PyList::new(
                py,
                right
                    .iter()
                    .map(|b| crate::arrow_compat::PyArrowBatch::new(b.clone()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                message: format!("co_group: right list build failed: {e}"),
            })?;

            let result = self
                .callable
                .call1(py, (key, left_list, right_list))
                .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                    message: format!("co_group callable failed: {e}"),
                })?;

            // Expect a list of RecordBatches back.
            let py_list = result.bind(py).cast::<PyList>().map_err(|_| {
                krishiv_plan::udf::UdfError::Execution {
                    message: "co_group callable must return a list[pyarrow.RecordBatch]".into(),
                }
            })?;

            let mut batches = Vec::with_capacity(py_list.len());
            for item in py_list.iter() {
                let batch = item
                    .extract::<crate::arrow_compat::PyArrowBatch>()
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: format!("co_group output batch extraction failed: {e}"),
                    })?
                    .into_inner();
                batches.push(batch);
            }
            Ok(batches)
        })
    }
}

/// Build a Python co-group map UDF.
pub(crate) fn build_python_co_group_udf(
    py: Python<'_>,
    name: String,
    callable: Py<PyAny>,
    left_types: &Bound<'_, PyDict>,
    right_types: &Bound<'_, PyDict>,
    output_types: &Bound<'_, PyDict>,
) -> PyResult<Arc<dyn krishiv_plan::udf::CoGroupUdf>> {
    let left_schema = schema_from_input_types(left_types)?;
    let right_schema = schema_from_input_types(right_types)?;
    let output_schema = schema_from_input_types(output_types)?;
    Ok(Arc::new(PythonCoGroupMapUdf {
        callable: callable.clone_ref(py),
        name,
        left_schema,
        right_schema,
        output_schema,
    }))
}

// ---------------------------------------------------------------------------
// MapPandasIter UDF
// ---------------------------------------------------------------------------

/// Python-backed map-pandas-iter UDF.
///
/// The Python callable receives a `list[pyarrow.RecordBatch]` and must return
/// a `list[pyarrow.RecordBatch]`.  On the Python side, callers may convert
/// each batch to pandas with `.to_pandas()` if they prefer.
pub(crate) struct PythonMapPandasIterUdf {
    callable: Py<PyAny>,
    name: String,
    input_schema: Schema,
    output_schema: Schema,
}

impl std::fmt::Debug for PythonMapPandasIterUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonMapPandasIterUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl krishiv_plan::udf::MapPandasIterUdf for PythonMapPandasIterUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> &Schema {
        &self.input_schema
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn map_batches(
        &self,
        batches: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>, krishiv_plan::udf::UdfError> {
        Python::attach(|py| {
            let py_batches = PyList::new(
                py,
                batches
                    .iter()
                    .map(|b| crate::arrow_compat::PyArrowBatch::new(b.clone()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                message: format!("map_pandas_iter: batch list build failed: {e}"),
            })?;

            let result = self.callable.call1(py, (py_batches,)).map_err(|e| {
                krishiv_plan::udf::UdfError::Execution {
                    message: format!("map_pandas_iter callable failed: {e}"),
                }
            })?;

            let py_list = result.bind(py).cast::<PyList>().map_err(|_| {
                krishiv_plan::udf::UdfError::Execution {
                    message: "map_pandas_iter callable must return a list[pyarrow.RecordBatch]"
                        .into(),
                }
            })?;

            let mut out_batches = Vec::with_capacity(py_list.len());
            for item in py_list.iter() {
                let batch = item
                    .extract::<crate::arrow_compat::PyArrowBatch>()
                    .map_err(|e| krishiv_plan::udf::UdfError::Execution {
                        message: format!("map_pandas_iter output batch extraction failed: {e}"),
                    })?
                    .into_inner();
                out_batches.push(batch);
            }
            Ok(out_batches)
        })
    }
}

/// Build a Python map-pandas-iter UDF.
pub(crate) fn build_python_map_pandas_iter_udf(
    py: Python<'_>,
    name: String,
    callable: Py<PyAny>,
    input_types: &Bound<'_, PyDict>,
    output_types: &Bound<'_, PyDict>,
) -> PyResult<Arc<dyn krishiv_plan::udf::MapPandasIterUdf>> {
    let input_schema = schema_from_input_types(input_types)?;
    let output_schema = schema_from_input_types(output_types)?;
    Ok(Arc::new(PythonMapPandasIterUdf {
        callable: callable.clone_ref(py),
        name,
        input_schema,
        output_schema,
    }))
}
