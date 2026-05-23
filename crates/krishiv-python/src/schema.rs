//! Python `Schema` base class — annotation → Arrow type mapping (ADR-R13-02).

use arrow::datatypes::{DataType, Field, Schema};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple, PyType};

use crate::SchemaError;

const ARROW_FIELDS_KEY: &str = "_krishiv_arrow_fields";

/// Base class for declarative Krishiv schemas (`class MySchema(ks.Schema): ...`).
#[pyclass(name = "Schema", subclass)]
pub struct PySchema;

#[pymethods]
impl PySchema {
    #[classmethod]
    #[pyo3(signature = (**_kwargs))]
    fn __init_subclass__(
        cls: &Bound<'_, PyType>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let annotations = cls.getattr("__annotations__")?;
        let ann_dict = annotations.cast::<PyDict>()?;
        let mut fields = Vec::new();

        for (name_any, type_hint) in ann_dict.iter() {
            let name: String = name_any.extract()?;
            if name.starts_with('_') {
                continue;
            }
            let data_type = python_type_to_arrow(&type_hint)?;
            fields.push(Field::new(name, data_type, true));
        }

        let py = cls.py();
        let field_list = PyList::empty(py);
        for field in &fields {
            let item = PyTuple::new(
                py,
                [
                    PyString::new(py, field.name()).into_any(),
                    PyString::new(py, &format!("{:?}", field.data_type())).into_any(),
                ],
            )?;
            field_list.append(item)?;
        }
        cls.setattr(ARROW_FIELDS_KEY, field_list)?;
        Ok(())
    }

    #[classmethod]
    fn arrow_schema(cls: &Bound<'_, PyType>) -> PyResult<Py<PyAny>> {
        let fields = load_arrow_fields(cls)?;
        let schema = Schema::new(fields);
        schema_to_pyarrow(cls.py(), &schema)
    }

    #[classmethod]
    fn _repr_html_(cls: &Bound<'_, PyType>) -> PyResult<String> {
        let fields = load_arrow_fields(cls)?;
        let mut rows = String::from("<table><tr><th>column</th><th>type</th></tr>");
        for field in fields {
            rows.push_str(&format!(
                "<tr><td>{}</td><td>{:?}</td></tr>",
                field.name(),
                field.data_type()
            ));
        }
        rows.push_str("</table>");
        Ok(rows)
    }

    #[classmethod]
    fn __repr__(cls: &Bound<'_, PyType>) -> PyResult<String> {
        let fields = load_arrow_fields(cls)?;
        let names: Vec<_> = fields.iter().map(|f| f.name().as_str()).collect();
        Ok(format!("Schema({names:?})"))
    }
}

pub(crate) fn load_arrow_fields(cls: &Bound<'_, PyType>) -> PyResult<Vec<Field>> {
    let stored = if cls.hasattr(ARROW_FIELDS_KEY)? {
        cls.getattr(ARROW_FIELDS_KEY)?
    } else {
        return Err(SchemaError::new_err(
            "Schema subclass has no resolved fields; inherit from Schema",
        ));
    };
    let list = stored.cast::<PyList>()?;
    let mut fields = Vec::with_capacity(list.len());
    for item in list.iter() {
        let tuple = item.cast::<PyTuple>()?;
        let name: String = tuple.get_item(0)?.extract()?;
        let type_str: String = tuple.get_item(1)?.extract()?;
        let data_type = arrow_type_from_debug_str(&type_str)?;
        fields.push(Field::new(name, data_type, true));
    }
    Ok(fields)
}

fn python_type_to_arrow(py_type: &Bound<'_, PyAny>) -> PyResult<DataType> {
    if py_type.is_none() {
        return Err(SchemaError::new_err("column type annotation cannot be None"));
    }

    if let Ok(py_type_obj) = py_type.cast::<PyType>() {
        let name = py_type_obj.name()?.to_string();
        return match name.as_str() {
            "str" => Ok(DataType::Utf8),
            "int" => Ok(DataType::Int64),
            "float" => Ok(DataType::Float64),
            "bool" => Ok(DataType::Boolean),
            "bytes" => Ok(DataType::LargeBinary),
            "datetime" => Ok(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            )),
            other => Err(SchemaError::new_err(format!(
                "unsupported schema column type: {other}"
            ))),
        };
    }

    Err(SchemaError::new_err(format!(
        "unsupported type annotation: {py_type}"
    )))
}

fn arrow_type_from_debug_str(s: &str) -> PyResult<DataType> {
    match s {
        "Utf8" => Ok(DataType::Utf8),
        "Int64" => Ok(DataType::Int64),
        "Float64" => Ok(DataType::Float64),
        "Boolean" => Ok(DataType::Boolean),
        "LargeBinary" => Ok(DataType::LargeBinary),
        s if s.starts_with("Timestamp") => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        other => Err(SchemaError::new_err(format!("unknown Arrow type: {other}"))),
    }
}

pub(crate) fn schema_to_pyarrow(py: Python<'_>, schema: &Schema) -> PyResult<Py<PyAny>> {
    let pa = py.import("pyarrow")?;
    let field_type = pa.getattr("field")?;
    let mut py_fields = Vec::new();
    for field in schema.fields() {
        let pa_type = data_type_to_pyarrow(py, field.data_type())?;
        let py_field = field_type.call1((field.name(), pa_type, field.is_nullable()))?;
        py_fields.push(py_field);
    }
    let fields_tuple = PyTuple::new(py, py_fields)?;
    Ok(pa.getattr("schema")?.call1((fields_tuple,))?.unbind())
}

fn data_type_to_pyarrow(py: Python<'_>, data_type: &DataType) -> PyResult<Py<PyAny>> {
    let pa = py.import("pyarrow")?;
    let value = match data_type {
        DataType::Utf8 => pa.getattr("string")?.call0()?,
        DataType::Int64 => pa.getattr("int64")?.call0()?,
        DataType::Float64 => pa.getattr("float64")?.call0()?,
        DataType::Boolean => pa.getattr("bool_")?.call0()?,
        DataType::LargeBinary => pa.getattr("large_binary")?.call0()?,
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Microsecond => pa.getattr("timestamp")?.call1(("us",))?,
            other => {
                return Err(SchemaError::new_err(format!(
                    "unsupported timestamp unit for pyarrow export: {other:?}"
                )));
            }
        },
        other => {
            return Err(SchemaError::new_err(format!(
                "unsupported Arrow type for pyarrow export: {other:?}"
            )));
        }
    };
    Ok(value.unbind())
}
