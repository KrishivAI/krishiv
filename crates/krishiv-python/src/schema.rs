//! `ks.Schema` — Python type annotations → Arrow types (ADR-R13-02).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};
use pyo3_arrow::PySchema as ArrowPySchema;

use crate::errors::SchemaError;

const FIELDS_ATTR: &str = "_krishiv_fields";

/// Declarative schema base class. Subclasses declare columns via PEP 484 annotations.
#[pyclass(name = "Schema", subclass)]
#[derive(Clone)]
pub struct PySchema {
    fields: Vec<(String, DataType)>,
}

impl PySchema {
    fn fields_from_class<'py>(cls: &Bound<'py, PyType>) -> PyResult<Vec<(String, DataType)>> {
        if let Ok(attr) = cls.getattr(FIELDS_ATTR) {
            if let Ok(dict) = attr.cast::<PyDict>() {
                let mut fields = Vec::new();
                for (name, dt_obj) in dict.iter() {
                    let name: String = name.extract()?;
                    let dt_str: String = dt_obj.extract()?;
                    let dt: DataType = dt_str.parse().map_err(
                        |_| {
                            PyRuntimeError::new_err("invalid Arrow type in schema".to_string())
                        },
                    )?;
                    fields.push((name, dt));
                }
                fields.sort_by(|a, b| a.0.cmp(&b.0));
                return Ok(fields);
            }
        }
        Ok(vec![])
    }

    fn store_fields_on_class<'py>(
        cls: &Bound<'py, PyType>,
        fields: &[(String, DataType)],
    ) -> PyResult<()> {
        let dict = PyDict::new(cls.py());
        for (name, dt) in fields {
            dict.set_item(name, dt.to_string())?;
        }
        cls.setattr(FIELDS_ATTR, dict)?;
        Ok(())
    }

    pub fn arrow_schema_from_class<'py>(cls: &Bound<'py, PyType>) -> PyResult<Arc<Schema>> {
        let fields = Self::fields_from_class(cls)?;
        if fields.is_empty() {
            return Err(SchemaError::new_err(
                "Schema subclass has no column annotations; declare fields like `name: str`",
            ));
        }
        let arrow_fields: Vec<Field> = fields
            .iter()
            .map(|(n, dt)| Field::new(n, dt.clone(), true))
            .collect();
        Ok(Arc::new(Schema::new(arrow_fields)))
    }
}

#[pymethods]
impl PySchema {
    #[new]
    fn new() -> Self {
        Self { fields: vec![] }
    }

    #[classmethod]
    fn __init_subclass__(
        cls: &Bound<'_, PyType>,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let py = cls.py();
        let annotations = match cls.getattr("__annotations__") {
            Ok(a) => a,
            Err(_) => return Ok(()),
        };
        let ann_dict = annotations.cast::<PyDict>()?;
        let mut fields = Vec::new();
        for (name, ann) in ann_dict.iter() {
            let name: String = name.extract()?;
            if name.starts_with('_') {
                continue;
            }
            let dt = python_annotation_to_arrow(py, ann)?;
            fields.push((name, dt));
        }
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        Self::store_fields_on_class(cls, &fields)?;
        Ok(())
    }

    #[classmethod]
    fn arrow_schema(cls: &Bound<'_, PyType>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = Self::arrow_schema_from_class(cls)?;
        let py_schema = ArrowPySchema::new(schema);
        Ok(py_schema.into_pyobject(py)?.into_any().unbind())
    }

    #[classmethod]
    fn column_names(cls: &Bound<'_, PyType>) -> PyResult<Vec<String>> {
        Ok(Self::fields_from_class(cls)?
            .into_iter()
            .map(|(n, _)| n)
            .collect())
    }

    #[classmethod]
    fn _repr_html_(cls: &Bound<'_, PyType>) -> PyResult<String> {
        let fields = PySchema::fields_from_class(&cls)?;
        let mut html = String::from(
            "<table><thead><tr><th>Column</th><th>Arrow type</th></tr></thead><tbody>",
        );
        for (name, dt) in fields {
            html.push_str(&format!(
                "<tr><td>{name}</td><td><code>{dt}</code></td></tr>"
            ));
        }
        html.push_str("</tbody></table>");
        Ok(html)
    }

    #[classmethod]
    fn __repr__(cls: &Bound<'_, PyType>) -> PyResult<String> {
        let names: Vec<String> = PySchema::fields_from_class(&cls)?
            .into_iter()
            .map(|(n, dt)| format!("{n}: {dt}"))
            .collect();
        Ok(format!("Schema({})", names.join(", ")))
    }
}

fn python_annotation_to_arrow(py: Python<'_>, ann: Bound<'_, PyAny>) -> PyResult<DataType> {
    let builtins = py.import("builtins")?;
    let typing = py.import("typing")?;
    if let Ok(origin) = ann.getattr("__origin__") {
        let union_type = typing.getattr("Union")?;
        if origin.is(&union_type) {
            let args_obj = ann.getattr("__args__")?;
            let args = args_obj.cast::<PyTuple>()?;
            let non_none: Vec<Bound<'_, PyAny>> = args
                .iter()
                .filter(|a| !a.is_none())
                .map(|a| a.clone())
                .collect();
            if non_none.len() == 1 {
                return python_annotation_to_arrow(py, non_none[0].clone());
            }
        }
    }

    let str_ty = builtins.getattr("str")?;
    let int_ty = builtins.getattr("int")?;
    let float_ty = builtins.getattr("float")?;
    let bool_ty = builtins.getattr("bool")?;
    let bytes_ty = builtins.getattr("bytes")?;

    if ann.is(&str_ty) {
        return Ok(DataType::Utf8);
    }
    if ann.is(&int_ty) {
        return Ok(DataType::Int64);
    }
    if ann.is(&float_ty) {
        return Ok(DataType::Float64);
    }
    if ann.is(&bool_ty) {
        return Ok(DataType::Boolean);
    }
    if ann.is(&bytes_ty) {
        return Ok(DataType::LargeBinary);
    }

    let datetime_mod = py.import("datetime")?;
    let datetime_ty = datetime_mod.getattr("datetime")?;
    if ann.is(&datetime_ty) {
        return Ok(DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    let name = ann.str()?.to_string();
    Err(SchemaError::new_err(format!(
        "unsupported schema annotation: {name}"
    )))
}

pub fn validate_batch_against_schema_class<'py>(
    cls: &Bound<'py, PyType>,
    batch: &arrow::record_batch::RecordBatch,
) -> PyResult<()> {
    let expected = PySchema::fields_from_class(cls)?;
    if expected.is_empty() {
        return Ok(());
    }
    let batch_schema = batch.schema();
    for (name, expected_dt) in expected {
        let idx = batch_schema.index_of(&name).map_err(|_| {
            SchemaError::new_err(format!("schema column '{name}' not found in batch"))
        })?;
        let actual = batch_schema.field(idx).data_type();
        if actual != &expected_dt {
            return Err(SchemaError::new_err(format!(
                "column '{name}': expected {expected_dt}, got {actual}"
            )));
        }
    }
    Ok(())
}
