//! Window aggregation expressions (`ks.agg.*`).

use pyo3::prelude::*;
use pyo3::types::PyDict;

use krishiv_plan::window::{AggFilterCompareOp, AggFilterValue, FloatLiteral, WindowAggFilter};

use crate::errors::QueryError;

#[derive(Debug, Clone)]
pub struct AggDescriptor {
    pub function: AggKind,
    pub input_column: Option<String>,
    pub output_name: String,
    /// Optional per-aggregate row predicate — the `AGG(x) FILTER (WHERE …)`
    /// / conditional-aggregate lowering. Rows failing it don't feed the agg.
    pub filter: Option<WindowAggFilter>,
}

fn parse_filter_op(op: &str) -> PyResult<AggFilterCompareOp> {
    Ok(match op {
        "=" | "==" => AggFilterCompareOp::Eq,
        "!=" | "<>" => AggFilterCompareOp::NotEq,
        "<" => AggFilterCompareOp::Lt,
        "<=" => AggFilterCompareOp::LtEq,
        ">" => AggFilterCompareOp::Gt,
        ">=" => AggFilterCompareOp::GtEq,
        other => {
            return Err(QueryError::new_err(format!(
                "unknown filter op '{other}'; use one of =, !=, <, <=, >, >="
            )));
        }
    })
}

fn parse_filter_value(v: &Bound<'_, PyAny>) -> PyResult<AggFilterValue> {
    // bool before int: Python bool is an int subclass.
    if let Ok(b) = v.extract::<bool>() {
        return Ok(AggFilterValue::Bool(b));
    }
    if let Ok(i) = v.extract::<i64>() {
        return Ok(AggFilterValue::Int(i));
    }
    if let Ok(f) = v.extract::<f64>() {
        return Ok(AggFilterValue::Float(FloatLiteral(f)));
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(AggFilterValue::Utf8(s));
    }
    Err(QueryError::new_err(
        "filter value must be str, int, float, or bool",
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggKind {
    Count,
    Sum,
    Min,
    Max,
    Mean,
}

/// One aggregation expression passed to `WindowedStream.agg(**exprs)`.
#[pyclass(from_py_object, name = "AggExpr")]
#[derive(Clone)]
pub struct PyAggExpr {
    #[pyo3(get)]
    pub function: String,
    #[pyo3(get)]
    pub input_column: Option<String>,
    #[pyo3(get)]
    pub output_name: String,
    pub filter: Option<WindowAggFilter>,
}

#[pymethods]
impl PyAggExpr {
    /// Restrict this aggregate to rows where ``column <op> value`` — the
    /// ``AGG(x) FILTER (WHERE …)`` / conditional-aggregate form. ``op`` is one
    /// of ``= != < <= > >=``; ``value`` may be a str, int, float, or bool.
    ///
    /// e.g. ``ks.agg.sum("amount").filter("status", "=", "paid")``.
    #[pyo3(signature = (column, op, value))]
    fn filter(&self, column: String, op: String, value: Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            filter: Some(WindowAggFilter::Compare {
                column,
                op: parse_filter_op(&op)?,
                value: parse_filter_value(&value)?,
            }),
            ..self.clone()
        })
    }

    /// Restrict this aggregate to rows where ``column`` is not null.
    #[pyo3(signature = (column))]
    fn filter_not_null(&self, column: String) -> Self {
        Self {
            filter: Some(WindowAggFilter::IsNotNull { column }),
            ..self.clone()
        }
    }
}

impl PyAggExpr {
    fn into_descriptor(self) -> Result<AggDescriptor, PyErr> {
        let function = match self.function.as_str() {
            "count" => AggKind::Count,
            "sum" => AggKind::Sum,
            "min" => AggKind::Min,
            "max" => AggKind::Max,
            "mean" => AggKind::Mean,
            other => {
                return Err(QueryError::new_err(format!(
                    "unknown aggregation function: {other}"
                )));
            }
        };
        if matches!(function, AggKind::Count) {
            // count ignores column
        } else if self.input_column.is_none() {
            return Err(QueryError::new_err(format!(
                "aggregation '{}' requires a column name",
                self.function
            )));
        }
        Ok(AggDescriptor {
            function,
            input_column: self.input_column,
            output_name: self.output_name,
            filter: self.filter,
        })
    }
}

fn make_agg(function: &str, column: Option<String>, output_name: String) -> PyAggExpr {
    PyAggExpr {
        function: function.to_string(),
        input_column: column,
        output_name,
        filter: None,
    }
}

#[pyfunction]
#[pyo3(name = "count", signature = (output_name=None))]
fn agg_count(output_name: Option<String>) -> PyAggExpr {
    make_agg("count", None, output_name.unwrap_or_else(|| "count".into()))
}

#[pyfunction]
#[pyo3(name = "sum", signature = (column, output_name=None))]
fn agg_sum(column: String, output_name: Option<String>) -> PyAggExpr {
    make_agg(
        "sum",
        Some(column.clone()),
        output_name.unwrap_or_else(|| format!("sum_{column}")),
    )
}

#[pyfunction]
#[pyo3(name = "min", signature = (column, output_name=None))]
fn agg_min(column: String, output_name: Option<String>) -> PyAggExpr {
    make_agg(
        "min",
        Some(column.clone()),
        output_name.unwrap_or_else(|| format!("min_{column}")),
    )
}

#[pyfunction]
#[pyo3(name = "max", signature = (column, output_name=None))]
fn agg_max(column: String, output_name: Option<String>) -> PyAggExpr {
    make_agg(
        "max",
        Some(column.clone()),
        output_name.unwrap_or_else(|| format!("max_{column}")),
    )
}

#[pyfunction]
#[pyo3(name = "mean", signature = (column, output_name=None))]
fn agg_mean(column: String, output_name: Option<String>) -> PyAggExpr {
    make_agg(
        "mean",
        Some(column.clone()),
        output_name.unwrap_or_else(|| format!("mean_{column}")),
    )
}

pub fn register_agg_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let agg = PyModule::new(py, "agg")?;
    agg.add_function(wrap_pyfunction!(agg_count, &agg)?)?;
    agg.add_function(wrap_pyfunction!(agg_sum, &agg)?)?;
    agg.add_function(wrap_pyfunction!(agg_min, &agg)?)?;
    agg.add_function(wrap_pyfunction!(agg_max, &agg)?)?;
    agg.add_function(wrap_pyfunction!(agg_mean, &agg)?)?;
    agg.add_class::<PyAggExpr>()?;
    parent.add_submodule(&agg)?;
    Ok(())
}

pub fn descriptors_from_kwargs(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<AggDescriptor>> {
    let Some(kwargs) = kwargs else {
        return Ok(vec![]);
    };
    let mut out = Vec::new();
    for (name, value) in kwargs.iter() {
        let output_name: String = name.extract()?;
        let expr: PyAggExpr = value.extract()?;
        let mut desc = expr.into_descriptor()?;
        desc.output_name = output_name;
        out.push(desc);
    }
    Ok(out)
}
