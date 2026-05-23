//! Window aggregation expressions (`ks.agg.*`).

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::errors::QueryError;

#[derive(Debug, Clone)]
pub struct AggDescriptor {
    pub function: AggKind,
    pub input_column: Option<String>,
    pub output_name: String,
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
#[pyclass(name = "AggExpr")]
#[derive(Clone)]
pub struct PyAggExpr {
    #[pyo3(get)]
    pub function: String,
    #[pyo3(get)]
    pub input_column: Option<String>,
    #[pyo3(get)]
    pub output_name: String,
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
        })
    }
}

fn make_agg(function: &str, column: Option<String>, output_name: String) -> PyAggExpr {
    PyAggExpr {
        function: function.to_string(),
        input_column: column,
        output_name,
    }
}

#[pyfunction]
#[pyo3(name = "count")]
fn agg_count(output_name: Option<String>) -> PyAggExpr {
    make_agg("count", None, output_name.unwrap_or_else(|| "count".into()))
}

#[pyfunction]
#[pyo3(name = "sum")]
fn agg_sum(column: String, output_name: Option<String>) -> PyAggExpr {
    make_agg(
        "sum",
        Some(column.clone()),
        output_name.unwrap_or_else(|| format!("sum_{column}")),
    )
}

#[pyfunction]
#[pyo3(name = "min")]
fn agg_min(column: String, output_name: Option<String>) -> PyAggExpr {
    make_agg(
        "min",
        Some(column.clone()),
        output_name.unwrap_or_else(|| format!("min_{column}")),
    )
}

#[pyfunction]
#[pyo3(name = "max")]
fn agg_max(column: String, output_name: Option<String>) -> PyAggExpr {
    make_agg(
        "max",
        Some(column.clone()),
        output_name.unwrap_or_else(|| format!("max_{column}")),
    )
}

#[pyfunction]
#[pyo3(name = "mean")]
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
