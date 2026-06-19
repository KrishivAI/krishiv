//! Python `Column` facade over the engine-owned expression AST.

use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

/// A lazy, typed expression. Construct columns with [`col`] and literals with [`lit`].
#[derive(Clone)]
#[pyclass(name = "Column", from_py_object)]
pub struct PyColumn {
    pub(crate) inner: krishiv_api::Expr,
}

impl PyColumn {
    pub(crate) fn new(inner: krishiv_api::Expr) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyColumn {
    pub fn alias(&self, name: String) -> Self {
        Self::new(self.inner.clone().alias(&name))
    }

    pub fn is_null(&self) -> Self {
        Self::new(self.inner.clone().is_null())
    }

    pub fn is_not_null(&self) -> Self {
        Self::new(self.inner.clone().is_not_null())
    }

    pub fn cast(&self, data_type: &str) -> PyResult<Self> {
        Ok(Self::new(
            self.inner.clone().cast(parse_data_type(data_type)?),
        ))
    }

    pub fn try_cast(&self, data_type: &str) -> PyResult<Self> {
        Ok(Self::new(
            self.inner.clone().try_cast(parse_data_type(data_type)?),
        ))
    }

    #[pyo3(signature = (partition_by=Vec::new(), order_by=Vec::new()))]
    pub fn over(&self, partition_by: Vec<PyColumn>, order_by: Vec<PyColumn>) -> Self {
        Self::new(
            self.inner.clone().over(
                partition_by
                    .into_iter()
                    .map(|column| column.inner)
                    .collect(),
                order_by.into_iter().map(|column| column.inner).collect(),
            ),
        )
    }

    pub fn asc(&self) -> Self {
        Self::new(self.inner.clone().asc())
    }

    pub fn desc(&self) -> Self {
        Self::new(self.inner.clone().desc())
    }

    pub fn normalized_ast(&self) -> PyResult<String> {
        self.inner
            .normalize_json()
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    pub fn sql(&self) -> String {
        self.inner.as_sql().to_owned()
    }

    fn __richcmp__(&self, other: &Bound<'_, PyAny>, op: CompareOp) -> PyResult<Self> {
        let left = self.inner.clone();
        let right = expression_from_python(other)?;
        let expression = match op {
            CompareOp::Eq => left.eq(right),
            CompareOp::Ne => left.not_eq(right),
            CompareOp::Lt => left.lt(right),
            CompareOp::Le => left.lt_eq(right),
            CompareOp::Gt => left.gt(right),
            CompareOp::Ge => left.gt_eq(right),
        };
        Ok(Self::new(expression))
    }

    fn __add__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(
            self.inner.clone().plus(expression_from_python(other)?),
        ))
    }

    fn __sub__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(
            self.inner.clone().minus(expression_from_python(other)?),
        ))
    }

    fn __mul__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(
            self.inner.clone().multiply(expression_from_python(other)?),
        ))
    }

    fn __truediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(
            self.inner.clone().divide(expression_from_python(other)?),
        ))
    }

    fn __and__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(
            self.inner.clone().and(expression_from_python(other)?),
        ))
    }

    fn __or__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(
            self.inner.clone().or(expression_from_python(other)?),
        ))
    }

    fn __bool__(&self) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "a Column is a lazy expression; use '&'/'|' instead of Python 'and'/'or'",
        ))
    }

    fn __repr__(&self) -> String {
        format!("Column({})", self.inner.as_sql())
    }
}

#[pyfunction]
pub fn col(name: &str) -> PyColumn {
    PyColumn::new(krishiv_api::col(name))
}

/// Explicit preview escape hatch for an expression written as SQL.
#[pyfunction]
pub fn expr(sql: String) -> PyColumn {
    PyColumn::new(krishiv_api::Expr::raw(sql))
}

#[pyfunction]
pub fn lit(value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    Ok(PyColumn::new(expression_from_python(value)?))
}

fn expression_from_python(value: &Bound<'_, PyAny>) -> PyResult<krishiv_api::Expr> {
    if let Ok(column) = value.extract::<PyColumn>() {
        return Ok(column.inner);
    }
    if value.is_none() {
        return Ok(krishiv_api::lit(krishiv_api::Literal::Null));
    }
    if let Ok(value) = value.extract::<bool>() {
        return Ok(krishiv_api::lit(value));
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(krishiv_api::lit(value));
    }
    if let Ok(value) = value.extract::<u64>() {
        return Ok(krishiv_api::lit(value));
    }
    if let Ok(value) = value.extract::<f64>() {
        return Ok(krishiv_api::lit(value));
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(krishiv_api::lit(value));
    }
    if let Ok(value) = value.extract::<Vec<u8>>() {
        return Ok(krishiv_api::lit(value));
    }
    Err(PyTypeError::new_err(
        "expected a Column or a literal None/bool/int/float/str/bytes value",
    ))
}

macro_rules! aggregate_function {
    ($rust_name:ident, $api_name:ident) => {
        #[pyfunction]
        pub fn $rust_name(column: PyColumn) -> PyColumn {
            PyColumn::new(krishiv_api::$api_name(column.inner))
        }
    };
}

aggregate_function!(count, count);
aggregate_function!(sum, sum);
aggregate_function!(avg, avg);
aggregate_function!(min, min);
aggregate_function!(max, max);

#[pyfunction]
pub fn count_all() -> PyColumn {
    PyColumn::new(krishiv_api::count_all())
}

#[pyfunction]
pub fn call_function(name: String, arguments: Vec<PyColumn>) -> PyColumn {
    PyColumn::new(krishiv_api::function(
        name,
        arguments.into_iter().map(|column| column.inner).collect(),
    ))
}

fn parse_data_type(value: &str) -> PyResult<krishiv_api::ExprDataType> {
    use krishiv_api::ExprDataType;
    match value.trim().to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Ok(ExprDataType::Boolean),
        "int" | "int64" | "bigint" => Ok(ExprDataType::Int64),
        "uint64" => Ok(ExprDataType::UInt64),
        "float" | "float64" | "double" => Ok(ExprDataType::Float64),
        "str" | "string" | "utf8" => Ok(ExprDataType::Utf8),
        "binary" | "bytes" => Ok(ExprDataType::Binary),
        "date" | "date32" => Ok(ExprDataType::Date32),
        other => Err(PyValueError::new_err(format!(
            "unsupported type '{other}'; use a stable primitive type name"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_column_uses_the_same_normalized_ast_as_rust() {
        let python = PyColumn::new(col("amount").inner.gt(lit_for_test(10).inner));
        let rust = krishiv_api::col("amount").gt(krishiv_api::lit(10));
        assert_eq!(
            python.inner.normalize_json().unwrap(),
            rust.normalize_json().unwrap()
        );
    }

    fn lit_for_test(value: i64) -> PyColumn {
        PyColumn::new(krishiv_api::lit(value))
    }
}
