//! Typed, engine-neutral DataFrame expressions.

/// A validated SQL expression used by the public DataFrame API.
///
/// Krishiv keeps DataFusion types out of the public API. Expressions therefore
/// carry a SQL representation that is parsed by the SQL crate at the lazy
/// transformation boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Expr {
    sql: String,
}

impl Expr {
    /// Construct an advanced expression from SQL text.
    pub fn raw(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }

    /// Return the SQL representation consumed by the engine.
    pub fn as_sql(&self) -> &str {
        &self.sql
    }

    /// Assign an output name to this expression.
    pub fn alias(self, name: &str) -> Self {
        Self::raw(format!("{} AS {}", self.sql, quote_identifier(name)))
    }

    pub fn eq(self, right: Expr) -> Self {
        self.binary("=", right)
    }

    pub fn not_eq(self, right: Expr) -> Self {
        self.binary("<>", right)
    }

    pub fn gt(self, right: Expr) -> Self {
        self.binary(">", right)
    }

    pub fn gt_eq(self, right: Expr) -> Self {
        self.binary(">=", right)
    }

    pub fn lt(self, right: Expr) -> Self {
        self.binary("<", right)
    }

    pub fn lt_eq(self, right: Expr) -> Self {
        self.binary("<=", right)
    }

    pub fn and(self, right: Expr) -> Self {
        self.binary("AND", right)
    }

    pub fn or(self, right: Expr) -> Self {
        self.binary("OR", right)
    }

    pub fn plus(self, right: Expr) -> Self {
        self.binary("+", right)
    }

    pub fn minus(self, right: Expr) -> Self {
        self.binary("-", right)
    }

    pub fn multiply(self, right: Expr) -> Self {
        self.binary("*", right)
    }

    pub fn divide(self, right: Expr) -> Self {
        self.binary("/", right)
    }

    pub fn is_null(self) -> Self {
        Self::raw(format!("({} IS NULL)", self.sql))
    }

    pub fn is_not_null(self) -> Self {
        Self::raw(format!("({} IS NOT NULL)", self.sql))
    }

    pub fn asc(self) -> Self {
        Self::raw(format!("{} ASC", self.sql))
    }

    pub fn desc(self) -> Self {
        Self::raw(format!("{} DESC", self.sql))
    }

    fn binary(self, op: &str, right: Expr) -> Self {
        Self::raw(format!("({} {op} {})", self.sql, right.sql))
    }
}

/// Reference a column using a safely quoted identifier.
pub fn col(name: &str) -> Expr {
    Expr::raw(quote_identifier(name))
}

/// Construct a typed SQL literal.
pub fn lit(value: impl Into<Literal>) -> Expr {
    Expr::raw(value.into().to_sql())
}

pub fn count(expr: Expr) -> Expr {
    Expr::raw(format!("COUNT({})", expr.sql))
}

pub fn count_all() -> Expr {
    Expr::raw("COUNT(*)")
}

pub fn sum(expr: Expr) -> Expr {
    Expr::raw(format!("SUM({})", expr.sql))
}

pub fn avg(expr: Expr) -> Expr {
    Expr::raw(format!("AVG({})", expr.sql))
}

pub fn min(expr: Expr) -> Expr {
    Expr::raw(format!("MIN({})", expr.sql))
}

pub fn max(expr: Expr) -> Expr {
    Expr::raw(format!("MAX({})", expr.sql))
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(String),
}

impl Literal {
    fn to_sql(&self) -> String {
        match self {
            Self::Null => "NULL".into(),
            Self::Boolean(value) => value.to_string().to_ascii_uppercase(),
            Self::Int64(value) => value.to_string(),
            Self::UInt64(value) => value.to_string(),
            Self::Float64(value) => value.to_string(),
            Self::String(value) => format!("'{}'", value.replace('\'', "''")),
        }
    }
}

impl From<&str> for Literal {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}
impl From<String> for Literal {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}
impl From<bool> for Literal {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}
impl From<i32> for Literal {
    fn from(value: i32) -> Self {
        Self::Int64(i64::from(value))
    }
}
impl From<i64> for Literal {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}
impl From<u32> for Literal {
    fn from(value: u32) -> Self {
        Self::UInt64(u64::from(value))
    }
}
impl From<u64> for Literal {
    fn from(value: u64) -> Self {
        Self::UInt64(value)
    }
}
impl From<f64> for Literal {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

fn quote_identifier(name: &str) -> String {
    name.split('.')
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_and_literals_are_escaped() {
        assert_eq!(col("orders.user\"id").as_sql(), "\"orders\".\"user\"\"id\"");
        assert_eq!(lit("O'Reilly").as_sql(), "'O''Reilly'");
    }

    #[test]
    fn typed_expression_builds_parenthesized_predicate() {
        let expr = col("amount").gt(lit(10)).and(col("active").eq(lit(true)));
        assert_eq!(expr.as_sql(), "((\"amount\" > 10) AND (\"active\" = TRUE))");
    }
}
