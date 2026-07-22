//! Typed, engine-neutral DataFrame expressions backed by the versioned plan AST.

use krishiv_plan::PlanError;
pub use krishiv_plan::expression::{
    AggregateFunction, BinaryOperator, EXPRESSION_FORMAT_VERSION, ExprDataType, ExprField,
    IntervalUnit, NullOrdering, ScalarValue, SortDirection, TimeUnit, WindowFrame,
    WindowFrameBound, WindowFrameUnits,
};

/// Public expression wrapper around Krishiv's structured, versioned AST.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Expr {
    node: krishiv_plan::expression::Expr,
    rendered_sql: String,
}

impl Expr {
    pub fn from_node(node: krishiv_plan::expression::Expr) -> Self {
        let rendered_sql = node.to_sql();
        Self { node, rendered_sql }
    }
    pub fn node(&self) -> &krishiv_plan::expression::Expr {
        &self.node
    }
    pub fn into_node(self) -> krishiv_plan::expression::Expr {
        self.node
    }
    /// Explicit preview escape hatch for advanced SQL not represented by the AST.
    pub fn raw(sql: impl Into<String>) -> Self {
        Self::from_node(krishiv_plan::expression::Expr::raw(sql))
    }
    /// SQL rendering used only for compatibility and diagnostics.
    pub fn as_sql(&self) -> &str {
        &self.rendered_sql
    }
    pub fn normalize_json(&self) -> Result<String, PlanError> {
        self.node.normalize_json()
    }
    pub fn encode_versioned(&self) -> Result<Vec<u8>, PlanError> {
        self.node.encode_versioned()
    }
    pub fn decode_versioned(bytes: &[u8]) -> Result<Self, PlanError> {
        krishiv_plan::expression::Expr::decode_versioned(bytes).map(Self::from_node)
    }
    pub fn alias(self, name: &str) -> Self {
        Self::from_node(self.node.alias(name))
    }
    pub fn eq(self, right: Expr) -> Self {
        self.binary(BinaryOperator::Eq, right)
    }
    pub fn not_eq(self, right: Expr) -> Self {
        self.binary(BinaryOperator::NotEq, right)
    }
    pub fn gt(self, right: Expr) -> Self {
        self.binary(BinaryOperator::Gt, right)
    }
    pub fn gt_eq(self, right: Expr) -> Self {
        self.binary(BinaryOperator::GtEq, right)
    }
    pub fn lt(self, right: Expr) -> Self {
        self.binary(BinaryOperator::Lt, right)
    }
    pub fn lt_eq(self, right: Expr) -> Self {
        self.binary(BinaryOperator::LtEq, right)
    }
    pub fn and(self, right: Expr) -> Self {
        self.binary(BinaryOperator::And, right)
    }
    pub fn or(self, right: Expr) -> Self {
        self.binary(BinaryOperator::Or, right)
    }
    pub fn plus(self, right: Expr) -> Self {
        self.binary(BinaryOperator::Plus, right)
    }
    pub fn minus(self, right: Expr) -> Self {
        self.binary(BinaryOperator::Minus, right)
    }
    pub fn multiply(self, right: Expr) -> Self {
        self.binary(BinaryOperator::Multiply, right)
    }
    pub fn divide(self, right: Expr) -> Self {
        self.binary(BinaryOperator::Divide, right)
    }
    pub fn is_null(self) -> Self {
        Self::from_node(krishiv_plan::expression::Expr::IsNull {
            expression: Box::new(self.node),
            negated: false,
        })
    }
    pub fn is_not_null(self) -> Self {
        Self::from_node(krishiv_plan::expression::Expr::IsNull {
            expression: Box::new(self.node),
            negated: true,
        })
    }
    pub fn cast(self, data_type: ExprDataType) -> Self {
        Self::from_node(self.node.cast(data_type))
    }
    pub fn try_cast(self, data_type: ExprDataType) -> Self {
        Self::from_node(self.node.try_cast(data_type))
    }
    pub fn asc(self) -> Self {
        Self::from_node(krishiv_plan::expression::Expr::Sort {
            expression: Box::new(self.node),
            direction: SortDirection::Ascending,
            nulls: NullOrdering::First,
        })
    }
    pub fn desc(self) -> Self {
        Self::from_node(krishiv_plan::expression::Expr::Sort {
            expression: Box::new(self.node),
            direction: SortDirection::Descending,
            nulls: NullOrdering::Last,
        })
    }
    pub fn over(self, partition_by: Vec<Expr>, order_by: Vec<Expr>) -> Self {
        Self::from_node(self.node.over(
            partition_by.into_iter().map(Expr::into_node).collect(),
            order_by.into_iter().map(Expr::into_node).collect(),
        ))
    }
    /// Attach a `ROWS`/`RANGE BETWEEN ... AND ...` frame to a window
    /// expression built by [`Expr::over`]. A no-op if called before `.over(...)`.
    pub fn frame(self, frame: WindowFrame) -> Self {
        Self::from_node(self.node.frame(frame))
    }
    fn binary(self, op: BinaryOperator, right: Expr) -> Self {
        Self::from_node(self.node.binary(op, right.node))
    }

    // ── Predicate & operator combinators ─────────────────────────────────────
    //
    // The plan AST has no dedicated modulo/NOT/LIKE/IN/BETWEEN/CASE nodes, so
    // these are composed as rendered-SQL fragments over the already-escaped
    // child rendering (`as_sql()` quotes identifiers and literals). They plan
    // through the same SQL path as [`Expr::raw`]. Structural constructs only —
    // no PySpark-vs-DataFusion semantic divergence (the exact-or-absent rule).

    /// `(self % right)` — SQL modulo (PySpark `%`).
    pub fn modulo(self, right: Expr) -> Self {
        Self::raw(format!("({} % {})", self.as_sql(), right.as_sql()))
    }
    /// `(- self)` — arithmetic negation (PySpark unary `-`).
    pub fn negate(self) -> Self {
        Self::raw(format!("(- {})", self.as_sql()))
    }
    /// `(NOT self)` — logical negation (PySpark unary `~`).
    pub fn logical_not(self) -> Self {
        Self::raw(format!("(NOT {})", self.as_sql()))
    }
    /// `POWER(self, exponent)` (PySpark `**` / `F.pow`).
    pub fn power(self, exponent: Expr) -> Self {
        function("power", vec![self, exponent])
    }
    /// `(self LIKE pattern)` (PySpark `Column.like`).
    pub fn like(self, pattern: Expr) -> Self {
        Self::raw(format!("({} LIKE {})", self.as_sql(), pattern.as_sql()))
    }
    /// `(self NOT LIKE pattern)`.
    pub fn not_like(self, pattern: Expr) -> Self {
        Self::raw(format!("({} NOT LIKE {})", self.as_sql(), pattern.as_sql()))
    }
    /// `(self ILIKE pattern)` — case-insensitive LIKE (PySpark `Column.ilike`).
    pub fn ilike(self, pattern: Expr) -> Self {
        Self::raw(format!("({} ILIKE {})", self.as_sql(), pattern.as_sql()))
    }
    /// `(self [NOT] IN (a, b, …))` (PySpark `Column.isin`).
    pub fn is_in(self, list: Vec<Expr>, negated: bool) -> Self {
        let items = list
            .iter()
            .map(Expr::as_sql)
            .collect::<Vec<_>>()
            .join(", ");
        Self::raw(format!(
            "({} {}IN ({}))",
            self.as_sql(),
            if negated { "NOT " } else { "" },
            items
        ))
    }
    /// `(self BETWEEN low AND high)` — inclusive (PySpark `Column.between`).
    pub fn between(self, low: Expr, high: Expr) -> Self {
        Self::raw(format!(
            "({} BETWEEN {} AND {})",
            self.as_sql(),
            low.as_sql(),
            high.as_sql()
        ))
    }
    /// `(self IS NOT DISTINCT FROM right)` — null-safe equality (PySpark `eqNullSafe`).
    pub fn eq_null_safe(self, right: Expr) -> Self {
        Self::raw(format!(
            "({} IS NOT DISTINCT FROM {})",
            self.as_sql(),
            right.as_sql()
        ))
    }
    /// `STARTS_WITH(self, prefix)` (PySpark `Column.startswith`).
    pub fn starts_with(self, prefix: Expr) -> Self {
        function("starts_with", vec![self, prefix])
    }
    /// `ENDS_WITH(self, suffix)` (PySpark `Column.endswith`).
    pub fn ends_with(self, suffix: Expr) -> Self {
        function("ends_with", vec![self, suffix])
    }
    /// `(STRPOS(self, substring) > 0)` — substring containment (PySpark `Column.contains`).
    pub fn contains(self, substring: Expr) -> Self {
        function("strpos", vec![self, substring]).gt(lit(0_i64))
    }
    /// `SUBSTR(self, pos, len)` — 1-based substring (PySpark `Column.substr`).
    pub fn substr(self, pos: Expr, len: Expr) -> Self {
        function("substr", vec![self, pos, len])
    }
    /// Append a `WHEN condition THEN value` branch to a `CASE` built by
    /// [`when`]. Only meaningful on a CASE expression (one rendered ending in
    /// `END`); otherwise the resulting SQL fails to plan (loud, not silent).
    pub fn when(self, condition: Expr, value: Expr) -> Self {
        let body = case_body(self.as_sql());
        Self::raw(format!(
            "{body} WHEN ({}) THEN ({}) END",
            condition.as_sql(),
            value.as_sql()
        ))
    }
    /// Attach the `ELSE value` default to a `CASE` built by [`when`].
    pub fn otherwise(self, value: Expr) -> Self {
        let body = case_body(self.as_sql());
        Self::raw(format!("{body} ELSE ({}) END", value.as_sql()))
    }
}

/// `CASE WHEN condition THEN value END` (PySpark `F.when`). Chain further
/// branches with [`Expr::when`] and a default with [`Expr::otherwise`]; on its
/// own it is a valid expression with an implicit `ELSE NULL`.
pub fn when(condition: Expr, value: Expr) -> Expr {
    Expr::raw(format!(
        "CASE WHEN ({}) THEN ({}) END",
        condition.as_sql(),
        value.as_sql()
    ))
}

/// Strip the trailing `END` from a rendered `CASE` so another branch or the
/// `ELSE` default can be appended. If the input is not a CASE (no `END`), it
/// is returned unchanged so the caller produces a clearly-invalid expression
/// rather than silently succeeding.
fn case_body(sql: &str) -> String {
    let trimmed = sql.trim_end();
    match trimmed.strip_suffix("END") {
        Some(body) => body.trim_end().to_string(),
        None => trimmed.to_string(),
    }
}

pub fn col(name: &str) -> Expr {
    Expr::from_node(krishiv_plan::expression::Expr::column(name))
}
pub fn lit(value: impl Into<Literal>) -> Expr {
    Expr::from_node(krishiv_plan::expression::Expr::literal(
        value.into().into_scalar(),
    ))
}
pub fn count(expr: Expr) -> Expr {
    aggregate(AggregateFunction::Count, Some(expr))
}
pub fn count_all() -> Expr {
    aggregate(AggregateFunction::Count, None)
}
pub fn sum(expr: Expr) -> Expr {
    aggregate(AggregateFunction::Sum, Some(expr))
}
pub fn avg(expr: Expr) -> Expr {
    aggregate(AggregateFunction::Avg, Some(expr))
}
pub fn min(expr: Expr) -> Expr {
    aggregate(AggregateFunction::Min, Some(expr))
}
pub fn max(expr: Expr) -> Expr {
    aggregate(AggregateFunction::Max, Some(expr))
}
pub fn function(name: impl Into<String>, arguments: Vec<Expr>) -> Expr {
    Expr::from_node(krishiv_plan::expression::Expr::function(
        name,
        arguments.into_iter().map(Expr::into_node).collect(),
    ))
}

// ── Scalar functions (F.*) ────────────────────────────────────────────────────
//
// Typed helpers over the shared SQL function registry (`function(name, args)` —
// one registry, all surfaces; Phase 61). Only functions whose SQL semantics
// match PySpark **exactly** are given typed helpers here (the Phase 60
// exact-or-absent rule): e.g. `concat` (Spark returns NULL if any arg is NULL,
// DataFusion skips nulls) and `round` (half-up vs half-even) are deliberately
// left to the generic `function(...)` bridge rather than aliased inexactly.

/// `COALESCE(a, b, …)` — the first non-null argument (PySpark `F.coalesce`).
pub fn coalesce(arguments: Vec<Expr>) -> Expr {
    function("coalesce", arguments)
}
/// `NVL(expr, default)` — `default` when `expr` is null (Spark alias, exact).
pub fn nvl(expr: Expr, default: Expr) -> Expr {
    function("nvl", vec![expr, default])
}
/// `UPPER(expr)` (PySpark `F.upper`).
pub fn upper(expr: Expr) -> Expr {
    function("upper", vec![expr])
}
/// `LOWER(expr)` (PySpark `F.lower`).
pub fn lower(expr: Expr) -> Expr {
    function("lower", vec![expr])
}
/// `LENGTH(expr)` — character length (PySpark `F.length`).
pub fn length(expr: Expr) -> Expr {
    function("length", vec![expr])
}
/// `TRIM(expr)` — strip leading and trailing spaces (PySpark `F.trim`).
pub fn trim(expr: Expr) -> Expr {
    function("trim", vec![expr])
}
/// `ABS(expr)` (PySpark `F.abs`).
pub fn abs(expr: Expr) -> Expr {
    function("abs", vec![expr])
}
/// `LTRIM(expr)` — strip leading spaces (PySpark `F.ltrim`).
pub fn ltrim(expr: Expr) -> Expr {
    function("ltrim", vec![expr])
}
/// `RTRIM(expr)` — strip trailing spaces (PySpark `F.rtrim`).
pub fn rtrim(expr: Expr) -> Expr {
    function("rtrim", vec![expr])
}
/// `CEIL(expr)` — round up to the nearest integer (PySpark `F.ceil`).
pub fn ceil(expr: Expr) -> Expr {
    function("ceil", vec![expr])
}
/// `FLOOR(expr)` — round down to the nearest integer (PySpark `F.floor`).
pub fn floor(expr: Expr) -> Expr {
    function("floor", vec![expr])
}
/// `SQRT(expr)` — square root (PySpark `F.sqrt`).
pub fn sqrt(expr: Expr) -> Expr {
    function("sqrt", vec![expr])
}
/// `SUBSTR(expr, pos, len)` — 1-indexed substring (PySpark `F.substring`).
pub fn substring(expr: Expr, pos: i64, len: i64) -> Expr {
    function("substr", vec![expr, lit(pos), lit(len)])
}

// ── Window functions ──────────────────────────────────────────────────────────
//
// Typed sugar over `function(name, args)`, meant to be chained with `.over(...)`
// (and optionally `.frame(...)`), e.g. `rank().over(vec![col("dept")], vec![col("salary").desc()])`.
// These render through the same SQL text path as any other `Expr`, so they
// work anywhere `over`/`frame` already do.

pub fn row_number() -> Expr {
    function("row_number", vec![])
}
pub fn rank() -> Expr {
    function("rank", vec![])
}
pub fn dense_rank() -> Expr {
    function("dense_rank", vec![])
}
pub fn percent_rank() -> Expr {
    function("percent_rank", vec![])
}
pub fn cume_dist() -> Expr {
    function("cume_dist", vec![])
}
pub fn ntile(n: i64) -> Expr {
    function("ntile", vec![lit(n)])
}
/// `LAG(expr, offset)`, or `LAG(expr, offset, default)` when `default` is given.
pub fn lag(expr: Expr, offset: i64, default: Option<Expr>) -> Expr {
    let mut args = vec![expr, lit(offset)];
    args.extend(default);
    function("lag", args)
}
/// `LEAD(expr, offset)`, or `LEAD(expr, offset, default)` when `default` is given.
pub fn lead(expr: Expr, offset: i64, default: Option<Expr>) -> Expr {
    let mut args = vec![expr, lit(offset)];
    args.extend(default);
    function("lead", args)
}
pub fn first_value(expr: Expr) -> Expr {
    function("first_value", vec![expr])
}
pub fn last_value(expr: Expr) -> Expr {
    function("last_value", vec![expr])
}
pub fn nth_value(expr: Expr, n: i64) -> Expr {
    function("nth_value", vec![expr, lit(n)])
}
fn aggregate(function: AggregateFunction, expression: Option<Expr>) -> Expr {
    Expr::from_node(krishiv_plan::expression::Expr::Aggregate {
        function,
        expression: expression.map(|value| Box::new(value.node)),
        distinct: false,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
}
impl Literal {
    fn into_scalar(self) -> ScalarValue {
        match self {
            Self::Null => ScalarValue::Null,
            Self::Boolean(v) => ScalarValue::Boolean(v),
            Self::Int64(v) => ScalarValue::Int64(v),
            Self::UInt64(v) => ScalarValue::UInt64(v),
            Self::Float64(v) => ScalarValue::float64(v),
            Self::String(v) => ScalarValue::Utf8(v),
            Self::Binary(v) => ScalarValue::Binary(v),
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
impl From<Vec<u8>> for Literal {
    fn from(value: Vec<u8>) -> Self {
        Self::Binary(value)
    }
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
    #[test]
    fn versioned_ast_round_trip() {
        let expr = sum(col("amount")).alias("total");
        let bytes = expr.encode_versioned().unwrap();
        assert_eq!(Expr::decode_versioned(&bytes).unwrap(), expr);
        assert!(expr.normalize_json().unwrap().contains("aggregate"));
    }
    #[test]
    fn nested_types_are_structured() {
        let ty = ExprDataType::List(Box::new(ExprDataType::Struct(vec![ExprField {
            name: "value".into(),
            data_type: ExprDataType::Decimal128 {
                precision: 12,
                scale: 2,
            },
            nullable: true,
        }])));
        assert_eq!(
            col("items").cast(ty).node().to_sql(),
            "CAST(\"items\" AS ARRAY<STRUCT<\"value\": DECIMAL(12, 2)>>)"
        );
    }
}
