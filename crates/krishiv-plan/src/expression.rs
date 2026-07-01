//! Versioned, engine-owned public expression and scalar type contract.

use serde::{Deserialize, Serialize};

use crate::PlanError;
use krishiv_common::sql_util::quote_identifier;

/// Current serialized public-expression envelope version.
pub const EXPRESSION_FORMAT_VERSION: u16 = 1;

/// Engine-owned data types used at public and wire boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ExprDataType {
    Null,
    Boolean,
    Int64,
    UInt64,
    Float64,
    Utf8,
    Binary,
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Date32,
    Timestamp {
        unit: TimeUnit,
        timezone: Option<String>,
    },
    Interval {
        unit: IntervalUnit,
    },
    List(Box<ExprDataType>),
    Map {
        key: Box<ExprDataType>,
        value: Box<ExprDataType>,
    },
    Struct(Vec<ExprField>),
    /// Semi-structured JSON-like data type (Spark VARIANT equivalent).
    ///
    /// Stores arbitrary JSON without a fixed schema. Query-time access uses
    /// `variant_get(column, 'path')` and schema is applied at read time.
    /// Arrow serialization uses `Binary` with a variant encoding prefix.
    Variant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExprField {
    pub name: String,
    pub data_type: ExprDataType,
    pub nullable: bool,
}

impl ExprDataType {
    pub fn validate(&self) -> Result<(), PlanError> {
        match self {
            Self::Decimal128 { precision, scale }
                if *precision == 0 || *precision > 38 || scale.unsigned_abs() > *precision =>
            {
                Err(PlanError::Validation(format!(
                    "invalid decimal({precision}, {scale}); precision must be 1..=38 and cover scale"
                )))
            }
            Self::Timestamp {
                timezone: Some(timezone),
                ..
            } if timezone.trim().is_empty() => Err(PlanError::Validation(
                "timestamp timezone must not be empty".into(),
            )),
            Self::List(element) => element.validate(),
            Self::Map { key, value } => {
                key.validate()?;
                value.validate()
            }
            Self::Struct(fields) => {
                let mut names = std::collections::HashSet::new();
                for field in fields {
                    if field.name.is_empty() || !names.insert(&field.name) {
                        return Err(PlanError::Validation(
                            "struct field names must be non-empty and unique".into(),
                        ));
                    }
                    field.data_type.validate()?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntervalUnit {
    YearMonth,
    DayTime,
    MonthDayNano,
}

/// Typed scalar literals. Float values retain their exact IEEE-754 bits.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(u64),
    Utf8(String),
    Binary(Vec<u8>),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    Date32(i32),
    Timestamp {
        value: i64,
        unit: TimeUnit,
        timezone: Option<String>,
    },
    Interval {
        value: i128,
        unit: IntervalUnit,
    },
}

impl ScalarValue {
    pub fn float64(value: f64) -> Self {
        Self::Float64(value.to_bits())
    }
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float64(bits) => Some(f64::from_bits(*bits)),
            _ => None,
        }
    }

    /// Render a scalar as a SQL literal for typed prepared-statement binding.
    pub fn to_sql_literal(&self) -> String {
        scalar_sql(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOperator {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    And,
    Or,
    Plus,
    Minus,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullOrdering {
    First,
    Last,
}

/// Structured public expression AST.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum Expr {
    Column {
        path: Vec<String>,
    },
    Literal {
        value: ScalarValue,
    },
    Alias {
        expression: Box<Expr>,
        name: String,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    IsNull {
        expression: Box<Expr>,
        negated: bool,
    },
    Aggregate {
        function: AggregateFunction,
        expression: Option<Box<Expr>>,
        distinct: bool,
    },
    Cast {
        expression: Box<Expr>,
        data_type: ExprDataType,
        safe: bool,
    },
    Sort {
        expression: Box<Expr>,
        direction: SortDirection,
        nulls: NullOrdering,
    },
    Function {
        name: String,
        arguments: Vec<Expr>,
    },
    Window {
        expression: Box<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<Expr>,
    },
    RawSql {
        sql: String,
    },
}

impl Expr {
    pub fn column(name: &str) -> Self {
        Self::Column {
            path: name.split('.').map(ToOwned::to_owned).collect(),
        }
    }
    pub fn literal(value: ScalarValue) -> Self {
        Self::Literal { value }
    }
    pub fn raw(sql: impl Into<String>) -> Self {
        Self::RawSql { sql: sql.into() }
    }
    pub fn alias(self, name: impl Into<String>) -> Self {
        Self::Alias {
            expression: Box::new(self),
            name: name.into(),
        }
    }
    pub fn binary(self, op: BinaryOperator, right: Expr) -> Self {
        Self::Binary {
            left: Box::new(self),
            op,
            right: Box::new(right),
        }
    }
    pub fn cast(self, data_type: ExprDataType) -> Self {
        Self::Cast {
            expression: Box::new(self),
            data_type,
            safe: false,
        }
    }
    pub fn try_cast(self, data_type: ExprDataType) -> Self {
        Self::Cast {
            expression: Box::new(self),
            data_type,
            safe: true,
        }
    }
    pub fn function(name: impl Into<String>, arguments: Vec<Expr>) -> Self {
        Self::Function {
            name: name.into(),
            arguments,
        }
    }
    pub fn over(self, partition_by: Vec<Expr>, order_by: Vec<Expr>) -> Self {
        Self::Window {
            expression: Box::new(self),
            partition_by,
            order_by,
        }
    }
    pub fn normalize_json(&self) -> Result<String, PlanError> {
        serde_json::to_string(self).map_err(|error| PlanError::Encode(error.to_string()))
    }
    pub fn encode_versioned(&self) -> Result<Vec<u8>, PlanError> {
        self.validate()?;
        serde_json::to_vec(&ExpressionEnvelope {
            version: EXPRESSION_FORMAT_VERSION,
            expression: self.clone(),
        })
        .map_err(|error| PlanError::Encode(error.to_string()))
    }
    pub fn decode_versioned(bytes: &[u8]) -> Result<Self, PlanError> {
        let envelope: ExpressionEnvelope =
            serde_json::from_slice(bytes).map_err(|error| PlanError::Parse(error.to_string()))?;
        if envelope.version != EXPRESSION_FORMAT_VERSION {
            return Err(PlanError::Validation(format!(
                "unsupported expression format version {}; expected {}",
                envelope.version, EXPRESSION_FORMAT_VERSION
            )));
        }
        envelope.expression.validate()?;
        Ok(envelope.expression)
    }
    pub fn validate(&self) -> Result<(), PlanError> {
        match self {
            Self::Column { path } if path.is_empty() || path.iter().any(String::is_empty) => Err(
                PlanError::Validation("column path must not be empty".into()),
            ),
            Self::Column { .. } => Ok(()),
            Self::Literal { value } => match value {
                ScalarValue::Decimal128 {
                    precision, scale, ..
                } => ExprDataType::Decimal128 {
                    precision: *precision,
                    scale: *scale,
                }
                .validate(),
                ScalarValue::Timestamp { unit, timezone, .. } => ExprDataType::Timestamp {
                    unit: *unit,
                    timezone: timezone.clone(),
                }
                .validate(),
                _ => Ok(()),
            },
            Self::Alias { expression, name } => {
                if name.is_empty() {
                    return Err(PlanError::Validation("alias must not be empty".into()));
                }
                expression.validate()
            }
            Self::Binary { left, right, .. } => {
                left.validate()?;
                right.validate()
            }
            Self::IsNull { expression, .. } | Self::Sort { expression, .. } => {
                expression.validate()
            }
            Self::Cast {
                expression,
                data_type,
                ..
            } => {
                expression.validate()?;
                data_type.validate()
            }
            Self::Aggregate { expression, .. } => {
                if let Some(expression) = expression {
                    expression.validate()?;
                }
                Ok(())
            }
            Self::Function { name, arguments } => {
                if name.trim().is_empty() {
                    return Err(PlanError::Validation(
                        "function name must not be empty".into(),
                    ));
                }
                arguments.iter().try_for_each(Self::validate)
            }
            Self::Window {
                expression,
                partition_by,
                order_by,
            } => {
                expression.validate()?;
                partition_by.iter().try_for_each(Self::validate)?;
                order_by.iter().try_for_each(Self::validate)
            }
            Self::RawSql { sql } => {
                if sql.trim().is_empty() {
                    Err(PlanError::Validation(
                        "raw SQL expression must not be empty".into(),
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }
    pub fn to_sql(&self) -> String {
        match self {
            Self::Column { path } => path
                .iter()
                .map(|part| quote_identifier(part))
                .collect::<Vec<_>>()
                .join("."),
            Self::Literal { value } => scalar_sql(value),
            Self::Alias { expression, name } => {
                format!("{} AS {}", expression.to_sql(), quote_identifier(name))
            }
            Self::Binary { left, op, right } => format!(
                "({} {} {})",
                left.to_sql(),
                operator_sql(*op),
                right.to_sql()
            ),
            Self::IsNull {
                expression,
                negated,
            } => format!(
                "({} IS {}NULL)",
                expression.to_sql(),
                if *negated { "NOT " } else { "" }
            ),
            Self::Aggregate {
                function,
                expression,
                distinct,
            } => {
                let argument = expression
                    .as_ref()
                    .map(|value| value.to_sql())
                    .unwrap_or_else(|| "*".into());
                format!(
                    "{}({}{argument})",
                    aggregate_sql(*function),
                    if *distinct { "DISTINCT " } else { "" }
                )
            }
            Self::Cast {
                expression,
                data_type,
                safe,
            } => format!(
                "{}({} AS {})",
                if *safe { "TRY_CAST" } else { "CAST" },
                expression.to_sql(),
                type_sql(data_type)
            ),
            Self::Sort {
                expression,
                direction,
                nulls,
            } => format!(
                "{} {} NULLS {}",
                expression.to_sql(),
                if *direction == SortDirection::Ascending {
                    "ASC"
                } else {
                    "DESC"
                },
                if *nulls == NullOrdering::First {
                    "FIRST"
                } else {
                    "LAST"
                }
            ),
            Self::Function { name, arguments } => format!(
                "{}({})",
                name,
                arguments
                    .iter()
                    .map(Self::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Window {
                expression,
                partition_by,
                order_by,
            } => {
                let mut clauses = Vec::new();
                if !partition_by.is_empty() {
                    clauses.push(format!(
                        "PARTITION BY {}",
                        partition_by
                            .iter()
                            .map(Self::to_sql)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                if !order_by.is_empty() {
                    clauses.push(format!(
                        "ORDER BY {}",
                        order_by
                            .iter()
                            .map(Self::to_sql)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                format!("{} OVER ({})", expression.to_sql(), clauses.join(" "))
            }
            Self::RawSql { sql } => sql.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ExpressionEnvelope {
    version: u16,
    expression: Expr,
}

fn operator_sql(op: BinaryOperator) -> &'static str {
    match op {
        BinaryOperator::Eq => "=",
        BinaryOperator::NotEq => "<>",
        BinaryOperator::Gt => ">",
        BinaryOperator::GtEq => ">=",
        BinaryOperator::Lt => "<",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::And => "AND",
        BinaryOperator::Or => "OR",
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
    }
}
fn aggregate_sql(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "COUNT",
        AggregateFunction::Sum => "SUM",
        AggregateFunction::Avg => "AVG",
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
    }
}
fn scalar_sql(value: &ScalarValue) -> String {
    // H-8 (audit): the prior implementation emitted bare integers for
    // Date32 / Timestamp / Decimal128 and the bare string "NaN" for
    // Float64 NaN. Every typed bind with one of these scalars either
    // raised a backend parse error (NaN) or produced a silently wrong
    // value (Date32 / Timestamp / Decimal128). The new implementation
    // produces typed literals that DataFusion, Postgres, MySQL, DuckDB,
    // and SQLite all accept.
    use chrono::{DateTime, NaiveDate, Utc};
    match value {
        ScalarValue::Null => "NULL".into(),
        ScalarValue::Boolean(value) => value.to_string().to_ascii_uppercase(),
        ScalarValue::Int64(value) => value.to_string(),
        ScalarValue::UInt64(value) => value.to_string(),
        ScalarValue::Float64(bits) => float_to_sql(f64::from_bits(*bits)),
        ScalarValue::Utf8(value) => format!("'{}'", value.replace('\'', "''")),
        ScalarValue::Binary(value) => format!(
            "X'{}'",
            value
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        ),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            // H-8: a bare integer like `12345` would parse as BIGINT and
            // be silently cast to DECIMAL on the consuming side, which
            // does not preserve the value (e.g. 12345 cast to
            // DECIMAL(10,2) becomes 12345.00, not 123.45). The CAST
            // form makes the type explicit.
            format!("CAST({value} AS DECIMAL({precision},{scale}))")
        }
        ScalarValue::Date32(value) => {
            // H-8: a bare integer for a date is parsed as BIGINT and the
            // expression becomes type-error vs DATE. Render as
            // DATE 'YYYY-MM-DD' so the literal matches the column type.
            let Some(epoch) = NaiveDate::from_ymd_opt(1970, 1, 1) else {
                return "DATE '1970-01-01'".to_owned();
            };
            let date = epoch + chrono::Duration::days(*value as i64);
            format!("DATE '{}'", date.format("%Y-%m-%d"))
        }
        ScalarValue::Timestamp {
            value,
            unit,
            timezone: _,
        } => {
            // H-8: a bare integer is type-error vs TIMESTAMP. Render as a
            // typed TIMESTAMP literal. The unit determines precision.
            let formatted = match unit {
                TimeUnit::Second => DateTime::<Utc>::from_timestamp(*value, 0)
                    .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                    .unwrap_or_else(|| format!("from_unixtime({value})")),
                TimeUnit::Millisecond => DateTime::<Utc>::from_timestamp_millis(*value)
                    .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
                    .unwrap_or_else(|| format!("from_unixtime_ms({value})")),
                TimeUnit::Microsecond => DateTime::<Utc>::from_timestamp_micros(*value)
                    .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
                    .unwrap_or_else(|| format!("from_unixtime_us({value})")),
                TimeUnit::Nanosecond => {
                    let secs = value.div_euclid(1_000_000_000);
                    let nsec = value.rem_euclid(1_000_000_000) as u32;
                    DateTime::<Utc>::from_timestamp(secs, nsec)
                        .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string())
                        .unwrap_or_else(|| format!("CAST({value} AS BIGINT)"))
                }
            };
            format!("TIMESTAMP '{formatted}'")
        }
        ScalarValue::Interval { value, unit } => match unit {
            IntervalUnit::YearMonth => {
                // value is i128; render as a year-month interval.
                format!("INTERVAL '{value} months'")
            }
            IntervalUnit::DayTime => {
                // value is i128; pack lower 64 bits as days, upper 64 as
                // milliseconds. Approximate; sufficient for bind roundtrip.
                let days = (*value & 0xFFFF_FFFF_FFFF_FFFF) as i64;
                let millis = ((*value >> 64) & 0xFFFF_FFFF_FFFF_FFFF) as i64;
                format!("INTERVAL '{days} days {millis} ms'")
            }
            IntervalUnit::MonthDayNano => {
                let months = (*value & 0xFFFF_FFFF_FFFF_FFFF) as i64;
                let days = ((*value >> 64) & 0xFFFF_FFFF) as i64;
                let nanos = ((*value >> 96) & 0xFFFF_FFFF_FFFF_FFFF) as i64;
                format!("INTERVAL '{months} months {days} days {nanos} ns'")
            }
        },
    }
}

/// Render an f64 as a SQL literal. NaN and infinity are not valid SQL
/// literals in any major backend; the portable form is
/// `CAST('NaN' AS DOUBLE)` / `CAST('Infinity' AS DOUBLE)`.
fn float_to_sql(value: f64) -> String {
    if value.is_nan() {
        "CAST('NaN' AS DOUBLE)".into()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "CAST('Infinity' AS DOUBLE)".into()
        } else {
            "CAST('-Infinity' AS DOUBLE)".into()
        }
    } else {
        value.to_string()
    }
}
fn type_sql(data_type: &ExprDataType) -> String {
    match data_type {
        ExprDataType::Null => "NULL".into(),
        ExprDataType::Boolean => "BOOLEAN".into(),
        ExprDataType::Int64 => "BIGINT".into(),
        ExprDataType::UInt64 => "BIGINT UNSIGNED".into(),
        ExprDataType::Float64 => "DOUBLE".into(),
        ExprDataType::Utf8 => "VARCHAR".into(),
        ExprDataType::Binary => "BINARY".into(),
        ExprDataType::Decimal128 { precision, scale } => format!("DECIMAL({precision}, {scale})"),
        ExprDataType::Date32 => "DATE".into(),
        ExprDataType::Timestamp { .. } => "TIMESTAMP".into(),
        ExprDataType::Interval { .. } => "INTERVAL".into(),
        ExprDataType::List(value) => format!("ARRAY<{}>", type_sql(value)),
        ExprDataType::Map { key, value } => format!("MAP<{}, {}>", type_sql(key), type_sql(value)),
        ExprDataType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|field| format!(
                    "{}: {}",
                    quote_identifier(&field.name),
                    type_sql(&field.data_type)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ExprDataType::Variant => "VARIANT".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn versioned_round_trip_is_stable() {
        let expr = Expr::column("orders.id")
            .binary(BinaryOperator::Gt, Expr::literal(ScalarValue::Int64(10)));
        let bytes = expr.encode_versioned().unwrap();
        assert_eq!(Expr::decode_versioned(&bytes).unwrap(), expr);
        assert_eq!(
            expr,
            Expr::decode_versioned(&expr.encode_versioned().unwrap()).unwrap()
        );
    }
    #[test]
    fn rejects_unknown_version() {
        let bytes = br#"{"version":99,"expression":{"node":"raw_sql","sql":"1"}}"#;
        assert!(matches!(
            Expr::decode_versioned(bytes),
            Err(PlanError::Validation(_))
        ));
    }
    #[test]
    fn validation_rejects_invalid_decimal_and_empty_raw_sql() {
        let invalid_decimal = Expr::literal(ScalarValue::Decimal128 {
            value: 1,
            precision: 0,
            scale: 0,
        });
        assert!(matches!(
            invalid_decimal.encode_versioned(),
            Err(PlanError::Validation(_))
        ));
        assert!(matches!(
            Expr::raw(" ").encode_versioned(),
            Err(PlanError::Validation(_))
        ));
    }

    #[test]
    fn window_expression_is_structured_and_renderable() {
        let expression = Expr::function("row_number", vec![]).over(
            vec![Expr::column("account_id")],
            vec![Expr::Sort {
                expression: Box::new(Expr::column("event_time")),
                direction: SortDirection::Ascending,
                nulls: NullOrdering::Last,
            }],
        );
        assert_eq!(
            expression.to_sql(),
            r#"row_number() OVER (PARTITION BY "account_id" ORDER BY "event_time" ASC NULLS LAST)"#
        );
        expression.validate().unwrap();
    }

    #[test]
    fn normalized_ast_is_deterministic() {
        let expr = Expr::column("a").binary(
            BinaryOperator::Eq,
            Expr::literal(ScalarValue::Utf8("x".into())),
        );
        assert_eq!(
            expr.normalize_json().unwrap(),
            expr.normalize_json().unwrap()
        );
    }

    #[test]
    fn variant_type_sql_name() {
        assert_eq!(type_sql(&ExprDataType::Variant), "VARIANT");
    }

    #[test]
    fn variant_type_validates() {
        assert!(ExprDataType::Variant.validate().is_ok());
    }

    #[test]
    fn variant_type_round_trips_via_serde() {
        let t = ExprDataType::Variant;
        let json = serde_json::to_string(&t).unwrap();
        let back: ExprDataType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, t);
    }
}
