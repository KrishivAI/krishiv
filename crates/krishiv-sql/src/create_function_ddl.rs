//! Pre-processor for `CREATE FUNCTION … RETURNS TABLE` DDL.
//!
//! DataFusion does not natively understand the Krishiv-extended
//! `CREATE FUNCTION … RETURNS TABLE (col TYPE, …) LANGUAGE … AS '…'` syntax.
//! This module intercepts such statements before they reach DataFusion and
//! registers a [`TableUdf`][krishiv_plan::udf::TableUdf] backed by either:
//!
//! * A SQL body (`LANGUAGE sql AS '…'`) — executed via the session context.
//! * A runtime-provided Rust closure — registered via `SqlEngine::register_table_udf_fn`.
//!
//! Unsupported DDL languages are rejected before any registry mutation.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use regex::Regex;

use krishiv_plan::udf::{ScalarValue, TableUdf, UdfError};

// ────────────────────────────────────────────────────────────────────────────
// Parsed CREATE FUNCTION descriptor
// ────────────────────────────────────────────────────────────────────────────

/// A column definition extracted from the `RETURNS TABLE (…)` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

/// A typed function argument extracted from the function signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionArgDef {
    pub name: String,
    pub data_type: DataType,
}

/// Parsed descriptor produced by [`parse_create_function`].
#[derive(Debug, Clone)]
pub struct CreateFunctionDdl {
    /// Function name as written in the SQL statement.
    pub function_name: String,
    /// Typed arguments declared in the function signature.
    pub arguments: Vec<FunctionArgDef>,
    /// Output columns declared in `RETURNS TABLE (…)`.
    pub return_columns: Vec<ColumnDef>,
    /// Language string (e.g. `RUST`, `PYTHON`), lower-cased.
    pub language: Option<String>,
    /// Raw function body from the `AS '…'` clause, if any.
    pub body: Option<String>,
}

// ────────────────────────────────────────────────────────────────────────────
// Parsing
// ────────────────────────────────────────────────────────────────────────────

/// Return `true` if `sql` looks like a `CREATE FUNCTION … RETURNS TABLE …`
/// statement (case-insensitive, leading/trailing whitespace allowed).
///
/// Handles both `CREATE FUNCTION` and `CREATE OR REPLACE FUNCTION`.
pub fn is_create_function_returns_table(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    (upper.starts_with("CREATE FUNCTION") || upper.starts_with("CREATE OR REPLACE FUNCTION"))
        && upper.contains("RETURNS TABLE")
}

/// Parse a `CREATE FUNCTION … RETURNS TABLE (…)` statement and return a
/// [`CreateFunctionDdl`] descriptor.
///
/// Returns an error string if the statement cannot be recognised.
pub fn parse_create_function(sql: &str) -> Result<CreateFunctionDdl, String> {
    // Regex: capture function name, RETURNS TABLE column list, optional
    // LANGUAGE clause, and optional AS body.
    //
    // Pattern (case-insensitive):
    //   CREATE [OR REPLACE] FUNCTION  <name> ( <args> )
    //   RETURNS TABLE ( <col_defs> )
    //   [LANGUAGE <lang>]
    //   [AS '<body>']
    let re = Regex::new(
        r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)\s*\(([^)]*)\)\s*RETURNS\s+TABLE\s*\(([^)]*)\)(?:\s+LANGUAGE\s+(\w+))?(?:\s+AS\s+'((?:[^']|'')*)')?\s*;?\s*$",
    )
    .map_err(|e| format!("internal regex error: {e}"))?;

    let caps = re
        .captures(sql)
        .ok_or_else(|| "SQL does not match CREATE FUNCTION … RETURNS TABLE pattern".to_string())?;

    let function_name = caps
        .get(1)
        .map(|m| m.as_str().to_string())
        .ok_or("could not extract function name")?;

    let arg_list = caps.get(2).map(|m| m.as_str()).unwrap_or("");
    let arguments = parse_argument_list(arg_list)?;

    let col_list = caps.get(3).map(|m| m.as_str()).unwrap_or("");
    let return_columns = parse_column_list(col_list)?;

    let language = caps.get(4).map(|m| m.as_str().to_ascii_lowercase());
    let body = caps.get(5).map(|m| m.as_str().replace("''", "'"));

    Ok(CreateFunctionDdl {
        function_name,
        arguments,
        return_columns,
        language,
        body,
    })
}

fn parse_argument_list(list: &str) -> Result<Vec<FunctionArgDef>, String> {
    parse_named_type_list(list, "argument")?
        .into_iter()
        .map(|(name, data_type)| Ok(FunctionArgDef { name, data_type }))
        .collect()
}

/// Parse a comma-separated `name TYPE` column list as it appears inside
/// `RETURNS TABLE (…)`.
fn parse_column_list(list: &str) -> Result<Vec<ColumnDef>, String> {
    parse_named_type_list(list, "column")?
        .into_iter()
        .map(|(name, data_type)| Ok(ColumnDef { name, data_type }))
        .collect()
}

fn parse_named_type_list(list: &str, item_kind: &str) -> Result<Vec<(String, DataType)>, String> {
    let list = list.trim();
    if list.is_empty() {
        return Ok(Vec::new());
    }
    let mut parsed = Vec::new();
    let mut names = std::collections::HashSet::new();
    for item in list.split(',') {
        let parts: Vec<&str> = item.split_whitespace().collect();
        if parts.len() < 2 {
            return Err(format!("invalid {item_kind} definition: '{item}'"));
        }
        let name = parts[0].to_string();
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!("duplicate {item_kind} name '{name}'"));
        }
        let type_str = parts[1..].join(" ");
        let data_type = sql_type_to_arrow(&type_str)?;
        parsed.push((name, data_type));
    }
    Ok(parsed)
}

/// Map a SQL type keyword (as used in DDL) to an Arrow [`DataType`].
///
/// Only the types commonly seen in `RETURNS TABLE` declarations are mapped.
/// Unknown types fall back to `DataType::Utf8`.
fn sql_type_to_arrow(type_str: &str) -> Result<DataType, String> {
    match type_str.trim().to_ascii_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "TINYINT" | "INT8" => Ok(DataType::Int8),
        "SMALLINT" | "INT16" => Ok(DataType::Int16),
        "INT" | "INTEGER" | "INT32" => Ok(DataType::Int32),
        "BIGINT" | "INT64" | "LONG" => Ok(DataType::Int64),
        "FLOAT" | "FLOAT32" | "REAL" => Ok(DataType::Float32),
        "DOUBLE" | "FLOAT64" | "DOUBLE PRECISION" => Ok(DataType::Float64),
        "TEXT" | "VARCHAR" | "STRING" | "CHARACTER VARYING" => Ok(DataType::Utf8),
        "BYTEA" | "BYTES" | "BINARY" | "BLOB" => Ok(DataType::Binary),
        "DATE" => Ok(DataType::Date32),
        "TIMESTAMP" | "DATETIME" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        _ => Err(format!(
            "unsupported SQL type '{type_str}' in CREATE FUNCTION DDL"
        )),
    }
}

// ────────────────────────────────────────────────────────────────────────────
// UDTF implementations
// ────────────────────────────────────────────────────────────────────────────

/// Body-function type alias for runtime-registered UDTFs.
pub type UdtfBodyFn = Arc<dyn Fn(&[ScalarValue]) -> Result<RecordBatch, UdfError> + Send + Sync>;

/// A [`TableUdf`] backed by a runtime-provided Rust closure.
#[derive(Clone)]
pub struct ClosureTableUdf {
    pub(crate) name: String,
    pub(crate) schema: Schema,
    body_fn: UdtfBodyFn,
}

impl std::fmt::Debug for ClosureTableUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosureTableUdf")
            .field("name", &self.name)
            .field("schema", &self.schema)
            .finish()
    }
}

impl ClosureTableUdf {
    /// Create a closure-backed UDTF with an explicit output schema.
    pub fn try_new(
        name: impl Into<String>,
        schema: Schema,
        body_fn: UdtfBodyFn,
    ) -> Result<Self, UdfError> {
        let name = name.into();
        validate_udtf_definition(&name, &schema)?;
        Ok(Self {
            name,
            schema,
            body_fn,
        })
    }
}

impl TableUdf for ClosureTableUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }

    fn call(&self, args: &[ScalarValue]) -> Result<RecordBatch, UdfError> {
        let batch =
            catch_unwind(AssertUnwindSafe(|| (self.body_fn)(args))).map_err(|payload| {
                let message = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("unknown panic");
                UdfError::Panic(format!("UDTF '{}': {message}", self.name))
            })??;
        if !schema_contract_matches(batch.schema().as_ref(), &self.schema) {
            return Err(UdfError::Execution {
                message: format!(
                    "UDTF '{}' returned schema {:?}, expected {:?}",
                    self.name,
                    batch.schema(),
                    self.schema
                ),
            });
        }
        Ok(batch)
    }
}

fn validate_udtf_definition(name: &str, schema: &Schema) -> Result<(), UdfError> {
    if name.trim().is_empty() {
        return Err(UdfError::InvalidArgument {
            message: String::from("UDTF name must not be empty"),
        });
    }
    if schema.fields().is_empty() {
        return Err(UdfError::InvalidArgument {
            message: format!("UDTF '{name}' must declare at least one output column"),
        });
    }
    let mut names = std::collections::HashSet::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if field.name().trim().is_empty() {
            return Err(UdfError::InvalidArgument {
                message: format!("UDTF '{name}' contains an empty output column name"),
            });
        }
        if !names.insert(field.name()) {
            return Err(UdfError::InvalidArgument {
                message: format!(
                    "UDTF '{name}' contains duplicate output column '{}'",
                    field.name()
                ),
            });
        }
    }
    Ok(())
}

fn schema_contract_matches(actual: &Schema, expected: &Schema) -> bool {
    actual.fields().len() == expected.fields().len()
        && actual
            .fields()
            .iter()
            .zip(expected.fields())
            .all(|(actual, expected)| {
                actual.name() == expected.name() && actual.data_type() == expected.data_type()
            })
}

/// A [`TableUdf`] whose body is a SQL query executed via a DataFusion session.
///
/// Created by `SqlEngine` when `CREATE FUNCTION … LANGUAGE sql AS '…'` is
/// processed.  Uses `block_in_place` so the sync `TableFunctionImpl::call()`
/// can safely block on async SQL execution without deadlocking the runtime.
#[derive(Clone)]
pub struct SqlBodyTableUdf {
    pub(crate) name: String,
    pub(crate) schema: Schema,
    body_sql: String,
    argument_count: usize,
    ctx: Arc<datafusion::prelude::SessionContext>,
}

impl std::fmt::Debug for SqlBodyTableUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlBodyTableUdf")
            .field("name", &self.name)
            .field("body_sql", &self.body_sql)
            .finish()
    }
}

impl SqlBodyTableUdf {
    pub fn try_new(
        name: impl Into<String>,
        schema: Schema,
        body_sql: impl Into<String>,
        argument_count: usize,
        ctx: Arc<datafusion::prelude::SessionContext>,
    ) -> Result<Self, UdfError> {
        let name = name.into();
        validate_udtf_definition(&name, &schema)?;
        let body_sql = body_sql.into();
        if body_sql.trim().is_empty() {
            return Err(UdfError::InvalidArgument {
                message: format!("SQL UDTF '{name}' body must not be empty"),
            });
        }
        let placeholder_args = vec![ScalarValue::Null; argument_count];
        bind_sql_body_args(&body_sql, &placeholder_args)?;
        Ok(Self {
            name,
            schema,
            body_sql,
            argument_count,
            ctx,
        })
    }
}

impl TableUdf for SqlBodyTableUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }

    fn call(&self, args: &[ScalarValue]) -> Result<RecordBatch, UdfError> {
        if args.len() != self.argument_count {
            return Err(UdfError::InvalidArgument {
                message: format!(
                    "UDTF '{}' expects {} arguments, got {}",
                    self.name,
                    self.argument_count,
                    args.len()
                ),
            });
        }

        // Execute the SQL body synchronously using block_in_place so this
        // sync call-site can safely await without deadlocking the executor.
        let ctx = Arc::clone(&self.ctx);
        let sql = bind_sql_body_args(&self.body_sql, args)?;
        let schema = Arc::new(self.schema.clone());
        let handle =
            tokio::runtime::Handle::try_current().map_err(|error| UdfError::Execution {
                message: format!(
                    "SQL UDTF '{}' requires an active Tokio runtime: {error}",
                    self.name
                ),
            })?;
        if !matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return Err(UdfError::Execution {
                message: format!(
                    "SQL UDTF '{}' requires a multi-thread Tokio runtime",
                    self.name
                ),
            });
        }
        catch_unwind(AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                handle.block_on(async {
                    let df = ctx.sql(&sql).await.map_err(|e| UdfError::Execution {
                        message: e.to_string(),
                    })?;
                    let batches = df.collect().await.map_err(|e| UdfError::Execution {
                        message: e.to_string(),
                    })?;
                    if batches.is_empty() {
                        return Ok(RecordBatch::new_empty(schema));
                    }
                    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches)
                        .map_err(|e| UdfError::Arrow(e.to_string()))?;
                    if !schema_contract_matches(batch.schema().as_ref(), schema.as_ref()) {
                        return Err(UdfError::Execution {
                            message: format!(
                                "SQL UDTF '{}' returned schema {:?}, expected {:?}",
                                self.name,
                                batch.schema(),
                                schema
                            ),
                        });
                    }
                    Ok(batch)
                })
            })
        }))
        .map_err(|payload| {
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown panic");
            UdfError::Panic(format!("SQL UDTF '{}': {message}", self.name))
        })?
    }
}

fn bind_sql_body_args(sql: &str, args: &[ScalarValue]) -> Result<String, UdfError> {
    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                index = copy_quoted_segment(sql, index, bytes[index], &mut output)?;
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                let end = sql[index..]
                    .find('\n')
                    .map_or(bytes.len(), |offset| index + offset + 1);
                output.push_str(&sql[index..end]);
                index = end;
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = copy_block_comment(sql, index, &mut output)?;
            }
            b'$' => {
                if let Some((delimiter, end)) = dollar_quote_delimiter(sql, index) {
                    let body_start = end;
                    let close_offset = sql[body_start..].find(delimiter).ok_or_else(|| {
                        UdfError::InvalidArgument {
                            message: "unterminated dollar-quoted SQL body".to_owned(),
                        }
                    })?;
                    let segment_end = body_start + close_offset + delimiter.len();
                    output.push_str(&sql[index..segment_end]);
                    index = segment_end;
                    continue;
                }

                let digit_start = index + 1;
                let mut end = digit_start;
                while bytes.get(end).is_some_and(u8::is_ascii_digit) {
                    end += 1;
                }
                if end == digit_start {
                    output.push('$');
                    index += 1;
                    continue;
                }

                let placeholder = sql[digit_start..end].parse::<usize>().map_err(|error| {
                    UdfError::InvalidArgument {
                        message: format!(
                            "invalid SQL UDTF placeholder '{}': {error}",
                            &sql[index..end]
                        ),
                    }
                })?;
                if placeholder == 0 {
                    return Err(UdfError::InvalidArgument {
                        message: "SQL UDTF placeholders are 1-based; $0 is invalid".to_owned(),
                    });
                }
                let value = args.get(placeholder - 1).ok_or_else(|| UdfError::InvalidArgument {
                    message: format!(
                        "SQL UDTF placeholder ${placeholder} has no matching argument; got {} arguments",
                        args.len()
                    ),
                })?;
                output.push_str(&scalar_to_sql_literal(value)?);
                index = end;
            }
            _ => {
                let ch = sql[index..]
                    .chars()
                    .next()
                    .expect("index is within the SQL string");
                output.push(ch);
                index += ch.len_utf8();
            }
        }
    }

    Ok(output)
}

fn copy_quoted_segment(
    sql: &str,
    start: usize,
    quote: u8,
    output: &mut String,
) -> Result<usize, UdfError> {
    let bytes = sql.as_bytes();
    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == quote {
            index += 1;
            if bytes.get(index) == Some(&quote) {
                index += 1;
                continue;
            }
            output.push_str(&sql[start..index]);
            return Ok(index);
        }
        let ch = sql[index..]
            .chars()
            .next()
            .expect("index is within the SQL string");
        index += ch.len_utf8();
    }
    Err(UdfError::InvalidArgument {
        message: "unterminated quoted SQL segment".to_owned(),
    })
}

fn copy_block_comment(sql: &str, start: usize, output: &mut String) -> Result<usize, UdfError> {
    let bytes = sql.as_bytes();
    let mut index = start + 2;
    let mut depth = 1usize;
    while index < bytes.len() {
        if bytes.get(index) == Some(&b'/') && bytes.get(index + 1) == Some(&b'*') {
            depth += 1;
            index += 2;
        } else if bytes.get(index) == Some(&b'*') && bytes.get(index + 1) == Some(&b'/') {
            depth -= 1;
            index += 2;
            if depth == 0 {
                output.push_str(&sql[start..index]);
                return Ok(index);
            }
        } else {
            let ch = sql[index..]
                .chars()
                .next()
                .expect("index is within the SQL string");
            index += ch.len_utf8();
        }
    }
    Err(UdfError::InvalidArgument {
        message: "unterminated SQL block comment".to_owned(),
    })
}

fn dollar_quote_delimiter(sql: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = sql.as_bytes();
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut index = start + 1;
    if bytes.get(index) == Some(&b'$') {
        return Some((&sql[start..=index], index + 1));
    }
    let first = *bytes.get(index)?;
    if !first.is_ascii_alphabetic() && first != b'_' {
        return None;
    }
    index += 1;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        index += 1;
    }
    if bytes.get(index) == Some(&b'$') {
        Some((&sql[start..=index], index + 1))
    } else {
        None
    }
}

fn scalar_to_sql_literal(value: &ScalarValue) -> Result<String, UdfError> {
    match value {
        ScalarValue::Null => Ok("NULL".to_owned()),
        ScalarValue::Int64(value) => Ok(value.to_string()),
        ScalarValue::Float64(value) if value.is_finite() => Ok(value.to_string()),
        ScalarValue::Float64(value) => Err(UdfError::InvalidArgument {
            message: format!("non-finite floating-point UDTF argument {value} is not supported"),
        }),
        ScalarValue::Utf8(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        ScalarValue::Boolean(value) => Ok(if *value { "TRUE" } else { "FALSE" }.to_owned()),
        ScalarValue::Bytes(_) => Err(UdfError::InvalidArgument {
            message: "binary UDTF arguments are not supported in SQL bodies".to_owned(),
        }),
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field};

    const BASIC_DDL: &str = "
        CREATE FUNCTION my_udtf(arg1 INT)
        RETURNS TABLE (col1 TEXT, col2 BIGINT)
        LANGUAGE RUST
        AS 'fn my_udtf(arg1: i64) -> Vec<Row> { vec![] }'
    ";

    #[test]
    fn detects_create_function_returns_table() {
        assert!(is_create_function_returns_table(BASIC_DDL));
        // CREATE OR REPLACE variant
        assert!(is_create_function_returns_table(
            "CREATE OR REPLACE FUNCTION g(x INT) RETURNS TABLE (v TEXT)"
        ));
        // Non-matching: plain SELECT
        assert!(!is_create_function_returns_table("SELECT 1"));
        // Non-matching: RETURNS scalar, not TABLE
        assert!(!is_create_function_returns_table(
            "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE SQL AS 'SELECT x'"
        ));
    }

    #[test]
    fn parses_function_name() {
        let ddl = parse_create_function(BASIC_DDL).expect("should parse");
        assert_eq!(ddl.function_name, "my_udtf");
    }

    #[test]
    fn parses_typed_arguments() {
        let ddl = parse_create_function(
            "CREATE FUNCTION typed_args(count BIGINT, label TEXT, enabled BOOLEAN) \
             RETURNS TABLE (value TEXT) LANGUAGE SQL AS 'SELECT $2 AS value'",
        )
        .expect("should parse");
        assert_eq!(
            ddl.arguments,
            vec![
                FunctionArgDef {
                    name: "count".to_owned(),
                    data_type: DataType::Int64,
                },
                FunctionArgDef {
                    name: "label".to_owned(),
                    data_type: DataType::Utf8,
                },
                FunctionArgDef {
                    name: "enabled".to_owned(),
                    data_type: DataType::Boolean,
                },
            ]
        );
    }

    #[test]
    fn parses_return_columns() {
        let ddl = parse_create_function(BASIC_DDL).expect("should parse");
        assert_eq!(ddl.return_columns.len(), 2);
        assert_eq!(ddl.return_columns[0].name, "col1");
        assert_eq!(ddl.return_columns[0].data_type, DataType::Utf8);
        assert_eq!(ddl.return_columns[1].name, "col2");
        assert_eq!(ddl.return_columns[1].data_type, DataType::Int64);
    }

    #[test]
    fn parses_language_and_body() {
        let ddl = parse_create_function(BASIC_DDL).expect("should parse");
        assert_eq!(ddl.language.as_deref(), Some("rust"));
        assert!(ddl.body.is_some());
    }

    #[test]
    fn parses_without_language_and_body() {
        let sql = "CREATE FUNCTION simple(x INT) RETURNS TABLE (val BIGINT)";
        let ddl = parse_create_function(sql).expect("should parse");
        assert_eq!(ddl.function_name, "simple");
        assert_eq!(ddl.return_columns.len(), 1);
        assert_eq!(ddl.language, None);
        assert_eq!(ddl.body, None);
    }

    #[test]
    fn parses_or_replace_variant() {
        let sql = "CREATE OR REPLACE FUNCTION f(x INT) RETURNS TABLE (a TEXT, b INT)";
        let ddl = parse_create_function(sql).expect("should parse");
        assert_eq!(ddl.function_name, "f");
        assert_eq!(ddl.return_columns.len(), 2);
    }

    #[test]
    fn parser_rejects_trailing_unparsed_sql() {
        let error = parse_create_function(&format!("{BASIC_DDL} SELECT 1"))
            .expect_err("trailing SQL must not be ignored");
        assert!(error.contains("does not match"));
    }

    #[test]
    fn parser_rejects_duplicate_argument_and_output_names() {
        let duplicate_arg = parse_create_function(
            "CREATE FUNCTION f(value INT, VALUE BIGINT) \
             RETURNS TABLE (result BIGINT) LANGUAGE SQL AS 'SELECT 1 AS result'",
        )
        .expect_err("argument names are case-insensitively unique");
        assert!(duplicate_arg.contains("duplicate argument"));

        let duplicate_output = parse_create_function(
            "CREATE FUNCTION f() RETURNS TABLE (value INT, VALUE BIGINT) \
             LANGUAGE SQL AS 'SELECT 1 AS value, 2 AS VALUE'",
        )
        .expect_err("output names are case-insensitively unique");
        assert!(duplicate_output.contains("duplicate column"));
    }

    #[test]
    fn closure_table_udf_executes_and_validates_output_schema() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let udf = ClosureTableUdf::try_new(
            "values",
            schema.clone(),
            Arc::new({
                let schema = Arc::new(schema);
                move |_| {
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef],
                    )
                    .map_err(UdfError::from)
                }
            }),
        )
        .unwrap();

        let batch = udf.call(&[]).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let wrong_schema = ClosureTableUdf::try_new(
            "wrong",
            Schema::new(vec![Field::new("expected", DataType::Int64, false)]),
            Arc::new(|_| {
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "actual",
                        DataType::Int64,
                        false,
                    )])),
                    vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
                )
                .map_err(UdfError::from)
            }),
        )
        .unwrap();
        assert!(matches!(
            wrong_schema.call(&[]),
            Err(UdfError::Execution { .. })
        ));
    }

    #[test]
    fn closure_table_udf_contains_panics() {
        let udf = ClosureTableUdf::try_new(
            "panic_udtf",
            Schema::new(vec![Field::new("value", DataType::Int64, false)]),
            Arc::new(|_| -> Result<RecordBatch, UdfError> { panic!("boom") }),
        )
        .unwrap();

        assert!(matches!(udf.call(&[]), Err(UdfError::Panic(_))));
    }

    #[test]
    fn sql_body_udtf_without_runtime_returns_typed_error() {
        let udf = SqlBodyTableUdf::try_new(
            "runtime_required",
            Schema::new(vec![Field::new("value", DataType::Int64, false)]),
            "SELECT 1 AS value",
            0,
            Arc::new(datafusion::prelude::SessionContext::new()),
        )
        .unwrap();

        let error = udf
            .call(&[])
            .expect_err("missing Tokio runtime must not panic");
        assert!(matches!(error, UdfError::Execution { .. }));
    }

    #[test]
    fn sql_body_binding_replaces_only_unquoted_placeholders() {
        let sql = "SELECT $1 AS n, '$1' AS literal, \"$2\" AS quoted, /* $2 */ $2 AS text";
        let bound = bind_sql_body_args(
            sql,
            &[
                ScalarValue::Int64(42),
                ScalarValue::Utf8("O'Reilly".to_owned()),
            ],
        )
        .expect("binding should succeed");
        assert_eq!(
            bound,
            "SELECT 42 AS n, '$1' AS literal, \"$2\" AS quoted, /* $2 */ 'O''Reilly' AS text"
        );
    }

    #[test]
    fn sql_body_binding_preserves_comments_and_dollar_quoted_segments() {
        let sql = "SELECT $$body $1$$ AS body, -- $1\n$1 AS value";
        let bound =
            bind_sql_body_args(sql, &[ScalarValue::Boolean(true)]).expect("binding should succeed");
        assert_eq!(bound, "SELECT $$body $1$$ AS body, -- $1\nTRUE AS value");
    }

    #[test]
    fn sql_body_binding_rejects_invalid_placeholders_and_values() {
        let zero = bind_sql_body_args("SELECT $0", &[ScalarValue::Int64(1)])
            .expect_err("$0 must be rejected");
        assert!(zero.to_string().contains("1-based"));

        let missing = bind_sql_body_args("SELECT $2", &[ScalarValue::Int64(1)])
            .expect_err("missing arguments must be rejected");
        assert!(missing.to_string().contains("no matching argument"));

        let binary = bind_sql_body_args("SELECT $1", &[ScalarValue::Bytes(vec![1, 2])])
            .expect_err("binary SQL literals must be rejected");
        assert!(binary.to_string().contains("binary"));
    }

    #[test]
    fn rejects_non_matching_sql() {
        let result = parse_create_function("SELECT 1");
        assert!(result.is_err());
    }

    #[test]
    fn all_supported_types_map() {
        let ddl = parse_create_function(
            "CREATE FUNCTION typed(x INT) RETURNS TABLE (
                a BOOLEAN,
                b TINYINT,
                c SMALLINT,
                d INT,
                e BIGINT,
                f FLOAT,
                g DOUBLE,
                h TEXT,
                i BYTEA,
                j DATE,
                k TIMESTAMP
            )",
        )
        .expect("should parse");
        assert_eq!(ddl.return_columns[0].data_type, DataType::Boolean);
        assert_eq!(ddl.return_columns[1].data_type, DataType::Int8);
        assert_eq!(ddl.return_columns[2].data_type, DataType::Int16);
        assert_eq!(ddl.return_columns[3].data_type, DataType::Int32);
        assert_eq!(ddl.return_columns[4].data_type, DataType::Int64);
        assert_eq!(ddl.return_columns[5].data_type, DataType::Float32);
        assert_eq!(ddl.return_columns[6].data_type, DataType::Float64);
        assert_eq!(ddl.return_columns[7].data_type, DataType::Utf8);
        assert_eq!(ddl.return_columns[8].data_type, DataType::Binary);
        assert_eq!(ddl.return_columns[9].data_type, DataType::Date32);
        assert_eq!(
            ddl.return_columns[10].data_type,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
    }
}
