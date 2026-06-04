//! Pre-processor for `CREATE FUNCTION … RETURNS TABLE` DDL.
//!
//! DataFusion does not natively understand the Krishiv-extended
//! `CREATE FUNCTION … RETURNS TABLE (col TYPE, …) LANGUAGE … AS '…'` syntax.
//! This module intercepts such statements before they reach DataFusion and
//! registers a [`TableUdf`][krishiv_udf::TableUdf] backed by either:
//!
//! * A SQL body (`LANGUAGE sql AS '…'`) — executed via the session context.
//! * A runtime-provided Rust closure — registered via `SqlEngine::register_table_udf_fn`.
//! * A stub that returns a clear "not yet implemented" error for other languages.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use regex::Regex;

use krishiv_udf::{ScalarValue, TableUdf, UdfError, UdfRegistry};

// ────────────────────────────────────────────────────────────────────────────
// Parsed CREATE FUNCTION descriptor
// ────────────────────────────────────────────────────────────────────────────

/// A column definition extracted from the `RETURNS TABLE (…)` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

/// Parsed descriptor produced by [`parse_create_function`].
#[derive(Debug, Clone)]
pub struct CreateFunctionDdl {
    /// Function name as written in the SQL statement.
    pub function_name: String,
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
    //   CREATE [OR REPLACE] FUNCTION  <name> (<args>)
    //   RETURNS TABLE ( <col_defs> )
    //   [LANGUAGE <lang>]
    //   [AS '<body>']
    let re = Regex::new(
        r"(?is)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)\s*\([^)]*\)\s*RETURNS\s+TABLE\s*\(([^)]*)\)(?:\s+LANGUAGE\s+(\w+))?(?:\s+AS\s+'((?:[^']|'')*)')?",
    )
    .map_err(|e| format!("internal regex error: {e}"))?;

    let caps = re
        .captures(sql)
        .ok_or_else(|| "SQL does not match CREATE FUNCTION … RETURNS TABLE pattern".to_string())?;

    let function_name = caps
        .get(1)
        .map(|m| m.as_str().to_string())
        .ok_or("could not extract function name")?;

    let col_list = caps.get(2).map(|m| m.as_str()).unwrap_or("");
    let return_columns = parse_column_list(col_list)?;

    let language = caps.get(3).map(|m| m.as_str().to_ascii_lowercase());
    let body = caps.get(4).map(|m| m.as_str().replace("''", "'"));

    Ok(CreateFunctionDdl {
        function_name,
        return_columns,
        language,
        body,
    })
}

/// Parse a comma-separated `name TYPE` column list as it appears inside
/// `RETURNS TABLE (…)`.
fn parse_column_list(list: &str) -> Result<Vec<ColumnDef>, String> {
    let list = list.trim();
    if list.is_empty() {
        return Ok(Vec::new());
    }
    list.split(',')
        .map(|col| {
            let parts: Vec<&str> = col.split_whitespace().collect();
            if parts.len() < 2 {
                return Err(format!("invalid column definition: '{col}'"));
            }
            let name = parts[0].to_string();
            let type_str = parts[1..].join(" ");
            let data_type = sql_type_to_arrow(&type_str)?;
            Ok(ColumnDef { name, data_type })
        })
        .collect()
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

/// A [`TableUdf`] backed by either a runtime closure or the "not yet
/// implemented" error for unsupported language bodies.
#[derive(Clone)]
pub struct StubTableUdf {
    pub(crate) name: String,
    pub(crate) schema: Schema,
    /// Optional runtime-provided body.  `None` → return a clear error.
    body_fn: Option<UdtfBodyFn>,
}

impl std::fmt::Debug for StubTableUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StubTableUdf")
            .field("name", &self.name)
            .field("has_body", &self.body_fn.is_some())
            .finish()
    }
}

impl StubTableUdf {
    /// Build a stub (no body) from a parsed [`CreateFunctionDdl`].
    pub fn from_ddl(ddl: &CreateFunctionDdl) -> Self {
        let fields: Vec<Field> = ddl
            .return_columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.clone(), true))
            .collect();
        Self {
            name: ddl.function_name.clone(),
            schema: Schema::new(fields),
            body_fn: None,
        }
    }

    /// Attach a runtime body function.
    pub fn with_body_fn(mut self, f: UdtfBodyFn) -> Self {
        self.body_fn = Some(f);
        self
    }
}

impl TableUdf for StubTableUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }

    fn call(&self, args: &[ScalarValue]) -> Result<RecordBatch, UdfError> {
        match &self.body_fn {
            Some(f) => f(args),
            None => Err(UdfError::Execution {
                message: format!(
                    "UDTF '{}': body execution is not yet implemented; \
                     register a body via SqlEngine::register_table_udf_fn() \
                     or use LANGUAGE sql AS '…'",
                    self.name
                ),
            }),
        }
    }
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
    pub fn new(
        name: impl Into<String>,
        schema: Schema,
        body_sql: impl Into<String>,
        ctx: Arc<datafusion::prelude::SessionContext>,
    ) -> Self {
        Self {
            name: name.into(),
            schema,
            body_sql: body_sql.into(),
            ctx,
        }
    }
}

impl TableUdf for SqlBodyTableUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }

    fn call(&self, _args: &[ScalarValue]) -> Result<RecordBatch, UdfError> {
        // Execute the SQL body synchronously using block_in_place so this
        // sync call-site can safely await without deadlocking the executor.
        // TODO: substitute $1, $2, … arg placeholders for parameterized UDTFs.
        let ctx = Arc::clone(&self.ctx);
        let sql = self.body_sql.clone();
        let schema = Arc::new(self.schema.clone());
        tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
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
                arrow::compute::concat_batches(&batches[0].schema(), &batches)
                    .map_err(|e| UdfError::Arrow(e.to_string()))
            })
        })
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Registration helper
// ────────────────────────────────────────────────────────────────────────────

/// Parse `sql` as a `CREATE FUNCTION … RETURNS TABLE` statement and register
/// a [`StubTableUdf`] in `registry`.
///
/// Returns the parsed [`CreateFunctionDdl`] on success.
pub fn register_udtf_from_sql(
    sql: &str,
    registry: &mut UdfRegistry,
) -> Result<CreateFunctionDdl, String> {
    let ddl = parse_create_function(sql)?;
    let stub = StubTableUdf::from_ddl(&ddl);
    registry.register_table(Arc::new(stub));
    Ok(ddl)
}

// ────────────────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

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
    fn stub_table_udf_call_returns_not_implemented_error() {
        // StubTableUdf is schema-only; calling it returns a clear "not implemented"
        // error so users get an actionable message rather than silent empty results.
        let ddl = parse_create_function(BASIC_DDL).expect("should parse");
        let stub = StubTableUdf::from_ddl(&ddl);
        let err = stub.call(&[]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("my_udtf"),
            "error should include function name"
        );
        assert!(
            msg.contains("not yet implemented") || msg.contains("not implemented"),
            "error should indicate unimplemented: {msg}"
        );
        // Schema is still accessible for planning.
        assert_eq!(stub.output_schema().field(0).name(), "col1");
        assert_eq!(stub.output_schema().field(1).name(), "col2");
    }

    #[test]
    fn register_udtf_from_sql_adds_to_registry() {
        let mut registry = UdfRegistry::new();
        let ddl = register_udtf_from_sql(BASIC_DDL, &mut registry).expect("registration ok");
        assert_eq!(ddl.function_name, "my_udtf");
        let found = registry.get_table("my_udtf").expect("must be registered");
        assert_eq!(found.name(), "my_udtf");
        assert_eq!(found.output_schema().fields().len(), 2);
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
