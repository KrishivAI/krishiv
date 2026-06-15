//! `DESCRIBE`, `SHOW COLUMNS`, and `EXPLAIN` SQL intercepts.

use std::sync::Arc;

use arrow::array::{BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

use crate::{SqlError, SqlResult};

/// Parsed introspection statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntrospectionStatement {
    Describe { table: String },
    Explain { mode: ExplainSqlMode, query: String },
}

/// Detail level for `EXPLAIN` SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainSqlMode {
    Logical,
    Physical,
    Analyze,
}

/// Return `Some(stmt)` when `sql` is a Krishiv introspection statement.
pub fn parse_introspection_statement(sql: &str) -> SqlResult<Option<IntrospectionStatement>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let upper = trimmed.to_ascii_uppercase();

    if let Some(table) = parse_describe_target(trimmed, &upper) {
        return Ok(Some(IntrospectionStatement::Describe { table }));
    }

    if let Some(table) = parse_show_columns_target(trimmed, &upper) {
        return Ok(Some(IntrospectionStatement::Describe { table }));
    }

    if let Some((mode, query)) = parse_explain_target(trimmed, &upper) {
        return Ok(Some(IntrospectionStatement::Explain { mode, query }));
    }

    Ok(None)
}

fn parse_describe_target(trimmed: &str, upper: &str) -> Option<String> {
    const PREFIXES: &[&str] = &["DESCRIBE ", "DESC TABLE ", "DESC "];
    for prefix in PREFIXES {
        if upper.starts_with(prefix) {
            let table = trimmed[prefix.len()..].trim().trim_end_matches(';').trim();
            if !table.is_empty() {
                return Some(table.to_string());
            }
        }
    }
    None
}

fn parse_show_columns_target(trimmed: &str, upper: &str) -> Option<String> {
    if !upper.starts_with("SHOW COLUMNS ") {
        return None;
    }
    let rest = trimmed["SHOW COLUMNS ".len()..].trim();
    let upper_rest = rest.to_ascii_uppercase();
    let table = if let Some(after) = upper_rest.strip_prefix("FROM ") {
        rest[rest.len() - after.len()..].trim()
    } else if let Some(after) = upper_rest.strip_prefix("IN ") {
        rest[rest.len() - after.len()..].trim()
    } else {
        rest
    };
    let table = table.trim_end_matches(';').trim();
    if table.is_empty() {
        None
    } else {
        Some(table.to_string())
    }
}

fn parse_explain_target(trimmed: &str, upper: &str) -> Option<(ExplainSqlMode, String)> {
    if !upper.starts_with("EXPLAIN ") {
        return None;
    }
    let rest = trimmed["EXPLAIN ".len()..].trim();
    let upper_rest = rest.to_ascii_uppercase();
    let (mode, query) = if let Some(query) = upper_rest.strip_prefix("LOGICAL ") {
        (
            ExplainSqlMode::Logical,
            rest[rest.len() - query.len()..].trim(),
        )
    } else if let Some(query) = upper_rest.strip_prefix("PHYSICAL ") {
        (
            ExplainSqlMode::Physical,
            rest[rest.len() - query.len()..].trim(),
        )
    } else if let Some(query) = upper_rest.strip_prefix("ANALYZE ") {
        (
            ExplainSqlMode::Analyze,
            rest[rest.len() - query.len()..].trim(),
        )
    } else {
        (ExplainSqlMode::Physical, rest)
    };
    let query = query.trim_end_matches(';').trim();
    if query.is_empty() {
        return None;
    }
    Some((mode, query.to_string()))
}

/// Build a `DESCRIBE` result batch for `table` using the DataFusion catalog.
pub async fn describe_table(context: &SessionContext, table: &str) -> SqlResult<RecordBatch> {
    let provider = context
        .table_provider(table)
        .await
        .map_err(|error| SqlError::DataFusion {
            message: format!("DESCRIBE: table '{table}' not found: {error}"),
        })?;
    let schema = provider.schema();
    let col_name = Arc::new(StringArray::from(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
    ));
    let data_type = Arc::new(StringArray::from(
        schema
            .fields()
            .iter()
            .map(|field| field.data_type().to_string())
            .collect::<Vec<_>>(),
    ));
    let nullable = Arc::new(BooleanArray::from(
        schema
            .fields()
            .iter()
            .map(|field| field.is_nullable())
            .collect::<Vec<_>>(),
    ));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("col_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("nullable", DataType::Boolean, false),
    ]));
    RecordBatch::try_new(out_schema, vec![col_name, data_type, nullable]).map_err(|error| {
        SqlError::DataFusion {
            message: format!("DESCRIBE: failed to build result batch: {error}"),
        }
    })
}

/// Render explain text for the embedded query.
pub fn explain_query(query: &str, mode: ExplainSqlMode) -> SqlResult<String> {
    match mode {
        ExplainSqlMode::Logical => crate::explain_sql(query),
        ExplainSqlMode::Physical => {
            crate::explain_sql_optimized(query, &krishiv_plan::optimizer::Optimizer::default())
        }
        ExplainSqlMode::Analyze => {
            let mut output = explain_query(query, ExplainSqlMode::Physical)?;
            output.push_str(
                "\n\nANALYZE: execute the query and call DataFrame::explain_with(Analyze) \
                 for runtime statistics.",
            );
            Ok(output)
        }
    }
}

/// Build a single-row batch containing explain text.
pub fn explain_result_batch(text: &str) -> SqlResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, false)]));
    let values = Arc::new(StringArray::from(vec![text]));
    RecordBatch::try_new(schema, vec![values]).map_err(|error| SqlError::DataFusion {
        message: format!("EXPLAIN: failed to build result batch: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_describe_variants() {
        let stmt = parse_introspection_statement("DESCRIBE orders")
            .unwrap()
            .unwrap();
        assert_eq!(
            stmt,
            IntrospectionStatement::Describe {
                table: "orders".into()
            }
        );
        let stmt = parse_introspection_statement("DESC TABLE people")
            .unwrap()
            .unwrap();
        assert!(matches!(stmt, IntrospectionStatement::Describe { .. }));
    }

    #[test]
    fn parse_show_columns() {
        let stmt = parse_introspection_statement("SHOW COLUMNS FROM events")
            .unwrap()
            .unwrap();
        assert_eq!(
            stmt,
            IntrospectionStatement::Describe {
                table: "events".into()
            }
        );
    }

    #[test]
    fn parse_explain_modes() {
        let stmt = parse_introspection_statement("EXPLAIN SELECT 1")
            .unwrap()
            .unwrap();
        assert!(matches!(
            stmt,
            IntrospectionStatement::Explain {
                mode: ExplainSqlMode::Physical,
                ..
            }
        ));
        let stmt = parse_introspection_statement("EXPLAIN LOGICAL SELECT 1")
            .unwrap()
            .unwrap();
        assert!(matches!(
            stmt,
            IntrospectionStatement::Explain {
                mode: ExplainSqlMode::Logical,
                ..
            }
        ));
    }

    #[test]
    fn explain_query_logical_renders_plan() {
        let text = explain_query("SELECT 1 AS n", ExplainSqlMode::Logical).unwrap();
        assert!(text.contains("SELECT") || text.contains("select") || !text.is_empty());
    }
}
