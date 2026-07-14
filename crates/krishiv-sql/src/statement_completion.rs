#![forbid(unsafe_code)]
//! Spark-reference session/navigation statements that DataFusion's planner does
//! not handle natively (Phase 60 statement completion).
//!
//! DataFusion 54 already plans `SET`/`RESET`, `SHOW TABLES|COLUMNS|FUNCTIONS`,
//! `SHOW CREATE`, `TRUNCATE TABLE`, `DESCRIBE <table>`, and `EXPLAIN`. This
//! module fills the two navigation gaps a SQL/BI client expects:
//!
//! - **`USE [CATALOG|SCHEMA|DATABASE|NAMESPACE] <name>`** — set the session's
//!   current catalog/schema so subsequent unqualified table references resolve
//!   there (Spark `USE`). `USE a.b` sets catalog `a`, schema `b`.
//! - **`SHOW DATABASES` / `SHOW SCHEMAS`** — list schemas; rewritten to an
//!   `information_schema.schemata` query returning a Spark-style `namespace`
//!   column.
//!
//! `CACHE/UNCACHE TABLE` (session materialization), `SHOW PARTITIONS` (Iceberg
//! metadata), and `DESCRIBE FUNCTION|DATABASE|QUERY` remain the itemized
//! statement shortfall in the matrix.

use datafusion::prelude::SessionContext;

/// The parsed target of a `USE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseTarget {
    /// New default catalog, if the statement set one.
    pub catalog: Option<String>,
    /// New default schema, if the statement set one.
    pub schema: Option<String>,
}

fn unquote(ident: &str) -> String {
    let t = ident.trim();
    for q in ['`', '"'] {
        if let Some(inner) = t.strip_prefix(q).and_then(|s| s.strip_suffix(q)) {
            return inner.to_string();
        }
    }
    t.to_string()
}

/// Parse a `USE …` statement into its catalog/schema target, or `None` if the
/// query is not a `USE` statement.
///
/// Forms (Spark/Databricks):
/// - `USE db` / `USE SCHEMA db` / `USE DATABASE db` / `USE NAMESPACE ns` — schema
/// - `USE CATALOG cat` — catalog
/// - `USE cat.schema` — both
pub fn parse_use(query: &str) -> Option<UseTarget> {
    let q = query.trim().trim_end_matches(';').trim();
    // First token must be USE (case-insensitive); the remainder is the target.
    let (head, remainder) = q.split_once(char::is_whitespace)?;
    if !head.eq_ignore_ascii_case("USE") {
        return None;
    }
    let remainder = remainder.trim();
    if remainder.is_empty() {
        return None;
    }
    // Optional leading CATALOG / SCHEMA / DATABASE / NAMESPACE keyword; whatever
    // follows it (which may be a back-quoted identifier containing spaces) is the
    // whole name.
    let (is_catalog, name_part) = match remainder.split_once(char::is_whitespace) {
        Some((w, r)) if w.eq_ignore_ascii_case("CATALOG") => (true, r.trim()),
        Some((w, r))
            if w.eq_ignore_ascii_case("SCHEMA")
                || w.eq_ignore_ascii_case("DATABASE")
                || w.eq_ignore_ascii_case("NAMESPACE") =>
        {
            (false, r.trim())
        }
        _ => (false, remainder),
    };
    let name = unquote(name_part);
    if name.is_empty() {
        return None;
    }
    if is_catalog {
        return Some(UseTarget {
            catalog: Some(name),
            schema: None,
        });
    }
    // Schema form: allow a qualified `catalog.schema`.
    if let Some((cat, sch)) = name.split_once('.') {
        return Some(UseTarget {
            catalog: Some(unquote(cat)),
            schema: Some(unquote(sch)),
        });
    }
    Some(UseTarget {
        catalog: None,
        schema: Some(name),
    })
}

/// Apply a `USE` target to the session by mutating the default catalog/schema.
/// Returns `Some(Ok(()))` when the query was a `USE` statement (handled), or
/// `None` when it was not.
pub fn apply_use(ctx: &SessionContext, query: &str) -> Option<Result<(), String>> {
    let target = parse_use(query)?;
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    let opts = state.config_mut().options_mut();
    if let Some(catalog) = target.catalog {
        opts.catalog.default_catalog = catalog;
    }
    if let Some(schema) = target.schema {
        opts.catalog.default_schema = schema;
    }
    Some(Ok(()))
}

/// If `query` is `SHOW DATABASES`/`SHOW SCHEMAS` (optionally `LIKE 'pat'`),
/// return an equivalent `information_schema.schemata` SELECT; otherwise `None`.
/// The result column is `namespace`, matching Spark's `SHOW DATABASES`.
pub fn rewrite_show_databases(query: &str) -> Option<String> {
    let q = query.trim().trim_end_matches(';').trim();
    let upper = q.to_ascii_uppercase();
    let is_show = upper.starts_with("SHOW DATABASES") || upper.starts_with("SHOW SCHEMAS");
    if !is_show {
        return None;
    }
    // Optional `LIKE 'pattern'` filter.
    let like_clause = if let Some(idx) = upper.find(" LIKE ") {
        let pat = q[idx + 6..].trim().trim_end_matches(';').trim();
        Some(format!(" WHERE schema_name LIKE {pat}"))
    } else {
        None
    };
    Some(format!(
        "SELECT schema_name AS namespace FROM information_schema.schemata{} ORDER BY namespace",
        like_clause.unwrap_or_default()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_use_forms() {
        assert_eq!(
            parse_use("USE analytics"),
            Some(UseTarget { catalog: None, schema: Some("analytics".into()) })
        );
        assert_eq!(
            parse_use("USE SCHEMA sales"),
            Some(UseTarget { catalog: None, schema: Some("sales".into()) })
        );
        assert_eq!(
            parse_use("USE DATABASE sales;"),
            Some(UseTarget { catalog: None, schema: Some("sales".into()) })
        );
        assert_eq!(
            parse_use("USE CATALOG lakehouse"),
            Some(UseTarget { catalog: Some("lakehouse".into()), schema: None })
        );
        assert_eq!(
            parse_use("USE lake.sales"),
            Some(UseTarget { catalog: Some("lake".into()), schema: Some("sales".into()) })
        );
        assert_eq!(parse_use("USE `my schema`").unwrap().schema.as_deref(), Some("my schema"));
        assert_eq!(parse_use("SELECT 1"), None);
    }

    #[test]
    fn show_databases_rewrite() {
        assert!(rewrite_show_databases("SHOW DATABASES")
            .unwrap()
            .contains("information_schema.schemata"));
        assert!(rewrite_show_databases("SHOW SCHEMAS")
            .unwrap()
            .contains("AS namespace"));
        let with_like = rewrite_show_databases("SHOW DATABASES LIKE 'sal%'").unwrap();
        assert!(with_like.contains("LIKE 'sal%'"));
        assert_eq!(rewrite_show_databases("SHOW TABLES"), None);
    }

    #[tokio::test]
    async fn use_changes_default_schema_end_to_end() {
        let engine = crate::SqlEngine::new();
        // USE mutates the session default schema, so a subsequent *unqualified*
        // reference to an information_schema relation now resolves.
        engine.sql("USE information_schema").await.expect("USE runs");
        let batches = engine
            .sql("SELECT count(*) AS c FROM tables")
            .await
            .expect("unqualified `tables` resolves via the new default schema")
            .collect()
            .await
            .expect("collect");
        let total: i64 = {
            use arrow::array::Int64Array;
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert!(total > 0, "information_schema.tables should be non-empty");
    }

    #[tokio::test]
    async fn show_databases_lists_schemas() {
        let engine = crate::SqlEngine::new();
        let batches = engine
            .sql("SHOW DATABASES")
            .await
            .expect("SHOW DATABASES runs")
            .collect()
            .await
            .expect("collect");
        // At least the default schemas are present, under a `namespace` column.
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total >= 1);
        assert_eq!(batches[0].schema().field(0).name(), "namespace");
    }
}
