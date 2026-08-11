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
//! Phase 60 close-out adds the remaining itemized statements:
//!
//! - **`TRUNCATE TABLE <t>`** — parsed here; the engine empties a memory
//!   table by provider swap and routes an Iceberg table through the
//!   copy-on-write delete path (parse only in this module).
//! - **`CACHE / UNCACHE TABLE <t>` and `CLEAR CACHE`** — session-scoped
//!   materialization (parse here, provider swap/restore in the engine).
//! - **`DESCRIBE FUNCTION|DATABASE <name>`** — rewritten to
//!   `information_schema` queries; **`DESCRIBE QUERY <select>`** — parsed
//!   here, schema described by the engine.
//! - **`SHOW PARTITIONS <t>`** — parsed here; the engine answers from the
//!   Iceberg partition spec (a clear error on unpartitioned/non-Iceberg
//!   tables, never a silently empty result).

use datafusion::prelude::SessionContext;

/// The parsed target of a `USE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseTarget {
    /// New default catalog, if the statement set one.
    pub catalog: Option<String>,
    /// New default schema, if the statement set one.
    pub schema: Option<String>,
}

/// Whether `ident` is wrapped in a matching pair of backticks or double quotes.
fn is_quoted(ident: &str) -> bool {
    let t = ident.trim();
    ['`', '"']
        .iter()
        .any(|q| t.len() >= 2 && t.starts_with(*q) && t.ends_with(*q))
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
    // A quoted identifier is one name even when it contains a dot: ``USE
    // `my.schema` `` asks for the schema literally called `my.schema`, not for
    // catalog `my` / schema `schema`. Only an *unquoted* name is qualified.
    let was_quoted = is_quoted(name_part);
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
    if let Some((cat, sch)) = name.split_once('.').filter(|_| !was_quoted) {
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

    // Split the tail into the optional `{FROM|IN} <catalog>` qualifier and the
    // optional `LIKE 'pattern'` filter.
    //
    // The catalog qualifier used to be ignored outright: `SHOW SCHEMAS IN prod`
    // listed the schemas of *every* catalog, silently answering a broader
    // question than the one asked. Spark's grammar is
    // `SHOW SCHEMAS [ { FROM | IN } catalog ] [ LIKE pattern ]`.
    let (before_like, like_pattern) = match upper.find(" LIKE ") {
        Some(idx) => (
            q.get(..idx).unwrap_or_default(),
            q.get(idx + " LIKE ".len()..).map(str::trim),
        ),
        None => (q, None),
    };

    let mut predicates: Vec<String> = Vec::new();
    if let Some(catalog) = catalog_qualifier(before_like) {
        // Single-quote escaping so a catalog name containing `'` cannot end the
        // literal early.
        predicates.push(format!(
            "catalog_name = '{}'",
            catalog.replace('\'', "''")
        ));
    }
    if let Some(pattern) = like_pattern.filter(|p| !p.is_empty()) {
        predicates.push(format!("schema_name LIKE {pattern}"));
    }

    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    Some(format!(
        "SELECT schema_name AS namespace FROM information_schema.schemata{where_clause} \
         ORDER BY namespace"
    ))
}

/// Parse `TRUNCATE TABLE <name>` (the `TABLE` keyword optional, per Spark)
/// into the table reference, or `None` for any other statement.
pub fn parse_truncate(query: &str) -> Option<String> {
    let q = query.trim().trim_end_matches(';').trim();
    let upper = q.to_ascii_uppercase();
    let rest = upper
        .strip_prefix("TRUNCATE TABLE ")
        .map(|_| &q["TRUNCATE TABLE ".len()..])
        .or_else(|| upper.strip_prefix("TRUNCATE ").map(|_| &q["TRUNCATE ".len()..]))?;
    let name = rest.trim();
    (!name.is_empty() && !name.contains(char::is_whitespace)).then(|| unquote(name))
}

/// A parsed cache-family statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheStatement {
    /// `CACHE TABLE <t>` — materialize and swap the provider.
    Cache(String),
    /// `UNCACHE TABLE <t>` — restore the original provider.
    Uncache(String),
    /// `CLEAR CACHE` — restore every cached table.
    Clear,
}

/// Parse the `CACHE TABLE` family, or `None` for other statements.
///
/// `Some(Err(_))` marks a cache statement using an unsupported form
/// (`LAZY`, `AS SELECT`, `OPTIONS`) — refused with direction rather than
/// silently approximated.
pub fn parse_cache(query: &str) -> Option<Result<CacheStatement, String>> {
    let q = query.trim().trim_end_matches(';').trim();
    let upper = q.to_ascii_uppercase();
    if upper == "CLEAR CACHE" {
        return Some(Ok(CacheStatement::Clear));
    }
    if upper.starts_with("CACHE LAZY") {
        return Some(Err(
            "CACHE LAZY TABLE is not supported — Krishiv caches eagerly; drop LAZY".into(),
        ));
    }
    for (prefix, is_cache) in [("CACHE TABLE ", true), ("UNCACHE TABLE ", false)] {
        if let Some(rest) = upper.strip_prefix(prefix) {
            if is_cache && (rest.contains(" AS ") || rest.contains("OPTIONS")) {
                return Some(Err(
                    "CACHE TABLE … AS SELECT / OPTIONS are not supported — CREATE VIEW the \
                     query, then CACHE TABLE the view"
                        .into(),
                ));
            }
            let name = q[prefix.len()..].trim();
            if name.is_empty() || name.contains(char::is_whitespace) {
                return Some(Err(format!(
                    "{} expects exactly one table name",
                    prefix.trim()
                )));
            }
            let name = unquote(name);
            return Some(Ok(if is_cache {
                CacheStatement::Cache(name)
            } else {
                CacheStatement::Uncache(name)
            }));
        }
    }
    None
}

/// Rewrite `DESCRIBE FUNCTION <name>` to an `information_schema.routines`
/// query (Spark-shaped columns), or `None` for other statements.
pub fn rewrite_describe_function(query: &str) -> Option<String> {
    let q = query.trim().trim_end_matches(';').trim();
    let upper = q.to_ascii_uppercase();
    let rest = ["DESCRIBE FUNCTION EXTENDED ", "DESCRIBE FUNCTION ", "DESC FUNCTION "]
        .iter()
        .find_map(|p| upper.starts_with(p).then(|| q[p.len()..].trim()))?;
    if rest.is_empty() || rest.contains(char::is_whitespace) {
        return None;
    }
    let name = unquote(rest).replace('\'', "''");
    Some(format!(
        "SELECT routine_name AS function_name, data_type AS return_type, \
                description, syntax_example \
         FROM information_schema.routines \
         WHERE lower(routine_name) = lower('{name}') \
         ORDER BY 2"
    ))
}

/// Rewrite `DESCRIBE DATABASE|SCHEMA <name>` to an
/// `information_schema.schemata` query, or `None` for other statements.
pub fn rewrite_describe_database(query: &str) -> Option<String> {
    let q = query.trim().trim_end_matches(';').trim();
    let upper = q.to_ascii_uppercase();
    let rest = [
        "DESCRIBE DATABASE EXTENDED ",
        "DESCRIBE DATABASE ",
        "DESCRIBE SCHEMA ",
        "DESC DATABASE ",
        "DESC SCHEMA ",
    ]
    .iter()
    .find_map(|p| upper.starts_with(p).then(|| q[p.len()..].trim()))?;
    if rest.is_empty() || rest.contains(char::is_whitespace) {
        return None;
    }
    let name = unquote(rest).replace('\'', "''");
    Some(format!(
        "SELECT catalog_name AS catalog, schema_name AS namespace \
         FROM information_schema.schemata \
         WHERE lower(schema_name) = lower('{name}')"
    ))
}

/// Parse `DESCRIBE QUERY <select>` into the inner query, or `None`.
pub fn parse_describe_query(query: &str) -> Option<String> {
    let q = query.trim().trim_end_matches(';').trim();
    let upper = q.to_ascii_uppercase();
    ["DESCRIBE QUERY ", "DESC QUERY "]
        .iter()
        .find_map(|p| upper.starts_with(p).then(|| q[p.len()..].trim().to_string()))
        .filter(|inner| !inner.is_empty())
}

/// Parse `SHOW PARTITIONS <table>` into the table reference, or `None`.
/// (Spark's optional `PARTITION (spec)` filter is not accepted in v1 —
/// the caller sees the whole tail and refuses it with direction.)
pub fn parse_show_partitions(query: &str) -> Option<String> {
    let q = query.trim().trim_end_matches(';').trim();
    let upper = q.to_ascii_uppercase();
    let rest = upper
        .strip_prefix("SHOW PARTITIONS ")
        .map(|_| q["SHOW PARTITIONS ".len()..].trim())?;
    (!rest.is_empty()).then(|| unquote(rest))
}

/// The catalog named by a trailing `{FROM|IN} <catalog>`, if present.
///
/// `head` is the statement up to any `LIKE`, e.g. `SHOW SCHEMAS IN prod`.
fn catalog_qualifier(head: &str) -> Option<String> {
    let mut tokens = head.split_whitespace().collect::<Vec<_>>();
    let catalog = tokens.pop()?;
    let keyword = tokens.pop()?;
    if keyword.eq_ignore_ascii_case("FROM") || keyword.eq_ignore_ascii_case("IN") {
        let name = unquote(catalog);
        (!name.is_empty()).then_some(name)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_use_forms() {
        assert_eq!(
            parse_use("USE analytics"),
            Some(UseTarget {
                catalog: None,
                schema: Some("analytics".into())
            })
        );
        assert_eq!(
            parse_use("USE SCHEMA sales"),
            Some(UseTarget {
                catalog: None,
                schema: Some("sales".into())
            })
        );
        assert_eq!(
            parse_use("USE DATABASE sales;"),
            Some(UseTarget {
                catalog: None,
                schema: Some("sales".into())
            })
        );
        assert_eq!(
            parse_use("USE CATALOG lakehouse"),
            Some(UseTarget {
                catalog: Some("lakehouse".into()),
                schema: None
            })
        );
        assert_eq!(
            parse_use("USE lake.sales"),
            Some(UseTarget {
                catalog: Some("lake".into()),
                schema: Some("sales".into())
            })
        );
        assert_eq!(
            parse_use("USE `my schema`").unwrap().schema.as_deref(),
            Some("my schema")
        );
        assert_eq!(parse_use("SELECT 1"), None);
    }

    #[test]
    fn show_databases_rewrite() {
        assert!(
            rewrite_show_databases("SHOW DATABASES")
                .unwrap()
                .contains("information_schema.schemata")
        );
        assert!(
            rewrite_show_databases("SHOW SCHEMAS")
                .unwrap()
                .contains("AS namespace")
        );
        let with_like = rewrite_show_databases("SHOW DATABASES LIKE 'sal%'").unwrap();
        assert!(with_like.contains("LIKE 'sal%'"));
        assert_eq!(rewrite_show_databases("SHOW TABLES"), None);
    }

    /// `SHOW SCHEMAS { FROM | IN } <catalog>` must scope to that catalog.
    ///
    /// The qualifier used to be ignored outright, so the statement listed the
    /// schemas of *every* catalog — a silently broader answer than the one
    /// asked for.
    #[test]
    fn show_databases_honours_the_catalog_qualifier() {
        for sql in ["SHOW SCHEMAS IN prod", "SHOW DATABASES FROM prod"] {
            let rewritten = rewrite_show_databases(sql).expect("recognised");
            assert!(
                rewritten.contains("catalog_name = 'prod'"),
                "{sql} must filter by catalog: {rewritten}"
            );
        }
    }

    /// Both filters compose rather than one displacing the other.
    #[test]
    fn catalog_qualifier_and_like_compose() {
        let rewritten =
            rewrite_show_databases("SHOW SCHEMAS IN prod LIKE 'sal%'").expect("recognised");
        assert!(rewritten.contains("catalog_name = 'prod'"), "{rewritten}");
        assert!(rewritten.contains("schema_name LIKE 'sal%'"), "{rewritten}");
        assert!(rewritten.contains(" AND "), "{rewritten}");
    }

    /// A bare `SHOW DATABASES` still has no WHERE at all.
    #[test]
    fn bare_show_databases_is_unfiltered() {
        let rewritten = rewrite_show_databases("SHOW DATABASES").expect("recognised");
        assert!(!rewritten.contains("WHERE"), "{rewritten}");
    }

    /// A quoted identifier is one name even when it contains a dot.
    #[test]
    fn a_quoted_use_target_is_not_split_on_its_dot() {
        assert_eq!(
            parse_use("USE `my.schema`"),
            Some(UseTarget {
                catalog: None,
                schema: Some("my.schema".into())
            }),
            "a back-quoted name is a single identifier, not catalog.schema"
        );
        // ...while an unquoted one still qualifies.
        assert_eq!(
            parse_use("USE lake.sales"),
            Some(UseTarget {
                catalog: Some("lake".into()),
                schema: Some("sales".into())
            })
        );
    }

    #[tokio::test]
    async fn use_changes_default_schema_end_to_end() {
        let engine = crate::SqlEngine::new();
        // USE mutates the session default schema, so a subsequent *unqualified*
        // reference to an information_schema relation now resolves.
        engine
            .sql("USE information_schema")
            .await
            .expect("USE runs");
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

    #[test]
    fn phase60_statement_parsers() {
        assert_eq!(parse_truncate("TRUNCATE TABLE t"), Some("t".into()));
        assert_eq!(parse_truncate("truncate t;"), Some("t".into()));
        assert_eq!(parse_truncate("TRUNCATE TABLE `t`"), Some("t".into()));
        assert_eq!(parse_truncate("SELECT 1"), None);
        assert_eq!(parse_truncate("TRUNCATE TABLE a b"), None);

        assert_eq!(
            parse_cache("CACHE TABLE t"),
            Some(Ok(CacheStatement::Cache("t".into())))
        );
        assert_eq!(
            parse_cache("UNCACHE TABLE t;"),
            Some(Ok(CacheStatement::Uncache("t".into())))
        );
        assert_eq!(parse_cache("CLEAR CACHE"), Some(Ok(CacheStatement::Clear)));
        assert!(matches!(parse_cache("CACHE LAZY TABLE t"), Some(Err(_))));
        assert!(matches!(
            parse_cache("CACHE TABLE t AS SELECT 1"),
            Some(Err(_))
        ));
        assert_eq!(parse_cache("SELECT 1"), None);

        let f = rewrite_describe_function("DESCRIBE FUNCTION abs").unwrap();
        assert!(f.contains("information_schema.routines"), "{f}");
        assert!(f.contains("lower('abs')"), "{f}");
        assert!(rewrite_describe_function("DESCRIBE FUNCTION").is_none());

        let d = rewrite_describe_database("DESC DATABASE public").unwrap();
        assert!(d.contains("information_schema.schemata"), "{d}");

        assert_eq!(
            parse_describe_query("DESCRIBE QUERY SELECT 1 AS x"),
            Some("SELECT 1 AS x".into())
        );
        assert_eq!(parse_describe_query("DESCRIBE t"), None);

        assert_eq!(
            parse_show_partitions("SHOW PARTITIONS ns.t"),
            Some("ns.t".into())
        );
        assert_eq!(parse_show_partitions("SHOW TABLES"), None);
    }

    #[tokio::test]
    async fn truncate_empties_a_memory_table() {
        let engine = crate::SqlEngine::new();
        engine
            .sql("CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3)) AS v(x)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("create collect");
        engine
            .sql("TRUNCATE TABLE t")
            .await
            .expect("truncate runs")
            .collect()
            .await
            .expect("truncate collect");
        let rows = engine
            .sql("SELECT COUNT(*) AS n FROM t")
            .await
            .expect("count")
            .collect()
            .await
            .expect("collect");
        use arrow::array::Int64Array;
        let n = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 0, "truncated table must be empty, schema intact");
    }

    #[tokio::test]
    async fn cache_uncache_roundtrip_preserves_rows() {
        let engine = crate::SqlEngine::new();
        engine
            .sql("CREATE TABLE c AS SELECT * FROM (VALUES (10), (20)) AS v(x)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("create collect");
        for stmt in ["CACHE TABLE c", "UNCACHE TABLE c", "CACHE TABLE c", "CLEAR CACHE"] {
            engine
                .sql(stmt)
                .await
                .unwrap_or_else(|e| panic!("{stmt} failed: {e}"))
                .collect()
                .await
                .expect("collect");
            let rows = engine
                .sql("SELECT SUM(x) AS s FROM c")
                .await
                .expect("sum")
                .collect()
                .await
                .expect("collect");
            use arrow::array::Int64Array;
            let s = rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(s, 30, "rows must survive {stmt}");
        }
        let err = engine.sql("UNCACHE TABLE c").await.expect_err("not cached");
        assert!(err.to_string().contains("not cached"), "{err}");
    }

    #[tokio::test]
    async fn describe_function_database_query_run() {
        let engine = crate::SqlEngine::new();
        let batches = engine
            .sql("DESCRIBE FUNCTION abs")
            .await
            .expect("describe function")
            .collect()
            .await
            .expect("collect");
        assert!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>() >= 1,
            "abs must be described"
        );

        let batches = engine
            .sql("DESCRIBE DATABASE public")
            .await
            .expect("describe database")
            .collect()
            .await
            .expect("collect");
        assert_eq!(batches[0].schema().field(1).name(), "namespace");

        let batches = engine
            .sql("DESCRIBE QUERY SELECT 1 AS answer, 'x' AS label")
            .await
            .expect("describe query")
            .collect()
            .await
            .expect("collect");
        use arrow::array::StringArray;
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "answer");
        assert_eq!(names.value(1), "label");
    }

    #[tokio::test]
    async fn show_partitions_refuses_non_iceberg() {
        let engine = crate::SqlEngine::new();
        engine
            .sql("CREATE TABLE p AS SELECT * FROM (VALUES (1)) AS v(x)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let err = engine.sql("SHOW PARTITIONS p").await.expect_err("refused");
        assert!(
            err.to_string().contains("Iceberg"),
            "must explain why: {err}"
        );
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
