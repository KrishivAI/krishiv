#![forbid(unsafe_code)]
//! `CREATE STREAMING TABLE <name> AS <select>` — the SQL front door for a
//! continuous streaming job (Phase 60 "SQL DDL for the other two engines").
//!
//! A SQL-only client (Flight SQL / JDBC / BI / the workbench) declares a
//! continuous job as a table-producing statement, the way Databricks Delta Live
//! Tables and Flink `CREATE TABLE … AS` do. The body must lower to a continuous
//! plan through the *same* front door as every other streaming query
//! ([`crate::streaming_window_plan::compile_streaming_window_sql`]), so an
//! unsupported body fails at the planner ("cannot lower to a continuous plan"),
//! not at a bespoke matcher.
//!
//! Parsing and validation are engine-local and unit-tested here. **Executing**
//! the job — placing the operator on the streaming coordinator/executors — needs
//! a running coordinator, so the pure [`crate::SqlEngine`] surfaces a clear
//! "requires a streaming coordinator" error and a cluster-attached session
//! submits the validated plan through the continuous-stream registration API.

/// A parsed `CREATE [OR REPLACE] STREAMING TABLE <name> AS <query>` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingTableDdl {
    /// The streaming table (continuous job output) name.
    pub name: String,
    /// The windowed streaming `SELECT` that defines the job.
    pub query: String,
    /// Whether `OR REPLACE` was given.
    pub or_replace: bool,
}

/// Parse a `CREATE [OR REPLACE] STREAMING TABLE <name> AS <query>` statement,
/// or `None` if `sql` is not one.
pub fn parse_create_streaming_table(sql: &str) -> Option<StreamingTableDdl> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let (prefix, or_replace) = if upper.starts_with("CREATE OR REPLACE STREAMING TABLE ") {
        ("CREATE OR REPLACE STREAMING TABLE ", true)
    } else if upper.starts_with("CREATE STREAMING TABLE ") {
        ("CREATE STREAMING TABLE ", false)
    } else {
        return None;
    };
    let rest = trimmed.get(prefix.len()..)?;
    // `<name> AS <query>` — the first top-level ` AS ` separates them.
    let as_pos = rest.to_ascii_uppercase().find(" AS ")?;
    let name = rest.get(..as_pos)?.trim().to_string();
    let query = rest.get(as_pos + 4..)?.trim().to_string();
    if name.is_empty() || query.is_empty() {
        return None;
    }
    Some(StreamingTableDdl {
        name,
        query,
        or_replace,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_streaming_table() {
        let ddl = parse_create_streaming_table(
            "CREATE STREAMING TABLE clicks_1m AS \
             SELECT k, COUNT(*) AS c FROM TUMBLE(TABLE clicks, DESCRIPTOR(ts), 60000) \
             GROUP BY k, window_start, window_end",
        )
        .expect("recognised");
        assert_eq!(ddl.name, "clicks_1m");
        assert!(!ddl.or_replace);
        assert!(ddl.query.to_uppercase().starts_with("SELECT"));
        assert!(ddl.query.contains("TUMBLE"));
    }

    #[test]
    fn parses_or_replace() {
        let ddl = parse_create_streaming_table("CREATE OR REPLACE STREAMING TABLE t AS SELECT 1")
            .expect("recognised");
        assert_eq!(ddl.name, "t");
        assert!(ddl.or_replace);
    }

    #[test]
    fn rejects_non_streaming_table_ddl() {
        assert!(parse_create_streaming_table("CREATE TABLE t AS SELECT 1").is_none());
        assert!(parse_create_streaming_table("SELECT 1").is_none());
        assert!(parse_create_streaming_table("CREATE MATERIALIZED VIEW v AS SELECT 1").is_none());
    }

    #[test]
    fn requires_name_and_query() {
        assert!(parse_create_streaming_table("CREATE STREAMING TABLE t").is_none());
        assert!(parse_create_streaming_table("CREATE STREAMING TABLE AS SELECT 1").is_none());
    }
}
