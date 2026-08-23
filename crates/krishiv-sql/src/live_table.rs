//! `CREATE LIVE TABLE` SQL extensions (R14 S1.1) — **parsed, then rejected.**
//!
//! Live tables were never implemented. The DDL parsed, wrote a name into a
//! registry and produced a `LogicalPlan` carrying `NodeOp::CreateLiveTable` /
//! `RefreshLiveTable` / `DropLiveTable` — plan ops that **no executor, planner
//! or scheduler in this repository handles**. `SqlEngine::sql` then threw the
//! plan away and answered with an empty result set, so
//! `CREATE LIVE TABLE t AS …` reported success and `SELECT * FROM t` failed
//! with "table not found".
//!
//! Rather than keep reporting success for a statement that does nothing,
//! [`execute_live_table_ddl`] now rejects every live-table statement with an
//! error that names the doors which do work:
//!
//! * `CREATE MATERIALIZED VIEW <name> AS <query>` (plus `CREATE SOURCE` /
//!   `CREATE SINK` / `START PIPELINE`) for an incrementally-maintained table —
//!   this is what `Session::create_live_table(.., Refresh::Incremental)` has
//!   always routed to, going around this module entirely; or
//! * `Session::create_live_table(name, query, Refresh::Batch)` for a one-shot
//!   materialized snapshot.
//!
//! The parser is kept so the rejection can name the statement and the table.

use crate::{SqlError, SqlResult};

/// Parsed live-table DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveTableStatement {
    Create { name: String, query: String },
    Refresh { name: String },
    Drop { name: String },
}

/// Parse `CREATE|REFRESH|DROP LIVE TABLE` statements.
pub fn parse_live_table_statement(sql: &str) -> SqlResult<Option<LiveTableStatement>> {
    let trimmed = sql.trim().trim_end_matches(';');
    // `to_ascii_uppercase`, not `to_uppercase`: offsets found in the uppercased
    // copy are applied to the original, so the two must have identical byte
    // lengths. Unicode case folding does not preserve length — `ﬁ` (U+FB01,
    // 3 bytes) uppercases to `FI` (2) — which truncated names and, when the
    // shifted offset landed inside a multi-byte character, panicked outright.
    // Every keyword matched here is ASCII, so ASCII folding is sufficient.
    let upper = trimmed.to_ascii_uppercase();

    if upper.starts_with("CREATE LIVE TABLE ") {
        let rest =
            trimmed
                .get("CREATE LIVE TABLE ".len()..)
                .ok_or_else(|| SqlError::Unsupported {
                    feature: "CREATE LIVE TABLE".into(),
                })?;
        let (name, query) = split_name_and_query(rest)?;
        return Ok(Some(LiveTableStatement::Create { name, query }));
    }

    if upper.starts_with("REFRESH LIVE TABLE ") {
        let name = trimmed
            .get("REFRESH LIVE TABLE ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "REFRESH LIVE TABLE".into(),
            })?
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        return Ok(Some(LiveTableStatement::Refresh { name }));
    }

    if upper.starts_with("DROP LIVE TABLE ") {
        let name = trimmed
            .get("DROP LIVE TABLE ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "DROP LIVE TABLE".into(),
            })?
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        return Ok(Some(LiveTableStatement::Drop { name }));
    }

    Ok(None)
}

fn split_name_and_query(rest: &str) -> SqlResult<(String, String)> {
    // Length-preserving folding — see the note in `parse_live_table_statement`.
    // `as_pos` below indexes `rest`, not `upper`.
    let upper = rest.to_ascii_uppercase();
    let as_pos = upper.find(" AS ").ok_or_else(|| SqlError::Unsupported {
        feature: "CREATE LIVE TABLE requires AS <query>".into(),
    })?;
    let name = rest[..as_pos].trim().to_string();
    let query = rest[as_pos + 4..].trim().to_string();
    if name.is_empty() {
        return Err(SqlError::EmptyTableName);
    }
    if query.is_empty() {
        return Err(SqlError::EmptyQuery);
    }
    Ok((name, query))
}

/// Reject a live-table statement, naming the doors that do work.
///
/// IVM-AUD-DDL-F1. This used to write the name into a registry, build a
/// `LogicalPlan` carrying `NodeOp::CreateLiveTable`, and return it — whereupon
/// `SqlEngine::sql` discarded the plan and answered with an empty result set.
/// No executor, planner or scheduler matched those plan ops, so the statement
/// reported success and the table did not exist; the next `SELECT` failed with
/// "table not found". Reporting success for a statement that does nothing is
/// the one outcome worse than rejecting it.
///
/// Returns `Ok(())` for anything that is not live-table DDL, so `SqlEngine`
/// can call this as a pass-through interceptor.
pub fn reject_live_table_ddl(sql: &str) -> SqlResult<()> {
    let Some(stmt) = parse_live_table_statement(sql)? else {
        return Ok(());
    };
    let (statement, name) = match &stmt {
        LiveTableStatement::Create { name, .. } => ("CREATE", name),
        LiveTableStatement::Refresh { name } => ("REFRESH", name),
        LiveTableStatement::Drop { name } => ("DROP", name),
    };
    Err(SqlError::Unsupported {
        feature: format!(
            "{statement} LIVE TABLE {name}: live tables are not implemented. \
             For an incrementally-maintained table use \
             `CREATE MATERIALIZED VIEW {name} AS <query>` (with CREATE SOURCE / \
             CREATE SINK / START PIPELINE to drive it); for a one-shot \
             materialized snapshot use \
             `Session::create_live_table(\"{name}\", <query>, Refresh::Batch)`"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_live_table() {
        let stmt = parse_live_table_statement(
            "CREATE LIVE TABLE orders_summary AS SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
        )
        .unwrap()
        .unwrap();
        match stmt {
            LiveTableStatement::Create { name, query } => {
                assert_eq!(name, "orders_summary");
                assert!(query.contains("SUM(amount)"));
            }
            _ => panic!("expected create"),
        }
    }

    /// The name offset is found in the uppercased copy and applied to the
    /// original, so the two must have identical byte lengths. Unicode
    /// `to_uppercase` does not guarantee that: `ﬁ` (U+FB01, 3 bytes) becomes
    /// `FI` (2 bytes), shifting every later offset.
    #[test]
    fn a_name_whose_uppercase_is_shorter_is_not_truncated() {
        let stmt = parse_live_table_statement("CREATE LIVE TABLE \u{FB01}x AS SELECT 1")
            .unwrap()
            .unwrap();
        match stmt {
            LiveTableStatement::Create { name, query } => {
                assert_eq!(name, "\u{FB01}x", "the whole name must survive");
                assert_eq!(query, "SELECT 1");
            }
            other => panic!("expected create, got {other:?}"),
        }
    }

    /// ...and when the shifted offset lands inside a multi-byte character,
    /// slicing the original panics outright.
    #[test]
    fn a_shifted_offset_landing_mid_character_does_not_panic() {
        let stmt = parse_live_table_statement("CREATE LIVE TABLE \u{FB01}\u{E9} AS SELECT 1")
            .unwrap()
            .unwrap();
        match stmt {
            LiveTableStatement::Create { name, .. } => {
                assert_eq!(name, "\u{FB01}\u{E9}");
            }
            other => panic!("expected create, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_missing_as_errors() {
        let err = parse_live_table_statement("CREATE LIVE TABLE t SELECT 1").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
    }

    #[test]
    fn parse_refresh_and_drop() {
        let r = parse_live_table_statement("REFRESH LIVE TABLE orders_summary")
            .unwrap()
            .unwrap();
        assert!(matches!(r, LiveTableStatement::Refresh { .. }));
        let d = parse_live_table_statement("DROP LIVE TABLE orders_summary")
            .unwrap()
            .unwrap();
        assert!(matches!(d, LiveTableStatement::Drop { .. }));
    }

    // ── every live-table statement is rejected ───────────────────────────────

    /// DDL-F1. `CREATE LIVE TABLE` used to return a plan and a `success`; the
    /// plan op had no handler, so the table never existed and the next
    /// `SELECT` failed with "table not found". It must fail at the statement.
    #[test]
    fn create_live_table_is_rejected_and_names_a_working_alternative() {
        let err = reject_live_table_ddl(
            "CREATE LIVE TABLE summary AS SELECT id, SUM(val) FROM events GROUP BY id",
        )
        .expect_err("CREATE LIVE TABLE must not report success for a no-op");

        match err {
            SqlError::Unsupported { feature } => {
                assert!(
                    feature.contains("summary"),
                    "the error must name the table; got {feature}"
                );
                assert!(
                    feature.contains("CREATE MATERIALIZED VIEW"),
                    "the error must name the working alternative; got {feature}"
                );
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn refresh_and_drop_live_table_are_rejected_too() {
        for sql in ["REFRESH LIVE TABLE summary", "DROP LIVE TABLE summary"] {
            let err = reject_live_table_ddl(sql).unwrap_err().to_string();
            assert!(
                err.contains("live tables are not implemented"),
                "{sql} must be rejected; got {err}"
            );
        }
    }

    /// The fall-through contract the `SqlEngine::sql` interceptor relies on:
    /// non-live-table SQL is not this module's business.
    #[test]
    fn non_live_table_sql_passes_through() {
        reject_live_table_ddl("SELECT 1 AS n")
            .expect("non-live-table SQL must pass through the interceptor untouched");
    }
}
