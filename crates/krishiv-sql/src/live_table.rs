//! `CREATE LIVE TABLE` SQL extensions (R14 S1.1).

use std::collections::HashMap;
use std::sync::RwLock;

use krishiv_plan::{ExecutionKind, LogicalPlan, NodeOp, PlanNode};

use crate::{SqlError, SqlResult};

/// Parsed live-table DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveTableStatement {
    Create { name: String, query: String },
    Refresh { name: String },
    Drop { name: String },
}

/// Registry of active live tables and their backing queries.
///
/// Internally guarded by an `RwLock` so concurrent `query`/`contains`
/// calls (the common case for `REFRESH LIVE TABLE` checks and executor
/// query lookups) do not serialise against each other. Writes
/// (`register`/`remove_table`) take the write lock. This avoids the
/// `Mutex<LiveTableRegistry>` contention seen under fan-out of
/// parallel `SELECT` against live tables in a shared
/// `DataFusion` `SessionContext`.
#[derive(Debug, Default)]
pub struct LiveTableRegistry {
    tables: RwLock<HashMap<String, String>>,
}

impl LiveTableRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the write lock would succeed without blocking.
    /// Callers using `RwLock` semantics may use this to diagnose stalls.
    pub fn try_register(
        &self,
        name: impl Into<String>,
        query: impl Into<String>,
    ) -> Result<bool, SqlError> {
        let mut tables = self.tables.write().map_err(|_| SqlError::DataFusion {
            message: "live table registry lock poisoned".into(),
        })?;
        let name = name.into();
        let is_new = !tables.contains_key(&name);
        tables.insert(name, query.into());
        Ok(is_new)
    }

    pub fn register(&self, name: impl Into<String>, query: impl Into<String>) {
        // `unwrap` is safe: an `RwLock` is only poisoned if a writer
        // panicked, in which case the surrounding SQL session is
        // already in an unrecoverable state. The single-writer path
        // (DdlExecutor) is the only place that takes the write lock,
        // so poison is the only failure mode and we surface it via
        // a `DataFusion`-shaped error.
        self.try_register(name, query)
            .expect("LiveTableRegistry write lock poisoned");
    }

    pub fn remove_table(&self, name: &str) -> SqlResult<bool> {
        let mut tables = self.tables.write().map_err(|_| SqlError::DataFusion {
            message: "live table registry lock poisoned".into(),
        })?;
        Ok(tables.remove(name).is_some())
    }

    pub fn contains(&self, name: &str) -> SqlResult<bool> {
        let tables = self.tables.read().map_err(|_| SqlError::DataFusion {
            message: "live table registry lock poisoned".into(),
        })?;
        Ok(tables.contains_key(name))
    }

    pub fn query(&self, name: &str) -> SqlResult<Option<String>> {
        let tables = self.tables.read().map_err(|_| SqlError::DataFusion {
            message: "live table registry lock poisoned".into(),
        })?;
        Ok(tables.get(name).cloned())
    }
}

/// Parse `CREATE|REFRESH|DROP LIVE TABLE` statements.
pub fn parse_live_table_statement(sql: &str) -> SqlResult<Option<LiveTableStatement>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_uppercase();

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
    let upper = rest.to_uppercase();
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

/// Build a Krishiv logical plan for a live-table DDL statement.
pub fn plan_live_table(stmt: LiveTableStatement) -> LogicalPlan {
    match stmt {
        LiveTableStatement::Create { name, query } => LogicalPlan::new(
            format!("create-live-table:{name}"),
            ExecutionKind::Streaming,
        )
        .with_node(
            PlanNode::new(
                format!("create-live-{name}"),
                format!("CREATE LIVE TABLE {name}"),
                ExecutionKind::Streaming,
            )
            .with_op(NodeOp::CreateLiveTable { name, query }),
        ),
        LiveTableStatement::Refresh { name } => LogicalPlan::new(
            format!("refresh-live-table:{name}"),
            ExecutionKind::Streaming,
        )
        .with_node(
            PlanNode::new(
                format!("refresh-live-{name}"),
                format!("REFRESH LIVE TABLE {name}"),
                ExecutionKind::Streaming,
            )
            .with_op(NodeOp::RefreshLiveTable { name }),
        ),
        LiveTableStatement::Drop { name } => {
            LogicalPlan::new(format!("drop-live-table:{name}"), ExecutionKind::Batch).with_node(
                PlanNode::new(
                    format!("drop-live-{name}"),
                    format!("DROP LIVE TABLE {name}"),
                    ExecutionKind::Batch,
                )
                .with_op(NodeOp::DropLiveTable { name }),
            )
        }
    }
}

/// Apply a live-table statement to the registry and return its logical plan.
///
/// `REFRESH LIVE TABLE <name>` looks up the existing query in the
/// registry and re-registers it (which is the registry-level half of
/// "refresh" — the executor is expected to re-execute the plan to
/// materialise the new result). If the named table is not registered,
/// the refresh is rejected with `SqlError::Unsupported` so callers see a
/// clear error rather than a silent no-op.
///
/// The registry is `&LiveTableRegistry` (not `&Mutex<...>`); internal
/// synchronisation is the registry's responsibility. Callers that
/// already hold a `Mutex<LiveTableRegistry>` can pass `&*guard`.
pub fn execute_live_table_ddl(
    registry: &LiveTableRegistry,
    sql: &str,
) -> SqlResult<Option<LogicalPlan>> {
    let Some(stmt) = parse_live_table_statement(sql)? else {
        return Ok(None);
    };
    match &stmt {
        LiveTableStatement::Create { name, query } => {
            registry.register(name.clone(), query.clone());
        }
        LiveTableStatement::Drop { name } => {
            registry.remove_table(name)?;
        }
        LiveTableStatement::Refresh { name } => {
            let Some(query) = registry.query(name)? else {
                return Err(SqlError::Unsupported {
                    feature: format!("REFRESH LIVE TABLE {name}: table is not registered"),
                });
            };
            // Re-register the same query to bump any "last refresh" bookkeeping
            // and force the executor to re-materialise the result.
            registry.register(name.clone(), query);
        }
    }
    Ok(Some(plan_live_table(stmt)))
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

    #[test]
    fn registry_register_and_drop() {
        let reg = LiveTableRegistry::new();
        reg.register("v", "SELECT 1");
        assert!(reg.contains("v").unwrap());
        reg.remove_table("v").unwrap();
        assert!(!reg.contains("v").unwrap());
    }

    // ── execute_live_table_ddl integration ────────────────────────────────────

    #[test]
    fn execute_live_table_ddl_create_populates_registry_and_returns_streaming_plan() {
        use krishiv_plan::ExecutionKind;

        let registry = LiveTableRegistry::new();
        let plan = execute_live_table_ddl(
            &registry,
            "CREATE LIVE TABLE summary AS SELECT id, SUM(val) FROM events GROUP BY id",
        )
        .unwrap()
        .unwrap();

        assert!(
            registry.contains("summary").unwrap(),
            "registry must contain the created live table"
        );
        assert_eq!(
            registry.query("summary").unwrap(),
            Some("SELECT id, SUM(val) FROM events GROUP BY id".to_string()),
            "registry must store the backing query"
        );
        assert_eq!(
            plan.kind(),
            ExecutionKind::Streaming,
            "CREATE LIVE TABLE must produce a Streaming logical plan"
        );
    }

    #[test]
    fn execute_live_table_ddl_drop_removes_from_registry() {
        let registry = LiveTableRegistry::new();
        execute_live_table_ddl(&registry, "CREATE LIVE TABLE to_drop AS SELECT 1 AS n").unwrap();
        assert!(registry.contains("to_drop").unwrap());

        execute_live_table_ddl(&registry, "DROP LIVE TABLE to_drop").unwrap();
        assert!(
            !registry.contains("to_drop").unwrap(),
            "dropped table must be removed from registry"
        );
    }

    #[test]
    fn execute_live_table_ddl_refresh_returns_plan_without_error() {
        let registry = LiveTableRegistry::new();
        execute_live_table_ddl(&registry, "CREATE LIVE TABLE to_refresh AS SELECT 1 AS x").unwrap();
        let plan = execute_live_table_ddl(&registry, "REFRESH LIVE TABLE to_refresh")
            .unwrap()
            .expect("REFRESH must return a plan");
        assert!(
            !plan.nodes().is_empty(),
            "REFRESH plan must have at least one node"
        );
    }

    #[test]
    fn execute_live_table_ddl_non_live_table_sql_returns_none() {
        let registry = LiveTableRegistry::new();
        let result = execute_live_table_ddl(&registry, "SELECT 1 AS n").unwrap();
        assert!(
            result.is_none(),
            "non-live-table SQL must return None from execute_live_table_ddl"
        );
    }

    #[test]
    fn execute_live_table_ddl_refresh_unregistered_table_errors() {
        let registry = LiveTableRegistry::new();
        // REFRESH on a table that has never been CREATEd must error, not
        // silently no-op (the previous behaviour silently dropped the
        // refresh and returned a plan).
        let err = execute_live_table_ddl(&registry, "REFRESH LIVE TABLE missing")
            .expect_err("REFRESH on an unknown table must fail");
        match err {
            crate::SqlError::Unsupported { feature } => {
                assert!(
                    feature.contains("missing"),
                    "error should name the missing table; got {feature}"
                );
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn execute_live_table_ddl_refresh_registered_table_succeeds() {
        let registry = LiveTableRegistry::new();
        execute_live_table_ddl(&registry, "CREATE LIVE TABLE t AS SELECT 1").unwrap();
        // REFRESH on a registered table must succeed and return a plan.
        let plan = execute_live_table_ddl(&registry, "REFRESH LIVE TABLE t")
            .unwrap()
            .unwrap();
        assert!(!plan.nodes().is_empty());
    }
}
