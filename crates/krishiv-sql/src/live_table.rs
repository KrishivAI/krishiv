//! `CREATE LIVE TABLE` SQL extensions (R14 S1.1).

use std::collections::HashMap;
use std::sync::Mutex;

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
#[derive(Debug, Default)]
pub struct LiveTableRegistry {
    tables: HashMap<String, String>,
}

impl LiveTableRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, name: impl Into<String>, query: impl Into<String>) {
        self.tables.insert(name.into(), query.into());
    }

    pub fn remove_table(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn query(&self, name: &str) -> Option<&str> {
        self.tables.get(name).map(String::as_str)
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
pub fn execute_live_table_ddl(
    registry: &Mutex<LiveTableRegistry>,
    sql: &str,
) -> SqlResult<Option<LogicalPlan>> {
    let Some(stmt) = parse_live_table_statement(sql)? else {
        return Ok(None);
    };
    match &stmt {
        LiveTableStatement::Create { name, query } => {
            registry
                .lock()
                .map_err(|_| SqlError::DataFusion {
                    message: "live table registry lock poisoned".into(),
                })?
                .register(name.clone(), query.clone());
        }
        LiveTableStatement::Drop { name } => {
            let mut reg = registry.lock().map_err(|_| SqlError::DataFusion {
                message: "live table registry lock poisoned".into(),
            })?;
            reg.remove_table(name);
        }
        LiveTableStatement::Refresh { .. } => {}
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
        let mut reg = LiveTableRegistry::new();
        reg.register("v", "SELECT 1");
        assert!(reg.contains("v"));
        reg.remove_table("v");
        assert!(!reg.contains("v"));
    }

    // ── execute_live_table_ddl integration ────────────────────────────────────

    #[test]
    fn execute_live_table_ddl_create_populates_registry_and_returns_streaming_plan() {
        use krishiv_plan::ExecutionKind;
        use std::sync::Mutex;

        let registry = Mutex::new(LiveTableRegistry::new());
        let plan = execute_live_table_ddl(
            &registry,
            "CREATE LIVE TABLE summary AS SELECT id, SUM(val) FROM events GROUP BY id",
        )
        .unwrap()
        .unwrap();

        let guard = registry.lock().unwrap();
        assert!(
            guard.contains("summary"),
            "registry must contain the created live table"
        );
        assert_eq!(
            guard.query("summary"),
            Some("SELECT id, SUM(val) FROM events GROUP BY id"),
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
        use std::sync::Mutex;

        let registry = Mutex::new(LiveTableRegistry::new());
        execute_live_table_ddl(&registry, "CREATE LIVE TABLE to_drop AS SELECT 1 AS n").unwrap();
        assert!(registry.lock().unwrap().contains("to_drop"));

        execute_live_table_ddl(&registry, "DROP LIVE TABLE to_drop").unwrap();
        assert!(
            !registry.lock().unwrap().contains("to_drop"),
            "dropped table must be removed from registry"
        );
    }

    #[test]
    fn execute_live_table_ddl_refresh_returns_plan_without_error() {
        use std::sync::Mutex;

        let registry = Mutex::new(LiveTableRegistry::new());
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
        use std::sync::Mutex;
        let registry = Mutex::new(LiveTableRegistry::new());
        let result = execute_live_table_ddl(&registry, "SELECT 1 AS n").unwrap();
        assert!(
            result.is_none(),
            "non-live-table SQL must return None from execute_live_table_ddl"
        );
    }
}
