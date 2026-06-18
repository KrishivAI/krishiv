#![forbid(unsafe_code)]

//! `CREATE INCREMENTAL VIEW` and `DECLARE RECURSIVE VIEW` SQL extensions.
//!
//! Supported DDL:
//!
//! ```sql
//! -- Non-recursive incremental view (IVM)
//! CREATE INCREMENTAL VIEW revenue AS
//!   SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
//!   LATENESS event_ts INTERVAL '5' MINUTE;
//!
//! -- Materialized variant (keeps a full snapshot in memory)
//! CREATE MATERIALIZED INCREMENTAL VIEW revenue AS ...;
//!
//! -- Recursive view (fixed-point iteration, auto-DISTINCT)
//! DECLARE RECURSIVE VIEW reachable AS
//!   SELECT dst FROM edges WHERE src = 0
//!   UNION ALL
//!   SELECT e.dst FROM edges e JOIN reachable r ON e.src = r.dst;
//!
//! -- Force a re-step (no-op for streaming; useful in batch/test mode)
//! REFRESH INCREMENTAL VIEW revenue;
//!
//! -- Remove view and its cached Trace state
//! DROP INCREMENTAL VIEW revenue;
//! ```

use std::collections::HashMap;
use std::sync::RwLock;

use crate::{SqlError, SqlResult};

// ── LATENESS spec ─────────────────────────────────────────────────────────────

/// One LATENESS annotation: `LATENESS <column> INTERVAL '<n>' <unit>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatenessAnnotation {
    pub column: String,
    pub lateness_ms: u64,
}

// ── Parsed DDL statement ───────────────────────────────────────────────────────

/// Parsed incremental-view DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalViewStatement {
    Create {
        name: String,
        body_sql: String,
        is_materialized: bool,
        lateness: Vec<LatenessAnnotation>,
    },
    DeclareRecursive {
        name: String,
        body_sql: String,
    },
    Refresh {
        name: String,
    },
    Drop {
        name: String,
    },
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Metadata stored for one registered incremental view.
#[derive(Debug, Clone)]
pub struct IncrementalViewEntry {
    pub body_sql: String,
    pub is_materialized: bool,
    pub is_recursive: bool,
    pub lateness: Vec<LatenessAnnotation>,
}

/// Registry of active incremental views (SQL metadata layer).
///
/// This is the SQL-layer registry — it stores the DDL metadata for each view.
/// The `krishiv-api` layer bridges this to the `krishiv-delta` incremental
/// operator pipeline.
#[derive(Debug, Default)]
pub struct IncrementalViewRegistry {
    views: RwLock<HashMap<String, IncrementalViewEntry>>,
}

impl IncrementalViewRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, name: impl Into<String>, entry: IncrementalViewEntry) -> SqlResult<()> {
        let mut views = self.views.write().map_err(|_| SqlError::DataFusion {
            message: "incremental view registry lock poisoned".into(),
        })?;
        views.insert(name.into(), entry);
        Ok(())
    }

    pub fn remove(&self, name: &str) -> SqlResult<bool> {
        let mut views = self.views.write().map_err(|_| SqlError::DataFusion {
            message: "incremental view registry lock poisoned".into(),
        })?;
        Ok(views.remove(name).is_some())
    }

    pub fn get(&self, name: &str) -> SqlResult<Option<IncrementalViewEntry>> {
        let views = self.views.read().map_err(|_| SqlError::DataFusion {
            message: "incremental view registry lock poisoned".into(),
        })?;
        Ok(views.get(name).cloned())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.views
            .read()
            .map(|v| v.contains_key(name))
            .unwrap_or(false)
    }

    pub fn view_names(&self) -> SqlResult<Vec<String>> {
        let views = self.views.read().map_err(|_| SqlError::DataFusion {
            message: "incremental view registry lock poisoned".into(),
        })?;
        Ok(views.keys().cloned().collect())
    }
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// Parse incremental-view DDL statements from a SQL string.
///
/// Returns `Ok(None)` if the statement is not an incremental-view DDL.
pub fn parse_incremental_view_statement(sql: &str) -> SqlResult<Option<IncrementalViewStatement>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_uppercase();

    // CREATE [MATERIALIZED] INCREMENTAL VIEW <name> AS <body>
    // [LATENESS <col> INTERVAL '<n>' <unit>]
    let is_materialized = upper.starts_with("CREATE MATERIALIZED INCREMENTAL VIEW ");
    if is_materialized || upper.starts_with("CREATE INCREMENTAL VIEW ") {
        let prefix = if is_materialized {
            "CREATE MATERIALIZED INCREMENTAL VIEW "
        } else {
            "CREATE INCREMENTAL VIEW "
        };
        let rest = trimmed
            .get(prefix.len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "CREATE INCREMENTAL VIEW".into(),
            })?;
        let (name, body_with_lateness) = split_name_and_body(rest)?;
        let (body_sql, lateness) = split_body_and_lateness(&body_with_lateness);
        return Ok(Some(IncrementalViewStatement::Create {
            name,
            body_sql,
            is_materialized,
            lateness,
        }));
    }

    // DECLARE RECURSIVE VIEW <name> AS <body>
    if upper.starts_with("DECLARE RECURSIVE VIEW ") {
        let rest = trimmed
            .get("DECLARE RECURSIVE VIEW ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "DECLARE RECURSIVE VIEW".into(),
            })?;
        let (name, body_sql) = split_name_and_body(rest)?;
        let (body_sql, _lateness) = split_body_and_lateness(&body_sql);
        return Ok(Some(IncrementalViewStatement::DeclareRecursive {
            name,
            body_sql,
        }));
    }

    // REFRESH INCREMENTAL VIEW <name>
    if upper.starts_with("REFRESH INCREMENTAL VIEW ") {
        let name = trimmed
            .get("REFRESH INCREMENTAL VIEW ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "REFRESH INCREMENTAL VIEW".into(),
            })?
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        return Ok(Some(IncrementalViewStatement::Refresh { name }));
    }

    // DROP INCREMENTAL VIEW <name>
    if upper.starts_with("DROP INCREMENTAL VIEW ") {
        let name = trimmed
            .get("DROP INCREMENTAL VIEW ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "DROP INCREMENTAL VIEW".into(),
            })?
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        return Ok(Some(IncrementalViewStatement::Drop { name }));
    }

    Ok(None)
}

/// Apply a parsed incremental-view DDL statement to the registry.
///
/// Returns `Some(name)` if the statement was an incremental-view DDL (so the
/// caller knows to return an empty DDL result rather than forwarding to
/// DataFusion), or `None` if the SQL was not an incremental-view DDL.
pub fn execute_incremental_view_ddl(
    registry: &IncrementalViewRegistry,
    sql: &str,
) -> SqlResult<Option<String>> {
    let Some(stmt) = parse_incremental_view_statement(sql)? else {
        return Ok(None);
    };

    match stmt {
        IncrementalViewStatement::Create {
            ref name,
            ref body_sql,
            is_materialized,
            ref lateness,
        } => {
            registry.register(
                name.clone(),
                IncrementalViewEntry {
                    body_sql: body_sql.clone(),
                    is_materialized,
                    is_recursive: false,
                    lateness: lateness.clone(),
                },
            )?;
            Ok(Some(name.clone()))
        }

        IncrementalViewStatement::DeclareRecursive {
            ref name,
            ref body_sql,
        } => {
            registry.register(
                name.clone(),
                IncrementalViewEntry {
                    body_sql: body_sql.clone(),
                    is_materialized: false,
                    is_recursive: true,
                    lateness: vec![],
                },
            )?;
            Ok(Some(name.clone()))
        }

        IncrementalViewStatement::Refresh { ref name } => {
            if !registry.contains(name) {
                return Err(SqlError::Unsupported {
                    feature: format!("REFRESH INCREMENTAL VIEW: view '{name}' is not registered"),
                });
            }
            Ok(Some(name.clone()))
        }

        IncrementalViewStatement::Drop { ref name } => {
            registry.remove(name)?;
            Ok(Some(name.clone()))
        }
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Split `<name> AS <body>` into `(name, body)`.
fn split_name_and_body(rest: &str) -> SqlResult<(String, String)> {
    let upper = rest.to_uppercase();
    let as_pos = upper.find(" AS ").ok_or_else(|| SqlError::Unsupported {
        feature: "CREATE INCREMENTAL VIEW / DECLARE RECURSIVE VIEW requires AS <query>".into(),
    })?;
    let name = rest[..as_pos].trim().to_string();
    let body = rest[as_pos + 4..].trim().to_string();
    if name.is_empty() {
        return Err(SqlError::EmptyTableName);
    }
    if body.is_empty() {
        return Err(SqlError::EmptyQuery);
    }
    Ok((name, body))
}

/// Split the view body from trailing `LATENESS` annotations.
///
/// Grammar: `<body_sql> LATENESS <col> INTERVAL '<n>' <unit> [, ...]`
/// where unit is SECOND | MINUTE | HOUR | DAY.
///
/// If no LATENESS clause is found, returns `(body, vec![])`.
fn split_body_and_lateness(body_with_lateness: &str) -> (String, Vec<LatenessAnnotation>) {
    let upper = body_with_lateness.to_uppercase();

    // Find the LAST occurrence of LATENESS (it follows the body SQL).
    // We look for the keyword followed by a valid column name and INTERVAL.
    let Some(lat_pos) = find_lateness_clause_start(&upper) else {
        return (body_with_lateness.trim().to_string(), vec![]);
    };

    let body_sql = body_with_lateness[..lat_pos].trim().to_string();
    let lateness_str = &body_with_lateness[lat_pos..];
    let lateness = parse_lateness_clauses(lateness_str);
    (body_sql, lateness)
}

/// Find the byte offset of the first top-level LATENESS keyword in `upper`.
fn find_lateness_clause_start(upper: &str) -> Option<usize> {
    // Simple scan: look for the word LATENESS not inside parentheses.
    let bytes = upper.as_bytes();
    let keyword = b"LATENESS";
    let mut depth = 0usize;
    let mut i = 0usize;
    while i + keyword.len() <= bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            _ if depth == 0 && bytes[i..].starts_with(keyword) => {
                // Ensure it's a word boundary
                let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                let after = i + keyword.len();
                let after_ok = after >= bytes.len() || !bytes[after].is_ascii_alphanumeric();
                if before_ok && after_ok {
                    return Some(i);
                }
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }
    None
}

/// Parse one or more `LATENESS <col> INTERVAL '<n>' <unit>` clauses.
fn parse_lateness_clauses(lateness_str: &str) -> Vec<LatenessAnnotation> {
    // Tokenize: split on LATENESS keyword (handling multiple)
    let upper = lateness_str.to_uppercase();
    let mut result = Vec::new();
    let mut remaining = lateness_str.trim();

    loop {
        let upper_rem = remaining.to_uppercase();
        let stripped = if upper_rem.starts_with("LATENESS ") {
            &remaining["LATENESS ".len()..]
        } else if upper_rem.starts_with(", LATENESS ") {
            &remaining[", LATENESS ".len()..]
        } else {
            break;
        };

        // Parse: <col> INTERVAL '<n>' <unit>
        let tokens: Vec<&str> = stripped.splitn(5, char::is_whitespace).collect();
        if tokens.len() < 4 {
            break;
        }
        let col = tokens[0].trim_matches(',').to_string();
        // tokens[1] should be INTERVAL (case-insensitive)
        let interval_str = tokens[2].trim_matches('\'');
        let unit_str = if tokens.len() >= 4 {
            tokens[3].trim_matches(',')
        } else {
            ""
        };
        let n: u64 = interval_str.parse().unwrap_or(0);
        let ms = match unit_str.to_uppercase().as_str() {
            "SECOND" | "SECONDS" => n * 1000,
            "MINUTE" | "MINUTES" => n * 60_000,
            "HOUR" | "HOURS" => n * 3_600_000,
            "DAY" | "DAYS" => n * 86_400_000,
            "MILLISECOND" | "MILLISECONDS" | "MS" => n,
            _ => n * 60_000, // default: treat as minutes
        };

        result.push(LatenessAnnotation {
            column: col,
            lateness_ms: ms,
        });

        // Advance past this clause
        let consumed_upper: String = upper_rem
            .chars()
            .take("LATENESS ".len() + stripped.len() - stripped.trim_start().len())
            .collect();
        let _ = consumed_upper; // advance is approximate; find next LATENESS
        // Find next "LATENESS" or ", LATENESS" in remaining
        let next = remaining[1..].to_uppercase().find("LATENESS");
        match next {
            Some(pos) => {
                remaining = &remaining[1 + pos..];
            }
            None => break,
        }
    }

    let _ = upper; // suppress unused warning
    result
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_incremental_view() {
        let sql = "CREATE INCREMENTAL VIEW revenue AS SELECT SUM(amount) FROM orders";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Create { ref name, is_materialized: false, .. }
            if name == "revenue"
        ));
    }

    #[test]
    fn parse_create_materialized_incremental_view() {
        let sql = "CREATE MATERIALIZED INCREMENTAL VIEW snap AS SELECT * FROM t";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Create {
                is_materialized: true,
                ..
            }
        ));
    }

    #[test]
    fn parse_declare_recursive_view() {
        let sql = "DECLARE RECURSIVE VIEW reach AS SELECT dst FROM edges WHERE src = 0";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::DeclareRecursive { ref name, .. } if name == "reach"
        ));
    }

    #[test]
    fn parse_refresh_incremental_view() {
        let sql = "REFRESH INCREMENTAL VIEW revenue";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Refresh { ref name } if name == "revenue"
        ));
    }

    #[test]
    fn parse_drop_incremental_view() {
        let sql = "DROP INCREMENTAL VIEW revenue;";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Drop { ref name } if name == "revenue"
        ));
    }

    #[test]
    fn non_incremental_sql_returns_none() {
        let sql = "SELECT 1";
        assert!(parse_incremental_view_statement(sql).unwrap().is_none());
    }

    #[test]
    fn parse_create_with_lateness() {
        let sql =
            "CREATE INCREMENTAL VIEW ev AS SELECT * FROM s LATENESS event_ts INTERVAL '5' MINUTE";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        if let IncrementalViewStatement::Create { lateness, .. } = stmt {
            assert_eq!(lateness.len(), 1);
            assert_eq!(lateness[0].column, "event_ts");
            assert_eq!(lateness[0].lateness_ms, 5 * 60_000);
        } else {
            panic!("expected Create");
        }
    }

    #[test]
    fn registry_register_and_get() {
        let reg = IncrementalViewRegistry::new();
        reg.register(
            "v1",
            IncrementalViewEntry {
                body_sql: "SELECT 1".into(),
                is_materialized: false,
                is_recursive: false,
                lateness: vec![],
            },
        )
        .unwrap();
        assert!(reg.contains("v1"));
        let entry = reg.get("v1").unwrap().unwrap();
        assert_eq!(entry.body_sql, "SELECT 1");
    }

    #[test]
    fn execute_ddl_create_and_drop() {
        let reg = IncrementalViewRegistry::new();
        let name =
            execute_incremental_view_ddl(&reg, "CREATE INCREMENTAL VIEW v AS SELECT 1").unwrap();
        assert_eq!(name.as_deref(), Some("v"));
        assert!(reg.contains("v"));

        execute_incremental_view_ddl(&reg, "DROP INCREMENTAL VIEW v").unwrap();
        assert!(!reg.contains("v"));
    }

    #[test]
    fn execute_ddl_refresh_missing_returns_error() {
        let reg = IncrementalViewRegistry::new();
        let err = execute_incremental_view_ddl(&reg, "REFRESH INCREMENTAL VIEW nonexistent");
        assert!(err.is_err());
    }
}
