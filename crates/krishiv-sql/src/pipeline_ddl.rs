//! Declarative pipeline DDL: `CREATE SOURCE` / `CREATE SINK` / `START PIPELINE`.
//!
//! This is the SQL surface for the Tier-2 pipeline layer, following the same
//! lightweight prefix-match approach as [`crate::incremental_view`]. It is a
//! **metadata layer only**: it parses and registers source/sink declarations.
//! Execution of `START PIPELINE` happens in `krishiv-api`, which resolves the
//! registered specs against a [`Session`](../../krishiv_api/struct.Session.html)
//! and compiles them to `session.pipeline()…run()`.
//!
//! # Grammar
//!
//! ```sql
//! CREATE SOURCE orders AS SELECT * FROM orders_raw;          -- bounded query source
//! CREATE INCREMENTAL VIEW revenue AS SELECT ... FROM orders; -- transform (see incremental_view)
//! CREATE SINK revenue_out FROM revenue;                      -- collect view output
//! START PIPELINE revenue_out;                                -- run; returns sink output
//! DROP SOURCE orders;  DROP SINK revenue_out;
//! ```

use std::collections::HashMap;
use std::sync::RwLock;

use crate::{SqlError, SqlResult};

// ── Parsed statement ────────────────────────────────────────────────────────

/// A parsed pipeline DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineStatement {
    /// `CREATE SOURCE <name> AS <query>` — a bounded query source.
    CreateSource { name: String, query: String },
    /// `CREATE SINK <name> FROM <view>` — collect a view's output.
    CreateSink { name: String, view: String },
    /// `START PIPELINE <sink_name>` — run the pipeline feeding `sink_name`.
    StartPipeline { sink: String },
    /// `DROP SOURCE <name>`.
    DropSource { name: String },
    /// `DROP SINK <name>`.
    DropSink { name: String },
}

// ── Registry ────────────────────────────────────────────────────────────────

/// A declared source: a bounded SQL query whose rows are fed as insertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpec {
    pub query: String,
}

/// A declared sink: which view's output it collects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkSpec {
    pub view: String,
}

/// SQL-layer registry of declared pipeline sources and sinks (metadata only).
#[derive(Debug, Default)]
pub struct PipelineRegistry {
    sources: RwLock<HashMap<String, SourceSpec>>,
    sinks: RwLock<HashMap<String, SinkSpec>>,
}

fn poisoned() -> SqlError {
    SqlError::DataFusion {
        message: "pipeline registry lock poisoned".into(),
    }
}

impl PipelineRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_source(&self, name: impl Into<String>, spec: SourceSpec) -> SqlResult<()> {
        self.sources
            .write()
            .map_err(|_| poisoned())?
            .insert(name.into(), spec);
        Ok(())
    }

    pub fn register_sink(&self, name: impl Into<String>, spec: SinkSpec) -> SqlResult<()> {
        self.sinks
            .write()
            .map_err(|_| poisoned())?
            .insert(name.into(), spec);
        Ok(())
    }

    pub fn source(&self, name: &str) -> SqlResult<Option<SourceSpec>> {
        Ok(self
            .sources
            .read()
            .map_err(|_| poisoned())?
            .get(name)
            .cloned())
    }

    pub fn sink(&self, name: &str) -> SqlResult<Option<SinkSpec>> {
        Ok(self
            .sinks
            .read()
            .map_err(|_| poisoned())?
            .get(name)
            .cloned())
    }

    /// All declared source specs `(name, spec)`.
    pub fn sources(&self) -> SqlResult<Vec<(String, SourceSpec)>> {
        Ok(self
            .sources
            .read()
            .map_err(|_| poisoned())?
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    pub fn remove_source(&self, name: &str) -> SqlResult<bool> {
        Ok(self
            .sources
            .write()
            .map_err(|_| poisoned())?
            .remove(name)
            .is_some())
    }

    pub fn remove_sink(&self, name: &str) -> SqlResult<bool> {
        Ok(self
            .sinks
            .write()
            .map_err(|_| poisoned())?
            .remove(name)
            .is_some())
    }
}

// ── Parser ──────────────────────────────────────────────────────────────────

/// Parse a pipeline DDL statement, or `Ok(None)` if it is not one.
pub fn parse_pipeline_statement(sql: &str) -> SqlResult<Option<PipelineStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("CREATE SOURCE ") {
        let rest = &trimmed["CREATE SOURCE ".len()..];
        let (name, query) = split_keyword(rest, " AS ").ok_or_else(|| SqlError::Unsupported {
            feature: "CREATE SOURCE requires '<name> AS <query>'".into(),
        })?;
        require_nonempty(&name)?;
        if query.trim().is_empty() {
            return Err(SqlError::Unsupported {
                feature: "CREATE SOURCE requires a query after AS".into(),
            });
        }
        return Ok(Some(PipelineStatement::CreateSource {
            name,
            query: query.trim().to_string(),
        }));
    }

    if upper.starts_with("CREATE SINK ") {
        let rest = &trimmed["CREATE SINK ".len()..];
        let (name, view) = split_keyword(rest, " FROM ").ok_or_else(|| SqlError::Unsupported {
            feature: "CREATE SINK requires '<name> FROM <view>'".into(),
        })?;
        require_nonempty(&name)?;
        let view = view.trim().to_string();
        require_nonempty(&view)?;
        return Ok(Some(PipelineStatement::CreateSink { name, view }));
    }

    if upper.starts_with("START PIPELINE ") {
        let sink = trimmed["START PIPELINE ".len()..].trim().to_string();
        require_nonempty(&sink)?;
        return Ok(Some(PipelineStatement::StartPipeline { sink }));
    }

    if upper.starts_with("DROP SOURCE ") {
        let name = trimmed["DROP SOURCE ".len()..].trim().to_string();
        require_nonempty(&name)?;
        return Ok(Some(PipelineStatement::DropSource { name }));
    }

    if upper.starts_with("DROP SINK ") {
        let name = trimmed["DROP SINK ".len()..].trim().to_string();
        require_nonempty(&name)?;
        return Ok(Some(PipelineStatement::DropSink { name }));
    }

    Ok(None)
}

/// Apply a CREATE/DROP pipeline DDL to the registry. `START PIPELINE` is **not**
/// handled here (it needs the `krishiv-api` execution layer); it returns
/// `Ok(None)` so the caller can intercept it.
///
/// Returns `Some(name)` if a CREATE/DROP statement was applied.
pub fn execute_pipeline_ddl(registry: &PipelineRegistry, sql: &str) -> SqlResult<Option<String>> {
    let Some(stmt) = parse_pipeline_statement(sql)? else {
        return Ok(None);
    };
    match stmt {
        PipelineStatement::CreateSource { name, query } => {
            registry.register_source(name.clone(), SourceSpec { query })?;
            Ok(Some(name))
        }
        PipelineStatement::CreateSink { name, view } => {
            registry.register_sink(name.clone(), SinkSpec { view })?;
            Ok(Some(name))
        }
        PipelineStatement::DropSource { name } => {
            registry.remove_source(&name)?;
            Ok(Some(name))
        }
        PipelineStatement::DropSink { name } => {
            registry.remove_sink(&name)?;
            Ok(Some(name))
        }
        // START PIPELINE is handled by the api layer, not the registry.
        PipelineStatement::StartPipeline { .. } => Ok(None),
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Split `rest` on the first case-insensitive occurrence of `keyword`
/// (e.g. " AS "), returning `(before_trimmed, after)`.
fn split_keyword(rest: &str, keyword: &str) -> Option<(String, String)> {
    let upper = rest.to_uppercase();
    let key_upper = keyword.to_uppercase();
    let idx = upper.find(&key_upper)?;
    let before = rest[..idx].trim().to_string();
    let after = rest[idx + keyword.len()..].to_string();
    Some((before, after))
}

fn require_nonempty(s: &str) -> SqlResult<()> {
    if s.trim().is_empty() {
        Err(SqlError::EmptyTableName)
    } else {
        Ok(())
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_source() {
        let s = parse_pipeline_statement("CREATE SOURCE orders AS SELECT * FROM raw").unwrap();
        assert_eq!(
            s,
            Some(PipelineStatement::CreateSource {
                name: "orders".into(),
                query: "SELECT * FROM raw".into(),
            })
        );
    }

    #[test]
    fn parse_create_sink() {
        let s = parse_pipeline_statement("CREATE SINK out FROM revenue;").unwrap();
        assert_eq!(
            s,
            Some(PipelineStatement::CreateSink {
                name: "out".into(),
                view: "revenue".into(),
            })
        );
    }

    #[test]
    fn parse_start_and_drops() {
        assert_eq!(
            parse_pipeline_statement("START PIPELINE out").unwrap(),
            Some(PipelineStatement::StartPipeline { sink: "out".into() })
        );
        assert_eq!(
            parse_pipeline_statement("DROP SOURCE orders").unwrap(),
            Some(PipelineStatement::DropSource {
                name: "orders".into()
            })
        );
        assert_eq!(
            parse_pipeline_statement("DROP SINK out").unwrap(),
            Some(PipelineStatement::DropSink { name: "out".into() })
        );
    }

    #[test]
    fn non_pipeline_sql_returns_none() {
        assert_eq!(parse_pipeline_statement("SELECT 1").unwrap(), None);
    }

    #[test]
    fn registry_create_drop_roundtrip() {
        let reg = PipelineRegistry::new();
        execute_pipeline_ddl(&reg, "CREATE SOURCE orders AS SELECT * FROM raw").unwrap();
        execute_pipeline_ddl(&reg, "CREATE SINK out FROM revenue").unwrap();
        assert_eq!(
            reg.source("orders").unwrap().unwrap().query,
            "SELECT * FROM raw"
        );
        assert_eq!(reg.sink("out").unwrap().unwrap().view, "revenue");
        // START PIPELINE is not consumed by the registry layer.
        assert_eq!(
            execute_pipeline_ddl(&reg, "START PIPELINE out").unwrap(),
            None
        );
        assert!(
            execute_pipeline_ddl(&reg, "DROP SOURCE orders")
                .unwrap()
                .is_some()
        );
        assert!(reg.source("orders").unwrap().is_none());
    }
}
