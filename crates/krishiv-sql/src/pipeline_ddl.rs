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
    /// `CREATE SOURCE <name> AS <query>` or `... FROM <CONNECTOR>(...)`.
    CreateSource { name: String, source: SourceSpec },
    /// `CREATE SINK <name> FROM <view> [INTO <CONNECTOR>(...)]`.
    CreateSink {
        name: String,
        view: String,
        connector: Option<ConnectorSpec>,
    },
    /// `START PIPELINE <sink_name>` — run the pipeline feeding `sink_name`.
    StartPipeline { sink: String },
    /// `REFRESH PIPELINE <sink_name> [FULL]` — re-run a pipeline; `full` resets
    /// its persisted state first (Spark SDP `--full-refresh`).
    RefreshPipeline { sink: String, full: bool },
    /// `DROP SOURCE <name>`.
    DropSource { name: String },
    /// `DROP SINK <name>`.
    DropSink { name: String },
}

// ── Registry ────────────────────────────────────────────────────────────────

/// A connector reference: a kind (`parquet`, `kafka`, …) plus key/value options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorSpec {
    pub kind: String,
    pub options: HashMap<String, String>,
}

impl ConnectorSpec {
    /// Fetch a required option, or a descriptive error if it is missing.
    pub fn require(&self, key: &str) -> SqlResult<&str> {
        self.options
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| SqlError::Unsupported {
                feature: format!("connector '{}' requires option '{key}'", self.kind),
            })
    }
}

/// A declared source: either a bounded SQL query (fed as insertions) or a
/// connector (e.g. `PARQUET(path='…')`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceSpec {
    /// `AS <query>` — rows from a bounded query.
    Query(String),
    /// `FROM <CONNECTOR>(...)` — rows pulled from a connector.
    Connector(ConnectorSpec),
}

/// A declared sink: which view's output it collects, and optionally where it is
/// written (a connector). With no connector, the output is returned as a result
/// set by `START PIPELINE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkSpec {
    pub view: String,
    pub connector: Option<ConnectorSpec>,
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

    /// View name backing the given sink, if the sink is registered.
    /// Returns `None` for unknown sinks.
    pub fn view_for_sink(&self, name: &str) -> SqlResult<Option<String>> {
        Ok(self
            .sinks
            .read()
            .map_err(|_| poisoned())?
            .get(name)
            .map(|spec| spec.view.clone()))
    }

    /// Names of all declared sinks.
    pub fn sink_names(&self) -> SqlResult<Vec<String>> {
        Ok(self
            .sinks
            .read()
            .map_err(|_| poisoned())?
            .keys()
            .cloned()
            .collect())
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
        // `... AS <query>` (query source) or `... FROM <CONNECTOR>(...)`.
        if let Some((name, query)) = split_keyword(rest, " AS ") {
            require_nonempty(&name)?;
            if query.trim().is_empty() {
                return Err(SqlError::Unsupported {
                    feature: "CREATE SOURCE requires a query after AS".into(),
                });
            }
            return Ok(Some(PipelineStatement::CreateSource {
                name,
                source: SourceSpec::Query(query.trim().to_string()),
            }));
        }
        if let Some((name, conn)) = split_keyword(rest, " FROM ") {
            require_nonempty(&name)?;
            return Ok(Some(PipelineStatement::CreateSource {
                name,
                source: SourceSpec::Connector(parse_connector_spec(&conn)?),
            }));
        }
        return Err(SqlError::Unsupported {
            feature: "CREATE SOURCE requires '<name> AS <query>' or '<name> FROM <connector>(...)'"
                .into(),
        });
    }

    if upper.starts_with("CREATE SINK ") {
        let rest = &trimmed["CREATE SINK ".len()..];
        let (name, after_from) =
            split_keyword(rest, " FROM ").ok_or_else(|| SqlError::Unsupported {
                feature: "CREATE SINK requires '<name> FROM <view>'".into(),
            })?;
        require_nonempty(&name)?;
        // Optional `INTO <CONNECTOR>(...)` after the view.
        let (view, connector) = if let Some((view, conn)) = split_keyword(&after_from, " INTO ") {
            (view, Some(parse_connector_spec(&conn)?))
        } else {
            (after_from.trim().to_string(), None)
        };
        let view = view.trim().to_string();
        require_nonempty(&view)?;
        return Ok(Some(PipelineStatement::CreateSink {
            name,
            view,
            connector,
        }));
    }

    if upper.starts_with("START PIPELINE ") {
        let sink = trimmed["START PIPELINE ".len()..].trim().to_string();
        require_nonempty(&sink)?;
        return Ok(Some(PipelineStatement::StartPipeline { sink }));
    }

    if upper.starts_with("REFRESH PIPELINE ") {
        let rest = trimmed["REFRESH PIPELINE ".len()..].trim();
        // Optional trailing FULL keyword.
        let (sink, full) = match rest.to_uppercase().strip_suffix(" FULL") {
            Some(_) => (rest[..rest.len() - " FULL".len()].trim().to_string(), true),
            None => (rest.to_string(), false),
        };
        require_nonempty(&sink)?;
        return Ok(Some(PipelineStatement::RefreshPipeline { sink, full }));
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
        PipelineStatement::CreateSource { name, source } => {
            registry.register_source(name.clone(), source)?;
            Ok(Some(name))
        }
        PipelineStatement::CreateSink {
            name,
            view,
            connector,
        } => {
            registry.register_sink(name.clone(), SinkSpec { view, connector })?;
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
        // START / REFRESH PIPELINE are handled by the api layer, not the registry.
        PipelineStatement::StartPipeline { .. } | PipelineStatement::RefreshPipeline { .. } => {
            Ok(None)
        }
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

/// Parse a connector reference of the form `KIND(key='value', key2='value2')`.
/// Keys are lowercased; values are unquoted (single or double quotes).
fn parse_connector_spec(s: &str) -> SqlResult<ConnectorSpec> {
    let s = s.trim();
    let open = s.find('(').ok_or_else(|| SqlError::Unsupported {
        feature: "connector spec must be '<KIND>(key='value', ...)'".into(),
    })?;
    let close = s.rfind(')').ok_or_else(|| SqlError::Unsupported {
        feature: "connector spec missing closing ')'".into(),
    })?;
    if close < open {
        return Err(SqlError::Unsupported {
            feature: "connector spec has malformed parentheses".into(),
        });
    }
    let kind = s[..open].trim().to_lowercase();
    require_nonempty(&kind)?;

    let mut options = HashMap::new();
    for part in s[open + 1..close].split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').ok_or_else(|| SqlError::Unsupported {
            feature: format!("connector option '{part}' must be 'key=value'"),
        })?;
        let v = v.trim().trim_matches(['\'', '"']);
        options.insert(k.trim().to_lowercase(), v.to_string());
    }
    Ok(ConnectorSpec { kind, options })
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_source_query() {
        let s = parse_pipeline_statement("CREATE SOURCE orders AS SELECT * FROM raw").unwrap();
        assert_eq!(
            s,
            Some(PipelineStatement::CreateSource {
                name: "orders".into(),
                source: SourceSpec::Query("SELECT * FROM raw".into()),
            })
        );
    }

    #[test]
    fn parse_create_source_connector() {
        let s =
            parse_pipeline_statement("CREATE SOURCE orders FROM PARQUET(path='/data/o.parquet')")
                .unwrap();
        let Some(PipelineStatement::CreateSource {
            name,
            source: SourceSpec::Connector(spec),
        }) = s
        else {
            panic!("expected connector source");
        };
        assert_eq!(name, "orders");
        assert_eq!(spec.kind, "parquet");
        assert_eq!(spec.require("path").unwrap(), "/data/o.parquet");
    }

    #[test]
    fn parse_create_sink_plain_and_connector() {
        // Plain (memory result) sink.
        assert_eq!(
            parse_pipeline_statement("CREATE SINK out FROM revenue;").unwrap(),
            Some(PipelineStatement::CreateSink {
                name: "out".into(),
                view: "revenue".into(),
                connector: None,
            })
        );
        // Connector sink.
        let Some(PipelineStatement::CreateSink {
            name,
            view,
            connector: Some(spec),
        }) = parse_pipeline_statement(
            "CREATE SINK out FROM revenue INTO PARQUET(path='/o.parquet')",
        )
        .unwrap()
        else {
            panic!("expected connector sink");
        };
        assert_eq!((name.as_str(), view.as_str()), ("out", "revenue"));
        assert_eq!(spec.kind, "parquet");
        assert_eq!(spec.require("path").unwrap(), "/o.parquet");
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
    fn parse_refresh_pipeline() {
        assert_eq!(
            parse_pipeline_statement("REFRESH PIPELINE out").unwrap(),
            Some(PipelineStatement::RefreshPipeline {
                sink: "out".into(),
                full: false
            })
        );
        assert_eq!(
            parse_pipeline_statement("REFRESH PIPELINE out FULL;").unwrap(),
            Some(PipelineStatement::RefreshPipeline {
                sink: "out".into(),
                full: true
            })
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
            reg.source("orders").unwrap().unwrap(),
            SourceSpec::Query("SELECT * FROM raw".into())
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
