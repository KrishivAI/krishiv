//! Build concrete connector `Source`/`Sink` from a parsed `ConnectorSpec`.
//!
//! This binds the SQL `CREATE SOURCE … FROM <CONNECTOR>(…)` / `CREATE SINK …
//! INTO <CONNECTOR>(…)` grammar to the `krishiv-connectors` implementations.
//!
//! Currently wired: `parquet` (file source/sink) — fully self-contained and
//! testable. Connectors that need external infrastructure (Kafka, Postgres
//! CDC, …) return a descriptive error pointing at the programmatic API; their
//! construction is a per-connector follow-up.

use krishiv_connectors::parquet::{ParquetSink, ParquetSource};
use krishiv_connectors::{DynSink, DynSource};
use krishiv_sql::pipeline_ddl::ConnectorSpec;

use crate::{KrishivError, Result};

fn conn_err(e: impl std::fmt::Display) -> KrishivError {
    KrishivError::Runtime {
        message: e.to_string(),
    }
}

fn unsupported(kind: &str, role: &str) -> KrishivError {
    KrishivError::Runtime {
        message: format!(
            "connector kind '{kind}' is not available as a SQL pipeline {role} yet; \
             supported: parquet. Construct it programmatically via Ingest/Egress::Connector."
        ),
    }
}

/// Build a `Box<dyn DynSource>` from a connector spec.
pub(crate) fn build_source(spec: &ConnectorSpec) -> Result<Box<dyn DynSource>> {
    match spec.kind.as_str() {
        "parquet" => {
            let path = spec.require("path").map_err(KrishivError::from)?;
            let src = ParquetSource::open(path).map_err(conn_err)?;
            Ok(Box::new(src))
        }
        other => Err(unsupported(other, "source")),
    }
}

/// Build a `Box<dyn DynSink>` from a connector spec.
pub(crate) fn build_sink(spec: &ConnectorSpec) -> Result<Box<dyn DynSink>> {
    match spec.kind.as_str() {
        "parquet" => {
            let path = spec.require("path").map_err(KrishivError::from)?;
            let sink = ParquetSink::create(path).map_err(conn_err)?;
            Ok(Box::new(sink))
        }
        other => Err(unsupported(other, "sink")),
    }
}
