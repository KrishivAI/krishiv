//! T9: JDBC source and sink drivers (Postgres / MySQL).

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::jdbc::{JdbcSink, JdbcSource};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::{SinkDriver, SourceDriver};
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::sink::DynSink;
use crate::source::DynSource;

fn require_url(config: &ConnectorConfig) -> ConnectorResult<String> {
    Ok(config.required("url")?.to_owned())
}

fn require_table(config: &ConnectorConfig) -> ConnectorResult<String> {
    Ok(config.required("table")?.to_owned())
}

/// Fail-fast validation of the incremental-cursor options, so a bad config
/// is rejected at `validate` time instead of after a live connection opens.
fn validate_cursor_options(config: &ConnectorConfig) -> ConnectorResult<()> {
    match (config.get("cursor.column"), config.get("cursor.after")) {
        (None, Some(_)) => Err(ConnectorError::Unsupported {
            message: "cursor.after requires cursor.column".into(),
        }),
        (_, Some(after)) if after.parse::<i64>().is_err() => {
            Err(ConnectorError::Unsupported {
                message: format!(
                    "cursor.after '{after}' must be a 64-bit integer \
                     (jdbc incremental cursors are Int64 in v1)"
                ),
            })
        }
        _ => Ok(()),
    }
}

// ── Source driver ─────────────────────────────────────────────────────────────

/// Driver for [`JdbcSource`].
///
/// Required config keys:
/// - `url` — bare Postgres connection URL (no `jdbc:` prefix)
/// - `table` — target table name
///
/// Optional config keys:
/// - `batch_size` — rows per page (default 1 000)
/// - `cursor.column` — keyset-pagination column for incremental pull
/// - `cursor.after` — Int64 cursor to resume strictly after (needs
///   `cursor.column`)
pub struct JdbcSourceDriver;

impl SourceDriver for JdbcSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Jdbc,
            ConnectorRole::Source,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        require_url(config)?;
        require_table(config)?;
        validate_cursor_options(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        Box::pin(async move {
            let url = require_url(config)?;
            let table = require_table(config)?;
            let batch_size: u32 = config
                .get("batch_size")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1_000);
            let mut source = JdbcSource::connect(&url, table)
                .await
                .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?
                .with_batch_size(batch_size);
            // Incremental pull: `cursor.column` switches to keyset pagination
            // (CONN-5) and `cursor.after` resumes strictly after a previously
            // ingested key, so a scheduled re-pull reads only new rows.
            if let Some(col) = config.get("cursor.column") {
                source = source.with_key_column(col);
                if let Some(after) = config.get("cursor.after") {
                    let cursor: i64 = after.parse().map_err(|_| ConnectorError::Unsupported {
                        message: format!(
                            "cursor.after '{after}' must be a 64-bit integer \
                             (jdbc incremental cursors are Int64 in v1)"
                        ),
                    })?;
                    source = source.with_cursor_after(cursor);
                }
            } else if config.get("cursor.after").is_some() {
                return Err(ConnectorError::Unsupported {
                    message: "cursor.after requires cursor.column".into(),
                });
            }
            Ok(Box::new(source) as Box<dyn DynSource>)
        })
    }
}

// ── Sink driver ───────────────────────────────────────────────────────────────

/// Driver for [`JdbcSink`].
///
/// Required config keys:
/// - `url` — bare Postgres connection URL
/// - `table` — target table name
pub struct JdbcSinkDriver;

impl SinkDriver for JdbcSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::JdbcSink,
            ConnectorRole::Sink,
            ConnectorCapabilities::new().with_bounded(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        require_url(config)?;
        require_table(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        Box::pin(async move {
            let url = require_url(config)?;
            let table = require_table(config)?;
            let sink = JdbcSink::connect(&url, table)
                .await
                .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
            Ok(Box::new(sink) as Box<dyn DynSink>)
        })
    }
}
