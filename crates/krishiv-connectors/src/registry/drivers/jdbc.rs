//! T9: JDBC source and sink drivers (Postgres / MySQL).

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::capabilities::DeliveryGuarantee;
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
        (_, Some(after)) if after.parse::<i64>().is_err() => Err(ConnectorError::Unsupported {
            message: format!(
                "cursor.after '{after}' must be a 64-bit integer \
                     (jdbc incremental cursors are Int64 in v1)"
            ),
        }),
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
///
/// Optional:
/// - `conflict_keys` — comma-separated columns forming the upsert
///   conflict target. **Presence of this key is what upgrades the sink
///   from at-least-once APPEND to at-least-once IDEMPOTENT UPSERT**, and
///   it is declared rather than inferred: guessing a primary key would
///   turn a duplicate-row bug into an overwrite bug, and those two fail
///   in opposite directions.
/// - `batch_rows` — rows per INSERT statement (default 500, clamped by
///   the Postgres 65535-bind-parameter limit).
pub struct JdbcSinkDriver;

impl SinkDriver for JdbcSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::JdbcSink,
            ConnectorRole::Sink,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_resumable_flush(),
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
            let conflict_keys = sink_conflict_keys(config);
            let batch_rows = config
                .get("batch_rows")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(500);
            let sink = JdbcSink::connect_with(&url, table, conflict_keys, batch_rows)
                .await
                .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
            Ok(Box::new(sink) as Box<dyn DynSink>)
        })
    }
}

/// The delivery guarantee this sink earns **for a given config**.
///
/// Deliberately a function of the config rather than a constant on the
/// descriptor. The descriptor says what the driver *can* do; only the
/// configuration says what a particular job *gets*. Advertising
/// `EffectivelyOnce` unconditionally would label an append-mode sink
/// idempotent, which is precisely the overclaim the platform's
/// delivery-label discipline exists to prevent.
///
/// - conflict keys declared → `EffectivelyOnce`: a replayed batch
///   converges to the same rows (verified against real Postgres: the
///   second delivery of a key UPDATES rather than duplicating).
/// - no conflict keys → `AtLeastOnce`: a replayed batch duplicates rows.
pub fn jdbc_sink_delivery(config: &ConnectorConfig) -> DeliveryGuarantee {
    if sink_conflict_keys(config).is_empty() {
        DeliveryGuarantee::AtLeastOnce
    } else {
        DeliveryGuarantee::EffectivelyOnce
    }
}

/// Parse `conflict_keys` into a column list. Empty/absent = append mode.
pub(crate) fn sink_conflict_keys(config: &ConnectorConfig) -> Vec<String> {
    config
        .get("conflict_keys")
        .map(|v| {
            v.split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod sink_delivery_tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut c = ConnectorConfig::new("probe", "jdbc_sink");
        for (k, v) in pairs {
            c = c.with_property(*k, *v);
        }
        c
    }

    #[test]
    fn the_label_follows_the_configuration_not_the_driver() {
        // Append mode may NOT be called idempotent — a replayed batch
        // duplicates rows.
        let append = config(&[("url", "postgres://h/d"), ("table", "t")]);
        assert_eq!(jdbc_sink_delivery(&append), DeliveryGuarantee::AtLeastOnce);

        // Declaring conflict keys is what earns the stronger label.
        let upsert = config(&[
            ("url", "postgres://h/d"),
            ("table", "t"),
            ("conflict_keys", "id"),
        ]);
        assert_eq!(
            jdbc_sink_delivery(&upsert),
            DeliveryGuarantee::EffectivelyOnce
        );
    }

    #[test]
    fn conflict_keys_parse_into_columns() {
        let c = config(&[("conflict_keys", " tenant_id , id ")]);
        assert_eq!(sink_conflict_keys(&c), vec!["tenant_id", "id"]);
        // An empty/whitespace value is append mode, not a one-element key.
        assert!(sink_conflict_keys(&config(&[("conflict_keys", " , ")])).is_empty());
        assert!(sink_conflict_keys(&config(&[])).is_empty());
    }
}
