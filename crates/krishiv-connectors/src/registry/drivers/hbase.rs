//! HBase sink driver.
//!
//! Config keys:
//! - `thrift_address` — HBase Thrift-1 gateway address (`host` or
//!   `host:port`; port defaults to 9090). **Canonical.**
//! - `zookeeper_quorum` — deprecated alias for `thrift_address`. Despite its
//!   name it was always used as the Thrift address; a multi-host,
//!   comma-separated quorum is rejected because it cannot name a single
//!   Thrift gateway.
//! - `table` — target table (required).
//! - `column_family` — column family for all columns (default `cf`).

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::hbase_connector::{HBaseConfig, HBaseSink};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::SinkDriver;
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::sink::DynSink;

pub struct HBaseSinkDriver;

/// Shared by the descriptor and the opened sink so the two cannot drift.
///
/// HBase Put operations are idempotent (same row key overwrites the previous
/// value), and each `write_batch` writes through (no internal buffering), so
/// `flush` leaves the sink usable — resumable flush.
fn hbase_sink_capabilities() -> ConnectorCapabilities {
    ConnectorCapabilities::new()
        .with_unbounded()
        .with_idempotent()
        .with_resumable_flush()
}

/// Resolve the Thrift gateway address from the config.
///
/// `thrift_address` is canonical; `zookeeper_quorum` survives as a
/// deprecated alias because the option was always used as the Thrift
/// address. A comma-separated multi-host quorum is an error — it cannot
/// address a single Thrift gateway.
fn resolve_thrift_address(config: &ConnectorConfig) -> ConnectorResult<String> {
    let raw = match (config.get("thrift_address"), config.get("zookeeper_quorum")) {
        (Some(addr), _) => addr.to_string(),
        (None, Some(zk)) => {
            if zk.contains(',') {
                return Err(ConnectorError::Config {
                    message: format!(
                        "hbase: zookeeper_quorum '{zk}' lists multiple hosts, but this \
                         option is (despite its name) the Thrift gateway address — set \
                         'thrift_address' to a single host[:port] instead"
                    ),
                });
            }
            tracing::warn!(
                "hbase: option 'zookeeper_quorum' is deprecated and is used as the \
                 Thrift gateway address; use 'thrift_address' instead"
            );
            zk.to_string()
        }
        (None, None) => {
            return Err(ConnectorError::Config {
                message: "hbase: missing required option 'thrift_address' (Thrift gateway \
                          host[:port])"
                    .into(),
            });
        }
    };
    Ok(if raw.contains(':') {
        raw
    } else {
        format!("{raw}:9090")
    })
}

impl SinkDriver for HBaseSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::HBase,
            ConnectorRole::Sink,
            hbase_sink_capabilities(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = resolve_thrift_address(config)?;
        config.required("table")?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        Box::pin(async move {
            let thrift_addr = resolve_thrift_address(config)?;
            let table = config.required("table")?.to_string();
            let column_family = config.get("column_family").unwrap_or("cf").to_string();

            let hbase_config = HBaseConfig::new(thrift_addr, table, column_family);
            let sink =
                HBaseSink::connect(hbase_config)
                    .await
                    .map_err(|e| ConnectorError::Config {
                        message: format!("hbase sink open failed: {e}"),
                    })?;
            Ok(Box::new(HBaseSinkWrapper(sink)) as Box<dyn DynSink>)
        })
    }
}

struct HBaseSinkWrapper(HBaseSink);

impl crate::sink::Sink for HBaseSinkWrapper {
    fn capabilities(&self) -> crate::capabilities::ConnectorCapabilities {
        hbase_sink_capabilities()
    }

    async fn write_batch(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> crate::error::ConnectorResult<()> {
        self.0.write_batch(&batch).await
    }

    async fn flush(&mut self) -> crate::error::ConnectorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut c = ConnectorConfig::new("probe", "hbase");
        for (k, v) in pairs {
            c = c.with_property(*k, *v);
        }
        c
    }

    /// The descriptor and the opened-instance wrapper share
    /// `hbase_sink_capabilities`, so the advertised and actual capability sets
    /// (including resumable flush for the write-through sink) cannot drift.
    #[test]
    fn hbase_sink_descriptor_matches_instance_capabilities() {
        let caps = HBaseSinkDriver.descriptor().default_capabilities;
        assert_eq!(caps, hbase_sink_capabilities());
        assert!(caps.resumable_flush(), "write-through flush is resumable");
        assert!(caps.is_idempotent());
    }

    #[test]
    fn thrift_address_is_canonical_and_gets_a_default_port() {
        let addr = resolve_thrift_address(&config(&[("thrift_address", "hb1")])).unwrap();
        assert_eq!(addr, "hb1:9090");
        let addr = resolve_thrift_address(&config(&[("thrift_address", "hb1:9091")])).unwrap();
        assert_eq!(addr, "hb1:9091");
    }

    #[test]
    fn zookeeper_quorum_alias_works_single_host_but_rejects_a_real_quorum() {
        let addr = resolve_thrift_address(&config(&[("zookeeper_quorum", "zk1")])).unwrap();
        assert_eq!(addr, "zk1:9090");

        // A multi-host quorum cannot be a Thrift address.
        let err =
            resolve_thrift_address(&config(&[("zookeeper_quorum", "zk1,zk2,zk3")])).unwrap_err();
        assert!(err.to_string().contains("thrift_address"), "{err}");

        // Neither option present is an error.
        assert!(resolve_thrift_address(&config(&[])).is_err());
    }
}
