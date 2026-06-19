//! HBase sink driver.

#![cfg(feature = "hbase")]

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

impl SinkDriver for HBaseSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::HBase,
            ConnectorRole::Sink,
            // HBase Put operations are idempotent (same row key overwrites previous value).
            ConnectorCapabilities::new()
                .with_unbounded()
                .with_idempotent(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        config.required("zookeeper_quorum")?;
        config.required("table")?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        Box::pin(async move {
            // zookeeper_quorum is translated to the Thrift server address.
            // In typical setups the HBase Thrift gateway runs on the same
            // host as ZooKeeper, listening on port 9090.
            let zk = config.required("zookeeper_quorum")?.to_string();
            let thrift_addr = if zk.contains(':') {
                zk.clone()
            } else {
                format!("{zk}:9090")
            };
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
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_idempotent()
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
