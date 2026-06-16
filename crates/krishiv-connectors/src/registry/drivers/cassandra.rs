//! Cassandra sink driver.

#![cfg(feature = "cassandra")]

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::cassandra_sink::{CassandraConfig, CassandraSink};
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::SinkDriver;
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::sink::DynSink;

pub struct CassandraSinkDriver;

impl SinkDriver for CassandraSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Cassandra,
            ConnectorRole::Sink,
            // Cassandra INSERT operations are idempotent when using the same row key.
            ConnectorCapabilities::new().with_unbounded().with_idempotent(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        config.required("contact_points")?;
        config.required("keyspace")?;
        config.required("table")?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        Box::pin(async move {
            let contact_points = config.required("contact_points")?.to_string();
            let keyspace = config.required("keyspace")?.to_string();
            let table = config.required("table")?.to_string();

            let cass_config = CassandraConfig::new(contact_points, keyspace, table);
            let sink = CassandraSink::connect(cass_config)
                .await
                .map_err(|e| ConnectorError::Config {
                    message: format!("cassandra sink open failed: {e}"),
                })?;
            Ok(Box::new(CassandraSinkWrapper(sink)) as Box<dyn DynSink>)
        })
    }
}

struct CassandraSinkWrapper(CassandraSink);

impl crate::sink::Sink for CassandraSinkWrapper {
    fn capabilities(&self) -> crate::capabilities::ConnectorCapabilities {
        ConnectorCapabilities::new().with_unbounded().with_idempotent()
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
