//! Elasticsearch sink driver.

#![cfg(feature = "elasticsearch")]

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::elasticsearch_sink::{ElasticsearchConfig, ElasticsearchSink};
use crate::error::{ConnectorError, ConnectorResult};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::SinkDriver;
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::sink::DynSink;

pub struct ElasticsearchSinkDriver;

impl SinkDriver for ElasticsearchSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Elasticsearch,
            ConnectorRole::Sink,
            // Elasticsearch bulk indexing is not idempotent (retries may produce duplicates
            // unless the caller provides explicit document IDs).
            ConnectorCapabilities::new().with_unbounded(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        config.required("url")?;
        config.required("index")?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        Box::pin(async move {
            let url = config.required("url")?.to_string();
            let index = config.required("index")?.to_string();

            let es_config = ElasticsearchConfig::new(url, index);
            let sink = ElasticsearchSink::connect(es_config)
                .await
                .map_err(|e| ConnectorError::Config {
                    message: format!("elasticsearch sink open failed: {e}"),
                })?;
            Ok(Box::new(ElasticsearchSinkWrapper(sink)) as Box<dyn DynSink>)
        })
    }
}

// Wrap ElasticsearchSink so it implements the crate Sink trait.
struct ElasticsearchSinkWrapper(ElasticsearchSink);

impl crate::sink::Sink for ElasticsearchSinkWrapper {
    fn capabilities(&self) -> crate::capabilities::ConnectorCapabilities {
        ConnectorCapabilities::new().with_unbounded()
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
