//! Elasticsearch sink driver.

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

/// Shared by the descriptor and the opened sink so the two cannot drift.
///
/// Elasticsearch bulk indexing is not idempotent (retries may produce
/// duplicates unless the caller provides explicit document IDs), but each
/// `write_batch` is a self-contained bulk request, so `flush` leaves the sink
/// usable — resumable flush.
fn elasticsearch_sink_capabilities() -> ConnectorCapabilities {
    ConnectorCapabilities::new()
        .with_unbounded()
        .with_resumable_flush()
}

impl SinkDriver for ElasticsearchSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Elasticsearch,
            ConnectorRole::Sink,
            elasticsearch_sink_capabilities(),
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

            let mut es_config = ElasticsearchConfig::new(url, index);
            // `id_column` is what turns a retried bulk request from
            // duplicates into an idempotent upsert — without it a movement
            // labeled "idempotent" would be lying. Optional because
            // append-only indexing (auto IDs) is a legitimate mode.
            if let Some(col) = config.get("id_column") {
                es_config = es_config.with_id_column(col);
            }
            if let (Some(user), Some(pass)) = (config.get("username"), config.get("password")) {
                es_config = es_config.with_credentials(user, pass);
            }
            let sink = ElasticsearchSink::connect(es_config).await.map_err(|e| {
                ConnectorError::Config {
                    message: format!("elasticsearch sink open failed: {e}"),
                }
            })?;
            Ok(Box::new(ElasticsearchSinkWrapper(sink)) as Box<dyn DynSink>)
        })
    }
}

// Wrap ElasticsearchSink so it implements the crate Sink trait.
struct ElasticsearchSinkWrapper(ElasticsearchSink);

impl crate::sink::Sink for ElasticsearchSinkWrapper {
    fn capabilities(&self) -> crate::capabilities::ConnectorCapabilities {
        elasticsearch_sink_capabilities()
    }

    async fn write_batch(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> crate::error::ConnectorResult<()> {
        self.0.write_batch(&batch).await
    }

    /// Each `write_batch` call performs a self-contained bulk index request
    /// (synchronous/blocking I/O). `flush` is a no-op — there is no internal
    /// buffering to drain.
    async fn flush(&mut self) -> crate::error::ConnectorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The descriptor and the opened-instance wrapper share
    /// `elasticsearch_sink_capabilities`, so the advertised and actual
    /// capability sets (including resumable flush for the self-contained bulk
    /// writes) cannot drift.
    #[test]
    fn elasticsearch_sink_descriptor_matches_instance_capabilities() {
        let caps = ElasticsearchSinkDriver.descriptor().default_capabilities;
        assert_eq!(caps, elasticsearch_sink_capabilities());
        assert!(caps.resumable_flush(), "bulk writes leave flush resumable");
        assert!(
            !caps.is_idempotent(),
            "bulk indexing without ids may duplicate"
        );
    }
}
