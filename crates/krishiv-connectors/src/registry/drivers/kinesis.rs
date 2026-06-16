//! Kinesis source driver.

#![cfg(feature = "kinesis")]

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::kinesis::{KinesisConfig, KinesisSource, ShardPosition};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::SourceDriver;
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::source::DynSource;

pub struct KinesisSourceDriver;

impl SourceDriver for KinesisSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Kinesis,
            ConnectorRole::Source,
            ConnectorCapabilities::new()
                .with_unbounded()
                .with_checkpoint(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        config.required("stream_name")?;
        config.required("region")?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        Box::pin(async move {
            let stream_name = config.required("stream_name")?.to_string();
            let region = config.required("region")?.to_string();
            let shard_id = config
                .get("shard_id")
                .unwrap_or("shardId-000000000000")
                .to_string();

            let start = match config.get("start_position").unwrap_or("trim_horizon") {
                "latest" => ShardPosition::Latest,
                seq if seq.starts_with("at:") => {
                    ShardPosition::AtSequenceNumber(seq[3..].to_string())
                }
                seq if seq.starts_with("after:") => {
                    ShardPosition::AfterSequenceNumber(seq[6..].to_string())
                }
                _ => ShardPosition::TrimHorizon,
            };

            let cfg = KinesisConfig::new(stream_name, region)
                .with_shard_id(shard_id)
                .with_start(start);

            let source = KinesisSource::new(cfg)
                .await
                .map_err(|e| ConnectorError::Config {
                    message: format!("kinesis source open failed: {e}"),
                })?;
            Ok(Box::new(source) as Box<dyn DynSource>)
        })
    }
}
