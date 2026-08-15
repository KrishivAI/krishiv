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

/// Strictly parse the `start_position` option. An unrecognised value is an
/// ERROR rather than a silent fall-back to `trim_horizon` — a typo like
/// `lastest` must not quietly replay the whole shard.
fn parse_start_position(value: &str) -> ConnectorResult<ShardPosition> {
    match value {
        "trim_horizon" => Ok(ShardPosition::TrimHorizon),
        "latest" => Ok(ShardPosition::Latest),
        seq if seq.starts_with("at:") => Ok(ShardPosition::AtSequenceNumber(seq[3..].to_string())),
        seq if seq.starts_with("after:") => {
            Ok(ShardPosition::AfterSequenceNumber(seq[6..].to_string()))
        }
        other => Err(ConnectorError::Config {
            message: format!(
                "kinesis start_position '{other}' is not recognised; valid values: \
                 'trim_horizon', 'latest', 'at:<sequence>', 'after:<sequence>'"
            ),
        }),
    }
}

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
        if let Some(start) = config.get("start_position") {
            let _ = parse_start_position(start)?;
        }
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

            let start =
                parse_start_position(config.get("start_position").unwrap_or("trim_horizon"))?;

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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut c = ConnectorConfig::new("probe", "kinesis");
        for (k, v) in pairs {
            c = c.with_property(*k, *v);
        }
        c
    }

    #[test]
    fn unrecognised_start_position_is_a_validate_error() {
        let bad = config(&[
            ("stream_name", "s"),
            ("region", "us-east-1"),
            ("start_position", "lastest"),
        ]);
        let err = KinesisSourceDriver.validate(&bad).unwrap_err();
        assert!(err.to_string().contains("trim_horizon"), "{err}");

        for good in ["trim_horizon", "latest", "at:123", "after:123"] {
            let cfg = config(&[
                ("stream_name", "s"),
                ("region", "us-east-1"),
                ("start_position", good),
            ]);
            assert!(KinesisSourceDriver.validate(&cfg).is_ok(), "{good}");
        }
    }
}
