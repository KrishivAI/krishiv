//! Pulsar source driver.

#![cfg(feature = "pulsar-source")]

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::pulsar_connector::{PulsarConfig, PulsarSource};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::SourceDriver;
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::source::DynSource;

pub struct PulsarSourceDriver;

impl SourceDriver for PulsarSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Pulsar,
            ConnectorRole::Source,
            ConnectorCapabilities::new().with_unbounded(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        config.required("broker_url")?;
        config.required("topic")?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        Box::pin(async move {
            let cfg = pulsar_config(config)?;

            let source = PulsarSource::connect(cfg)
                .await
                .map_err(|e| ConnectorError::Config {
                    message: format!("pulsar source open failed: {e}"),
                })?;
            Ok(Box::new(source) as Box<dyn DynSource>)
        })
    }
}

/// The consumer config a registry-opened Pulsar source runs with.
fn pulsar_config(config: &ConnectorConfig) -> ConnectorResult<PulsarConfig> {
    let broker_url = config.required("broker_url")?.to_string();
    let topic = config.required("topic")?.to_string();
    let subscription = config
        .get("subscription")
        .unwrap_or("krishiv-default")
        .to_string();
    let start_at_earliest = match config.get("start_position").map(str::to_ascii_lowercase) {
        None => false,
        Some(p) if p == "latest" => false,
        Some(p) if p == "earliest" => true,
        Some(other) => {
            return Err(ConnectorError::Config {
                message: format!("pulsar start_position '{other}': expected earliest or latest"),
            });
        }
    };
    // Registry callers drive the source only through `read_batch`, so the
    // source acks each batch when the next is requested (M17).
    Ok(PulsarConfig::new(broker_url, topic)
        .with_subscription(subscription)
        .with_start_at_earliest(start_at_earliest)
        .with_ack_on_next_read(true))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        pairs
            .iter()
            .fold(ConnectorConfig::new("p", "pulsar"), |c, (k, v)| {
                c.with_property(*k, *v)
            })
    }

    /// M17: the registry path acks as it reads, and honours start_position.
    #[test]
    fn registry_pulsar_config_acks_and_honours_start_position() {
        let base = [("broker_url", "pulsar://b:6650"), ("topic", "t")];
        let cfg = pulsar_config(&config(&base)).unwrap();
        assert!(cfg.ack_on_next_read);
        assert!(!cfg.start_at_earliest);
        let mut earliest = base.to_vec();
        earliest.push(("start_position", "earliest"));
        assert!(pulsar_config(&config(&earliest)).unwrap().start_at_earliest);
        let mut bad = base.to_vec();
        bad.push(("start_position", "yesterday"));
        assert!(pulsar_config(&config(&bad)).is_err());
    }
}
