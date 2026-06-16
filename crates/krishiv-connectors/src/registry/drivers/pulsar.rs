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
            ConnectorCapabilities::new().with_unbounded().with_checkpoint(),
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
            let broker_url = config.required("broker_url")?.to_string();
            let topic = config.required("topic")?.to_string();
            let subscription = config
                .get("subscription")
                .unwrap_or("krishiv-default")
                .to_string();

            let cfg = PulsarConfig::new(broker_url, topic).with_subscription(subscription);

            let source =
                PulsarSource::connect(cfg)
                    .await
                    .map_err(|e| ConnectorError::Config {
                        message: format!("pulsar source open failed: {e}"),
                    })?;
            Ok(Box::new(source) as Box<dyn DynSource>)
        })
    }
}
