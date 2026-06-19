//! Kafka source and sink drivers.

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::ConnectorResult;
use crate::kafka::{KafkaConfig, KafkaSink, KafkaSource};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::{SinkDriver, SourceDriver};
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::sink::DynSink;
use crate::source::DynSource;

pub struct KafkaSourceDriver;

impl SourceDriver for KafkaSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Kafka,
            ConnectorRole::Source,
            ConnectorCapabilities::new().with_unbounded(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = KafkaConfig::from_config(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        Box::pin(async move {
            let kafka_config = KafkaConfig::from_config(config)?;
            let source = KafkaSource::new(kafka_config)?;
            Ok(Box::new(source) as Box<dyn DynSource>)
        })
    }
}

pub struct KafkaSinkDriver;

impl SinkDriver for KafkaSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Kafka,
            ConnectorRole::Sink,
            ConnectorCapabilities::new().with_unbounded(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = KafkaConfig::from_config(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        Box::pin(async move {
            let kafka_config = KafkaConfig::from_config(config)?;
            let sink = KafkaSink::new(kafka_config)?;
            Ok(Box::new(sink) as Box<dyn DynSink>)
        })
    }
}
