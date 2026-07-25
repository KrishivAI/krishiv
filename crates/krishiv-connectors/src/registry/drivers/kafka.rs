//! Kafka source and sink drivers.

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::ConnectorResult;
use crate::kafka::{KafkaConfig, KafkaSink, KafkaSource};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::kafka_transactional_sink::RdkafkaTransactionalSink;
use crate::registry::driver::{OpenedTwoPhaseSink, SinkDriver, SourceDriver, TwoPhaseSinkDriver};
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

/// Two-phase-commit Kafka sink driver (`kafka-transactional`).
///
/// [`ConnectorKind::KafkaTransactional`] parsed as a valid kind but had no
/// driver registered under any role, so it was unreachable everywhere — a
/// phantom entry in the taxonomy. The exactly-once implementation itself is
/// real and shipped ([`RdkafkaTransactionalSink`], driving the streaming
/// `KafkaSink` output contract), so the honest close is to register it as the
/// kind's `TwoPhaseSink` driver rather than delete a kind that names a
/// capability the engine genuinely has.
///
/// Config: `bootstrap_servers`, `topic`, `transactional_id`. The caller owns
/// transactional-id uniqueness per task slot — two live producers sharing one
/// id fence each other.
pub struct KafkaTransactionalSinkDriver;

impl TwoPhaseSinkDriver for KafkaTransactionalSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::KafkaTransactional,
            ConnectorRole::TwoPhaseSink,
            ConnectorCapabilities::new()
                .with_unbounded()
                .with_two_phase_commit(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        config.required("bootstrap_servers")?;
        config.required("topic")?;
        config.required("transactional_id")?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<OpenedTwoPhaseSink>> + Send + 'a>> {
        Box::pin(async move {
            let bootstrap_servers = config.required("bootstrap_servers")?.to_owned();
            let topic = config.required("topic")?.to_owned();
            let transactional_id = config.required("transactional_id")?.to_owned();
            // init_transactions() talks to the broker, so keep it off the async
            // reactor like every other blocking connector open in this crate.
            let sink = tokio::task::spawn_blocking(move || {
                RdkafkaTransactionalSink::new(bootstrap_servers, topic, transactional_id)
            })
            .await
            .map_err(|error| crate::error::ConnectorError::Kafka {
                message: format!("kafka-transactional sink open panicked: {error}"),
                retriable: false,
            })??;
            Ok(OpenedTwoPhaseSink::KafkaTransactional(sink))
        })
    }
}
