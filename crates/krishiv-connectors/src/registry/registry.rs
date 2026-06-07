//! Connector driver registry.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::descriptor::ConnectorDescriptor;
use super::driver::{
    OpenedTwoPhaseSink, SharedSinkDriver, SharedSourceDriver, SharedTwoPhaseSinkDriver,
};
#[cfg(feature = "vector-sinks")]
use super::driver::SharedVectorSinkDriver;
use super::kind::{ConnectorKind, ConnectorRole};
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::sink::DynSink;
use crate::source::DynSource;

/// Registry of connector drivers keyed by [`ConnectorKind`] and role.
#[derive(Default)]
pub struct ConnectorRegistry {
    sources: HashMap<ConnectorKind, SharedSourceDriver>,
    sinks: HashMap<ConnectorKind, SharedSinkDriver>,
    two_phase_sinks: HashMap<ConnectorKind, SharedTwoPhaseSinkDriver>,
    #[cfg(feature = "vector-sinks")]
    vector_sinks: HashMap<ConnectorKind, SharedVectorSinkDriver>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_source(&mut self, driver: SharedSourceDriver) {
        let kind = driver.descriptor().kind;
        self.sources.insert(kind, driver);
    }

    pub fn register_sink(&mut self, driver: SharedSinkDriver) {
        let kind = driver.descriptor().kind;
        self.sinks.insert(kind, driver);
    }

    pub fn register_two_phase_sink(&mut self, driver: SharedTwoPhaseSinkDriver) {
        let kind = driver.descriptor().kind;
        self.two_phase_sinks.insert(kind, driver);
    }

    #[cfg(feature = "vector-sinks")]
    pub fn register_vector_sink(&mut self, driver: SharedVectorSinkDriver) {
        let kind = driver.descriptor().kind;
        self.vector_sinks.insert(kind, driver);
    }

    pub fn source_descriptor(&self, kind: ConnectorKind) -> Option<ConnectorDescriptor> {
        self.sources.get(&kind).map(|d| d.descriptor())
    }

    pub fn sink_descriptor(&self, kind: ConnectorKind) -> Option<ConnectorDescriptor> {
        self.sinks.get(&kind).map(|d| d.descriptor())
    }

    pub fn validate_source(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let kind = ConnectorKind::parse(&config.kind)?;
        let driver = self.sources.get(&kind).ok_or_else(|| ConnectorError::Config {
            message: format!(
                "no source driver registered for kind '{}'",
                config.kind
            ),
        })?;
        driver.validate(config)
    }

    pub fn validate_sink(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let kind = ConnectorKind::parse(&config.kind)?;
        let driver = self.sinks.get(&kind).ok_or_else(|| ConnectorError::Config {
            message: format!("no sink driver registered for kind '{}'", config.kind),
        })?;
        driver.validate(config)
    }

    pub fn open_source<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        match self.lookup_source(config) {
            Ok(driver) => driver.open(config),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    pub fn open_sink<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        match self.lookup_sink(config) {
            Ok(driver) => driver.open(config),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    pub fn open_two_phase_sink<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<OpenedTwoPhaseSink>> + Send + 'a>> {
        match self.lookup_two_phase_sink(config) {
            Ok(driver) => driver.open(config),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    #[cfg(feature = "vector-sinks")]
    pub fn open_vector_sink<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<
        Box<
            dyn Future<Output = ConnectorResult<Arc<dyn crate::vector::VectorSink>>> + Send + 'a,
        >,
    > {
        match self.lookup_vector_sink(config) {
            Ok(driver) => driver.open(config),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn lookup_source(&self, config: &ConnectorConfig) -> ConnectorResult<&SharedSourceDriver> {
        let kind = ConnectorKind::parse(&config.kind)?;
        self.sources.get(&kind).ok_or_else(|| ConnectorError::Config {
            message: format!(
                "no source driver registered for kind '{}'",
                config.kind
            ),
        })
    }

    fn lookup_sink(&self, config: &ConnectorConfig) -> ConnectorResult<&SharedSinkDriver> {
        let kind = ConnectorKind::parse(&config.kind)?;
        self.sinks.get(&kind).ok_or_else(|| ConnectorError::Config {
            message: format!("no sink driver registered for kind '{}'", config.kind),
        })
    }

    fn lookup_two_phase_sink(
        &self,
        config: &ConnectorConfig,
    ) -> ConnectorResult<&SharedTwoPhaseSinkDriver> {
        let kind = ConnectorKind::parse(&config.kind)?;
        self.two_phase_sinks.get(&kind).ok_or_else(|| {
            ConnectorError::Config {
                message: format!(
                    "no two-phase sink driver registered for kind '{}'",
                    config.kind
                ),
            }
        })
    }

    #[cfg(feature = "vector-sinks")]
    fn lookup_vector_sink(
        &self,
        config: &ConnectorConfig,
    ) -> ConnectorResult<&SharedVectorSinkDriver> {
        let kind = ConnectorKind::parse(&config.kind)?;
        self.vector_sinks.get(&kind).ok_or_else(|| {
            ConnectorError::Config {
                message: format!(
                    "no vector sink driver registered for kind '{}'",
                    config.kind
                ),
            }
        })
    }

    /// Return descriptors for all registered drivers.
    pub fn descriptors(&self) -> Vec<ConnectorDescriptor> {
        let mut out: Vec<ConnectorDescriptor> = self
            .sources
            .values()
            .map(|driver| driver.descriptor())
            .chain(self.sinks.values().map(|driver| driver.descriptor()))
            .chain(
                self.two_phase_sinks
                    .values()
                    .map(|driver| driver.descriptor()),
            )
            .collect();
        #[cfg(feature = "vector-sinks")]
        {
            out.extend(
                self.vector_sinks
                    .values()
                    .map(|driver| driver.descriptor()),
            );
        }
        out.sort_by(|left, right| {
            role_rank(left.role)
                .cmp(&role_rank(right.role))
                .then_with(|| left.kind.as_str().cmp(right.kind.as_str()))
        });
        out
    }

    /// Return whether a driver is registered for `kind` and `role`.
    pub fn has_driver(&self, kind: ConnectorKind, role: ConnectorRole) -> bool {
        match role {
            ConnectorRole::Source => self.sources.contains_key(&kind),
            ConnectorRole::Sink => self.sinks.contains_key(&kind),
            ConnectorRole::TwoPhaseSink => self.two_phase_sinks.contains_key(&kind),
            #[cfg(feature = "vector-sinks")]
            ConnectorRole::VectorSink => self.vector_sinks.contains_key(&kind),
        }
    }
}

fn role_rank(role: ConnectorRole) -> u8 {
    match role {
        ConnectorRole::Source => 0,
        ConnectorRole::Sink => 1,
        ConnectorRole::TwoPhaseSink => 2,
        #[cfg(feature = "vector-sinks")]
        ConnectorRole::VectorSink => 3,
    }
}

/// Build a registry with all built-in drivers for the enabled feature set.
pub fn default_registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register_source(Arc::new(super::drivers::ParquetSourceDriver));
    registry.register_sink(Arc::new(super::drivers::ParquetSinkDriver));
    registry.register_source(Arc::new(super::drivers::S3SourceDriver));
    registry.register_sink(Arc::new(super::drivers::S3SinkDriver));
    registry.register_two_phase_sink(Arc::new(
        super::drivers::LocalParquetTwoPhaseSinkDriver,
    ));
    #[cfg(feature = "kafka")]
    {
        registry.register_source(Arc::new(super::drivers::KafkaSourceDriver));
        registry.register_sink(Arc::new(super::drivers::KafkaSinkDriver));
    }
    #[cfg(feature = "vector-sinks")]
    {
        super::vector_drivers::register_builtin_vector_drivers(&mut registry);
    }
    registry
}
