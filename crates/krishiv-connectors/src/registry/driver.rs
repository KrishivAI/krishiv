//! Connector driver traits.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::descriptor::ConnectorDescriptor;
use crate::config::ConnectorConfig;
use crate::error::ConnectorResult;
use crate::sink::DynSink;
use crate::source::DynSource;
use crate::two_phase::LocalParquetTwoPhaseCommitSink;

/// Built-in two-phase sink instances opened by the registry.
#[non_exhaustive]
pub enum OpenedTwoPhaseSink {
    LocalParquet(LocalParquetTwoPhaseCommitSink),
}

/// Builds [`DynSource`] instances from validated [`ConnectorConfig`] values.
pub trait SourceDriver: Send + Sync {
    fn descriptor(&self) -> ConnectorDescriptor;

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()>;

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>>;
}

/// Builds [`DynSink`] instances from validated [`ConnectorConfig`] values.
pub trait SinkDriver: Send + Sync {
    fn descriptor(&self) -> ConnectorDescriptor;

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()>;

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>>;
}

/// Builds exactly-once two-phase sink instances.
pub trait TwoPhaseSinkDriver: Send + Sync {
    fn descriptor(&self) -> ConnectorDescriptor;

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()>;

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<OpenedTwoPhaseSink>> + Send + 'a>>;
}

/// Builds vector-store sinks when the `vector-sinks` feature is enabled.
#[cfg(feature = "vector-sinks")]
pub trait VectorSinkDriver: Send + Sync {
    fn descriptor(&self) -> ConnectorDescriptor;

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()>;

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Arc<dyn crate::vector::VectorSink>>> + Send + 'a>>;
}

pub type SharedSourceDriver = Arc<dyn SourceDriver>;
pub type SharedSinkDriver = Arc<dyn SinkDriver>;
pub type SharedTwoPhaseSinkDriver = Arc<dyn TwoPhaseSinkDriver>;
#[cfg(feature = "vector-sinks")]
pub type SharedVectorSinkDriver = Arc<dyn VectorSinkDriver>;
