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

pub type OpenSourceFuture<'a> =
    Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>>;
pub type OpenSinkFuture<'a> =
    Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>>;
pub type OpenTwoPhaseSinkFuture<'a> =
    Pin<Box<dyn Future<Output = ConnectorResult<OpenedTwoPhaseSink>> + Send + 'a>>;
#[cfg(feature = "vector-sinks")]
pub type OpenVectorSinkFuture<'a> = Pin<
    Box<dyn Future<Output = ConnectorResult<Arc<dyn crate::vector::VectorSink>>> + Send + 'a>,
>;

/// Builds [`DynSource`] instances from validated [`ConnectorConfig`] values.
pub trait SourceDriver: Send + Sync {
    fn descriptor(&self) -> ConnectorDescriptor;

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()>;

    fn open<'a>(&'a self, config: &'a ConnectorConfig) -> OpenSourceFuture<'a>;

    /// Return the total row count for this source without fully opening it,
    /// if the driver can cheaply determine it (e.g. Parquet footer metadata).
    /// Returns `None` when not available or when reading fails.
    fn estimated_row_count(&self, _config: &ConnectorConfig) -> Option<u64> {
        None
    }
}

/// Builds [`DynSink`] instances from validated [`ConnectorConfig`] values.
pub trait SinkDriver: Send + Sync {
    fn descriptor(&self) -> ConnectorDescriptor;

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()>;

    fn open<'a>(&'a self, config: &'a ConnectorConfig) -> OpenSinkFuture<'a>;
}

/// Builds exactly-once two-phase sink instances.
pub trait TwoPhaseSinkDriver: Send + Sync {
    fn descriptor(&self) -> ConnectorDescriptor;

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()>;

    fn open<'a>(&'a self, config: &'a ConnectorConfig) -> OpenTwoPhaseSinkFuture<'a>;
}

/// Builds vector-store sinks when the `vector-sinks` feature is enabled.
#[cfg(feature = "vector-sinks")]
pub trait VectorSinkDriver: Send + Sync {
    fn descriptor(&self) -> ConnectorDescriptor;

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()>;

    fn open<'a>(&'a self, config: &'a ConnectorConfig) -> OpenVectorSinkFuture<'a>;
}

pub type SharedSourceDriver = Arc<dyn SourceDriver>;
pub type SharedSinkDriver = Arc<dyn SinkDriver>;
pub type SharedTwoPhaseSinkDriver = Arc<dyn TwoPhaseSinkDriver>;
#[cfg(feature = "vector-sinks")]
pub type SharedVectorSinkDriver = Arc<dyn VectorSinkDriver>;
