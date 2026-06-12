//! Connector driver registry and built-in driver registrations.

mod connector_registry;
mod descriptor;
mod driver;
mod drivers;
mod kind;

#[cfg(feature = "vector-sinks")]
mod vector_drivers;

#[cfg(test)]
mod tests;

pub use connector_registry::{ConnectorRegistry, default_registry};
pub use descriptor::ConnectorDescriptor;
pub use driver::{
    OpenSinkFuture, OpenSourceFuture, OpenTwoPhaseSinkFuture, OpenedTwoPhaseSink, SharedSinkDriver,
    SharedSourceDriver, SharedTwoPhaseSinkDriver, SinkDriver, SourceDriver, TwoPhaseSinkDriver,
};
#[cfg(feature = "vector-sinks")]
pub use driver::{OpenVectorSinkFuture, SharedVectorSinkDriver, VectorSinkDriver};
pub use kind::{ConnectorKind, ConnectorRole};
