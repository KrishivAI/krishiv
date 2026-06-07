//! Connector driver registry and built-in driver registrations.

mod descriptor;
mod driver;
mod drivers;
mod kind;
mod registry;

#[cfg(feature = "vector-sinks")]
mod vector_drivers;

#[cfg(test)]
mod tests;

pub use descriptor::ConnectorDescriptor;
pub use driver::{
    OpenedTwoPhaseSink, SharedSinkDriver, SharedSourceDriver, SharedTwoPhaseSinkDriver, SinkDriver,
    SourceDriver, TwoPhaseSinkDriver,
};
#[cfg(feature = "vector-sinks")]
pub use driver::{SharedVectorSinkDriver, VectorSinkDriver};
pub use kind::{ConnectorKind, ConnectorRole};
pub use registry::{ConnectorRegistry, default_registry};
