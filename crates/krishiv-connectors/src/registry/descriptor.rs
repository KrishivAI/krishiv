//! Connector driver metadata.

use super::kind::{ConnectorKind, ConnectorRole};
use crate::capabilities::ConnectorCapabilities;

/// Metadata describing a registered connector driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorDescriptor {
    pub kind: ConnectorKind,
    pub role: ConnectorRole,
    pub default_capabilities: ConnectorCapabilities,
}

impl ConnectorDescriptor {
    pub fn new(
        kind: ConnectorKind,
        role: ConnectorRole,
        default_capabilities: ConnectorCapabilities,
    ) -> Self {
        Self {
            kind,
            role,
            default_capabilities,
        }
    }
}
