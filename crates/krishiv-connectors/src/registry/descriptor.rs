//! Connector driver metadata.

use super::kind::{ConnectorKind, ConnectorRole};
use crate::capabilities::{ConnectorCapabilities, ConnectorMaturity};

/// Metadata describing a registered connector driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorDescriptor {
    pub kind: ConnectorKind,
    pub role: ConnectorRole,
    pub default_capabilities: ConnectorCapabilities,
    pub maturity: ConnectorMaturity,
}

impl ConnectorDescriptor {
    pub fn new(
        kind: ConnectorKind,
        role: ConnectorRole,
        default_capabilities: ConnectorCapabilities,
    ) -> Self {
        Self {
            maturity: kind.default_maturity(),
            kind,
            role,
            default_capabilities,
        }
    }

    /// Override the published maturity for a specific driver implementation.
    #[must_use]
    pub fn with_maturity(mut self, maturity: ConnectorMaturity) -> Self {
        self.maturity = maturity;
        self
    }
}
