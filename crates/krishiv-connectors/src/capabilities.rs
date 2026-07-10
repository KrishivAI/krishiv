//! Connector capability flags.

use crate::error::{ConnectorError, ConnectorResult};

/// Strongest delivery guarantee a connector can participate in.
///
/// This is a capability, not an unconditional end-to-end claim. The effective
/// job guarantee is the weakest guarantee across the source, sink, checkpoint
/// storage, and execution profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryGuarantee {
    /// Records may be lost or duplicated after a failure.
    BestEffort,
    /// Records are replayed after failure; non-idempotent sinks may see duplicates.
    AtLeastOnce,
    /// Replayed writes are safe because the sink is idempotent.
    EffectivelyOnce,
    /// Source position and sink commit participate in one checkpoint transaction.
    ExactlyOnce,
}

impl DeliveryGuarantee {
    /// Stable label for manifests, status APIs, and documentation generation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BestEffort => "best-effort",
            Self::AtLeastOnce => "at-least-once",
            Self::EffectivelyOnce => "effectively-once",
            Self::ExactlyOnce => "exactly-once",
        }
    }
}

impl std::fmt::Display for DeliveryGuarantee {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Capability metadata for the streaming Iceberg sink (G7).
///
/// Lives here — not behind the `iceberg` feature — so a coordinator can
/// report delivery guarantees for registered jobs without compiling the sink
/// implementation. `IcebergStreamingSink::sink_capabilities` delegates to
/// this function, so the metadata and the implementation cannot diverge.
pub fn iceberg_streaming_sink_capabilities() -> ConnectorCapabilities {
    ConnectorCapabilities::new()
        .with_unbounded()
        .with_transactional()
        .with_checkpoint()
        .with_two_phase_commit()
}

/// Published support level for a connector implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ConnectorMaturity {
    /// API and behavior may change; not covered by the certified failure matrix.
    Experimental,
    /// Intended for evaluation and broader testing, but not yet certified.
    Preview,
    /// Covered by the connector contract and failure/recovery certification suite.
    Certified,
}

impl ConnectorMaturity {
    /// Stable value for manifests, status APIs, and documentation generation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Experimental => "experimental",
            Self::Preview => "preview",
            Self::Certified => "certified",
        }
    }
}

impl std::fmt::Display for ConnectorMaturity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// ConnectorCapabilities
// ---------------------------------------------------------------------------

/// Describes what guarantees and modes a connector supports.
///
/// All flags default to `false`. Use the builder methods to opt-in to
/// capabilities the connector actually provides.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectorCapabilities {
    bounded: bool,
    unbounded: bool,
    rewindable: bool,
    transactional: bool,
    idempotent: bool,
    /// Can participate in the barrier checkpoint protocol (R6).
    supports_checkpoint: bool,
    /// Implements `TwoPhaseCommitSink` for exactly-once delivery (R6).
    supports_two_phase_commit: bool,
}

impl ConnectorCapabilities {
    /// Create a new capabilities instance with all flags disabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the connector as producing a bounded (finite) data stream.
    ///
    /// Clears the `unbounded` flag: a connector cannot be both bounded and unbounded.
    #[must_use]
    pub fn with_bounded(mut self) -> Self {
        self.bounded = true;
        self.unbounded = false;
        debug_assert!(self.bounded ^ self.unbounded);
        self
    }

    /// Mark the connector as producing an unbounded (infinite) data stream.
    ///
    /// Clears the `bounded` flag: a connector cannot be both bounded and unbounded.
    #[must_use]
    pub fn with_unbounded(mut self) -> Self {
        self.unbounded = true;
        self.bounded = false;
        debug_assert!(self.bounded ^ self.unbounded);
        self
    }

    /// Validate capability invariants.
    ///
    /// Returns an error when mutually exclusive stream modes are combined or
    /// when two-phase commit is advertised without its required transactional
    /// and checkpoint capabilities.
    pub fn validate(&self) -> ConnectorResult<()> {
        if self.bounded && self.unbounded {
            return Err(ConnectorError::Config {
                message: "connector capabilities: bounded and unbounded cannot both be true".into(),
            });
        }
        if self.supports_two_phase_commit && (!self.transactional || !self.supports_checkpoint) {
            return Err(ConnectorError::Config {
                message: "connector capabilities: two-phase commit requires transactional and \
                          checkpoint capabilities"
                    .into(),
            });
        }
        Ok(())
    }

    /// Mark the connector as supporting rewind to a previous offset.
    #[must_use]
    pub fn with_rewindable(mut self) -> Self {
        self.rewindable = true;
        self
    }

    /// Mark the connector as supporting transactional commits.
    #[must_use]
    pub fn with_transactional(mut self) -> Self {
        self.transactional = true;
        self
    }

    /// Mark the connector as supporting idempotent writes.
    #[must_use]
    pub fn with_idempotent(mut self) -> Self {
        self.idempotent = true;
        self
    }

    /// Mark the connector as capable of participating in the barrier checkpoint protocol.
    #[must_use]
    pub fn with_checkpoint(mut self) -> Self {
        self.supports_checkpoint = true;
        self
    }

    /// Mark the connector as implementing two-phase commit for exactly-once delivery.
    ///
    /// Two-phase commit necessarily participates in checkpoint coordination and
    /// commits transactionally, so those prerequisite flags are enabled as part
    /// of this builder operation.
    #[must_use]
    pub fn with_two_phase_commit(mut self) -> Self {
        self.supports_two_phase_commit = true;
        self.transactional = true;
        self.supports_checkpoint = true;
        self
    }

    /// Returns `true` if the data stream is bounded (finite).
    pub fn is_bounded(&self) -> bool {
        self.bounded
    }

    /// Returns `true` if the data stream is unbounded (infinite).
    pub fn is_unbounded(&self) -> bool {
        self.unbounded
    }

    /// Returns `true` if the connector supports rewind to a previous offset.
    pub fn is_rewindable(&self) -> bool {
        self.rewindable
    }

    /// Returns `true` if the connector supports transactional commits.
    pub fn is_transactional(&self) -> bool {
        self.transactional
    }

    /// Returns `true` if writes are idempotent (safe to replay).
    pub fn is_idempotent(&self) -> bool {
        self.idempotent
    }

    /// Returns `true` if the connector can participate in the barrier checkpoint protocol.
    pub fn is_checkpoint_capable(&self) -> bool {
        self.supports_checkpoint
    }

    /// Returns `true` if the connector implements two-phase commit for exactly-once delivery.
    pub fn is_two_phase_commit_capable(&self) -> bool {
        self.supports_two_phase_commit
    }

    /// Return the strongest delivery guarantee advertised by these capabilities.
    ///
    /// End-to-end exactly-once additionally requires durable checkpoint storage,
    /// a rewindable/checkpointed source, and a compatible runtime profile.
    pub fn delivery_guarantee(&self) -> DeliveryGuarantee {
        if self.supports_two_phase_commit && self.transactional && self.supports_checkpoint {
            DeliveryGuarantee::ExactlyOnce
        } else if self.idempotent {
            DeliveryGuarantee::EffectivelyOnce
        } else if self.rewindable || self.supports_checkpoint {
            DeliveryGuarantee::AtLeastOnce
        } else {
            DeliveryGuarantee::BestEffort
        }
    }

    /// Returns `true` if at least one capability flag is set.
    pub fn has_any(&self) -> bool {
        self.bounded
            || self.unbounded
            || self.rewindable
            || self.transactional
            || self.idempotent
            || self.supports_checkpoint
            || self.supports_two_phase_commit
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    #[test]
    fn delivery_guarantee_is_derived_conservatively() {
        assert_eq!(
            ConnectorCapabilities::new().delivery_guarantee(),
            DeliveryGuarantee::BestEffort
        );
        assert_eq!(
            ConnectorCapabilities::new()
                .with_rewindable()
                .delivery_guarantee(),
            DeliveryGuarantee::AtLeastOnce
        );
        assert_eq!(
            ConnectorCapabilities::new()
                .with_idempotent()
                .delivery_guarantee(),
            DeliveryGuarantee::EffectivelyOnce
        );
        assert_eq!(
            ConnectorCapabilities::new()
                .with_two_phase_commit()
                .delivery_guarantee(),
            DeliveryGuarantee::ExactlyOnce
        );
    }
}
