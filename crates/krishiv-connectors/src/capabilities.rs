//! Connector capability flags.

use crate::error::{ConnectorError, ConnectorResult};

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
        debug_assert!(!self.bounded || !self.unbounded);
        self
    }

    /// Mark the connector as producing an unbounded (infinite) data stream.
    ///
    /// Clears the `bounded` flag: a connector cannot be both bounded and unbounded.
    #[must_use]
    pub fn with_unbounded(mut self) -> Self {
        self.unbounded = true;
        self.bounded = false;
        debug_assert!(!self.bounded || !self.unbounded);
        self
    }

    /// Validate capability invariants.
    ///
    /// Returns an error if both `bounded` and `unbounded` are set simultaneously.
    pub fn validate(&self) -> ConnectorResult<()> {
        if self.bounded && self.unbounded {
            return Err(ConnectorError::Config {
                message: "connector capabilities: bounded and unbounded cannot both be true".into(),
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
    #[must_use]
    pub fn with_two_phase_commit(mut self) -> Self {
        self.supports_two_phase_commit = true;
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
