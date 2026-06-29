#![forbid(unsafe_code)]

//! Per-operator execution configuration.
//!
//! [`OperatorConfig`] specifies parallelism and savepoint-identity for a
//! streaming operator. The [`OperatorUid`] survives job restarts and is used
//! by the savepoint/checkpoint mechanism to match persisted state back to the
//! correct operator.

// ── OperatorUid ──────────────────────────────────────────────────────────────

/// A stable string identifier for a streaming operator.
///
/// Survives job restarts and is used to correlate operator state in savepoints.
/// Two operators with the same UID in topologically equivalent positions will
/// share state across a restart.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperatorUid(String);

impl OperatorUid {
    /// Create a new `OperatorUid` from any string-like value.
    pub fn new(uid: impl Into<String>) -> Self {
        Self(uid.into())
    }

    /// Return the UID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for OperatorUid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ── OperatorConfig ────────────────────────────────────────────────────────────

/// Per-operator execution configuration.
///
/// Holds the parallelism settings and the stable UID required for savepoint
/// compatibility. The `behavior_version` field is used by the incremental
/// computing layer: bumping it signals that the operator's logic has changed,
/// causing any cached `Trace` state to be discarded and recomputed from scratch.
#[derive(Debug, Clone)]
pub struct OperatorConfig {
    /// Stable UID used to match state in savepoints and incremental Traces.
    pub uid: OperatorUid,
    /// Actual degree of parallelism (`1` means no key partitioning).
    pub parallelism: usize,
    /// Maximum parallelism determines the key-space partitioning granularity.
    ///
    /// This value must not change across savepoint restores.
    pub max_parallelism: usize,
    /// Incremental computing: bump this when the operator's UDF or transformation
    /// logic changes. The `LogicFingerprint` is derived from `(uid, behavior_version)`.
    /// When the fingerprint changes, the cached incremental `Trace` for this
    /// operator is invalidated and recomputed from scratch. Default: `0`.
    pub behavior_version: u64,
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            uid: OperatorUid::new("default"),
            parallelism: 1,
            max_parallelism: 128,
            behavior_version: 0,
        }
    }
}

impl OperatorConfig {
    /// Create a new `OperatorConfig` with the given UID and default parallelism.
    pub fn new(uid: impl Into<String>) -> Self {
        Self {
            uid: OperatorUid::new(uid),
            ..Self::default()
        }
    }

    /// Set the parallelism for this operator.
    pub fn with_parallelism(mut self, p: usize) -> Self {
        self.parallelism = p;
        self
    }

    /// Set the maximum parallelism for this operator.
    pub fn with_max_parallelism(mut self, mp: usize) -> Self {
        self.max_parallelism = mp;
        self
    }

    /// Set the behavior version for incremental Trace cache invalidation.
    ///
    /// Bump this (e.g., `0` → `1`) when UDF logic changes so that previously
    /// cached incremental results are discarded and recomputed correctly.
    pub fn with_behavior_version(mut self, v: u64) -> Self {
        self.behavior_version = v;
        self
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_uid_roundtrip() {
        let uid = OperatorUid::new("my-op-1");
        assert_eq!(uid.as_str(), "my-op-1");
        assert_eq!(uid.to_string(), "my-op-1");
    }

    #[test]
    fn operator_config_defaults() {
        let cfg = OperatorConfig::default();
        assert_eq!(cfg.uid.as_str(), "default");
        assert_eq!(cfg.parallelism, 1);
        assert_eq!(cfg.max_parallelism, 128);
    }

    #[test]
    fn operator_config_builder() {
        let cfg = OperatorConfig::new("my-op")
            .with_parallelism(4)
            .with_max_parallelism(256);
        assert_eq!(cfg.uid.as_str(), "my-op");
        assert_eq!(cfg.parallelism, 4);
        assert_eq!(cfg.max_parallelism, 256);
    }
}
