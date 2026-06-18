#![forbid(unsafe_code)]

//! Behavior versioning for operator-level cache invalidation.
//!
//! A `LogicFingerprint` uniquely identifies an operator's *logic* (its code +
//! version). When the fingerprint changes (because `behavior_version` was
//! bumped), any cached Trace state for that operator is invalidated and
//! recomputed from scratch.

use std::hash::Hasher;
use twox_hash::XxHash64;

/// A stable 64-bit fingerprint of an operator's logic identity.
///
/// Computed from `operator_uid || behavior_version` via XxHash64.
/// Changing either field changes the fingerprint → triggers full recompute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct LogicFingerprint(pub u64);

impl LogicFingerprint {
    /// Compute the fingerprint for a given operator UID and behavior version.
    pub fn compute(uid: &str, behavior_version: u64) -> Self {
        let mut h = XxHash64::with_seed(0);
        h.write(uid.as_bytes());
        h.write_u8(0); // separator
        h.write_u64(behavior_version);
        Self(h.finish())
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for LogicFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Key for the incremental memo/Trace store: identifies one operator's
/// persisted Trace by (logic_fingerprint, partition_id).
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct MemoKey {
    pub fingerprint: LogicFingerprint,
    pub partition_id: u32,
}

impl MemoKey {
    pub fn new(fingerprint: LogicFingerprint, partition_id: u32) -> Self {
        Self {
            fingerprint,
            partition_id,
        }
    }

    pub fn single(fingerprint: LogicFingerprint) -> Self {
        Self::new(fingerprint, 0)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_uid_and_version_gives_same_fingerprint() {
        let a = LogicFingerprint::compute("op-join-1", 0);
        let b = LogicFingerprint::compute("op-join-1", 0);
        assert_eq!(a, b);
    }

    #[test]
    fn different_version_gives_different_fingerprint() {
        let a = LogicFingerprint::compute("op-join-1", 0);
        let b = LogicFingerprint::compute("op-join-1", 1);
        assert_ne!(a, b);
    }

    #[test]
    fn different_uid_gives_different_fingerprint() {
        let a = LogicFingerprint::compute("op-A", 0);
        let b = LogicFingerprint::compute("op-B", 0);
        assert_ne!(a, b);
    }

    #[test]
    fn memo_key_equality() {
        let fp = LogicFingerprint::compute("x", 1);
        let k1 = MemoKey::new(fp, 0);
        let k2 = MemoKey::new(fp, 0);
        assert_eq!(k1, k2);
    }
}
