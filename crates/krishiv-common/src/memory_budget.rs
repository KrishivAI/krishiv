#![forbid(unsafe_code)]

//! Runtime memory accounting shared across operators within one task.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Per-task memory accounting.  Created from the coordinator-supplied
/// `memory_limit_bytes` and shared (via `Arc`) across every operator that
/// runs inside the same task slot.
///
/// All operations use `Relaxed` ordering: the counters are advisory limits,
/// not synchronisation primitives. False-positives (accepting a reservation
/// that briefly exceeds the limit due to a race) are tolerable; the
/// hard OOM killer remains the OS fallback.
#[derive(Debug)]
pub struct MemoryBudget {
    used_bytes: AtomicU64,
    limit_bytes: Option<u64>,
}

impl MemoryBudget {
    /// Create a budget with no limit (used when the assignment carries no
    /// `memory_limit_bytes`).
    pub fn unlimited() -> Arc<Self> {
        Arc::new(Self {
            used_bytes: AtomicU64::new(0),
            limit_bytes: None,
        })
    }

    /// Create a budget capped at `limit_bytes`.
    pub fn limited(limit_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            used_bytes: AtomicU64::new(0),
            limit_bytes: Some(limit_bytes),
        })
    }

    /// Build from the optional proto field (None → unlimited).
    pub fn from_limit(limit_bytes: Option<u64>) -> Arc<Self> {
        match limit_bytes {
            Some(b) if b > 0 => Self::limited(b),
            _ => Self::unlimited(),
        }
    }

    /// Return the configured limit, or `None` for unlimited budgets.
    pub fn limit(&self) -> Option<u64> {
        self.limit_bytes
    }

    /// Current byte count tracked by this budget.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    /// Try to reserve `bytes` bytes of memory.
    ///
    /// Returns `true` if the reservation was accepted (or the budget is
    /// unlimited).  Returns `false` if doing so would exceed `limit_bytes`; the
    /// caller should spill or return an OOM error.
    pub fn try_reserve(&self, bytes: u64) -> bool {
        let Some(limit) = self.limit_bytes else {
            self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
            return true;
        };
        let prev = self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
        if prev + bytes > limit {
            // Roll back the speculative add.
            self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    /// Release previously reserved `bytes`. Saturates at zero on underflow.
    pub fn release(&self, bytes: u64) {
        let _ = self.used_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |cur| Some(cur.saturating_sub(bytes)),
        );
    }

    /// Remaining bytes before the limit is hit, or `None` for unlimited.
    pub fn remaining(&self) -> Option<u64> {
        self.limit_bytes
            .map(|l| l.saturating_sub(self.used_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_always_accepts() {
        let b = MemoryBudget::unlimited();
        assert!(b.try_reserve(u64::MAX / 2));
        assert!(b.try_reserve(u64::MAX / 2));
        assert!(b.limit().is_none());
        assert!(b.remaining().is_none());
    }

    #[test]
    fn limited_rejects_over_limit() {
        let b = MemoryBudget::limited(100);
        assert!(b.try_reserve(60));
        assert!(!b.try_reserve(60)); // would be 120 > 100
        assert_eq!(b.used_bytes(), 60); // rolled back
    }

    #[test]
    fn release_reduces_counter() {
        let b = MemoryBudget::limited(100);
        assert!(b.try_reserve(80));
        b.release(40);
        assert_eq!(b.used_bytes(), 40);
        assert!(b.try_reserve(60)); // 40 + 60 = 100 ≤ 100
    }

    #[test]
    fn release_saturates_at_zero() {
        let b = MemoryBudget::unlimited();
        b.release(999); // no panic
        assert_eq!(b.used_bytes(), 0);
    }

    #[test]
    fn remaining_tracks_available() {
        let b = MemoryBudget::limited(200);
        assert_eq!(b.remaining(), Some(200));
        b.try_reserve(50);
        assert_eq!(b.remaining(), Some(150));
    }

    #[test]
    fn from_limit_none_is_unlimited() {
        let b = MemoryBudget::from_limit(None);
        assert!(b.limit().is_none());
    }

    #[test]
    fn from_limit_zero_is_unlimited() {
        let b = MemoryBudget::from_limit(Some(0));
        assert!(b.limit().is_none());
    }
}
