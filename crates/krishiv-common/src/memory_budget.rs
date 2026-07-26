#![forbid(unsafe_code)]

//! Runtime memory accounting shared across operators within one task.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// High-water mark of `used_bytes` over the budget's lifetime.
    peak_bytes: AtomicU64,
    limit_bytes: Option<u64>,
}

impl MemoryBudget {
    /// Create a budget with no limit (used when the assignment carries no
    /// `memory_limit_bytes`).
    pub fn unlimited() -> Arc<Self> {
        Arc::new(Self {
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            limit_bytes: None,
        })
    }

    /// Create a budget capped at `limit_bytes`.
    pub fn limited(limit_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
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

    /// High-water mark of `used_bytes` over this budget's lifetime.
    pub fn peak_bytes(&self) -> u64 {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    /// Try to reserve `bytes` bytes of memory.
    ///
    /// Returns `true` if the reservation was accepted (or the budget is
    /// unlimited).  Returns `false` if doing so would exceed `limit_bytes`; the
    /// caller should spill or return an OOM error.
    pub fn try_reserve(&self, bytes: u64) -> bool {
        let Some(limit) = self.limit_bytes else {
            let new = self
                .used_bytes
                .fetch_add(bytes, Ordering::Relaxed)
                .saturating_add(bytes);
            self.peak_bytes.fetch_max(new, Ordering::Relaxed);
            return true;
        };
        let prev = self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
        if prev + bytes > limit {
            // Roll back the speculative add.
            self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
            false
        } else {
            self.peak_bytes
                .fetch_max(prev.saturating_add(bytes), Ordering::Relaxed);
            true
        }
    }

    /// Release previously reserved `bytes`. Saturates at zero on underflow.
    pub fn release(&self, bytes: u64) {
        let _ = self
            .used_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(bytes))
            });
    }

    /// Remaining bytes before the limit is hit, or `None` for unlimited.
    pub fn remaining(&self) -> Option<u64> {
        self.limit_bytes
            .map(|l| l.saturating_sub(self.used_bytes()))
    }
}

/// Read this process's cgroup memory limit in bytes (v2 first, then v1).
///
/// Returns `None` when no limit applies: file absent (not in a container),
/// v2 `max`, or a v1 sentinel of effectively-unlimited (≥ 2^60).
pub fn cgroup_memory_limit_bytes() -> Option<u64> {
    const V1_UNLIMITED_FLOOR: u64 = 1 << 60;
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Ok(raw) = std::fs::read_to_string(path) {
            let trimmed = raw.trim();
            if trimmed == "max" {
                return None;
            }
            return trimmed
                .parse::<u64>()
                .ok()
                .filter(|&n| n > 0 && n < V1_UNLIMITED_FLOOR);
        }
    }
    None
}

/// What the cgroup is actually charged, split by kind.
///
/// The budgets in this crate all track *anonymous* memory — heap the process
/// asked for. The kernel charges a container for more than that, and the
/// difference is invisible to every `MemoryBudget` and to DataFusion's
/// `MemoryPool`. On the SF100 benchmark that difference was 1.5–1.8 GB of a
/// 4.5 GB limit, and three separate heap-side "fixes" failed to stop the OOM
/// kills because the memory was never on the heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CgroupMemoryUsage {
    /// Total charged to the cgroup — what `memory.max` is compared against.
    pub current_bytes: u64,
    /// Anonymous memory: heap, stacks. What the budgets model.
    pub anon_bytes: u64,
    /// Page cache charged to this cgroup, including cache for files *this*
    /// container wrote. Reclaimable while clean, but not while dirty — and a
    /// burst of dirty pages is what turns this into an OOM kill rather than
    /// into reclaim.
    pub file_bytes: u64,
}

impl CgroupMemoryUsage {
    /// Bytes charged to the cgroup that anonymous accounting cannot explain.
    pub fn unaccounted_bytes(&self) -> u64 {
        self.current_bytes.saturating_sub(self.anon_bytes)
    }
}

/// Read the cgroup v2 memory breakdown, if one is present.
///
/// Returns `None` outside a cgroup v2 container (including on cgroup v1, whose
/// `memory.stat` uses different keys) — callers treat that as "unconstrained"
/// exactly like [`cgroup_memory_limit_bytes`] does.
pub fn cgroup_memory_usage() -> Option<CgroupMemoryUsage> {
    let current = std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    let stat = std::fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?;
    let mut anon = 0u64;
    let mut file = 0u64;
    for line in stat.lines() {
        let mut parts = line.split_ascii_whitespace();
        match (parts.next(), parts.next()) {
            (Some("anon"), Some(v)) => anon = v.parse().unwrap_or(0),
            (Some("file"), Some(v)) => file = v.parse().unwrap_or(0),
            _ => {}
        }
    }
    Some(CgroupMemoryUsage {
        current_bytes: current,
        anon_bytes: anon,
        file_bytes: file,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reader must degrade to `None` rather than panicking or inventing
    /// numbers when there is no cgroup v2 to read — the same contract as
    /// `cgroup_memory_limit_bytes`, and the reason callers can log it blindly.
    #[test]
    fn cgroup_usage_is_absent_or_internally_consistent() {
        if let Some(usage) = cgroup_memory_usage() {
            assert!(
                usage.current_bytes >= usage.anon_bytes,
                "anon ({}) cannot exceed total charge ({})",
                usage.anon_bytes,
                usage.current_bytes
            );
            assert_eq!(
                usage.unaccounted_bytes(),
                usage.current_bytes - usage.anon_bytes
            );
        }
    }

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

    #[test]
    fn peak_tracks_high_water_mark() {
        let b = MemoryBudget::limited(100);
        assert!(b.try_reserve(80));
        b.release(80);
        assert!(b.try_reserve(30));
        assert_eq!(b.used_bytes(), 30);
        assert_eq!(b.peak_bytes(), 80, "peak must survive releases");
    }

    #[test]
    fn peak_ignores_rejected_reservations() {
        let b = MemoryBudget::limited(100);
        assert!(b.try_reserve(50));
        assert!(!b.try_reserve(60)); // rejected
        assert_eq!(b.peak_bytes(), 50);
    }

    #[test]
    fn peak_tracked_for_unlimited_budget() {
        let b = MemoryBudget::unlimited();
        b.try_reserve(1000);
        b.release(500);
        assert_eq!(b.peak_bytes(), 1000);
    }
}
