//! SH7: Process-wide unified memory manager.
//!
//! Tracks memory usage across three subsystems — shuffle I/O, batch execution
//! (Arrow buffers), and streaming window state — within a single total pool.
//! Each subsystem has a configurable soft minimum (as a fraction of the total)
//! below which the region is "protected".  Any bytes above that minimum are
//! treated as borrowable by the other regions.
//!
//! # Design
//!
//! This mirrors Spark's `UnifiedMemoryManager`: a shared pool divided into
//! soft regions, where each region can expand into another's unused headroom.
//! Unlike Spark, eviction is the caller's responsibility — the manager only
//! gives a boolean answer to `try_reserve`, which drives the caller's own
//! spill/backpressure logic.
//!
//! Atomics use `Relaxed` ordering.  The counters are advisory limits, not
//! synchronisation primitives.  A brief over-limit race is tolerable; the OS
//! OOM killer remains the hard backstop.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// The three memory regions managed by [`UnifiedMemoryManager`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryRegion {
    /// Arrow record batch buffers materialised during shuffle writes/reads.
    Shuffle,
    /// Arrow record batch buffers used by plan fragment execution.
    Execution,
    /// Serialised per-key state stored by streaming window/aggregation operators.
    State,
}

/// Configuration for a [`UnifiedMemoryManager`].
///
/// Each fraction is the *minimum* share of the total pool reserved for that
/// region.  The three fractions must sum to ≤ 1.0; the remainder (if any) is
/// freely usable by any region.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UnifiedMemoryConfig {
    /// Total process-wide byte budget.
    pub total_bytes: u64,
    /// Minimum fraction reserved for shuffle memory (default 0.3).
    pub shuffle_min_fraction: f64,
    /// Minimum fraction reserved for execution memory (default 0.4).
    pub execution_min_fraction: f64,
    /// Minimum fraction reserved for streaming state memory (default 0.2).
    pub state_min_fraction: f64,
}

impl Default for UnifiedMemoryConfig {
    fn default() -> Self {
        Self {
            total_bytes: 512 * 1024 * 1024, // 512 MiB
            shuffle_min_fraction: 0.3,
            execution_min_fraction: 0.4,
            state_min_fraction: 0.2,
        }
    }
}

impl UnifiedMemoryConfig {
    /// Create a config from the given total bytes and default fractions.
    pub fn with_total(total_bytes: u64) -> Self {
        Self {
            total_bytes,
            ..Default::default()
        }
    }

    /// Override shuffle min fraction.
    #[must_use]
    pub fn with_shuffle_min(mut self, f: f64) -> Self {
        self.shuffle_min_fraction = f.clamp(0.0, 1.0);
        self
    }

    /// Override execution min fraction.
    #[must_use]
    pub fn with_execution_min(mut self, f: f64) -> Self {
        self.execution_min_fraction = f.clamp(0.0, 1.0);
        self
    }

    /// Override state min fraction.
    #[must_use]
    pub fn with_state_min(mut self, f: f64) -> Self {
        self.state_min_fraction = f.clamp(0.0, 1.0);
        self
    }
}

/// Process-wide unified memory manager.
///
/// Create via [`UnifiedMemoryManager::new`] and share an `Arc` clone across
/// executor subsystems.
pub struct UnifiedMemoryManager {
    config: UnifiedMemoryConfig,
    shuffle_used: AtomicU64,
    execution_used: AtomicU64,
    state_used: AtomicU64,
}

impl std::fmt::Debug for UnifiedMemoryManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedMemoryManager")
            .field("total_bytes", &self.config.total_bytes)
            .field("shuffle_used", &self.shuffle_used.load(Ordering::Relaxed))
            .field("execution_used", &self.execution_used.load(Ordering::Relaxed))
            .field("state_used", &self.state_used.load(Ordering::Relaxed))
            .finish()
    }
}

/// Snapshot of current usage for telemetry / metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryUsageSnapshot {
    pub shuffle_bytes: u64,
    pub execution_bytes: u64,
    pub state_bytes: u64,
    pub total_used_bytes: u64,
    pub total_capacity_bytes: u64,
}

impl MemoryUsageSnapshot {
    /// Free bytes remaining in the pool.
    pub fn free_bytes(&self) -> u64 {
        self.total_capacity_bytes.saturating_sub(self.total_used_bytes)
    }

    /// Fractional utilization 0.0–1.0.
    pub fn utilization(&self) -> f64 {
        if self.total_capacity_bytes == 0 {
            1.0
        } else {
            self.total_used_bytes as f64 / self.total_capacity_bytes as f64
        }
    }
}

impl UnifiedMemoryManager {
    pub fn new(config: UnifiedMemoryConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            shuffle_used: AtomicU64::new(0),
            execution_used: AtomicU64::new(0),
            state_used: AtomicU64::new(0),
        })
    }

    /// Create with default config and the given total byte budget.
    pub fn with_total(total_bytes: u64) -> Arc<Self> {
        Self::new(UnifiedMemoryConfig::with_total(total_bytes))
    }

    fn region_used(&self, region: MemoryRegion) -> &AtomicU64 {
        match region {
            MemoryRegion::Shuffle => &self.shuffle_used,
            MemoryRegion::Execution => &self.execution_used,
            MemoryRegion::State => &self.state_used,
        }
    }

    fn region_min_bytes(&self, region: MemoryRegion) -> u64 {
        let frac = match region {
            MemoryRegion::Shuffle => self.config.shuffle_min_fraction,
            MemoryRegion::Execution => self.config.execution_min_fraction,
            MemoryRegion::State => self.config.state_min_fraction,
        };
        (self.config.total_bytes as f64 * frac) as u64
    }

    /// Total bytes currently in use across all regions.
    pub fn total_used_bytes(&self) -> u64 {
        self.shuffle_used.load(Ordering::Relaxed)
            + self.execution_used.load(Ordering::Relaxed)
            + self.state_used.load(Ordering::Relaxed)
    }

    /// Bytes used by `region`.
    pub fn region_used_bytes(&self, region: MemoryRegion) -> u64 {
        self.region_used(region).load(Ordering::Relaxed)
    }

    /// Try to reserve `bytes` for `region`.
    ///
    /// Returns `true` if the reservation fits within the total pool.
    /// The caller should spill or apply backpressure when this returns `false`.
    pub fn try_reserve(&self, region: MemoryRegion, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        let total_used = self.total_used_bytes();
        if total_used + bytes > self.config.total_bytes {
            return false;
        }
        self.region_used(region).fetch_add(bytes, Ordering::Relaxed);
        true
    }

    /// Release `bytes` previously reserved for `region`. Saturates at zero.
    pub fn release(&self, region: MemoryRegion, bytes: u64) {
        let _ = self.region_used(region).fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |cur| Some(cur.saturating_sub(bytes)),
        );
    }

    /// Return the current usage snapshot.
    pub fn snapshot(&self) -> MemoryUsageSnapshot {
        let shuffle_bytes = self.shuffle_used.load(Ordering::Relaxed);
        let execution_bytes = self.execution_used.load(Ordering::Relaxed);
        let state_bytes = self.state_used.load(Ordering::Relaxed);
        MemoryUsageSnapshot {
            shuffle_bytes,
            execution_bytes,
            state_bytes,
            total_used_bytes: shuffle_bytes + execution_bytes + state_bytes,
            total_capacity_bytes: self.config.total_bytes,
        }
    }

    /// Remaining bytes available in the total pool.
    pub fn remaining_bytes(&self) -> u64 {
        self.config
            .total_bytes
            .saturating_sub(self.total_used_bytes())
    }

    /// Bytes available for `region` before it starts borrowing from protected
    /// minimums of other regions.
    ///
    /// Returns `0` when the total pool is exhausted or the region has exceeded
    /// its soft minimum.
    pub fn available_for_region(&self, region: MemoryRegion) -> u64 {
        let used = self.region_used(region).load(Ordering::Relaxed);
        let min = self.region_min_bytes(region);
        let region_headroom = min.saturating_sub(used);
        let total_free = self.remaining_bytes();
        total_free.max(region_headroom)
    }

    /// Whether `region` is under pressure: it has exceeded its soft minimum
    /// AND total pool utilization exceeds the given threshold.
    pub fn is_region_under_pressure(&self, region: MemoryRegion, threshold: f64) -> bool {
        let total_used = self.total_used_bytes();
        let capacity = self.config.total_bytes;
        if capacity == 0 {
            return true;
        }
        let util = total_used as f64 / capacity as f64;
        if util < threshold {
            return false;
        }
        let used = self.region_used(region).load(Ordering::Relaxed);
        used > self.region_min_bytes(region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserves_within_total_cap() {
        let umm = UnifiedMemoryManager::with_total(1000);
        assert!(umm.try_reserve(MemoryRegion::Shuffle, 300));
        assert!(umm.try_reserve(MemoryRegion::Execution, 400));
        assert!(umm.try_reserve(MemoryRegion::State, 200));
        assert_eq!(umm.total_used_bytes(), 900);
        assert!(!umm.try_reserve(MemoryRegion::Shuffle, 200), "would exceed 1000");
        assert!(umm.try_reserve(MemoryRegion::Shuffle, 100), "exactly fits");
        assert_eq!(umm.total_used_bytes(), 1000);
    }

    #[test]
    fn release_reduces_total() {
        let umm = UnifiedMemoryManager::with_total(1000);
        umm.try_reserve(MemoryRegion::Execution, 600);
        umm.release(MemoryRegion::Execution, 200);
        assert_eq!(umm.total_used_bytes(), 400);
        assert!(umm.try_reserve(MemoryRegion::Shuffle, 600));
    }

    #[test]
    fn cross_region_borrowing_fills_pool() {
        let umm = UnifiedMemoryManager::with_total(1000);
        // Shuffle can use more than its min (300) if execution is idle.
        assert!(umm.try_reserve(MemoryRegion::Shuffle, 700));
        assert!(umm.try_reserve(MemoryRegion::Shuffle, 200));
        assert!(!umm.try_reserve(MemoryRegion::Shuffle, 200), "pool full");
    }

    #[test]
    fn snapshot_sums_all_regions() {
        let umm = UnifiedMemoryManager::with_total(2000);
        umm.try_reserve(MemoryRegion::Shuffle, 100);
        umm.try_reserve(MemoryRegion::Execution, 200);
        umm.try_reserve(MemoryRegion::State, 300);
        let snap = umm.snapshot();
        assert_eq!(snap.shuffle_bytes, 100);
        assert_eq!(snap.execution_bytes, 200);
        assert_eq!(snap.state_bytes, 300);
        assert_eq!(snap.total_used_bytes, 600);
        assert_eq!(snap.free_bytes(), 1400);
    }

    #[test]
    fn is_region_under_pressure_respects_threshold() {
        let config = UnifiedMemoryConfig::with_total(1000)
            .with_shuffle_min(0.3)    // min = 300
            .with_execution_min(0.4)  // min = 400
            .with_state_min(0.2);     // min = 200
        let umm = UnifiedMemoryManager::new(config);

        // Fill to 80% total (800 bytes), 400 in shuffle (exceeds min 300).
        umm.try_reserve(MemoryRegion::Shuffle, 400);
        umm.try_reserve(MemoryRegion::Execution, 400);

        // Threshold 0.75 → util 0.80 > 0.75, shuffle used 400 > min 300 → pressure.
        assert!(umm.is_region_under_pressure(MemoryRegion::Shuffle, 0.75));
        // Execution used 400 = min 400 → NOT over min, no pressure.
        assert!(!umm.is_region_under_pressure(MemoryRegion::Execution, 0.75));
    }
}
