//! Per-source backpressure credit table for the `ThrottleDecision` protocol (R7.2).
//!
//! The coordinator sends `HeartbeatThrottleCommand` entries in the executor
//! heartbeat response.  This module stores the current `rows_per_second` limit
//! for each `source_id` and exposes a lightweight check that source operators
//! can call when deciding how many rows to emit.
//!
//! A `None` limit means "unlimited" (the throttle has been cleared).
//!
//! # Design notes
//!
//! The table is wrapped in an `Arc<DashMap>` so it can be shared between the
//! heartbeat loop (writer) and the task runner clones (readers) without a
//! coarse lock.  Real token-bucket enforcement is left as a follow-on task;
//! for now the table records the limit and emits a `tracing::info!` each time
//! a source is polled while a limit is active.

use std::sync::Arc;

use dashmap::DashMap;

/// Shared, clone-safe table of `source_id → rows_per_second` throttle limits.
///
/// Clone is cheap (`Arc` clone).  All clones share the same underlying map.
#[derive(Clone, Debug, Default)]
pub struct SourceThrottleTable {
    inner: Arc<DashMap<String, Option<u64>>>,
}

impl SourceThrottleTable {
    /// Create an empty throttle table.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// Apply a throttle limit for `source_id`.
    ///
    /// Passing `rows_per_second = None` clears the throttle (unlimited).
    pub fn apply(&self, source_id: impl Into<String>, rows_per_second: Option<u64>) {
        let source_id = source_id.into();
        match rows_per_second {
            Some(rps) => {
                tracing::info!(
                    source_id = %source_id,
                    rows_per_second = rps,
                    "source throttle applied"
                );
                self.inner.insert(source_id, Some(rps));
            }
            None => {
                tracing::info!(source_id = %source_id, "source throttle cleared (unlimited)");
                self.inner.insert(source_id, None);
            }
        }
    }

    /// Return the current limit for `source_id`, or `None` if no limit is set.
    ///
    /// A return value of `Some(None)` means the source has been explicitly set
    /// to unlimited; `None` means no entry exists in the table.
    pub fn limit_for(&self, source_id: &str) -> Option<Option<u64>> {
        self.inner.get(source_id).map(|v| *v)
    }

    /// Check whether `source_id` has an active (non-`None`) throttle limit.
    ///
    /// Returns the limit when active, or `None` when there is no limit or the
    /// entry is set to unlimited.  Source operators should call this before
    /// emitting a batch and log accordingly.
    pub fn active_limit(&self, source_id: &str) -> Option<u64> {
        self.inner.get(source_id).and_then(|v| *v)
    }

    /// Log a trace-level note when a source is polled under a throttle limit.
    ///
    /// Call this at the start of each source-poll cycle to make throttle
    /// enforcement visible in traces before a full token-bucket is wired in.
    pub fn check_and_log(&self, source_id: &str) {
        if let Some(rps) = self.active_limit(source_id) {
            tracing::info!(
                source_id = %source_id,
                rows_per_second = rps,
                "source poll: throttle limit active (enforcement pending)"
            );
        }
    }

    /// Number of entries currently tracked.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` if no entries are tracked.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_and_read_limits() {
        let table = SourceThrottleTable::new();
        assert!(table.is_empty());

        table.apply("src-a", Some(1000));
        assert_eq!(table.active_limit("src-a"), Some(1000));
        assert_eq!(table.limit_for("src-a"), Some(Some(1000)));

        // Clear the throttle.
        table.apply("src-a", None);
        assert_eq!(table.active_limit("src-a"), None);
        assert_eq!(table.limit_for("src-a"), Some(None));

        // Unknown source.
        assert_eq!(table.active_limit("src-z"), None);
        assert_eq!(table.limit_for("src-z"), None);
    }

    #[test]
    fn check_and_log_does_not_panic() {
        let table = SourceThrottleTable::new();
        table.apply("src-b", Some(500));
        // Should emit a tracing event but not panic.
        table.check_and_log("src-b");
        table.check_and_log("src-unknown");
    }

    #[test]
    fn shared_across_clones() {
        let table = SourceThrottleTable::new();
        let clone = table.clone();
        table.apply("src-c", Some(250));
        assert_eq!(clone.active_limit("src-c"), Some(250));
    }
}
