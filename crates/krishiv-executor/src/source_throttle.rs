//! Per-source backpressure credit table for the `ThrottleDecision` protocol (R7.2).
//!
//! The coordinator sends `HeartbeatThrottleCommand` entries in the executor
//! heartbeat response.  This module stores the current `rows_per_second` limit
//! for each `source_id` and enforces a token-bucket algorithm.
//!
//! A `None` limit means "unlimited" (the throttle has been cleared).

use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;

/// Shared, clone-safe table of `source_id → rows_per_second` throttle limits
/// with per-source token-bucket enforcement.
///
/// Clone is cheap (`Arc` clone).  All clones share the same underlying map.
#[derive(Clone, Debug)]
pub struct SourceThrottleTable {
    inner: Arc<DashMap<String, TokenBucket>>,
}

impl Default for SourceThrottleTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-source token bucket for rate limiting.
#[derive(Debug)]
struct TokenBucket {
    rate: u64,
    capacity: u64,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate: u64) -> Self {
        Self {
            rate,
            capacity: rate.max(1),
            tokens: rate as f64,
            last_refill: Instant::now(),
        }
    }

    fn clear() -> Self {
        Self::new(0)
    }

    fn is_active(&self) -> bool {
        self.rate > 0
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed_secs = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed_secs * self.rate as f64).min(self.capacity as f64);
        self.last_refill = now;
    }

    fn try_consume(&mut self, requested: u64) -> u64 {
        if self.rate == 0 {
            return requested;
        }
        self.refill();
        let available = self.tokens.floor() as u64;
        let granted = requested.min(available);
        self.tokens -= granted as f64;
        granted
    }
}

impl SourceThrottleTable {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// Apply a throttle limit for `source_id`.
    pub fn apply(&self, source_id: impl Into<String>, rows_per_second: Option<u64>) {
        let source_id = source_id.into();
        match rows_per_second {
            Some(rps) if rps > 0 => {
                self.inner.insert(source_id, TokenBucket::new(rps));
            }
            _ => {
                self.inner.insert(source_id, TokenBucket::clear());
            }
        }
    }

    /// Return the current rate limit for `source_id`, or `None` if no
    /// limit has been applied.
    pub fn limit_for(&self, source_id: &str) -> Option<u64> {
        let mut bucket = self.inner.get_mut(source_id)?;
        bucket.refill();
        Some(bucket.rate)
    }

    /// Number of rows the source is allowed to emit right now.
    /// Returns the rate limit if active, or `None` if unlimited/unknown.
    pub fn active_limit(&self, source_id: &str) -> Option<u64> {
        let mut bucket = self.inner.get_mut(source_id)?;
        bucket.refill();
        if bucket.is_active() {
            Some(bucket.rate)
        } else {
            None
        }
    }

    /// Try to consume `requested` rows from the source's token bucket.
    /// Returns the number of rows that may be emitted (0..=requested).
    pub fn try_consume(&self, source_id: &str, requested: u64) -> u64 {
        let Some(mut bucket) = self.inner.get_mut(source_id) else {
            return requested; // no throttle → unlimited
        };
        bucket.try_consume(requested)
    }

    /// Log the current throttle state for observability.
    pub fn check_and_log(&self, source_id: &str) {
        if let Some(rps) = self.active_limit(source_id) {
            tracing::debug!(
                source_id = %source_id,
                rows_per_second = rps,
                "source throttle active"
            );
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_new_has_full_capacity() {
        let mut tb = TokenBucket::new(100);
        assert!(tb.is_active());
        // Should be able to consume up to rate immediately (burst = rate).
        let granted = tb.try_consume(100);
        assert_eq!(granted, 100);
        // Next consume should have 0 tokens (unless time has passed).
        let granted = tb.try_consume(1);
        assert_eq!(granted, 0);
    }

    #[test]
    fn token_bucket_empty_grants_all() {
        let mut tb = TokenBucket::clear();
        assert!(!tb.is_active());
        let granted = tb.try_consume(10_000);
        assert_eq!(granted, 10_000);
    }

    #[test]
    fn token_bucket_partial_consume() {
        let mut tb = TokenBucket::new(50);
        let granted = tb.try_consume(200);
        assert_eq!(granted, 50);
    }

    #[test]
    fn apply_and_read_limits() {
        let table = SourceThrottleTable::new();
        assert!(table.is_empty());

        table.apply("src-a", Some(1000));
        let limit = table.active_limit("src-a");
        assert_eq!(limit, Some(1000));

        table.apply("src-a", Some(0));
        assert_eq!(table.active_limit("src-a"), None);

        table.apply("src-a", None);
        assert_eq!(table.active_limit("src-a"), None);

        assert_eq!(table.active_limit("src-z"), None);
    }

    #[test]
    fn try_consume_respects_limit() {
        let table = SourceThrottleTable::new();
        table.apply("src-x", Some(500));
        let granted = table.try_consume("src-x", 1000);
        assert!(granted <= 500);
        assert!(granted > 0);
    }

    #[test]
    fn try_consume_unlimited_when_no_entry() {
        let table = SourceThrottleTable::new();
        let granted = table.try_consume("src-y", 10_000);
        assert_eq!(granted, 10_000);
    }

    #[test]
    fn check_and_log_does_not_panic() {
        let table = SourceThrottleTable::new();
        table.apply("src-b", Some(500));
        table.check_and_log("src-b");
        table.check_and_log("src-unknown");
    }

    #[test]
    fn shared_across_clones() {
        let table = SourceThrottleTable::new();
        let clone = table.clone();
        table.apply("src-c", Some(250));
        let limit = clone.active_limit("src-c");
        assert_eq!(limit, Some(250));
    }
}
