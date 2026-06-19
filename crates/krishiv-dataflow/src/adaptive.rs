use std::collections::HashMap;

// ── R7.2 Hot-key detection (SpaceSaving algorithm) ───────────────────────────

/// A key frequency estimate from the SpaceSaving tracker.
#[derive(Debug, Clone, PartialEq)]
pub struct HotKeyReport {
    /// The key value as a string representation.
    pub key: String,
    /// Estimated occurrence count (may be an overestimate).
    pub estimated_count: u64,
    /// Maximum possible error in the count estimate.
    pub max_error: u64,
    /// Heat score: estimated_count / total_items_seen (0.0 – 1.0).
    pub heat_score: f64,
}

impl HotKeyReport {
    /// Whether this key is considered "hot" at the given threshold.
    pub fn is_hot(&self, threshold: f64) -> bool {
        self.heat_score >= threshold
    }
}

/// SpaceSaving top-K frequent-item tracker.
///
/// Uses O(K) memory regardless of key cardinality.  Any key appearing in
/// more than `1/K` fraction of items is guaranteed to be tracked.
///
/// Reference: Metwally, Agarwal, Abbadi — "Efficient Computation of Frequent
/// and Top-k Elements in Data Streams" (ICDT 2005).
#[derive(Debug, Clone)]
pub struct HeavyHittersTracker {
    /// Maximum number of counters (K).
    capacity: usize,
    /// (key, estimated_count, max_error).
    counters: Vec<(String, u64, u64)>,
    /// O(1) index: key → position in `counters`.
    index: HashMap<String, usize>,
    /// Total items processed.
    total: u64,
    /// Cached position of the minimum-count entry; avoids O(n) scan on eviction.
    min_pos: usize,
}

impl HeavyHittersTracker {
    /// Create a tracker with `capacity` counter slots.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            counters: Vec::with_capacity(capacity),
            index: HashMap::new(),
            total: 0,
            min_pos: 0,
        }
    }

    /// Record an occurrence of `key`.
    pub fn observe(&mut self, key: impl Into<String>) {
        let key = key.into();
        self.total += 1;

        // O(1) lookup via index map.
        if let Some(&pos) = self.index.get(&key) {
            self.counters[pos].1 += 1;
            return;
        }

        if self.counters.len() < self.capacity {
            let pos = self.counters.len();
            // New entries start with count=1; track min_pos if this is the smallest.
            if pos == 0 || self.counters[self.min_pos].1 > 1 {
                self.min_pos = pos;
            }
            self.counters.push((key.clone(), 1, 0));
            self.index.insert(key, pos);
            return;
        }

        // Replace the minimum-count entry (SpaceSaving eviction rule).
        // Use cached min_pos — O(1) lookup instead of O(n) scan.
        let min_pos = self.min_pos;
        let min_count = self.counters[min_pos].1;
        let old_key = self.counters[min_pos].0.clone();
        self.index.remove(&old_key);
        self.counters[min_pos] = (key.clone(), min_count + 1, min_count);
        self.index.insert(key, min_pos);

        // Re-scan for the new minimum after eviction (unavoidable after replacement).
        self.min_pos = self
            .counters
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, count, _))| *count)
            .map(|(i, _)| i)
            .unwrap_or(0);
    }

    /// Return the top-K entries by estimated count, highest first.
    pub fn top_k(&self) -> Vec<HotKeyReport> {
        let mut entries: Vec<HotKeyReport> = self
            .counters
            .iter()
            .map(|(key, count, err)| HotKeyReport {
                key: key.clone(),
                estimated_count: *count,
                max_error: *err,
                heat_score: if self.total == 0 {
                    0.0
                } else {
                    *count as f64 / self.total as f64
                },
            })
            .collect();
        entries.sort_by(|a, b| {
            b.estimated_count
                .cmp(&a.estimated_count)
                .then(a.key.cmp(&b.key))
        });
        entries
    }

    /// Return entries whose heat score exceeds `threshold`.
    pub fn hot_keys(&self, threshold: f64) -> Vec<HotKeyReport> {
        self.top_k()
            .into_iter()
            .filter(|r| r.is_hot(threshold))
            .collect()
    }

    /// Total number of items observed.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Reset all counters (e.g., at checkpoint epoch boundary).
    pub fn reset(&mut self) {
        self.counters.clear();
        self.index.clear();
        self.total = 0;
        self.min_pos = 0;
    }
}

// ── R7.2 Source throttling ────────────────────────────────────────────────────

/// A throttle command sent from the coordinator to a source operator.
#[derive(Debug, Clone, PartialEq)]
pub struct ThrottleCommand {
    /// Target source operator id.
    pub source_id: String,
    /// Maximum rows per second (None = unlimited / clear throttle).
    pub rows_per_second: Option<u64>,
}

/// Token-bucket rate limiter used by `ThrottledSource`.
///
/// Replenishes `rows_per_second` tokens per second.  Callers `consume(n)`
/// tokens and are told how long to wait if the bucket is empty.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    rows_per_second: u64,
    tokens: f64,
    last_refill_ms: Option<u64>,
}

impl RateLimiter {
    /// Create a rate limiter initially full.
    pub fn new(rows_per_second: u64) -> Self {
        Self {
            rows_per_second,
            tokens: rows_per_second as f64,
            last_refill_ms: None,
        }
    }

    /// Refill tokens based on elapsed time and attempt to consume `n` tokens.
    ///
    /// Returns the number of milliseconds the caller should wait before
    /// retrying if the bucket doesn't have enough tokens, or `None` if the
    /// consumption was satisfied immediately.
    pub fn try_consume(&mut self, n: u64, now_ms: u64) -> Option<u64> {
        // On the first call, set the refill timestamp without adding tokens.
        // This prevents the huge epoch-ms elapsed time from over-filling the bucket.
        if let Some(last) = self.last_refill_ms {
            let elapsed_ms = now_ms.saturating_sub(last);
            let new_tokens = (elapsed_ms as f64 / 1000.0) * self.rows_per_second as f64;
            self.tokens = (self.tokens + new_tokens).min(self.rows_per_second as f64);
        }
        self.last_refill_ms = Some(now_ms);

        if self.tokens >= n as f64 {
            self.tokens -= n as f64;
            None
        } else {
            let deficit = n as f64 - self.tokens;
            let wait_ms = ((deficit / self.rows_per_second as f64) * 1000.0).ceil() as u64;
            Some(wait_ms.max(1))
        }
    }

    /// Update the rate limit. Excess tokens are clamped to the new rate.
    pub fn set_rate(&mut self, rows_per_second: u64) {
        self.rows_per_second = rows_per_second;
        self.tokens = self.tokens.min(rows_per_second as f64);
    }

    /// Rows per second this limiter is configured for.
    pub fn rate(&self) -> u64 {
        self.rows_per_second
    }
}

// ── R7.2 Slow-sink detection ─────────────────────────────────────────────────

/// Running statistics for one sink's write latency.
///
/// Uses Welford's online algorithm for the mean to avoid integer saturation
/// that would occur with a running total on high-frequency sinks.
#[derive(Debug, Clone, Default)]
pub struct SinkLatencyTracker {
    write_count: u64,
    running_mean: f64,
    max_latency_ms: u64,
}

impl SinkLatencyTracker {
    /// Record one write operation with `latency_ms` duration.
    pub fn record_write(&mut self, latency_ms: u64) {
        self.write_count += 1;
        // Welford: mean_n = mean_{n-1} + (x - mean_{n-1}) / n
        self.running_mean += (latency_ms as f64 - self.running_mean) / self.write_count as f64;
        self.max_latency_ms = self.max_latency_ms.max(latency_ms);
    }

    /// Average write latency in milliseconds.
    pub fn avg_latency_ms(&self) -> f64 {
        self.running_mean
    }

    /// Maximum observed write latency.
    pub fn max_latency_ms(&self) -> u64 {
        self.max_latency_ms
    }

    /// Whether this sink is "slow" relative to `threshold_ms`.
    pub fn is_slow(&self, threshold_ms: u64) -> bool {
        self.write_count > 0 && self.avg_latency_ms() > threshold_ms as f64
    }

    /// Total writes recorded.
    pub fn write_count(&self) -> u64 {
        self.write_count
    }
}

// ── R7.2 Adaptive repartitioning ─────────────────────────────────────────────

/// The kind of adaptive decision taken or suppressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveDecisionKind {
    /// A hot key was detected and sub-partition splitting was applied.
    HotKeySplit,
    /// The downstream stage partition count was increased due to skew.
    Repartition,
    /// A source was throttled to relieve downstream pressure.
    SourceThrottle,
    /// A slow sink was detected.
    SlowSinkDetected,
}

impl std::fmt::Display for AdaptiveDecisionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HotKeySplit => f.write_str("hot-key-split"),
            Self::Repartition => f.write_str("repartition"),
            Self::SourceThrottle => f.write_str("source-throttle"),
            Self::SlowSinkDetected => f.write_str("slow-sink"),
        }
    }
}

/// One recorded adaptive decision (applied or suppressed by manual override).
#[derive(Debug, Clone)]
pub struct AdaptiveDecisionLog {
    pub timestamp_ms: u64,
    pub kind: AdaptiveDecisionKind,
    pub affected_job_id: String,
    pub details: String,
    /// `true` if the decision was actually applied; `false` if suppressed.
    pub applied: bool,
}

/// Configuration for manual override of adaptive behaviors.
#[derive(Debug, Clone, Default)]
pub struct AdaptiveOverrideConfig {
    /// Disable hot-key splitting for all jobs.
    pub disable_hot_key_splitting: bool,
    /// Disable adaptive partition-count increases for all jobs.
    pub disable_adaptive_repartition: bool,
    /// Disable coordinator-driven source throttling for all jobs.
    pub disable_source_throttling: bool,
}

// ── StreamingPartitionAdvisor ─────────────────────────────────────────────────

/// Target bytes per streaming partition (128 MiB, same as batch default).
const STREAMING_TARGET_BYTES_PER_PARTITION: u64 =
    krishiv_common::partition::TARGET_BYTES_PER_PARTITION;

/// EMA-based partition count advisor for streaming jobs.
///
/// Maintains an exponential moving average of observed batch byte sizes and
/// recommends a bucket count derived from that estimate. This lets streaming
/// jobs auto-adapt parallelism without user configuration:
///
/// - When batches are large the advisor increases bucket count so no single
///   executor is overwhelmed.
/// - When batches shrink (e.g., after a source slow-down) the advisor reduces
///   bucket count to avoid unnecessary fan-out overhead.
///
/// The recommended count is always clamped to `[min_buckets, max_buckets]`.
#[derive(Debug, Clone)]
pub struct StreamingPartitionAdvisor {
    /// Exponential moving average of bytes-per-batch.
    ema_bytes: f64,
    /// EMA smoothing factor α ∈ (0, 1]. Higher = more reactive.
    alpha: f64,
    current_buckets: u32,
    min_buckets: u32,
    max_buckets: u32,
    observations: u64,
}

impl StreamingPartitionAdvisor {
    /// Create an advisor.
    ///
    /// `initial_buckets` is the starting recommendation before any data is
    /// observed. `alpha` controls how quickly the EMA reacts to new
    /// observations; 0.2 is a reasonable default (5-batch lag).
    pub fn new(initial_buckets: u32, min_buckets: u32, max_buckets: u32) -> Self {
        let min = min_buckets.max(1);
        let max = max_buckets.max(min);
        Self {
            ema_bytes: 0.0,
            alpha: 0.2,
            current_buckets: initial_buckets.clamp(min, max),
            min_buckets: min,
            max_buckets: max,
            observations: 0,
        }
    }

    /// Set the EMA smoothing factor (must be in `(0.0, 1.0]`).
    #[must_use]
    pub fn with_alpha(mut self, alpha: f64) -> Self {
        self.alpha = alpha.clamp(f64::EPSILON, 1.0);
        self
    }

    /// Record one batch observation and return the updated bucket recommendation.
    ///
    /// `batch_bytes` is the Arrow memory footprint of the batch
    /// (`RecordBatch::get_array_memory_size()`).
    pub fn observe_batch_bytes(&mut self, batch_bytes: u64) -> u32 {
        if self.observations == 0 {
            // Seed EMA with the first observation to avoid a zero-start bias.
            self.ema_bytes = batch_bytes as f64;
        } else {
            self.ema_bytes = self.alpha * batch_bytes as f64 + (1.0 - self.alpha) * self.ema_bytes;
        }
        self.observations += 1;

        // Same shared sizing brain as batch/bounded-window/IVM, fed by the EMA.
        let target = krishiv_common::partition::recommend_buckets(
            self.ema_bytes as u64,
            self.min_buckets,
            self.max_buckets,
            STREAMING_TARGET_BYTES_PER_PARTITION,
        );
        self.current_buckets = target;
        target
    }

    /// Current bucket recommendation without recording a new observation.
    pub fn current_buckets(&self) -> u32 {
        self.current_buckets
    }

    /// Number of observations recorded so far.
    pub fn observations(&self) -> u64 {
        self.observations
    }
}

#[cfg(test)]
mod streaming_advisor_tests {
    use super::*;

    #[test]
    fn advisor_starts_at_initial_buckets() {
        let advisor = StreamingPartitionAdvisor::new(4, 1, 32);
        assert_eq!(advisor.current_buckets(), 4);
    }

    #[test]
    fn advisor_clamps_initial_to_range() {
        let advisor = StreamingPartitionAdvisor::new(100, 1, 8);
        assert_eq!(advisor.current_buckets(), 8);
    }

    #[test]
    fn advisor_recommends_more_buckets_for_large_batches() {
        let mut advisor = StreamingPartitionAdvisor::new(1, 1, 64);
        // 512 MiB batch → should recommend 4 buckets (512/128)
        let large_batch = 512 * 1024 * 1024u64;
        let rec = advisor.observe_batch_bytes(large_batch);
        assert!(
            rec >= 4,
            "expected >= 4 buckets for 512 MiB batch, got {rec}"
        );
    }

    #[test]
    fn advisor_stays_within_bounds() {
        let mut advisor = StreamingPartitionAdvisor::new(2, 2, 8).with_alpha(1.0);
        // Gigantic batch: recommendation must not exceed max_buckets.
        advisor.observe_batch_bytes(u64::MAX / 2);
        assert_eq!(advisor.current_buckets(), 8);
        // Tiny batch: recommendation must not go below min_buckets.
        advisor.observe_batch_bytes(1);
        assert_eq!(advisor.current_buckets(), 2);
    }

    #[test]
    fn advisor_tracks_observations_count() {
        let mut advisor = StreamingPartitionAdvisor::new(1, 1, 16);
        assert_eq!(advisor.observations(), 0);
        advisor.observe_batch_bytes(1024);
        assert_eq!(advisor.observations(), 1);
        advisor.observe_batch_bytes(2048);
        assert_eq!(advisor.observations(), 2);
    }

    #[test]
    fn advisor_ema_smooths_spikes() {
        // After a spike followed by small batches, EMA should decay back down.
        let mut advisor = StreamingPartitionAdvisor::new(1, 1, 64).with_alpha(0.5);
        let small = 1024u64;
        let spike = 512 * 1024 * 1024u64;
        advisor.observe_batch_bytes(spike);
        let after_spike = advisor.current_buckets();
        // Feed many small batches — EMA should drift back toward 1 bucket.
        for _ in 0..20 {
            advisor.observe_batch_bytes(small);
        }
        let after_recovery = advisor.current_buckets();
        assert!(
            after_recovery < after_spike,
            "EMA should decay: after_spike={after_spike}, after_recovery={after_recovery}"
        );
    }
}
