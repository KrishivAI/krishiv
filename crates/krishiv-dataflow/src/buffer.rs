//! Output buffer policy for controlling flush behavior.
//!
//! `OutputBufferPolicy` determines when buffered data should be flushed
//! based on row count, byte size, or time intervals.

use krishiv_common::async_util::unix_now_ms;

/// Controls when buffered data is flushed.
///
/// The policy can trigger flush based on one or more conditions:
/// row count, byte size, or time interval. The `flush_on_any` flag
/// determines whether any single condition triggers a flush or all
/// conditions must be met.
#[derive(Debug, Clone)]
pub struct OutputBufferPolicy {
    /// Maximum rows before flush.
    pub max_rows: Option<usize>,
    /// Maximum bytes before flush.
    pub max_bytes: Option<u64>,
    /// Maximum time (ms) before flush.
    pub flush_interval_ms: Option<u64>,
    /// If true, flush on any condition; if false, flush on all conditions.
    pub flush_on_any: bool,
}

impl Default for OutputBufferPolicy {
    fn default() -> Self {
        Self {
            max_rows: Some(10_000),
            max_bytes: Some(1024 * 1024), // 1MB
            flush_interval_ms: Some(100),
            flush_on_any: true,
        }
    }
}

impl OutputBufferPolicy {
    /// Create a new policy with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a low-latency policy (flush quickly).
    pub fn low_latency() -> Self {
        Self {
            max_rows: Some(1_000),
            max_bytes: Some(64 * 1024), // 64KB
            flush_interval_ms: Some(10),
            flush_on_any: true,
        }
    }

    /// Create a throughput policy (batch aggressively).
    pub fn throughput() -> Self {
        Self {
            max_rows: Some(100_000),
            max_bytes: Some(10 * 1024 * 1024), // 10MB
            flush_interval_ms: Some(1000),
            flush_on_any: true,
        }
    }

    /// Check if buffer should be flushed.
    pub fn should_flush(&self, current_rows: usize, current_bytes: u64, elapsed_ms: u64) -> bool {
        let row_limit = self.max_rows.is_some_and(|limit| current_rows >= limit);
        let byte_limit = self.max_bytes.is_some_and(|limit| current_bytes >= limit);
        let time_limit = self
            .flush_interval_ms
            .is_some_and(|limit| elapsed_ms >= limit);

        if self.flush_on_any {
            row_limit || byte_limit || time_limit
        } else {
            row_limit && byte_limit && time_limit
        }
    }
}

/// Stateful buffer that tracks current metrics and checks flush conditions.
pub struct OutputBuffer {
    /// The policy controlling flush behavior.
    policy: OutputBufferPolicy,
    /// Current row count.
    rows: usize,
    /// Current byte count.
    bytes: u64,
    /// Timestamp of last flush.
    last_flush_ms: i64,
}

impl OutputBuffer {
    /// Create a new buffer with the given policy.
    pub fn new(policy: OutputBufferPolicy) -> Self {
        Self {
            policy,
            rows: 0,
            bytes: 0,
            last_flush_ms: unix_now_ms(),
        }
    }

    /// Add rows to the buffer.
    pub fn add(&mut self, num_rows: usize, num_bytes: u64) {
        self.rows += num_rows;
        self.bytes += num_bytes;
    }

    /// Check if the buffer should be flushed.
    pub fn should_flush(&self) -> bool {
        let elapsed_ms = unix_now_ms().saturating_sub(self.last_flush_ms) as u64;
        self.policy.should_flush(self.rows, self.bytes, elapsed_ms)
    }

    /// Reset the buffer after a flush.
    pub fn reset(&mut self) {
        self.rows = 0;
        self.bytes = 0;
        self.last_flush_ms = unix_now_ms();
    }

    /// Get current row count.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Get current byte count.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}
