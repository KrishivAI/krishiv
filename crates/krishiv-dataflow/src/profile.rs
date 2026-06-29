//! Streaming execution profile for runtime behavior configuration.
//!
//! `StreamingExecutionProfile` determines how the streaming runtime
//! balances latency vs. throughput.

use krishiv_common::async_util::unix_now_ms;

/// Runtime execution profile for streaming jobs.
///
/// The execution profile determines batch sizing, flush intervals,
/// and backpressure behavior to optimize for either latency or throughput.
#[derive(Debug, Clone)]
pub enum StreamingExecutionProfile {
    /// Optimize for low latency (p99 < 100ms).
    LowLatency {
        /// Maximum rows per batch.
        max_rows: usize,
        /// Maximum bytes per batch.
        max_bytes: usize,
        /// Flush interval in milliseconds.
        flush_interval_ms: u64,
    },

    /// Optimize for throughput (rows/sec).
    Throughput {
        /// Maximum rows per batch.
        max_rows: usize,
        /// Maximum bytes per batch.
        max_bytes: usize,
        /// Flush interval in milliseconds.
        flush_interval_ms: u64,
    },

    /// Auto-switch based on backlog.
    Auto {
        /// Backlog threshold in bytes to switch profiles.
        backlog_threshold_bytes: usize,
        /// Hysteresis factor (0.0-1.0) to prevent oscillation.
        hysteresis: f64,
        /// Minimum interval between profile switches.
        min_switch_interval_ms: u64,
    },
}

impl Default for StreamingExecutionProfile {
    fn default() -> Self {
        Self::LowLatency {
            max_rows: 10_000,
            max_bytes: 1024 * 1024, // 1MB
            flush_interval_ms: 100,
        }
    }
}

impl StreamingExecutionProfile {
    /// Create a low-latency profile with custom parameters.
    pub fn low_latency(max_rows: usize, max_bytes: usize, flush_interval_ms: u64) -> Self {
        Self::LowLatency {
            max_rows,
            max_bytes,
            flush_interval_ms,
        }
    }

    /// Create a throughput profile with custom parameters.
    pub fn throughput(max_rows: usize, max_bytes: usize, flush_interval_ms: u64) -> Self {
        Self::Throughput {
            max_rows,
            max_bytes,
            flush_interval_ms,
        }
    }

    /// Create an auto profile with custom parameters.
    pub fn auto(
        backlog_threshold_bytes: usize,
        hysteresis: f64,
        min_switch_interval_ms: u64,
    ) -> Self {
        Self::Auto {
            backlog_threshold_bytes,
            hysteresis,
            min_switch_interval_ms,
        }
    }

    /// Get the maximum rows for this profile.
    pub fn max_rows(&self) -> usize {
        match self {
            Self::LowLatency { max_rows, .. } => *max_rows,
            Self::Throughput { max_rows, .. } => *max_rows,
            Self::Auto { .. } => 10_000, // default
        }
    }

    /// Get the maximum bytes for this profile.
    pub fn max_bytes(&self) -> usize {
        match self {
            Self::LowLatency { max_bytes, .. } => *max_bytes,
            Self::Throughput { max_bytes, .. } => *max_bytes,
            Self::Auto { .. } => 1024 * 1024, // default
        }
    }

    /// Get the flush interval in milliseconds.
    pub fn flush_interval_ms(&self) -> u64 {
        match self {
            Self::LowLatency {
                flush_interval_ms, ..
            } => *flush_interval_ms,
            Self::Throughput {
                flush_interval_ms, ..
            } => *flush_interval_ms,
            Self::Auto { .. } => 100, // default
        }
    }
}

/// Auto-switching execution profile manager.
///
/// Manages switching between LowLatency and Throughput profiles based on
/// backlog size, with hysteresis to prevent oscillation.
pub struct AutoProfileManager {
    /// Current active profile.
    current: StreamingExecutionProfile,
    /// When the last switch happened.
    last_switch_ms: i64,
    /// Current backlog size in bytes.
    backlog_bytes: usize,
}

impl AutoProfileManager {
    /// Create a new auto profile manager.
    pub fn new(_config: super::buffer::OutputBufferPolicy) -> Self {
        Self {
            current: StreamingExecutionProfile::default(),
            last_switch_ms: unix_now_ms(),
            backlog_bytes: 0,
        }
    }

    /// Update the backlog size and potentially switch profiles.
    pub fn update(&mut self, backlog_bytes: usize, config: &StreamingExecutionProfile) {
        self.backlog_bytes = backlog_bytes;

        if let StreamingExecutionProfile::Auto {
            backlog_threshold_bytes,
            hysteresis,
            min_switch_interval_ms,
        } = config
        {
            let now = unix_now_ms();
            let elapsed = now - self.last_switch_ms;

            if (elapsed as u64) < *min_switch_interval_ms {
                return; // Too soon to switch
            }

            let high_threshold = *backlog_threshold_bytes;
            let low_threshold = (*backlog_threshold_bytes as f64 * (1.0 - hysteresis)) as usize;

            let new_profile = if self.backlog_bytes > high_threshold {
                // High backlog → switch to throughput mode
                StreamingExecutionProfile::Throughput {
                    max_rows: 100_000,
                    max_bytes: 10 * 1024 * 1024,
                    flush_interval_ms: 1000,
                }
            } else if self.backlog_bytes < low_threshold {
                // Low backlog → switch to low-latency mode
                StreamingExecutionProfile::LowLatency {
                    max_rows: 1_000,
                    max_bytes: 64 * 1024,
                    flush_interval_ms: 10,
                }
            } else {
                return; // In hysteresis zone, don't switch
            };

            // Only switch if profile actually changed
            let is_different = !matches!(
                (&self.current, &new_profile),
                (
                    StreamingExecutionProfile::LowLatency { .. },
                    StreamingExecutionProfile::LowLatency { .. },
                ) | (
                    StreamingExecutionProfile::Throughput { .. },
                    StreamingExecutionProfile::Throughput { .. },
                )
            );

            if is_different {
                self.current = new_profile;
                self.last_switch_ms = now;
            }
        }
    }

    /// Get the current active profile.
    pub fn current(&self) -> &StreamingExecutionProfile {
        &self.current
    }
}
