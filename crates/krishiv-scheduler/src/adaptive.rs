//! Adaptive governance types.

use std::fmt;

use krishiv_proto::{InitiateCheckpointCommand, JobId, LeaseGeneration};

// ── R7.2 Adaptive governance types ───────────────────────────────────────────

/// The kind of adaptive decision taken or suppressed by the coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveDecisionKind {
    HotKeySplit,
    Repartition,
    SourceThrottle,
    SlowSinkDetected,
}

impl fmt::Display for AdaptiveDecisionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    pub affected_job_id: JobId,
    pub details: String,
    /// `true` if the decision was actually applied; `false` if suppressed.
    pub applied: bool,
}

/// Manual override configuration for adaptive behaviors in the coordinator.
#[derive(Debug, Clone)]
pub struct AdaptiveOverrideConfig {
    pub disable_hot_key_splitting: bool,
    pub disable_adaptive_repartition: bool,
    pub disable_source_throttling: bool,
    /// Base ingestion rate (rows/s) used when computing hot-key throttle levels.
    /// Defaults to 10,000. Set via `KRISHIV_HOT_KEY_BASE_ROWS_PER_SECOND` at startup
    /// or override directly for tests.
    pub hot_key_base_rows_per_second: u64,
}

impl Default for AdaptiveOverrideConfig {
    fn default() -> Self {
        Self {
            disable_hot_key_splitting: false,
            disable_adaptive_repartition: false,
            disable_source_throttling: false,
            hot_key_base_rows_per_second: std::env::var("KRISHIV_HOT_KEY_BASE_ROWS_PER_SECOND")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
        }
    }
}

/// A throttle command the coordinator sends back to an executor in the
/// heartbeat response (R7.2 Group C).
///
/// The executor forwards this to its source operators to apply rate limiting.
#[derive(Debug, Clone, PartialEq)]
pub struct ThrottleDecision {
    /// Source operator id on the executor.
    pub source_id: String,
    /// Maximum rows per second (`None` clears the throttle).
    pub rows_per_second: Option<u64>,
}

/// Side effects returned from a successful executor heartbeat.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutorHeartbeatEffects {
    /// Source-operator throttle directives (R7.2).
    pub source_throttles: Vec<ThrottleDecision>,
    pub checkpoint_commands: Vec<InitiateCheckpointCommand>,
    /// Committed-epoch notifications driving transactional-sink commits.
    pub checkpoint_complete_commands: Vec<krishiv_proto::CheckpointCompleteCommand>,
    /// Restore directives driving executor-side state/offset reload.
    pub restore_commands: Vec<krishiv_proto::RestoreFromCheckpointCommand>,
    pub lease_generation: LeaseGeneration,
}
