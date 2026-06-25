//! Coordinator configuration.

use krishiv_proto::JobSpec;

use crate::error::SchedulerResult;

/// Job submission interface supporting both gRPC (process mode) and Kubernetes
/// CRD (operator mode) submission paths.
///
/// `GrpcJobSubmitter` and `KubernetesJobSubmitter` are deferred; the trait is
/// defined here so callers can depend on the abstraction immediately.
pub trait JobSubmitter: Send + Sync {
    fn submit(&self, spec: &JobSpec) -> SchedulerResult<()>;
}

/// Coordinator behavior knobs for deterministic R2 scheduler tests.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CoordinatorConfig {
    max_stage_retries: u32,
    heartbeat_timeout_ticks: u64,
    memory_threshold_bytes: Option<u64>,
    /// Number of ticks after coordinator restart during which streaming-job
    /// executor leases are not evicted for missing heartbeats.  Executors
    /// running streaming tasks need time to re-register after a coordinator
    /// restart; evicting them immediately would force a full re-run.
    streaming_reattach_grace_ticks: u64,
    /// Wall-clock milliseconds represented by one heartbeat tick.
    ///
    /// Used to convert tick counts into elapsed-time estimates for the
    /// per-job checkpoint interval timer.  Defaults to 1 000 ms (1 second).
    tick_period_ms: u64,
    /// Maximum wall-clock time a checkpoint epoch may wait for executor acks
    /// before the coordinator aborts it and allows the next epoch to proceed.
    checkpoint_ack_timeout_ms: u64,

    /// Consecutive task failures after which an executor is avoided by the
    /// basic circuit breaker (PRR Immediate + Short term).
    circuit_breaker_failure_threshold: u32,

    /// Maximum size in bytes for a single InlineIpc partition payload.
    ///
    /// Partitions larger than this limit are rejected with [`SchedulerError::InvalidJob`].
    /// The default is 3 MiB (3 × 1 024 × 1 024 bytes), which matches the historic
    /// hard-coded constant. Operators with large in-memory tables can raise this
    /// limit; operators with memory-constrained coordinators can lower it.
    inline_partition_limit_bytes: usize,

    /// Wall-clock milliseconds a task may stay in `Running` state without
    /// progress before the coordinator resets it (R5 stall detection).
    ///
    /// Default: 30 minutes (`30 * 60 * 1_000`). Streaming jobs with long
    /// micro-batches may need a higher value; batch jobs with a strict SLA
    /// can lower it.
    task_stall_timeout_ms: u64,

    /// Enable speculative re-execution of straggler tasks.
    ///
    /// When `true`, the coordinator periodically checks for Running tasks that
    /// are taking significantly longer than their sibling tasks in the same
    /// stage, and preemptively re-schedules them to a different executor.
    /// Requires at least `speculative_min_completed_tasks` tasks to have
    /// already Succeeded in the stage so a meaningful median can be computed.
    ///
    /// Default: `false` (opt-in).
    speculative_execution_enabled: bool,

    /// Slowdown factor that triggers speculation.
    ///
    /// A Running task is a straggler when its elapsed time exceeds
    /// `median_completed_duration_ms * speculative_slowdown_factor`.
    /// Default: `1.5` (50 % slower than the median).
    speculative_slowdown_factor: f64,

    /// Minimum number of Succeeded tasks in the stage before speculation fires.
    /// Ensures the median estimate is stable.  Default: `3`.
    speculative_min_completed_tasks: usize,

    // ── SC11: cascade circuit breaker ──────────────────────────────────────

    /// Number of executor losses in `cascade_window_ms` that trips the cascade
    /// circuit breaker.  Default: `5` (5 losses → cascade detected).
    cascade_failure_threshold: usize,

    /// Sliding window in milliseconds over which executor losses are counted.
    /// Default: `30_000` (30 s).
    cascade_window_ms: u64,

    /// Cooldown in milliseconds after the cascade circuit breaker trips: no new
    /// task assignments are issued during this period so the cluster can stabilise.
    /// Default: `60_000` (60 s).
    cascade_cooldown_ms: u64,
}

impl CoordinatorConfig {
    /// Create a coordinator config.
    pub fn new(max_stage_retries: u32, heartbeat_timeout_ticks: u64) -> Self {
        Self {
            max_stage_retries,
            heartbeat_timeout_ticks: heartbeat_timeout_ticks.max(1),
            memory_threshold_bytes: None,
            streaming_reattach_grace_ticks: 5,
            tick_period_ms: 1_000,
            checkpoint_ack_timeout_ms: 30_000,
            circuit_breaker_failure_threshold: 5,
            inline_partition_limit_bytes: 3 * 1024 * 1024,
            task_stall_timeout_ms: 30 * 60 * 1_000,
            speculative_execution_enabled: false,
            speculative_slowdown_factor: 1.5,
            speculative_min_completed_tasks: 3,
            cascade_failure_threshold: 5,
            cascade_window_ms: 30_000,
            cascade_cooldown_ms: 60_000,
        }
    }

    /// Set the memory threshold above which executors are skipped for placement.
    #[must_use]
    pub fn with_memory_threshold(mut self, bytes: u64) -> Self {
        self.memory_threshold_bytes = Some(bytes);
        self
    }

    /// Set the streaming re-attach grace period in heartbeat ticks.
    #[must_use]
    pub fn with_streaming_reattach_grace_ticks(mut self, ticks: u64) -> Self {
        self.streaming_reattach_grace_ticks = ticks;
        self
    }

    /// Set the wall-clock duration of one heartbeat tick in milliseconds.
    #[must_use]
    pub fn with_tick_period_ms(mut self, ms: u64) -> Self {
        self.tick_period_ms = ms.max(1);
        self
    }

    #[must_use]
    pub fn with_checkpoint_ack_timeout_ms(mut self, ms: u64) -> Self {
        self.checkpoint_ack_timeout_ms = ms.max(1);
        self
    }

    /// Maximum number of stage-level retries after an executor reports failure.
    pub fn max_stage_retries(&self) -> u32 {
        self.max_stage_retries
    }

    /// Number of scheduler ticks an executor can miss before it is marked lost.
    pub fn heartbeat_timeout_ticks(&self) -> u64 {
        self.heartbeat_timeout_ticks
    }

    /// Memory threshold above which executors are skipped for placement.
    pub fn memory_threshold_bytes(&self) -> Option<u64> {
        self.memory_threshold_bytes
    }

    /// Grace period after coordinator restart before streaming executor leases expire.
    pub fn streaming_reattach_grace_ticks(&self) -> u64 {
        self.streaming_reattach_grace_ticks
    }

    /// Wall-clock milliseconds per heartbeat tick.
    pub fn tick_period_ms(&self) -> u64 {
        self.tick_period_ms
    }

    pub fn checkpoint_ack_timeout_ms(&self) -> u64 {
        self.checkpoint_ack_timeout_ms
    }

    /// Consecutive failures threshold for the basic circuit breaker.
    pub fn circuit_breaker_failure_threshold(&self) -> u32 {
        self.circuit_breaker_failure_threshold
    }

    /// Maximum size in bytes for a single InlineIpc partition payload.
    pub fn inline_partition_limit_bytes(&self) -> usize {
        self.inline_partition_limit_bytes
    }

    /// Override the InlineIpc partition size limit.
    #[must_use]
    pub fn with_inline_partition_limit_bytes(mut self, limit: usize) -> Self {
        self.inline_partition_limit_bytes = limit;
        self
    }

    /// R5 stall detection threshold in milliseconds.
    pub fn task_stall_timeout_ms(&self) -> u64 {
        self.task_stall_timeout_ms
    }

    /// Override the R5 stall detection timeout.
    #[must_use]
    pub fn with_task_stall_timeout_ms(mut self, ms: u64) -> Self {
        self.task_stall_timeout_ms = ms.max(1);
        self
    }

    /// Enable or disable speculative re-execution of straggler tasks.
    #[must_use]
    pub fn with_speculative_execution(mut self, enabled: bool) -> Self {
        self.speculative_execution_enabled = enabled;
        self
    }

    /// Set the slowdown factor that triggers speculation (default: 1.5).
    #[must_use]
    pub fn with_speculative_slowdown_factor(mut self, factor: f64) -> Self {
        self.speculative_slowdown_factor = factor.max(1.0);
        self
    }

    /// Set the minimum number of completed tasks needed before speculation fires.
    #[must_use]
    pub fn with_speculative_min_completed_tasks(mut self, n: usize) -> Self {
        self.speculative_min_completed_tasks = n.max(1);
        self
    }

    /// Whether speculative re-execution is enabled.
    pub fn speculative_execution_enabled(&self) -> bool {
        self.speculative_execution_enabled
    }

    /// Slowdown factor threshold for speculation.
    pub fn speculative_slowdown_factor(&self) -> f64 {
        self.speculative_slowdown_factor
    }

    /// Minimum completed tasks before speculation may fire.
    pub fn speculative_min_completed_tasks(&self) -> usize {
        self.speculative_min_completed_tasks
    }

    // ── SC11: cascade circuit breaker accessors ────────────────────────────

    /// Number of executor losses within `cascade_window_ms` that trips the
    /// cascade circuit breaker.
    pub fn cascade_failure_threshold(&self) -> usize {
        self.cascade_failure_threshold
    }

    /// Sliding window for cascade failure counting, in milliseconds.
    pub fn cascade_window_ms(&self) -> u64 {
        self.cascade_window_ms
    }

    /// Cooldown after cascade trip: no task assignments for this many ms.
    pub fn cascade_cooldown_ms(&self) -> u64 {
        self.cascade_cooldown_ms
    }

    /// Override the cascade failure threshold (default 5).
    #[must_use]
    pub fn with_cascade_failure_threshold(mut self, n: usize) -> Self {
        self.cascade_failure_threshold = n.max(1);
        self
    }

    /// Override the cascade window (default 30 000 ms).
    #[must_use]
    pub fn with_cascade_window_ms(mut self, ms: u64) -> Self {
        self.cascade_window_ms = ms.max(1);
        self
    }

    /// Override the cascade cooldown (default 60 000 ms).
    #[must_use]
    pub fn with_cascade_cooldown_ms(mut self, ms: u64) -> Self {
        self.cascade_cooldown_ms = ms.max(1);
        self
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self::new(1, 3)
    }
}
// ── TLS configuration ─────────────────────────────────────────────────────────

/// TLS configuration for the coordinator/executor gRPC transport.
///
/// When `None` is passed to the TLS-aware server builder, connections are
/// plaintext (appropriate for K8s pod-to-pod within a NetworkPolicy-controlled
/// namespace, or local development).
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// PEM-encoded server certificate chain.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded server private key.
    pub key_pem: Vec<u8>,
    /// Optional PEM-encoded CA certificate for client certificate verification
    /// (mTLS). When `None`, client certificates are not required.
    pub ca_pem: Option<Vec<u8>>,
}

impl TlsConfig {
    /// Build a `TlsConfig` from PEM byte slices.
    pub fn new(cert_pem: impl Into<Vec<u8>>, key_pem: impl Into<Vec<u8>>) -> Self {
        Self {
            cert_pem: cert_pem.into(),
            key_pem: key_pem.into(),
            ca_pem: None,
        }
    }

    /// Attach a CA certificate for mTLS peer verification.
    #[must_use]
    pub fn with_ca(mut self, ca_pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(ca_pem.into());
        self
    }
}
