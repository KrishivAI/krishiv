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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Job-level LLM request quota per minute (R17).
    llm_quota_requests_per_minute: u32,
    /// Job-level LLM token quota per minute (R17).
    llm_quota_tokens_per_minute: u64,

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
            llm_quota_requests_per_minute: 100,
            llm_quota_tokens_per_minute: 10_000,
            circuit_breaker_failure_threshold: 5,
            inline_partition_limit_bytes: 3 * 1024 * 1024,
        }
    }

    /// Override job-level LLM request quota (R17).
    ///
    /// # Panics
    ///
    /// Configures per-minute LLM quota limits. Zero values are clamped to 1
    /// with a warning — LLM quota enforcement requires positive limits.
    #[must_use]
    pub fn with_llm_quota(mut self, requests_per_minute: u32, tokens_per_minute: u64) -> Self {
        let rpm = if requests_per_minute == 0 {
            tracing::warn!("llm_quota_requests_per_minute is zero, clamping to 1");
            1
        } else {
            requests_per_minute
        };
        let tpm = if tokens_per_minute == 0 {
            tracing::warn!("llm_quota_tokens_per_minute is zero, clamping to 1");
            1
        } else {
            tokens_per_minute
        };
        self.llm_quota_requests_per_minute = rpm;
        self.llm_quota_tokens_per_minute = tpm;
        self
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

    /// Job-level LLM request quota per minute (R17).
    pub fn llm_quota_requests_per_minute(&self) -> u32 {
        self.llm_quota_requests_per_minute
    }

    /// Job-level LLM token quota per minute (R17).
    pub fn llm_quota_tokens_per_minute(&self) -> u64 {
        self.llm_quota_tokens_per_minute
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
