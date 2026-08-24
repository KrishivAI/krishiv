// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_common::async_util::block_on;
use krishiv_connectors::ConnectorConfig;
use krishiv_connectors::registry::default_registry;
use krishiv_plan::governance::{AuthProvider, PolicyHook};
use krishiv_plan::udf::{AggregateUdf, ScalarUdf, TableUdf, UdfRegistry};
use krishiv_runtime::{
    BatchTableRegistration, ExecutionPlacement, ExecutionRuntime, InProcessCluster, JobState,
    JobStatus, LocalJobRegistry, LocalWindowExecutionSpec, RuntimeMode, build_execution_runtime,
};
use krishiv_sql::{ContinuousTableInput, SqlEngine};

use crate::compute::job::{FeedableJob, StepReport};
use crate::dataframe::DataFrame;
use crate::error::{KrishivError, Result};
use crate::stream::Stream;
use crate::types::{DeploymentTarget, ExecutionMode, StreamBatch, StreamMode};
use crate::window::StateTtlConfig;

/// Session-local lifecycle state for a background SQL job submitted through
/// [`Session::submit_sql_background`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmittedSqlJobState {
    /// Accepted and registered, but not yet running.
    Pending,
    /// Running in this process through the session's normal mode-aware runtime.
    Running,
    /// Completed successfully.
    Succeeded,
    /// Failed with an error message.
    Failed,
    /// Cancellation was requested and the driver task was aborted.
    Cancelled,
}

impl SubmittedSqlJobState {
    /// Stable lowercase state token for frontends.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the job has reached a terminal state.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for SubmittedSqlJobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Snapshot of a SQL job submitted in the background through a [`Session`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmittedSqlJobStatus {
    job_id: String,
    name: String,
    engine: crate::EngineKind,
    state: SubmittedSqlJobState,
    error: Option<String>,
}

impl SubmittedSqlJobStatus {
    fn new(
        job_id: impl Into<String>,
        name: impl Into<String>,
        engine: crate::EngineKind,
        state: SubmittedSqlJobState,
        error: Option<String>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            name: name.into(),
            engine,
            state,
            error,
        }
    }

    /// Job id used by the engine and local registry.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// User-facing job name from the compiled SQL pipeline.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Engine selected by the SQL job compiler.
    pub fn engine(&self) -> crate::EngineKind {
        self.engine
    }

    /// Current lifecycle state.
    pub fn state(&self) -> SubmittedSqlJobState {
        self.state
    }

    /// Terminal failure reason, if any.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

/// Compiled continuous streaming job shape derived from windowed SQL.
#[derive(Debug, Clone)]
pub struct CompiledContinuousStreamJob {
    source: String,
    spec: LocalWindowExecutionSpec,
}

impl CompiledContinuousStreamJob {
    fn new(source: impl Into<String>, spec: LocalWindowExecutionSpec) -> Self {
        Self {
            source: source.into(),
            spec,
        }
    }

    /// The source table referenced by the window TVF.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The compiled continuous window execution spec.
    pub fn spec(&self) -> &LocalWindowExecutionSpec {
        &self.spec
    }

    /// Consume the compiled shape into its local execution spec.
    pub fn into_spec(self) -> LocalWindowExecutionSpec {
        self.spec
    }
}

/// Session-tracked continuous stream registration metadata.
#[derive(Debug, Clone)]
pub struct RegisteredContinuousStreamJob {
    name: String,
    source: Option<String>,
    spec: LocalWindowExecutionSpec,
}

impl RegisteredContinuousStreamJob {
    fn new(
        name: impl Into<String>,
        source: Option<String>,
        spec: LocalWindowExecutionSpec,
    ) -> Self {
        Self {
            name: name.into(),
            source,
            spec,
        }
    }

    /// Stable job name/id.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Source table referenced by the original windowed SQL, when available.
    pub fn source(&self) -> Option<&str> {
        self.source.as_deref()
    }

    /// Local execution spec registered for this job.
    pub fn spec(&self) -> &LocalWindowExecutionSpec {
        &self.spec
    }
}

/// Mode-aware status view of a continuous stream job.
#[derive(Debug, Clone)]
pub struct ContinuousStreamStatus {
    job_id: String,
    source: Option<String>,
    spec: LocalWindowExecutionSpec,
    state: String,
    uses_remote_execution: bool,
    task_count: Option<usize>,
    assigned_task_count: Option<usize>,
    running_task_count: Option<usize>,
    succeeded_task_count: Option<usize>,
    failed_task_count: Option<usize>,
    pending_input_batches: Option<usize>,
    last_watermark_ms: Option<i64>,
    persisted_watermark_ms: Option<i64>,
    snapshot_available: bool,
    cycle_in_flight: Option<bool>,
}

impl ContinuousStreamStatus {
    fn local(
        job_id: impl Into<String>,
        source: Option<String>,
        spec: LocalWindowExecutionSpec,
        pending_input_batches: usize,
        last_watermark_ms: Option<i64>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            source,
            spec,
            state: String::from("registered"),
            uses_remote_execution: false,
            task_count: None,
            assigned_task_count: None,
            running_task_count: None,
            succeeded_task_count: None,
            failed_task_count: None,
            pending_input_batches: Some(pending_input_batches),
            last_watermark_ms,
            persisted_watermark_ms: last_watermark_ms,
            snapshot_available: true,
            cycle_in_flight: None,
        }
    }

    fn remote(
        view: krishiv_runtime::RemoteContinuousStreamJobView,
        source: Option<String>,
        uses_remote_execution: bool,
    ) -> Self {
        let job_id = view.job_id.clone();
        let state = view.state.clone();
        let spec = krishiv_runtime::plan_spec_to_local(&view.spec);
        Self {
            job_id,
            source,
            spec,
            state,
            uses_remote_execution,
            task_count: Some(view.task_count),
            assigned_task_count: Some(view.assigned_task_count),
            running_task_count: Some(view.running_task_count),
            succeeded_task_count: Some(view.succeeded_task_count),
            failed_task_count: Some(view.failed_task_count),
            pending_input_batches: None,
            last_watermark_ms: view.last_watermark_ms,
            persisted_watermark_ms: view.persisted_watermark_ms,
            snapshot_available: view.snapshot_available,
            cycle_in_flight: Some(view.cycle_in_flight),
        }
    }

    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    pub fn source(&self) -> Option<&str> {
        self.source.as_deref()
    }

    pub fn spec(&self) -> &LocalWindowExecutionSpec {
        &self.spec
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn uses_remote_execution(&self) -> bool {
        self.uses_remote_execution
    }

    pub fn task_count(&self) -> Option<usize> {
        self.task_count
    }

    pub fn assigned_task_count(&self) -> Option<usize> {
        self.assigned_task_count
    }

    pub fn running_task_count(&self) -> Option<usize> {
        self.running_task_count
    }

    pub fn succeeded_task_count(&self) -> Option<usize> {
        self.succeeded_task_count
    }

    pub fn failed_task_count(&self) -> Option<usize> {
        self.failed_task_count
    }

    pub fn pending_input_batches(&self) -> Option<usize> {
        self.pending_input_batches
    }

    pub fn last_watermark_ms(&self) -> Option<i64> {
        self.last_watermark_ms
    }

    pub fn persisted_watermark_ms(&self) -> Option<i64> {
        self.persisted_watermark_ms
    }

    pub fn snapshot_available(&self) -> bool {
        self.snapshot_available
    }

    pub fn cycle_in_flight(&self) -> Option<bool> {
        self.cycle_in_flight
    }
}

/// Exported checkpoint bytes for a continuous stream job.
#[derive(Debug, Clone)]
pub struct ContinuousStreamCheckpoint {
    job_id: String,
    source: Option<String>,
    spec: LocalWindowExecutionSpec,
    snapshot_bytes: Option<Vec<u8>>,
    watermark_ms: Option<i64>,
    snapshot_available: bool,
    uses_remote_execution: bool,
}

impl ContinuousStreamCheckpoint {
    fn local(
        job_id: impl Into<String>,
        source: Option<String>,
        spec: LocalWindowExecutionSpec,
        snapshot_bytes: Vec<u8>,
        watermark_ms: i64,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            source,
            spec,
            snapshot_bytes: Some(snapshot_bytes),
            watermark_ms: Some(watermark_ms),
            snapshot_available: true,
            uses_remote_execution: false,
        }
    }

    fn remote(
        checkpoint: krishiv_runtime::RemoteContinuousStreamCheckpoint,
        source: Option<String>,
        uses_remote_execution: bool,
    ) -> Self {
        let job_id = checkpoint.job_id.clone();
        let spec = krishiv_runtime::plan_spec_to_local(&checkpoint.spec);
        Self {
            job_id,
            source,
            spec,
            snapshot_bytes: checkpoint.snapshot_bytes,
            watermark_ms: checkpoint.watermark_ms,
            snapshot_available: checkpoint.snapshot_available,
            uses_remote_execution,
        }
    }

    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    pub fn source(&self) -> Option<&str> {
        self.source.as_deref()
    }

    pub fn spec(&self) -> &LocalWindowExecutionSpec {
        &self.spec
    }

    pub fn snapshot_bytes(&self) -> Option<&[u8]> {
        self.snapshot_bytes.as_deref()
    }

    pub fn watermark_ms(&self) -> Option<i64> {
        self.watermark_ms
    }

    pub fn snapshot_available(&self) -> bool {
        self.snapshot_available
    }

    pub fn uses_remote_execution(&self) -> bool {
        self.uses_remote_execution
    }
}

/// Builder for Krishiv sessions.
#[derive(Clone)]
pub struct SessionBuilder {
    mode: ExecutionMode,
    deployment_target: DeploymentTarget,
    auth: Option<Arc<dyn AuthProvider>>,
    policy: Option<Arc<dyn PolicyHook>>,
    coordinator_url: Option<String>,
    coordinator_http_url: Option<String>,
    local_cluster_grpc: Option<String>,
    state_ttl: Option<StateTtlConfig>,
    target_parallelism: Option<std::num::NonZeroUsize>,
    /// When true, route data-plane work to the remote Flight endpoint (no local fallback).
    remote_execution: bool,
    /// Reuse an existing in-process cluster (continuous stream registry, coordinator bridge).
    in_process_cluster: Option<Arc<InProcessCluster>>,
    /// Whether `with_remote_execution` was called explicitly (B2): controls
    /// whether `build()` flips Distributed mode to `remote_execution = true`
    /// automatically. Explicitly disabling it in Distributed mode now fails
    /// during build instead of silently creating a local fallback runtime.
    remote_execution_explicit: bool,
    /// Shuffle partition count override.  When set, `AutoPartitionRule` is
    /// skipped and this value is used as the bucket count for all `Hash` /
    /// `RoundRobin` exchange nodes.
    shuffle_partitions: Option<u32>,
    /// User-visible session properties propagated to the built session.
    config: std::collections::BTreeMap<String, String>,
    /// Iceberg catalogs to register with the DataFusion session, each with a
    /// catalog name. Accumulated by `with_iceberg_catalog`.
    #[cfg(feature = "iceberg-catalog")]
    iceberg_catalogs: Vec<(Arc<krishiv_sql::catalog::unified::KrishivCatalog>, String)>,
}

impl fmt::Debug for SessionBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionBuilder")
            .field("mode", &self.mode)
            .field("auth", &self.auth.as_ref().map(|_| "<AuthProvider>"))
            .field("policy", &self.policy.as_ref().map(|_| "<PolicyHook>"))
            .field("coordinator_url", &self.coordinator_url)
            .field("local_cluster_grpc", &self.local_cluster_grpc)
            .field("state_ttl", &self.state_ttl)
            .field("target_parallelism", &self.target_parallelism)
            .field("remote_execution", &self.remote_execution)
            .field("config", &self.config)
            .field(
                "in_process_cluster",
                &self
                    .in_process_cluster
                    .as_ref()
                    .map(|_| "<InProcessCluster>"),
            )
            .finish()
    }
}

impl Default for SessionBuilder {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Embedded,
            deployment_target: DeploymentTarget::Embedded,
            auth: None,
            policy: None,
            coordinator_url: None,
            coordinator_http_url: None,
            local_cluster_grpc: None,
            state_ttl: None,
            target_parallelism: None,
            remote_execution: remote_execution_from_env(),
            in_process_cluster: None,
            remote_execution_explicit: false,
            shuffle_partitions: None,
            config: std::collections::BTreeMap::new(),
            #[cfg(feature = "iceberg-catalog")]
            iceberg_catalogs: Vec::new(),
        }
    }
}

/// Read the `KRISHIV_REMOTE_EXEC` env var. Returns `Some(true|false)` if set,
/// `None` if absent so the session builder can choose mode-aware defaults.
fn remote_execution_from_env_opt() -> Option<bool> {
    std::env::var("KRISHIV_REMOTE_EXEC").ok().map(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn remote_execution_from_env() -> bool {
    remote_execution_from_env_opt().unwrap_or(false)
}

/// Returns a per-call fresh embedded runtime for orphan `DataFrame::new`/
/// `Stream::new` constructors. Previously this returned a process-global
/// `OnceLock` instance shared across every session — a major contention
/// hotspot under parallel workloads (C1).
///
/// The returned runtime carries its own `InProcessCluster` and ids; dropping
/// it tears down the cluster.
///
/// Returns an error rather than panicking so that callers can surface
/// cluster/runtime construction failures (e.g. resource exhaustion) through
/// the public `Result`-based API instead of aborting the process.
pub(crate) fn shared_embedded_runtime() -> Result<Arc<dyn ExecutionRuntime>> {
    let cluster = InProcessCluster::new().map_err(|e| {
        tracing::error!(error = %e, "failed to create in-process cluster");
        KrishivError::Runtime {
            message: format!("orphan embedded in-process cluster: {e}"),
        }
    })?;
    build_execution_runtime(
        RuntimeMode::Embedded,
        Some(Arc::new(cluster)),
        None,
        None,
        ExecutionPlacement::LocalInProcess,
    )
    .map_err(|e| {
        tracing::error!(error = %e, "failed to build embedded runtime");
        KrishivError::Runtime {
            message: format!("embedded in-process runtime placement is invalid: {e}"),
        }
    })
}

fn execution_mode_to_runtime_mode(mode: ExecutionMode) -> RuntimeMode {
    match mode {
        ExecutionMode::Embedded => RuntimeMode::Embedded,
        ExecutionMode::SingleNode => RuntimeMode::SingleNode,
        ExecutionMode::Distributed => RuntimeMode::Distributed,
    }
}

/// Returns `true` when a coordinator URL points at the local host, so the
/// session can default to `SingleNode` placement (the local-daemon case) when
/// `KRISHIV_MODE` is unset. Anything else is treated as `Distributed` to
/// satisfy the architecture invariant that remote cluster URLs are never
/// silently routed through the single-node path.
fn is_loopback_coordinator_url(url: &str) -> bool {
    // Extract the host portion between `://` and the next `/` (or end).
    let after_scheme = match url.find("://") {
        Some(i) => &url[i + 3..],
        None => return false,
    };
    let host = match after_scheme.find('/') {
        Some(i) => &after_scheme[..i],
        None => after_scheme,
    };
    // Strip userinfo (`user@host`) and port (`:port`).
    let host_no_user = host.rsplit('@').next().unwrap_or(host);
    let host_only = match host_no_user.rfind(':') {
        // IPv6 addresses use `:` as the separator; if the remaining
        // substring contains '%' (zone) or matches an IPv6 pattern, leave
        // it alone. For the loopback checks below, only `localhost`,
        // `127.0.0.1`, and `[::1]` are loopback; we treat any host with
        // a non-loopback IPv6 address as non-loopback.
        Some(i) if !host_no_user.starts_with('[') => &host_no_user[..i],
        _ => host_no_user,
    };
    let host_only = host_only.trim_matches(|c| c == '[' || c == ']');
    matches!(host_only, "localhost" | "127.0.0.1" | "::1" | "0.0.0.0")
}

fn normalize_stream_input_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let mut changed = false;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if column.data_type() == &DataType::Utf8View {
            let casted = cast(column.as_ref(), &DataType::Utf8).map_err(|error| {
                KrishivError::Runtime {
                    message: format!(
                        "failed to normalize continuous stream column '{}' from Utf8View to Utf8: {error}",
                        field.name()
                    ),
                }
            })?;
            fields.push(Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            ));
            columns.push(casted);
            changed = true;
        } else {
            fields.push(field.as_ref().clone());
            columns.push(column.clone());
        }
    }
    if !changed {
        return Ok(batch);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| {
        KrishivError::Runtime {
            message: format!("failed to rebuild normalized continuous stream batch: {error}"),
        }
    })
}

impl SessionBuilder {
    /// Create a session builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a session from environment variables.
    ///
    /// | Variable | Values | Default |
    /// |---|---|---|
    /// | `KRISHIV_MODE` | `embedded`, `single-node`, `distributed`, `k8s`, `bare-metal` | `embedded` |
    /// | `KRISHIV_COORDINATOR_URL` | Arrow Flight URL | — |
    /// | `KRISHIV_COORDINATOR` | alias for `KRISHIV_COORDINATOR_URL` | — |
    /// | `KRISHIV_REMOTE_EXEC` | `1`, `true` | derived from mode |
    /// | `KRISHIV_TARGET_PARALLELISM` | positive integer | `1` (embedded) or `available_parallelism()` |
    ///
    /// Preferred entry point for k8s and bare-metal deployments where the
    /// execution mode is injected via environment variables rather than
    /// hard-coded in source.
    pub fn from_env() -> Result<Self> {
        let mode_raw = std::env::var("KRISHIV_MODE").unwrap_or_default();
        let coordinator_url = krishiv_common::coordinator_url_env();

        let mut builder = SessionBuilder::new();

        match mode_raw.trim().to_ascii_lowercase().as_str() {
            // No mode: infer from coordinator URL presence.
            // A loopback URL defaults to SingleNode (the local daemon case).
            // A non-loopback URL must be treated as Distributed, not as
            // SingleNode, to satisfy the architecture invariant that
            // RemoteClusterRequired placement is selected for any
            // non-loopback coordinator. This prevents the silent
            // placement-drift bug where a production cluster is contacted
            // via the SingleNode path.
            "" => {
                if let Some(url) = coordinator_url {
                    if is_loopback_coordinator_url(&url) {
                        builder = builder.with_local_cluster(url);
                    } else {
                        builder = builder
                            .with_coordinator(url)
                            .with_deployment_target(DeploymentTarget::BareMetal);
                    }
                }
            }
            "embedded" => {}
            "single-node" | "single_node" | "singlenode" | "local" => {
                builder = builder.with_execution_mode(ExecutionMode::SingleNode);
                if let Some(url) = coordinator_url {
                    builder = builder.with_local_cluster(url);
                }
            }
            "distributed" | "cluster" => {
                let url = coordinator_url.ok_or_else(|| {
                    KrishivError::unsupported(
                        "KRISHIV_MODE=distributed requires KRISHIV_COORDINATOR_URL",
                    )
                })?;
                builder = builder
                    .with_coordinator(url)
                    .with_deployment_target(DeploymentTarget::BareMetal);
            }
            "bare-metal" | "baremetal" => {
                let url = coordinator_url.ok_or_else(|| {
                    KrishivError::unsupported(
                        "KRISHIV_MODE=bare-metal requires KRISHIV_COORDINATOR_URL",
                    )
                })?;
                builder = builder
                    .with_coordinator(url)
                    .with_deployment_target(DeploymentTarget::BareMetal);
            }
            "k8s" | "kubernetes" => {
                let url = coordinator_url.ok_or_else(|| {
                    KrishivError::unsupported(
                        "KRISHIV_MODE=k8s requires KRISHIV_COORDINATOR_URL \
                         (typically the coordinator service's Flight SQL endpoint)",
                    )
                })?;
                builder = builder
                    .with_coordinator(url)
                    .with_deployment_target(DeploymentTarget::Kubernetes);
            }
            other => {
                return Err(KrishivError::unsupported(format!(
                    "unknown KRISHIV_MODE '{other}'; valid values: \
                     embedded, single-node, distributed, bare-metal, k8s"
                )));
            }
        }

        if let Some(remote) = remote_execution_from_env_opt() {
            builder = builder.with_remote_execution(remote);
        }

        if let Ok(val) = std::env::var("KRISHIV_TARGET_PARALLELISM") {
            let n: usize = val.trim().parse().map_err(|_| {
                KrishivError::unsupported(format!(
                    "KRISHIV_TARGET_PARALLELISM must be a positive integer, got '{val}'"
                ))
            })?;
            let nz = std::num::NonZeroUsize::new(n).ok_or_else(|| {
                KrishivError::unsupported(
                    "KRISHIV_TARGET_PARALLELISM must be greater than 0".to_string(),
                )
            })?;
            builder = builder.with_target_parallelism(nz);
        }

        if let Ok(val) = std::env::var("KRISHIV_SHUFFLE_PARTITIONS") {
            let n: u32 = val.trim().parse().map_err(|_| {
                KrishivError::unsupported(format!(
                    "KRISHIV_SHUFFLE_PARTITIONS must be a positive integer, got '{val}'"
                ))
            })?;
            builder = builder.with_shuffle_partitions(n);
        }

        if krishiv_common::profile_requires_fail_closed_metadata(
            krishiv_common::resolve_durability_profile(),
        ) && builder.mode == ExecutionMode::Embedded
        {
            return Err(KrishivError::unsupported(
                "embedded session mode is dev-only; set KRISHIV_MODE to single-node, \
                 distributed, bare-metal, or k8s for durable deployments",
            ));
        }

        Ok(builder)
    }

    /// Override the deployment target (where the cluster physically runs).
    /// `from_env()` sets this automatically from `KRISHIV_MODE`.
    #[must_use]
    pub fn with_deployment_target(mut self, target: DeploymentTarget) -> Self {
        self.deployment_target = target;
        self
    }

    /// Select an execution mode.
    #[must_use]
    pub fn with_execution_mode(mut self, mode: ExecutionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Attach an [`AuthProvider`] for API-key authentication.
    #[must_use]
    pub fn with_auth(mut self, auth: Arc<dyn AuthProvider>) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Attach a [`PolicyHook`] for table-access control and column masking.
    #[must_use]
    pub fn with_policy(mut self, policy: Arc<dyn PolicyHook>) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Configure a remote coordinator URL and automatically switch to
    /// [`ExecutionMode::Distributed`].  The URL is the Arrow Flight endpoint
    /// of the coordinator (e.g. `"http://coordinator:50051"`).
    #[must_use]
    pub fn with_coordinator(mut self, url: impl Into<String>) -> Self {
        self.coordinator_url = Some(url.into());
        self.mode = ExecutionMode::Distributed;
        self
    }

    /// Configure the coordinator's **HTTP** base URL (e.g. `"http://coordinator:2002"`).
    ///
    /// This is where management endpoints live — notably distributed incremental
    /// view maintenance (`/api/v1/ivm/*`). It is a *different* port from the Arrow
    /// Flight data endpoint set by [`with_coordinator`](Self::with_coordinator).
    /// When unset, [`Session::ivm`](Session::ivm) falls back to the Flight URL for
    /// backward compatibility with single-port/local setups.
    #[must_use]
    pub fn with_coordinator_http(mut self, url: impl Into<String>) -> Self {
        self.coordinator_http_url = Some(url.into());
        self
    }

    /// Connect a [`ExecutionMode::SingleNode`] session to a local cluster without
    /// switching to distributed mode (Spark-like `local[*]` client).
    #[must_use]
    pub fn with_local_cluster(mut self, flight_url: impl Into<String>) -> Self {
        self.coordinator_url = Some(flight_url.into());
        self.mode = ExecutionMode::SingleNode;
        self
    }

    /// When true, batch and streaming data-plane work is routed to the configured
    /// Flight endpoint. Distributed mode requires this placement and fails closed
    /// if it is explicitly disabled.
    #[must_use]
    pub fn with_remote_execution(mut self, enabled: bool) -> Self {
        self.remote_execution = enabled;
        self.remote_execution_explicit = true;
        self
    }

    /// gRPC coordinator address (control-plane), separate from the Flight SQL URL.
    ///
    /// The coordinator exposes two protocols: Arrow Flight SQL (data-plane, set via
    /// [`with_coordinator`]) and gRPC (control-plane, set here). Both must point to
    /// the **same coordinator host** — `build()` returns an error if the hostnames
    /// differ (R12).
    #[must_use]
    pub fn with_coordinator_grpc(mut self, url: impl Into<String>) -> Self {
        self.local_cluster_grpc = Some(url.into());
        self
    }

    /// Attach streaming operator state TTL (wired to `krishiv-state` backends).
    #[must_use]
    pub fn with_state_ttl(mut self, ttl: StateTtlConfig) -> Self {
        self.state_ttl = Some(ttl);
        self
    }

    /// Reuse an existing [`InProcessCluster`] (shared continuous stream registry).
    #[must_use]
    pub fn with_in_process_cluster(mut self, cluster: Arc<InProcessCluster>) -> Self {
        self.in_process_cluster = Some(cluster);
        self
    }

    /// Set DataFusion `target_partitions` parallelism for this session.
    ///
    /// Higher values parallelise hash-join build, aggregation spilling, and
    /// parquet scans. Default: `1` (embedded) or `available_parallelism()`
    /// (single-node/daemon). Override via `KRISHIV_TARGET_PARALLELISM`.
    #[must_use]
    pub fn with_target_parallelism(mut self, n: std::num::NonZeroUsize) -> Self {
        self.target_parallelism = Some(n);
        self
    }

    /// Override the auto-computed shuffle partition count for all exchange
    /// nodes in the session.  When set, `AutoPartitionRule` is bypassed and
    /// this value is used directly.
    #[must_use]
    pub fn with_shuffle_partitions(mut self, n: u32) -> Self {
        self.shuffle_partitions = Some(n);
        self
    }

    /// Build a session.
    /// Set a user-visible session property.
    #[must_use]
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }

    /// Register an Iceberg catalog to be attached to the DataFusion session.
    ///
    /// Tables in `catalog` are resolved as `<catalog_name>.<namespace>.<table>` in SQL
    /// queries. Multiple catalogs can be registered under different names.
    ///
    /// Requires the `iceberg-catalog` feature.
    #[cfg(feature = "iceberg-catalog")]
    #[must_use]
    pub fn with_iceberg_catalog(
        mut self,
        catalog: Arc<krishiv_sql::catalog::unified::KrishivCatalog>,
        catalog_name: impl Into<String>,
    ) -> Self {
        self.iceberg_catalogs.push((catalog, catalog_name.into()));
        self
    }

    pub fn build(self) -> Result<Session> {
        // R12: Validate that coordinator_url and local_cluster_grpc point to the
        // same coordinator when both are set. The two fields serve different
        // protocols (Flight SQL vs. gRPC control-plane) but must target the same
        // physical host; mismatches cause silent routing failures.
        if let (Some(flight_url), Some(grpc_url)) =
            (&self.coordinator_url, &self.local_cluster_grpc)
        {
            fn extract_authority(url: &str) -> Option<String> {
                // Extract host:port (the full authority) without pulling in a URL
                // parser. Comparing host:port prevents same-host / different-port
                // collisions where two separate coordinator processes on the same
                // machine would silently pass a hostname-only check.
                let after_scheme = url.find("://").map(|i| &url[i + 3..]).unwrap_or(url);
                let authority = after_scheme.split('/').next().unwrap_or(after_scheme);
                Some(authority.to_string())
            }
            let flight_authority = extract_authority(flight_url);
            let grpc_authority = extract_authority(grpc_url);
            if flight_authority.is_some()
                && grpc_authority.is_some()
                && flight_authority != grpc_authority
            {
                return Err(KrishivError::unsupported(format!(
                    "coordinator Flight URL authority ('{}') and gRPC URL authority ('{}') must \
                     match; both must point to the same coordinator process",
                    flight_authority.unwrap_or_default(),
                    grpc_authority.unwrap_or_default(),
                )));
            }
        }

        let udf_registry = Arc::new(RwLock::new(UdfRegistry::new()));
        let parallelism = self.target_parallelism.unwrap_or_else(|| {
            std::thread::available_parallelism().unwrap_or(std::num::NonZeroUsize::MIN)
        });
        #[cfg_attr(not(feature = "iceberg-catalog"), allow(unused_mut))]
        let mut sql_engine = SqlEngine::new()
            .with_target_parallelism(parallelism)
            .with_udf_registry(Arc::clone(&udf_registry))
            .with_shuffle_partitions(self.shuffle_partitions);
        #[cfg(feature = "iceberg-catalog")]
        for (catalog, name) in self.iceberg_catalogs {
            sql_engine = sql_engine.with_iceberg_catalog(catalog, name);
        }
        let local_cluster = match self.in_process_cluster {
            Some(cluster) => cluster,
            None => Arc::new(InProcessCluster::new().map_err(|e| KrishivError::Runtime {
                message: e.to_string(),
            })?),
        };

        let remote_execution =
            if self.remote_execution_explicit || remote_execution_from_env_opt().is_some() {
                self.remote_execution
            } else {
                matches!(self.mode, ExecutionMode::Distributed)
                    || (matches!(self.mode, ExecutionMode::SingleNode)
                        && self.coordinator_url.is_some())
            };

        if matches!(self.mode, ExecutionMode::Distributed) && self.coordinator_url.is_none() {
            return Err(KrishivError::unsupported(
                "Distributed mode requires SessionBuilder::with_coordinator(<flight_url>); \
                 otherwise use Embedded or SingleNode",
            ));
        }

        let placement = match self.mode {
            ExecutionMode::Embedded => ExecutionPlacement::LocalInProcess,
            ExecutionMode::SingleNode if self.coordinator_url.is_some() => {
                ExecutionPlacement::SingleNodeDaemon
            }
            ExecutionMode::SingleNode => {
                return Err(KrishivError::InvalidConfig {
                    message: "SingleNode mode requires a coordinator Flight URL. \
                              Call with_coordinator() or set a coordinator URL. \
                              For in-process execution, use Embedded mode."
                        .into(),
                });
            }
            ExecutionMode::Distributed if remote_execution => {
                ExecutionPlacement::RemoteClusterRequired
            }
            ExecutionMode::Distributed => {
                return Err(KrishivError::unsupported(
                    "Distributed mode requires remote execution; remove with_remote_execution(false) \
                     or use Embedded/SingleNode for local in-process execution",
                ));
            }
        };

        let runtime = build_execution_runtime(
            execution_mode_to_runtime_mode(self.mode),
            Some(Arc::clone(&local_cluster)),
            self.coordinator_url.clone(),
            self.local_cluster_grpc.clone(),
            placement,
        )
        .map_err(|e| KrishivError::Runtime {
            message: e.to_string(),
        })?;
        // Derive deployment_target from builder field; if not explicitly set,
        // fall back to the mode-based default.
        let deployment_target = if self.deployment_target == DeploymentTarget::default()
            && self.mode != ExecutionMode::Embedded
        {
            DeploymentTarget::from(self.mode)
        } else {
            self.deployment_target
        };

        Ok(Session {
            mode: self.mode,
            deployment_target,
            sql_engine,
            jobs: Arc::new(Mutex::new(LocalJobRegistry::default())),
            next_job_id: Arc::new(AtomicU64::new(1)),
            coordinator_url: self.coordinator_url,
            coordinator_http_url: self.coordinator_http_url,
            coordinator_grpc_url: self.local_cluster_grpc,
            state_ttl: self.state_ttl,
            memory_streams: Arc::new(DashMap::new()),
            udf_registry,
            local_cluster,
            runtime,
            registered_parquet: Arc::new(DashMap::new()),
            scalar_sql_functions: Arc::new(RwLock::new(std::collections::HashMap::new())),
            python_udfs: Arc::new(DashMap::new()),
            python_udafs: Arc::new(DashMap::new()),
            registered_sources: Arc::new(DashMap::new()),
            registered_sinks: Arc::new(DashMap::new()),
            submitted_sql_jobs: Arc::new(DashMap::new()),
            submitted_sql_job_aborts: Arc::new(DashMap::new()),
            stream_jobs: Arc::new(DashMap::new()),
            unbounded_streams: Arc::new(DashMap::new()),
            config: Arc::new(DashMap::from_iter(self.config)),
            auth: self.auth,
            policy: self.policy,
            ivm_registry: Arc::new(krishiv_runtime::IvmJobRegistry::new()),
            table_spill_dir: Arc::new(Mutex::new(None)),
        })
    }
}

/// A cloudpickled Python UDF/UDAF shipped to distributed executors:
/// `(pickle base64, input Arrow type names, output type name)`.
type ShippedPythonUdf = (String, Vec<String>, String);

/// A parquet table registered on a session, with whatever the caller declared
/// about it.
///
/// This used to be a bare `PathBuf`. It is a struct so a declared primary key
/// travels *with* the table rather than in a second map that a future
/// registration site could forget to populate — the failure mode being an
/// optimization that silently stops applying on one code path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredParquet {
    /// Where the table's parquet lives (local path or object-store URI).
    pub path: PathBuf,
    /// Columns that jointly form the table's primary key, if declared.
    ///
    /// Informational and unverified — see
    /// [`Session::register_parquet_with_primary_key`].
    pub primary_key: Vec<String>,
}

impl RegisteredParquet {
    fn new<S: AsRef<str>>(path: PathBuf, primary_key: &[S]) -> Self {
        Self {
            path,
            primary_key: primary_key.iter().map(|c| c.as_ref().to_owned()).collect(),
        }
    }

    /// The form the runtime hands to a coordinator for distributed planning.
    pub(crate) fn to_registration(&self, table_name: &str) -> BatchTableRegistration {
        BatchTableRegistration::new(table_name.to_owned(), self.path.clone())
            .with_primary_key(self.primary_key.iter().cloned())
    }
}

/// Owns the temporary directory holding parquet spills of in-memory tables
/// registered in a remote session (see
/// [`Session::register_record_batches_async`]).
///
/// Held behind an `Arc` in [`Session`], which is `Clone`, so the directory
/// outlives every clone and is removed when the last one drops. A spill must
/// live as long as the registration — unlike the per-query spills in
/// `connector_runtime`, which are dropped when the query ends.
/// Distinguishes concurrent sessions' spill directories in one process.
static SESSION_SPILL_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct SessionSpillDir {
    dir: PathBuf,
}

impl Drop for SessionSpillDir {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.dir) {
            // Nothing actionable at drop time, and the directory lives under
            // the system temp root; log rather than fail a session teardown.
            tracing::debug!(
                path = %self.dir.display(),
                error = %error,
                "failed to remove session table-spill directory"
            );
        }
    }
}

/// User-facing Krishiv session.
#[derive(Clone)]
pub struct Session {
    mode: ExecutionMode,
    deployment_target: DeploymentTarget,
    sql_engine: SqlEngine,
    jobs: Arc<Mutex<LocalJobRegistry>>,
    next_job_id: Arc<AtomicU64>,
    pub(crate) coordinator_url: Option<String>,
    pub(crate) coordinator_http_url: Option<String>,
    coordinator_grpc_url: Option<String>,
    state_ttl: Option<StateTtlConfig>,
    memory_streams: Arc<DashMap<String, Vec<RecordBatch>>>,
    udf_registry: Arc<RwLock<UdfRegistry>>,
    local_cluster: Arc<InProcessCluster>,
    runtime: Arc<dyn ExecutionRuntime>,
    registered_parquet: Arc<DashMap<String, RegisteredParquet>>,
    /// Scalar SQL-expression user functions, inlined into native SQL before
    /// planning so they run anywhere the engine runs — including distributed
    /// execution on the Rust executors (no Python worker required).
    scalar_sql_functions:
        Arc<RwLock<std::collections::HashMap<String, krishiv_sql::scalar_udf::ScalarSqlFunction>>>,
    /// Cloudpickled Python UDFs to ship with distributed queries (name →
    /// (pickle base64, input Arrow type names, output type name)). Shipped as a
    /// comment directive so executors run them via a python worker subprocess.
    python_udfs: Arc<DashMap<String, ShippedPythonUdf>>,
    /// Cloudpickled Python aggregate UDFs (GROUPED_AGG), same value shape as
    /// [`Self::python_udfs`]; shipped as a `register-python-udaf` directive.
    python_udafs: Arc<DashMap<String, ShippedPythonUdf>>,
    registered_sources: Arc<DashMap<String, ConnectorConfig>>,
    registered_sinks: Arc<DashMap<String, ConnectorConfig>>,
    submitted_sql_jobs: Arc<DashMap<String, SubmittedSqlJobStatus>>,
    submitted_sql_job_aborts: Arc<DashMap<String, tokio::task::AbortHandle>>,
    stream_jobs: Arc<DashMap<String, RegisteredContinuousStreamJob>>,
    unbounded_streams: Arc<DashMap<String, Arc<ContinuousTableInput>>>,
    config: Arc<DashMap<String, String>>,
    auth: Option<Arc<dyn AuthProvider>>,
    policy: Option<Arc<dyn PolicyHook>>,
    /// Per-session registry of embedded IVM jobs created via [`Session::ivm`].
    ivm_registry: krishiv_runtime::SharedIvmJobRegistry,
    /// Lazily-created temp directory for parquet spills of in-memory tables
    /// registered in a remote session. `None` until the first such
    /// registration, so embedded sessions never create a directory.
    table_spill_dir: Arc<Mutex<Option<Arc<SessionSpillDir>>>>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("mode", &self.mode)
            .field("sql_engine", &self.sql_engine)
            .finish_non_exhaustive()
    }
}

/// How a live table created by [`Session::create_live_table`] is kept current.
///
/// This is the API-level expression of the engine-core contract (Phase 61
/// keystone): the *refresh* mode selects the compute engine behind **one**
/// declarative surface — the same query, the same result, a different engine
/// chosen by how the table is materialized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refresh {
    /// One-shot batch snapshot of the query result (Batch engine). The table
    /// holds the rows as of creation; re-run `create_live_table` to refresh.
    Batch,
    /// Continuously maintained by the incremental-view (IVM) engine — the table
    /// is a materialized view kept up to date as its inputs change.
    Incremental,
    /// Continuously maintained by the streaming engine. Placing the operator on
    /// the streaming coordinator/executors needs a running coordinator, so an
    /// embedded session surfaces a clear error rather than silently degrading.
    Continuous,
}

impl Refresh {
    /// The `engine-core` [`EngineKind`](krishiv_engine_core::EngineKind) this
    /// refresh mode targets — the API-level expression of the engine-core
    /// contract (STRUCT-1/2). The refresh mode a live table is created with *is*
    /// the compute engine that maintains it: Batch → Batch, Incremental →
    /// Incremental (IVM), Continuous → Streaming. As the engine crates come to
    /// implement `ComputeEngine` directly, this is the single mapping the
    /// unified API dispatches through.
    pub fn engine_kind(&self) -> krishiv_engine_core::EngineKind {
        match self {
            Refresh::Batch => krishiv_engine_core::EngineKind::Batch,
            Refresh::Incremental => krishiv_engine_core::EngineKind::Incremental,
            Refresh::Continuous => krishiv_engine_core::EngineKind::Streaming,
        }
    }
}

impl Session {
    /// Start building a session.
    pub fn builder() -> SessionBuilder {
        SessionBuilder::new()
    }

    /// API-11: Explicitly close the session, releasing all server-side state.
    ///
    /// Aborts background SQL jobs, stops continuous streams, and clears all
    /// per-session registries.  Without this, continuous stream registrations
    /// can outlive the session object via cloned `Arc<dyn ExecutionRuntime>`
    /// handles held by spawned DataFrames.
    pub fn close(self) {
        // Abort submitted SQL job tasks.
        for entry in self.submitted_sql_job_aborts.iter() {
            entry.value().abort();
        }
        self.submitted_sql_job_aborts.clear();
        self.submitted_sql_jobs.clear();
        // Clear stream jobs.
        self.stream_jobs.clear();
        self.unbounded_streams.clear();
        // Clear other registries.
        self.registered_parquet.clear();
        self.registered_sources.clear();
        self.registered_sinks.clear();
        self.memory_streams.clear();
        self.config.clear();
        tracing::debug!("Session closed: all registries cleared and background tasks aborted");
    }

    /// Build a session from environment variables.
    ///
    /// Reads `KRISHIV_MODE`, `KRISHIV_COORDINATOR_URL` / `KRISHIV_COORDINATOR`,
    /// and `KRISHIV_REMOTE_EXEC`.  See [`SessionBuilder::from_env`] for the full
    /// variable reference.
    ///
    /// This is the recommended entry point for container and k8s deployments
    /// where modes are configured via environment injection rather than source.
    pub fn from_env() -> Result<Self> {
        SessionBuilder::from_env()?.build()
    }

    /// Current execution mode.
    pub fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// Where the cluster physically runs (embedded, single-node, bare-metal, kubernetes).
    ///
    /// Set automatically by [`Session::from_env`] from `KRISHIV_MODE`.
    /// Override explicitly with [`SessionBuilder::with_deployment_target`].
    pub fn deployment_target(&self) -> DeploymentTarget {
        self.deployment_target
    }

    /// Set or replace a session property for client-side API behavior and tooling.
    pub fn set_config(&self, key: impl Into<String>, value: impl Into<String>) {
        self.config.insert(key.into(), value.into());
    }

    /// Return a session property.
    pub fn get_config(&self, key: &str) -> Option<String> {
        self.config.get(key).map(|value| value.value().clone())
    }

    /// Remove and return a session property.
    pub fn unset_config(&self, key: &str) -> Option<String> {
        self.config.remove(key).map(|(_, value)| value)
    }

    /// Snapshot all session properties in deterministic key order.
    pub fn configs(&self) -> std::collections::BTreeMap<String, String> {
        self.config
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Start a generic file reader builder.
    pub fn read(&self) -> crate::DataFrameReader {
        crate::DataFrameReader::new(self.clone())
    }

    /// Streaming state TTL configuration, if set.
    pub fn state_ttl(&self) -> Option<StateTtlConfig> {
        self.state_ttl
    }

    /// Convert session TTL config to a `krishiv-state` [`TtlConfig`].
    pub fn state_ttl_config(&self) -> Option<krishiv_state::TtlConfig> {
        self.state_ttl.map(StateTtlConfig::to_ttl_config)
    }

    /// Session-scoped local coordinator + executor cluster.
    pub fn local_cluster(&self) -> &InProcessCluster {
        &self.local_cluster
    }

    /// Unified execution runtime for this session.
    pub fn execution_runtime(&self) -> Arc<dyn ExecutionRuntime> {
        Arc::clone(&self.runtime)
    }

    /// Validate that the session routing configuration is consistent.
    ///
    /// **Important**: `SessionBuilder::build` now rejects distributed local
    /// fallback, so this method is mostly a defensive guard for prebuilt or
    /// externally assembled sessions.
    pub fn check_routing(&self) -> Result<()> {
        match self.mode {
            ExecutionMode::Distributed => {
                if !self.runtime.uses_remote_execution() {
                    return Err(KrishivError::unsupported(
                        "Distributed session is using a local fallback runtime; call \
                         with_remote_execution(true) or unset KRISHIV_REMOTE_EXEC=0",
                    ));
                }
                if self.coordinator_url.is_none() {
                    return Err(KrishivError::unsupported(
                        "Distributed session has no coordinator URL; call with_coordinator()",
                    ));
                }
            }
            ExecutionMode::SingleNode | ExecutionMode::Embedded => {}
        }
        Ok(())
    }

    /// Submit a continuous streaming job (unbounded sources). Returns a handle id.
    pub fn submit_stream_job(
        &self,
        name: impl Into<String>,
        spec: LocalWindowExecutionSpec,
    ) -> Result<String> {
        let name = name.into();
        self.runtime
            .register_continuous_stream(&name, &spec)
            .map_err(KrishivError::from)?;
        self.remember_stream_job(&name, None, spec);
        Ok(name)
    }

    /// Compile a windowed streaming SQL query to the continuous stream runtime shape.
    pub fn compile_continuous_stream_job(
        &self,
        query: &str,
    ) -> Result<CompiledContinuousStreamJob> {
        let plan = krishiv_sql::streaming_window_plan::compile_streaming_window_sql(query)
            .map_err(KrishivError::from)?;
        Ok(CompiledContinuousStreamJob::new(
            plan.source,
            krishiv_runtime::plan_spec_to_local(&plan.spec),
        ))
    }

    /// Register a continuous stream job from windowed streaming SQL.
    ///
    /// The query must compile through the canonical `TUMBLE` / `HOP` / `SESSION`
    /// windowed SQL path. The returned metadata is also stored in the session's
    /// continuous stream registry so frontends can list and inspect the job later.
    pub async fn register_stream_job_sql(
        &self,
        name: impl Into<String>,
        query: &str,
    ) -> Result<RegisteredContinuousStreamJob> {
        let compiled = self.compile_continuous_stream_job(query)?;
        let name = name.into();
        self.stream_async(name.clone(), compiled.spec().clone())
            .await?;
        let registered = RegisteredContinuousStreamJob::new(
            name.clone(),
            Some(compiled.source().to_string()),
            compiled.into_spec(),
        );
        self.stream_jobs.insert(name, registered.clone());
        Ok(registered)
    }

    /// Snapshot registered continuous stream jobs in deterministic name order.
    pub fn registered_stream_jobs(&self) -> Vec<RegisteredContinuousStreamJob> {
        let mut jobs = self
            .stream_jobs
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        jobs.sort_by(|left, right| left.name.cmp(&right.name));
        jobs
    }

    /// Return registered continuous stream metadata by name.
    pub fn registered_stream_job(&self, name: &str) -> Option<RegisteredContinuousStreamJob> {
        self.stream_jobs
            .get(name)
            .map(|entry| entry.value().clone())
    }

    fn continuous_stream_source(&self, job_id: &str) -> Option<String> {
        self.registered_stream_job(job_id)
            .and_then(|job| job.source().map(ToOwned::to_owned))
    }

    fn has_remote_continuous_control_plane(&self) -> bool {
        self.ivm_http_url().is_some() && self.execution_runtime().uses_remote_execution()
    }

    fn local_continuous_stream_status(&self, job_id: &str) -> Result<ContinuousStreamStatus> {
        let spec = self
            .local_cluster
            .continuous_job_spec(job_id)
            .map_err(KrishivError::from)?;
        let pending_input_batches = self
            .local_cluster
            .continuous_job_pending_batch_depth(job_id)
            .map_err(KrishivError::from)?;
        let snapshot = self
            .local_cluster
            .snapshot_continuous_job(job_id)
            .map_err(KrishivError::from)?;
        Ok(ContinuousStreamStatus::local(
            job_id,
            self.continuous_stream_source(job_id),
            spec,
            pending_input_batches,
            Some(snapshot.watermark_ms),
        ))
    }

    /// List continuous streams through the active runtime control plane.
    pub async fn list_continuous_stream_statuses(&self) -> Result<Vec<ContinuousStreamStatus>> {
        if self.has_remote_continuous_control_plane() {
            let url = self.ivm_http_url().ok_or_else(|| {
                KrishivError::unsupported("continuous stream listing requires a coordinator URL")
            })?;
            let mut streams = krishiv_runtime::execute_coordinator_list_continuous_streams(url)
                .await
                .map_err(KrishivError::from)?
                .into_iter()
                .map(|view| {
                    let source = self.continuous_stream_source(&view.job_id);
                    ContinuousStreamStatus::remote(
                        view,
                        source,
                        self.execution_runtime().uses_remote_execution(),
                    )
                })
                .collect::<Vec<_>>();
            streams.sort_by(|left, right| left.job_id.cmp(&right.job_id));
            return Ok(streams);
        }

        let mut jobs = self.local_cluster.list_continuous_jobs();
        jobs.sort();
        jobs.into_iter()
            .map(|job_id| self.local_continuous_stream_status(&job_id))
            .collect()
    }

    /// Inspect one continuous stream job if it exists.
    pub async fn continuous_stream_status(
        &self,
        job_id: &str,
    ) -> Result<Option<ContinuousStreamStatus>> {
        if self.has_remote_continuous_control_plane() {
            let url = self.ivm_http_url().ok_or_else(|| {
                KrishivError::unsupported("continuous stream status requires a coordinator URL")
            })?;
            let view = krishiv_runtime::execute_coordinator_get_continuous_stream(url, job_id)
                .await
                .map_err(KrishivError::from)?;
            return Ok(view.map(|view| {
                let source = self.continuous_stream_source(&view.job_id);
                ContinuousStreamStatus::remote(
                    view,
                    source,
                    self.execution_runtime().uses_remote_execution(),
                )
            }));
        }

        if !self
            .local_cluster
            .list_continuous_jobs()
            .iter()
            .any(|id| id == job_id)
        {
            return Ok(None);
        }
        self.local_continuous_stream_status(job_id).map(Some)
    }

    /// Export checkpoint bytes for a continuous stream job.
    pub async fn checkpoint_continuous_stream(
        &self,
        job_id: &str,
    ) -> Result<ContinuousStreamCheckpoint> {
        if self.has_remote_continuous_control_plane() {
            let url = self.ivm_http_url().ok_or_else(|| {
                KrishivError::unsupported(
                    "continuous stream checkpoint export requires a coordinator URL",
                )
            })?;
            let checkpoint =
                krishiv_runtime::execute_coordinator_checkpoint_continuous_stream(url, job_id)
                    .await
                    .map_err(KrishivError::from)?;
            return Ok(ContinuousStreamCheckpoint::remote(
                checkpoint,
                self.continuous_stream_source(job_id),
                self.execution_runtime().uses_remote_execution(),
            ));
        }

        let spec = self
            .local_cluster
            .continuous_job_spec(job_id)
            .map_err(KrishivError::from)?;
        let snapshot = self
            .local_cluster
            .snapshot_continuous_job(job_id)
            .map_err(KrishivError::from)?;
        Ok(ContinuousStreamCheckpoint::local(
            job_id,
            self.continuous_stream_source(job_id),
            spec,
            snapshot.snapshot_bytes,
            snapshot.watermark_ms,
        ))
    }

    /// Restore a continuous stream job from checkpoint bytes.
    ///
    /// The job must already be registered. In remote modes, the restore is
    /// staged on the coordinator and applied on the next fenced input cycle.
    pub async fn restore_continuous_stream(
        &self,
        job_id: &str,
        snapshot_bytes: &[u8],
    ) -> Result<ContinuousStreamStatus> {
        if snapshot_bytes.is_empty() {
            return Err(KrishivError::InvalidConfig {
                message: format!("continuous stream {job_id} restore snapshot must not be empty"),
            });
        }

        if self.has_remote_continuous_control_plane() {
            let url = self.ivm_http_url().ok_or_else(|| {
                KrishivError::unsupported("continuous stream restore requires a coordinator URL")
            })?;
            krishiv_runtime::execute_coordinator_restore_continuous_stream(
                url,
                job_id,
                snapshot_bytes,
            )
            .await
            .map_err(KrishivError::from)?;
            return self.continuous_stream_status(job_id).await?.ok_or_else(|| {
                KrishivError::unsupported(format!("continuous stream '{job_id}' is not registered"))
            });
        }

        self.local_cluster
            .restore_continuous_job(job_id, snapshot_bytes)
            .map_err(KrishivError::from)?;
        self.local_continuous_stream_status(job_id)
    }

    pub fn push_stream_job_input(&self, job_id: &str, batches: Vec<RecordBatch>) -> Result<()> {
        let batches = batches
            .into_iter()
            .map(normalize_stream_input_batch)
            .collect::<Result<Vec<_>>>()?;
        if self.stream_jobs.contains_key(job_id) {
            return self
                .runtime
                .push_continuous_stream_input(job_id, batches)
                .map_err(KrishivError::from);
        }
        if let Some(input) = self.unbounded_streams.get(job_id) {
            for batch in batches {
                input.try_send(batch).map_err(KrishivError::from)?;
            }
            return Ok(());
        }
        self.runtime
            .push_continuous_stream_input(job_id, batches)
            .map_err(KrishivError::from)
    }

    /// Asynchronously drain newly emitted batches from a continuous streaming job.
    pub async fn poll_stream_job(&self, job_id: &str) -> Result<Vec<RecordBatch>> {
        self.runtime
            .drain_continuous_stream(job_id)
            .map_err(KrishivError::from)
    }

    /// Close every window a continuous job still holds open, because its source
    /// is exhausted.
    ///
    /// [`Session::poll_stream_job`] returns only the windows the watermark has
    /// already passed. A bounded feed whose final events land inside a window
    /// nothing later closes therefore leaves that window unemitted — the caller
    /// polls, gets nothing, and reasonably concludes the job is done.
    ///
    /// The flush verb existed end-to-end through the runtime trait, both
    /// runtimes, the Flight host and the coordinator, and was reachable from
    /// none of them here — this façade is the door Python and user Rust go
    /// through, so a complete machine below was unusable above.
    ///
    /// # Errors
    ///
    /// Returns [`KrishivError`] if the job is unknown, or if this session's
    /// runtime cannot flush — in which case the answer is genuinely
    /// incomplete and the caller must decide what to do, rather than being told
    /// nothing.
    /// Register a continuous streaming job with execution-model options —
    /// run-loop parallelism, checkpointing, and a sink (task #150 P2). The
    /// options travel through the execution runtime's fail-closed seam: a
    /// runtime that cannot honour them refuses by name, and remote
    /// registrations verify the coordinator's echo of the applied shape.
    pub fn submit_stream_job_with_options(
        &self,
        name: impl Into<String>,
        spec: LocalWindowExecutionSpec,
        options: &krishiv_runtime::ContinuousRegisterOptions,
    ) -> Result<String> {
        let name = name.into();
        self.runtime
            .register_continuous_stream_with_options(&name, &spec, options)
            .map_err(KrishivError::from)?;
        self.remember_stream_job(&name, None, spec);
        Ok(name)
    }

    /// Compile streaming SQL through the SAME class ladder the engine uses
    /// (pipeline -> join -> window -> stateless) into a [`StreamingDataFrame`]
    /// that carries the compiled task, so `write().start()` registers the
    /// class-routed job instead of deriving a window spec from builder verbs.
    /// This is the SQL door into the converged terminal: every NEXMark class
    /// — two-source joins, join-to-aggregation pipelines, windows, stateless
    /// projections — goes through the one `write()` surface (task #151).
    ///
    /// The optional `side_tables` are bounded reference tables a stateless
    /// query joins against (e.g. NEXMark q13's id->label table), delivered in
    /// the spec exactly as the wire-native registration does.
    pub fn stream_sql_with_sides(
        &self,
        sql: &str,
        side_tables: Vec<krishiv_plan::stream_task::SideTableSpec>,
    ) -> Result<crate::streaming_dataframe::StreamingDataFrame> {
        use krishiv_plan::stream_task::{StatelessQuerySpec, StreamingTaskSpec};
        let task: StreamingTaskSpec =
            if krishiv_sql::streaming_pipeline_plan::looks_like_streaming_pipeline(sql) {
                StreamingTaskSpec::Pipeline(Box::new(
                    krishiv_sql::streaming_pipeline_plan::compile_streaming_pipeline_sql(sql)
                        .map_err(|e| KrishivError::unsupported(e.to_string()))?
                        .spec,
                ))
            } else if krishiv_sql::streaming_join_plan::looks_like_streaming_join(sql) {
                StreamingTaskSpec::Join(Box::new(
                    krishiv_sql::streaming_join_plan::compile_streaming_join_sql(sql)
                        .map_err(|e| KrishivError::unsupported(e.to_string()))?
                        .spec,
                ))
            } else if krishiv_sql::streaming_tvf::find_window_tvf(sql).is_some() {
                StreamingTaskSpec::Window(Box::new(
                    krishiv_sql::streaming_window_plan::compile_streaming_window_sql(sql)
                        .map_err(|e| KrishivError::unsupported(e.to_string()))?
                        .spec,
                ))
            } else {
                let source = stateless_source_table(sql).ok_or_else(|| {
                    KrishivError::unsupported(
                        "stream_sql: a stateless streaming query needs a FROM table to name \
                         the input its pushes bind to",
                    )
                })?;
                StreamingTaskSpec::Stateless(Box::new(StatelessQuerySpec {
                    sql: sql.to_owned(),
                    source,
                    side_tables,
                }))
            };
        let seed = self.sql("SELECT 1 AS seed")?;
        Ok(seed.stream().with_task_override(task))
    }

    /// [`Session::stream_sql_with_sides`] without reference side tables.
    pub fn stream_sql(&self, sql: &str) -> Result<crate::streaming_dataframe::StreamingDataFrame> {
        self.stream_sql_with_sides(sql, Vec::new())
    }

    /// Stop a continuous streaming job: its loops exit, operator state and
    /// undrained egress are discarded, and the id becomes free for a fresh
    /// registration. Routed through the execution runtime, so it reaches the
    /// in-process registry on embedded sessions and the coordinator on
    /// single-node/distributed ones.
    pub fn stop_stream_job(&self, job_id: &str) -> Result<()> {
        self.runtime
            .deregister_continuous_stream(job_id)
            .map_err(KrishivError::from)?;
        self.stream_jobs.remove(job_id);
        Ok(())
    }

    pub fn flush_stream_job(&self, job_id: &str) -> Result<Vec<RecordBatch>> {
        self.runtime
            .flush_continuous_stream(job_id)
            .map_err(KrishivError::from)
    }

    fn remember_stream_job(
        &self,
        name: &str,
        source: Option<String>,
        spec: LocalWindowExecutionSpec,
    ) {
        self.stream_jobs.insert(
            name.to_string(),
            RegisteredContinuousStreamJob::new(name, source, spec),
        );
    }

    /// Register in-memory stream batches for `memory:<name>` pipeline sources.
    pub fn register_memory_stream(
        &self,
        name: impl Into<String>,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        self.memory_streams.insert(name.into(), batches);
        Ok(())
    }

    /// Resolve batches previously registered with [`register_memory_stream`].
    pub fn memory_stream_batches(&self, name: &str) -> Option<Vec<RecordBatch>> {
        self.memory_streams.get(name).map(|v| v.clone())
    }

    /// Known local jobs.
    pub fn jobs(&self) -> Vec<JobStatus> {
        match self.jobs.lock() {
            Ok(guard) => guard.snapshot(),
            Err(e) => {
                tracing::warn!("job registry lock poisoned: {e}; returning empty job list");
                e.into_inner().snapshot()
            }
        }
    }

    /// Submit a SQL pipeline script on a background task and track its local
    /// lifecycle in this session.
    ///
    /// Execution still goes through [`Session::submit`], so embedded,
    /// single-node, and distributed placement rules stay centralized. The
    /// returned status is an immediate snapshot; use
    /// [`submitted_sql_job_status`](Self::submitted_sql_job_status) or
    /// [`submitted_sql_jobs`](Self::submitted_sql_jobs) to poll.
    pub fn submit_sql_background(&self, sql: &str) -> Result<SubmittedSqlJobStatus> {
        tokio::runtime::Handle::try_current().map_err(|_| KrishivError::Runtime {
            message: "submit_sql_background requires an active Tokio runtime".into(),
        })?;

        let job = self.compile_sql_job(sql)?;
        let job_id = krishiv_runtime::JobId::try_new(job.name.clone()).map_err(|error| {
            KrishivError::InvalidConfig {
                message: format!("invalid SQL job name '{}': {error}", job.name),
            }
        })?;
        let key = job_id.as_str().to_owned();
        if let Some(existing) = self.submitted_sql_jobs.get(&key)
            && !existing.state().is_terminal()
        {
            return Err(KrishivError::InvalidConfig {
                message: format!("SQL job '{key}' is already running in this session"),
            });
        }

        let initial = SubmittedSqlJobStatus::new(
            key.clone(),
            job.name.clone(),
            job.engine,
            SubmittedSqlJobState::Running,
            None,
        );
        self.submitted_sql_jobs.insert(key.clone(), initial.clone());
        self.set_local_job_state(&job_id, &job.name, JobState::Running);

        let runner = self.clone();
        let supervisor = self.clone();
        let job_name = job.name.clone();
        let engine = job.engine;
        let worker = tokio::spawn(async move { runner.submit(job).await });
        self.submitted_sql_job_aborts
            .insert(key.clone(), worker.abort_handle());

        tokio::spawn(async move {
            let (state, error) = match worker.await {
                Ok(Ok(handle)) => match handle.status() {
                    krishiv_engine_core::JobStatus::Completed => {
                        (SubmittedSqlJobState::Succeeded, None)
                    }
                    krishiv_engine_core::JobStatus::Failed => (
                        SubmittedSqlJobState::Failed,
                        Some("engine returned failed job status".to_string()),
                    ),
                    krishiv_engine_core::JobStatus::Running => (
                        SubmittedSqlJobState::Failed,
                        Some("engine returned non-terminal running status after task exit".into()),
                    ),
                },
                Ok(Err(error)) => (SubmittedSqlJobState::Failed, Some(error.to_string())),
                Err(join_error) if join_error.is_cancelled() => (
                    SubmittedSqlJobState::Cancelled,
                    Some("job cancelled by caller".to_string()),
                ),
                Err(join_error) => (
                    SubmittedSqlJobState::Failed,
                    Some(format!(
                        "background SQL job task failed to join: {join_error}"
                    )),
                ),
            };
            let final_status =
                SubmittedSqlJobStatus::new(key.clone(), job_name.clone(), engine, state, error);
            supervisor.submitted_sql_job_aborts.remove(&key);
            supervisor
                .submitted_sql_jobs
                .insert(key.clone(), final_status);
            if let Ok(job_id) = krishiv_runtime::JobId::try_new(key) {
                supervisor.set_local_job_state(
                    &job_id,
                    &job_name,
                    submitted_sql_state_to_job_state(state),
                );
            }
        });

        Ok(initial)
    }

    /// Compile a SQL pipeline script using this session's registered connector
    /// configs to resolve bare connector names.
    pub fn compile_sql_job(&self, sql: &str) -> Result<crate::CompiledJob> {
        crate::sql_job::compile_sql_job_with_registered_connectors(
            sql,
            &self.registered_source_configs(),
            &self.registered_sink_configs(),
        )
    }

    /// Snapshot all SQL jobs submitted through [`submit_sql_background`].
    pub fn submitted_sql_jobs(&self) -> Vec<SubmittedSqlJobStatus> {
        let mut jobs = self
            .submitted_sql_jobs
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        jobs.sort_by(|left, right| left.job_id().cmp(right.job_id()));
        jobs
    }

    /// Return one background SQL job status by id.
    pub fn submitted_sql_job_status(&self, job_id: &str) -> Option<SubmittedSqlJobStatus> {
        self.submitted_sql_jobs
            .get(job_id)
            .map(|entry| entry.value().clone())
    }

    /// Cancel a background SQL job submitted through this session.
    pub fn cancel_submitted_sql_job(&self, job_id: &str) -> Result<SubmittedSqlJobStatus> {
        let current = self.submitted_sql_job_status(job_id).ok_or_else(|| {
            KrishivError::unsupported(format!("SQL job '{job_id}' is not tracked in this session"))
        })?;
        if current.state().is_terminal() {
            return Ok(current);
        }

        if let Some((_, abort)) = self.submitted_sql_job_aborts.remove(job_id) {
            abort.abort();
        }
        let cancelled = SubmittedSqlJobStatus::new(
            current.job_id().to_string(),
            current.name().to_string(),
            current.engine(),
            SubmittedSqlJobState::Cancelled,
            Some("job cancelled by caller".into()),
        );
        self.submitted_sql_jobs
            .insert(job_id.to_string(), cancelled.clone());
        if let Ok(id) = krishiv_runtime::JobId::try_new(job_id) {
            self.set_local_job_state(&id, current.name(), JobState::Cancelled);
        }
        Ok(cancelled)
    }

    fn set_local_job_state(&self, job_id: &krishiv_runtime::JobId, name: &str, state: JobState) {
        let mut jobs = self.jobs.lock().unwrap_or_else(|error| error.into_inner());
        jobs.upsert(JobStatus::new(job_id.clone(), name, state));
    }

    /// A-4: list every job tracked by the configured coordinator
    /// (`GET /api/v1/jobs`).
    ///
    /// For local jobs, use [`Session::jobs`]. For a single job by name
    /// (distributed), use [`Session::get_job_remote`].
    pub async fn list_jobs_remote(&self) -> Result<Vec<krishiv_scheduler::LiveJobView>> {
        let url = self.ivm_http_url().ok_or_else(|| {
            KrishivError::unsupported(
                "list_jobs_remote requires a coordinator URL; \
                 call with_coordinator() or set KRISHIV_COORDINATOR_URL",
            )
        })?;
        let jobs = krishiv_runtime::execute_coordinator_list_jobs(url)
            .await
            .map_err(KrishivError::from)?;
        Ok(jobs)
    }

    /// List every executor tracked by the configured coordinator
    /// (`GET /api/v1/executors`).
    pub async fn list_executors_remote(&self) -> Result<Vec<krishiv_scheduler::LiveExecutorView>> {
        let url = self.ivm_http_url().ok_or_else(|| {
            KrishivError::unsupported(
                "list_executors_remote requires a coordinator URL; \
                 call with_coordinator() or set KRISHIV_COORDINATOR_URL",
            )
        })?;
        let executors = krishiv_runtime::execute_coordinator_list_executors(url)
            .await
            .map_err(KrishivError::from)?;
        Ok(executors)
    }

    /// Cancel a job on the configured coordinator
    /// (`POST /api/v1/jobs/{job_id}/cancel`).
    pub async fn cancel_job_remote(&self, job_id: &str) -> Result<()> {
        let url = self.ivm_http_url().ok_or_else(|| {
            KrishivError::unsupported(
                "cancel_job_remote requires a coordinator URL; \
                 call with_coordinator() or set KRISHIV_COORDINATOR_URL",
            )
        })?;
        krishiv_runtime::execute_coordinator_cancel_job(url, job_id)
            .await
            .map_err(KrishivError::from)
    }

    /// Poll materialized results for a batch SQL job on the configured
    /// coordinator (`GET /api/v1/batch-sql/{job_id}`).
    pub async fn get_job_result_remote(
        &self,
        job_id: &str,
    ) -> Result<krishiv_runtime::CoordinatorBatchSqlJobResult> {
        let url = self.ivm_http_url().ok_or_else(|| {
            KrishivError::unsupported(
                "get_job_result_remote requires a coordinator URL; \
                 call with_coordinator() or set KRISHIV_COORDINATOR_URL",
            )
        })?;
        krishiv_runtime::execute_coordinator_batch_sql_result(url, job_id)
            .await
            .map_err(KrishivError::from)
    }

    /// A-4: look up a single job by name on the configured coordinator
    /// (`GET /api/v1/jobs/{job_id}`). Returns `Ok(None)` if the coordinator
    /// doesn't know about the job.
    pub async fn get_job_remote(
        &self,
        job_id: &str,
    ) -> Result<Option<krishiv_scheduler::LiveJobView>> {
        let url = self.ivm_http_url().ok_or_else(|| {
            KrishivError::unsupported(
                "get_job_remote requires a coordinator URL; \
                 call with_coordinator() or set KRISHIV_COORDINATOR_URL",
            )
        })?;
        let view = krishiv_runtime::execute_coordinator_get_job(url, job_id)
            .await
            .map_err(KrishivError::from)?;
        Ok(view)
    }

    /// Optional control-plane gRPC endpoint associated with this session.
    pub fn coordinator_grpc_url(&self) -> Option<&str> {
        self.coordinator_grpc_url.as_deref()
    }

    /// Effective coordinator HTTP base used for management endpoints.
    ///
    /// This prefers the explicit HTTP URL configured with
    /// [`SessionBuilder::with_coordinator_http`] and falls back to the
    /// coordinator/Flight URL for single-port local deployments.
    pub fn coordinator_http_url(&self) -> Option<&str> {
        self.ivm_http_url()
    }

    /// Shared UDF registry for this session.
    pub fn udf_registry(&self) -> Arc<RwLock<UdfRegistry>> {
        Arc::clone(&self.udf_registry)
    }

    /// Register a scalar SQL-expression function (e.g. name `tax`, params
    /// `["x"]`, body `"x * 1.1"`). Calls to it in subsequent queries are inlined
    /// into native SQL before planning, so the function works EVERYWHERE the
    /// engine runs — including distributed execution on the Rust executors, with
    /// no Python worker. This is the portable counterpart to Python-callable
    /// scalar UDFs (which run only in the embedded engine). Re-registering a name
    /// replaces the prior definition.
    pub fn register_scalar_sql_function(
        &self,
        name: &str,
        params: &[String],
        body: &str,
    ) -> Result<()> {
        let func = krishiv_sql::scalar_udf::ScalarSqlFunction::new(name, params, body)
            .map_err(|message| KrishivError::InvalidConfig { message })?;
        let mut map = self
            .scalar_sql_functions
            .write()
            .unwrap_or_else(|e| e.into_inner());
        map.insert(name.trim().to_lowercase(), func);
        Ok(())
    }

    /// Register a cloudpickled Python UDF to ship with distributed queries so it
    /// runs on the executors via a python worker subprocess. `input_types` /
    /// `output_type` are Arrow type names. Embedded execution uses the in-process
    /// UDF instead (registered separately).
    pub fn register_python_udf_bytes(
        &self,
        name: &str,
        pickle: &[u8],
        input_types: &[String],
        output_type: &str,
    ) {
        use base64::Engine as _;
        let pickle_b64 = base64::engine::general_purpose::STANDARD.encode(pickle);
        self.python_udfs.insert(
            name.trim().to_lowercase(),
            (pickle_b64, input_types.to_vec(), output_type.to_string()),
        );
    }

    /// Register a cloudpickled Python **aggregate** UDF (GROUPED_AGG) to ship
    /// with distributed queries; it runs on the executors via the python worker
    /// (buffer-all then finalize, mergeable across partitions). `input_types` /
    /// `output_type` are Arrow type names.
    pub fn register_python_udaf_bytes(
        &self,
        name: &str,
        pickle: &[u8],
        input_types: &[String],
        output_type: &str,
    ) {
        use base64::Engine as _;
        let pickle_b64 = base64::engine::general_purpose::STANDARD.encode(pickle);
        self.python_udafs.insert(
            name.trim().to_lowercase(),
            (pickle_b64, input_types.to_vec(), output_type.to_string()),
        );
    }

    /// Prepend `register-python-udf`/`register-python-udaf` comment directives
    /// for every registered Python scalar/aggregate UDF, so they travel with a
    /// distributed query to the executors.
    fn prepend_python_udfs(&self, query: &str) -> String {
        let directives = self.python_udf_directives();
        if directives.is_empty() {
            return query.to_string();
        }
        format!("{directives}\n{query}")
    }

    /// The registered Python UDF/UDAF registration directives, newline-joined.
    ///
    /// Executors decode these SQL comments to reconstruct the worker-backed
    /// function, so they must precede any query text that leaves the process.
    /// Returns an empty string when nothing is registered.
    fn python_udf_directives(&self) -> String {
        if self.python_udfs.is_empty() && self.python_udafs.is_empty() {
            return String::new();
        }
        let mut comments: Vec<String> = self
            .python_udfs
            .iter()
            .map(|entry| {
                let (pickle_b64, input_types, output_type) = entry.value();
                krishiv_runtime::flight_protocol::encode_python_udf(
                    entry.key(),
                    input_types,
                    output_type,
                    pickle_b64,
                )
            })
            .collect();
        comments.extend(self.python_udafs.iter().map(|entry| {
            let (pickle_b64, input_types, output_type) = entry.value();
            krishiv_runtime::flight_protocol::encode_python_udaf(
                entry.key(),
                input_types,
                output_type,
                pickle_b64,
            )
        }));
        comments.join("\n")
    }

    /// Inline any registered scalar SQL functions in `query`, returning native
    /// SQL. Cheap no-op when nothing is registered.
    fn expand_scalar_sql(&self, query: &str) -> Result<String> {
        let map = self
            .scalar_sql_functions
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if map.is_empty() {
            return Ok(query.to_string());
        }
        krishiv_sql::scalar_udf::expand_scalar_sql_functions(query, &map)
            .map_err(|message| KrishivError::InvalidConfig { message })
    }

    /// Register a vectorized scalar UDF for this session.
    ///
    /// Registration fails closed when native UDFs are forbidden by the active
    /// durability profile or when the DataFusion bridge cannot be synchronized.
    pub fn register_scalar_udf(&self, udf: Arc<dyn ScalarUdf>) -> Result<()> {
        let profile = krishiv_common::resolve_durability_profile();
        self.register_scalar_udf_for_policy(
            udf,
            krishiv_common::NativeScalarUdfPolicy::resolve(profile),
        )
    }

    fn register_scalar_udf_for_policy(
        &self,
        udf: Arc<dyn ScalarUdf>,
        policy: krishiv_common::NativeScalarUdfPolicy,
    ) -> Result<()> {
        let name = udf.name().to_owned();
        if name.trim().is_empty() {
            return Err(KrishivError::InvalidConfig {
                message: "scalar UDF name must not be empty".into(),
            });
        }

        if policy.is_forbidden() {
            return Err(KrishivError::InvalidConfig {
                message: format!(
                    "native scalar UDF registration is forbidden under durability profile \
                     '{}' (set KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS=1 to override)",
                    policy.profile()
                ),
            });
        }

        let previous = {
            let mut registry =
                self.udf_registry
                    .write()
                    .map_err(|error| KrishivError::Runtime {
                        message: format!("scalar UDF registry lock poisoned: {error}"),
                    })?;
            let previous = registry.get_scalar(&name).cloned();
            registry.register_scalar(Arc::clone(&udf));
            previous
        };

        let sync_result = block_on(self.sql_engine.sync_scalar_udfs_with_limits_for_policy(
            krishiv_plan::udf::ResourceLimits::default(),
            policy,
        ));
        if let Err(error) = sync_result {
            let mut registry =
                self.udf_registry
                    .write()
                    .map_err(|rollback_error| KrishivError::Runtime {
                        message: format!(
                            "scalar UDF synchronization failed ({error}); registry rollback \
                             failed because the lock is poisoned: {rollback_error}"
                        ),
                    })?;
            let still_current = registry
                .get_scalar(&name)
                .is_some_and(|current| Arc::ptr_eq(current, &udf));
            if still_current {
                if let Some(previous) = previous {
                    registry.register_scalar(previous);
                } else {
                    registry.remove_scalar(&name);
                }
            }
            return Err(KrishivError::from(error));
        }

        Ok(())
    }

    /// Names of scalar UDFs registered on this session.
    pub fn scalar_udf_names(&self) -> Vec<String> {
        match self.udf_registry.read() {
            Ok(guard) => guard
                .scalar_names()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            Err(e) => {
                tracing::warn!("UDF registry lock poisoned: {e}; returning empty scalar UDF list");
                e.into_inner()
                    .scalar_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            }
        }
    }

    /// Register an aggregate UDAF on this session.
    ///
    /// The UDAF becomes available in SQL via `SELECT my_udaf(col) FROM ...`.
    pub fn register_aggregate_udf(&self, udf: Arc<dyn AggregateUdf>) -> Result<()> {
        let name = udf.name().to_owned();
        if name.trim().is_empty() {
            return Err(KrishivError::InvalidConfig {
                message: "aggregate UDF name must not be empty".into(),
            });
        }
        self.udf_registry
            .write()
            .map_err(|e| KrishivError::Runtime {
                message: format!("aggregate UDF registry lock poisoned: {e}"),
            })?
            .register_aggregate(udf);
        block_on(self.sql_engine.sync_aggregate_udfs()).map_err(KrishivError::from)
    }

    /// Names of aggregate UDAFs registered on this session.
    pub fn aggregate_udf_names(&self) -> Vec<String> {
        match self.udf_registry.read() {
            Ok(guard) => guard
                .aggregate_names()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            Err(e) => {
                tracing::warn!(
                    "UDF registry lock poisoned: {e}; returning empty aggregate UDF list"
                );
                e.into_inner()
                    .aggregate_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            }
        }
    }

    /// Register a table UDTF on this session.
    ///
    /// The UDTF becomes available in SQL via `SELECT * FROM my_udtf(arg)`.
    pub fn register_table_udf(&self, udf: Arc<dyn TableUdf>) -> Result<()> {
        let name = udf.name().to_owned();
        if name.trim().is_empty() {
            return Err(KrishivError::InvalidConfig {
                message: "table UDF name must not be empty".into(),
            });
        }
        self.udf_registry
            .write()
            .map_err(|e| KrishivError::Runtime {
                message: format!("table UDF registry lock poisoned: {e}"),
            })?
            .register_table(udf);
        block_on(self.sql_engine.sync_table_udfs()).map_err(KrishivError::from)
    }

    /// Names of table UDTFs registered on this session.
    pub fn table_udf_names(&self) -> Vec<String> {
        match self.udf_registry.read() {
            Ok(guard) => guard.table_names().into_iter().map(str::to_owned).collect(),
            Err(e) => {
                tracing::warn!("UDF registry lock poisoned: {e}; returning empty table UDF list");
                e.into_inner()
                    .table_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            }
        }
    }

    /// Register a local Parquet path as a SQL table.
    pub fn register_parquet(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        self.register_parquet_with_primary_key(table_name, path, &[] as &[String])
    }

    /// As [`Self::register_parquet`], declaring a primary key for the table.
    ///
    /// Parquet carries no key, so the declaration is the only way the optimizer
    /// learns that one column determines the others. It is what enables
    /// functional-dependency group-by pruning and the late-materialisation
    /// join-back, and it is **informational and unverified** — the same
    /// contract as Spark/Databricks `RELY`. A key that is not actually unique
    /// and non-null yields wrong answers, not slow ones.
    ///
    /// The declaration is kept on the session so it also travels with the table
    /// when the query is handed to a coordinator (`BatchSqlTable::primary_key`).
    /// Without that, an embedded session and a distributed one would plan
    /// different queries from the same code.
    pub fn register_parquet_with_primary_key<S: AsRef<str>>(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<Path>,
        primary_key: &[S],
    ) -> Result<()> {
        let table_name = table_name.as_ref().to_owned();
        let path = path.as_ref().to_path_buf();
        // Own the key before the async block: `block_on` requires a `Send`
        // future, and a borrowed `&[S]` is only `Send` when `S: Sync`. Cloning
        // a handful of column names is cheaper than putting that bound on a
        // public signature every caller would then have to satisfy.
        let declared: Vec<String> = primary_key.iter().map(|c| c.as_ref().to_owned()).collect();
        block_on({
            let declared = declared.clone();
            let table_name = table_name.clone();
            let path = path.clone();
            let engine = self.sql_engine.clone();
            async move {
                engine
                    .register_parquet_with_primary_key(&table_name, &path, &declared)
                    .await
                    .map_err(KrishivError::from)
            }
        })?;
        self.registered_parquet.insert(
            table_name,
            RegisteredParquet {
                path,
                primary_key: declared,
            },
        );
        Ok(())
    }

    /// Register a Parquet file as an unbounded (streaming) SQL table.
    ///
    /// This causes the engine to process the file as an unbounded stream
    /// via the `ExecutionKind::Streaming` paths.
    pub fn register_parquet_stream(&self, name: &str, path: &Path) -> Result<()> {
        self.register_parquet(name, path)?;
        self.sql_engine
            .register_streaming_source_name(name)
            .map_err(KrishivError::from)?;
        Ok(())
    }
    ///
    /// Alias for [`register_parquet`] that matches the `register_unbounded` naming.
    pub fn register_bounded(&self, name: &str, path: &Path) -> Result<()> {
        self.register_parquet(name, path)
    }

    /// Mark a table name as an unbounded streaming source in the SQL engine.
    ///
    /// After this call, [`Session::is_streaming_query`] returns `true` for
    /// any SQL that references `name`. Submit batches with
    /// [`Session::push_stream_job_input`] and terminate the source with
    /// [`Session::close_unbounded_input`]. A continuous table has one consuming
    /// query; a second execution receives an explicit stream error.
    pub fn register_unbounded(&self, name: &str, schema: SchemaRef) -> Result<()> {
        self.register_unbounded_input(name, schema, None)?;
        Ok(())
    }

    /// Register an unbounded streaming table and return its push handle.
    ///
    /// Connector bridge loops (the Kinesis/Pulsar consumers in the Python
    /// bindings) push batches through the handle; terminate the source with
    /// [`Session::close_unbounded_input`].
    pub fn register_unbounded_source(
        &self,
        name: &str,
        schema: SchemaRef,
    ) -> Result<Arc<ContinuousTableInput>> {
        self.register_unbounded_input(name, schema, None)
    }

    /// Register an unbounded streaming table with a specific queue capacity.
    pub fn register_unbounded_with_capacity(
        &self,
        name: &str,
        schema: SchemaRef,
        capacity: usize,
    ) -> Result<()> {
        self.register_unbounded_input(name, schema, Some(capacity))?;
        Ok(())
    }

    fn register_unbounded_input(
        &self,
        name: &str,
        schema: SchemaRef,
        capacity: Option<usize>,
    ) -> Result<Arc<ContinuousTableInput>> {
        if schema.fields().is_empty() {
            return Err(KrishivError::InvalidConfig {
                message: "unbounded stream schema must contain at least one field".into(),
            });
        }
        let input = match capacity {
            Some(capacity) => self
                .sql_engine
                .register_streaming_table_with_capacity(name, schema, capacity),
            None => self.sql_engine.register_streaming_table(name, schema),
        }
        .map_err(KrishivError::from)?;
        self.unbounded_streams
            .insert(name.to_string(), Arc::clone(&input));
        Ok(input)
    }

    /// Close a registered unbounded input by name.
    pub fn close_unbounded_input(&self, name: &str) -> Result<bool> {
        let input =
            self.unbounded_streams
                .get(name)
                .ok_or_else(|| KrishivError::InvalidConfig {
                    message: format!("unbounded stream '{name}' is not registered"),
                })?;
        input.close().map_err(KrishivError::from)
    }

    /// Returns `true` if `sql` references any registered streaming source.
    pub fn is_streaming_query(&self, sql: &str) -> Result<bool> {
        self.sql_engine
            .is_streaming_query(sql)
            .map_err(KrishivError::from)
    }

    /// Register a Kafka topic as a streaming SQL table.
    ///
    /// After registration, the table is queryable as `SELECT * FROM "{name}"`.
    /// The `schema` describes the expected Arrow schema for deserialized records.
    /// Pass `group_id` to set the Kafka consumer group (defaults to `"krishiv-default"`
    /// when empty).
    #[cfg(feature = "kafka")]
    pub fn register_kafka_source(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Result<()> {
        let bootstrap_servers = bootstrap_servers.into();
        let topic = topic.into();
        let group_id = group_id.into();
        self.sql_engine
            .register_kafka_source(
                name,
                Arc::clone(&schema),
                &bootstrap_servers,
                &topic,
                &group_id,
            )
            .map_err(KrishivError::from)?;
        // In distributed mode, forward the registration to the remote coordinator
        // so the remote SQL engine can plan streaming queries over Kafka topics.
        if self.runtime.uses_remote_execution() {
            let schema_ipc_b64 = krishiv_runtime::encode_schema_ipc_b64(&schema).map_err(|e| {
                KrishivError::Runtime {
                    message: e.to_string(),
                }
            })?;
            self.runtime
                .register_kafka_source(name, &schema_ipc_b64, &bootstrap_servers, &topic, &group_id)
                .map_err(|e| KrishivError::Runtime {
                    message: e.to_string(),
                })?;
        }
        Ok(())
    }

    /// Asynchronously register a local Parquet path as a SQL table.
    pub async fn register_parquet_async(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let table_name = table_name.as_ref().to_owned();
        let path = path.as_ref().to_path_buf();
        self.sql_engine
            .register_parquet(&table_name, &path)
            .await
            .map_err(KrishivError::from)?;
        self.registered_parquet
            .insert(table_name, RegisteredParquet::new(path, &[] as &[String]));
        Ok(())
    }

    /// Register a Parquet path (local or `s3://`) for REMOTE execution WITHOUT
    /// reading it on the client.
    ///
    /// Unlike [`register_parquet`](Self::register_parquet), this builds no local
    /// table provider — it does no client-side schema inference or object-store
    /// access. It exists for the distributed case where only the coordinator and
    /// executors can reach the data: an `s3://` dataset whose credentials live on
    /// the cluster (mounted secret), not on the client. The path is shipped as a
    /// `BatchTableRegistration` with every `execute_remote` / distributed collect;
    /// the coordinator resolves the schema and the executors read the data with
    /// their own credentials.
    ///
    /// Query such a table through [`execute_remote`](Self::execute_remote) — local
    /// `sql()` planning cannot see it (there is no local provider), by design.
    /// Requires a distributed session with remote execution enabled.
    pub fn register_remote_parquet(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        if !self.runtime.uses_remote_execution() {
            return Err(KrishivError::unsupported(
                "register_remote_parquet requires a distributed session with remote execution; \
                 use register_parquet for local reads",
            ));
        }
        self.registered_parquet.insert(
            table_name.as_ref().to_owned(),
            RegisteredParquet::new(path.as_ref().to_path_buf(), &[] as &[String]),
        );
        Ok(())
    }

    /// Validate and register a connector source configuration in this session.
    ///
    /// The registration is session metadata used by SQL job compilation and
    /// frontends. Direct ad-hoc SQL access still requires registering a table
    /// provider (for example [`register_parquet_async`](Self::register_parquet_async)).
    pub fn register_source_config(&self, config: ConnectorConfig) -> Result<()> {
        default_registry()
            .validate_source(&config)
            .map_err(KrishivError::from)?;
        self.registered_sources.insert(config.name.clone(), config);
        Ok(())
    }

    /// Return a registered source config by name.
    pub fn source_config(&self, name: &str) -> Option<ConnectorConfig> {
        self.registered_sources
            .get(name)
            .map(|entry| entry.value().clone())
    }

    /// Snapshot registered source configs in deterministic name order.
    pub fn registered_source_configs(&self) -> Vec<ConnectorConfig> {
        let mut sources = self
            .registered_sources
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        sources.sort_by(|left, right| left.name.cmp(&right.name));
        sources
    }

    /// Validate and register a connector sink configuration in this session.
    ///
    /// The registration is session metadata: execution still happens through
    /// explicit job/pipeline APIs (`submit_sql`, `submit`, streaming builders).
    /// Keeping the config here lets frontends validate and reuse named sinks
    /// without bypassing connector capability checks.
    pub fn register_sink_config(&self, config: ConnectorConfig) -> Result<()> {
        default_registry()
            .validate_sink(&config)
            .map_err(KrishivError::from)?;
        self.registered_sinks.insert(config.name.clone(), config);
        Ok(())
    }

    /// Return a registered sink config by name.
    pub fn sink_config(&self, name: &str) -> Option<ConnectorConfig> {
        self.registered_sinks
            .get(name)
            .map(|entry| entry.value().clone())
    }

    /// Snapshot registered sink configs in deterministic name order.
    pub fn registered_sink_configs(&self) -> Vec<ConnectorConfig> {
        let mut sinks = self
            .registered_sinks
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        sinks.sort_by(|left, right| left.name.cmp(&right.name));
        sinks
    }

    fn dataframe_from_sql(
        &self,
        sql_dataframe: impl krishiv_sql::KrishivDataFrameOps + 'static,
    ) -> DataFrame {
        // Audit: `sql_query` is the text a remote session ships to the
        // coordinator, and it used to travel bare. `execute_remote_async`
        // prepended the registered Python UDF directives; this path — the one
        // `session.sql(q).collect()` takes — did not, so the same UDF worked
        // through one entry point and failed with "unknown function" on the
        // other. The directives are SQL comments the executors decode to
        // reconstruct the worker-backed UDF, so they belong on every query
        // that leaves the process.
        //
        // Applied only when the session actually executes remotely: the
        // embedded path resolves UDFs in-process and never reads this field,
        // and prepending comments there would change the text of the query
        // shown in diagnostics for no gain. The DataFrame plans against
        // `sql_dataframe` (built from the plain query), so this affects
        // submission only, never planning.
        let sql_query = sql_dataframe.query().map(str::to_owned);
        // The prelude travels as its own field rather than baked into
        // `sql_query`: a DataFrame transform discards `sql_query` and re-derives
        // the text from the plan (see `DataFrame::query_for_remote`), and the
        // directives have to survive that. Empty for embedded sessions, which
        // resolve UDFs in process and ship nothing.
        let remote_prelude = self
            .runtime
            .uses_remote_execution()
            .then(|| self.python_udf_directives())
            .filter(|prelude| !prelude.is_empty());
        DataFrame::from_sql_dataframe(
            self.mode,
            sql_dataframe,
            sql_query,
            self.jobs.clone(),
            self.next_job_id.clone(),
            self.coordinator_url.clone(),
            self.coordinator_http_url.clone(),
            self.runtime.clone(),
            self.registered_parquet.clone(),
            remote_prelude,
            // IVM-AUD-INT-F15 / API-D5: `to_incremental` used to make a private
            // registry per call, so its view was invisible to `Session::ivm`
            // and to `reset_ivm_job`, and two calls with the same name built
            // two isolated views. Every session-built DataFrame now carries the
            // session's registry.
            Some(self.ivm_registry.clone()),
        )
    }

    pub(crate) fn dataframe_from_batches(&self, batches: Vec<RecordBatch>) -> DataFrame {
        DataFrame::from_batches(
            self.mode,
            batches,
            self.jobs.clone(),
            self.next_job_id.clone(),
            self.runtime.clone(),
            self.registered_parquet.clone(),
        )
    }

    /// Create a DataFrame from a SQL query.
    pub fn sql(&self, query: impl AsRef<str> + Send) -> Result<DataFrame> {
        block_on(self.sql_async(query))
    }

    /// Asynchronously create a DataFrame from a SQL query.
    pub async fn sql_async(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        // Inline any registered scalar SQL functions to native SQL first, so
        // they work identically in embedded and distributed execution.
        let query = self.expand_scalar_sql(query.as_ref())?;

        // Intercept START / REFRESH PIPELINE first so the policy check
        // can include the resolved source/sink tables of the pipeline.
        // (Otherwise a policy-deny rule on a source table could be
        // bypassed by referencing the table from a `START PIPELINE`.)
        use krishiv_sql::pipeline_ddl::PipelineStatement;
        let pipeline = krishiv_sql::pipeline_ddl::parse_pipeline_statement(&query)
            .map_err(KrishivError::from)?;

        // Enforce table-access policy on every SQL entry point.
        if let Some(policy) = &self.policy {
            let mut tables: Vec<String> = match krishiv_sql::referenced_table_names(&query) {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, query = %query, "failed to extract table references for policy check");
                    return Err(KrishivError::AccessDenied {
                        reason: format!("access denied: could not verify query policy ({e})"),
                    });
                }
            };
            // For pipeline statements, also enforce policy on the resolved
            // view + sink names registered in the pipeline registry.
            if let Some(stmt) = &pipeline {
                match stmt {
                    PipelineStatement::StartPipeline { sink }
                    | PipelineStatement::RefreshPipeline { sink, .. } => {
                        if let Some(view) = self
                            .sql_engine
                            .pipeline_registry()
                            .view_for_sink(sink)
                            .map_err(KrishivError::from)?
                        {
                            tables.push(view);
                        }
                        tables.push(sink.clone());
                    }
                    PipelineStatement::DropSink { name }
                    | PipelineStatement::DropSource { name } => {
                        tables.push(name.clone());
                    }
                    _ => {}
                }
            }
            for table in &tables {
                if !policy.check_table_access(table) {
                    return Err(KrishivError::AccessDenied {
                        reason: format!("access denied to table: {table}"),
                    });
                }
            }
        }

        match pipeline {
            Some(PipelineStatement::StartPipeline { sink }) => {
                return self.run_sql_pipeline(&sink).await;
            }
            Some(PipelineStatement::RefreshPipeline { sink, full }) => {
                if full {
                    // Full refresh: reset the persisted job before re-running.
                    self.reset_ivm_job(&format!("sql::{sink}"));
                }
                return self.run_sql_pipeline(&sink).await;
            }
            _ => {}
        }

        self.sql_plain(&query).await
    }

    /// Run a SQL statement without the `START PIPELINE` interception. Used
    /// internally (e.g. by `run_sql_pipeline`) to avoid async-recursion.
    async fn sql_plain(&self, query: &str) -> Result<DataFrame> {
        let sql_dataframe = self.sql_engine.sql(query).await?;
        Ok(self.dataframe_from_sql(sql_dataframe))
    }

    /// Names of all sinks declared via `CREATE SINK` (for CLI pipeline tooling).
    pub fn declared_pipeline_sinks(&self) -> Result<Vec<String>> {
        self.sql_engine
            .pipeline_registry()
            .sink_names()
            .map_err(KrishivError::from)
    }

    /// Validate a SQL-declared pipeline (`dry run`) without executing it: checks
    /// that the sink is declared, references a defined incremental view, and that
    /// every declared source/sink connector kind is supported. Does not read data.
    pub fn validate_pipeline(&self, sink_name: &str) -> Result<()> {
        let pipe_reg = self.sql_engine.pipeline_registry();
        let view_reg = self.sql_engine.incremental_view_registry();

        let sink = pipe_reg
            .sink(sink_name)
            .map_err(KrishivError::from)?
            .ok_or_else(|| KrishivError::Runtime {
                message: format!("pipeline sink '{sink_name}' is not declared"),
            })?;
        if view_reg
            .get(&sink.view)
            .map_err(KrishivError::from)?
            .is_none()
        {
            return Err(KrishivError::Runtime {
                message: format!(
                    "pipeline sink '{sink_name}' references undefined incremental view '{}'",
                    sink.view
                ),
            });
        }
        // Connector kinds must be available in this build (dry run — no I/O,
        // no connection opened; validation is delegated to the registry).
        if let Some(c) = &sink.connector {
            crate::pipeline::validate_sink_spec(sink_name, c)?;
        }
        for (src_name, src) in pipe_reg.sources().map_err(KrishivError::from)? {
            if let krishiv_sql::pipeline_ddl::SourceSpec::Connector(c) = src {
                crate::pipeline::validate_source_spec(&src_name, &c)?;
            }
        }
        Ok(())
    }

    /// Execute a `START PIPELINE <sink>` by compiling the registered SOURCE/SINK
    /// specs + the sink's INCREMENTAL VIEW into `Session::pipeline()…run()`,
    /// returning the sink's collected output as a `DataFrame`.
    async fn run_sql_pipeline(&self, sink_name: &str) -> Result<DataFrame> {
        use std::sync::{Arc, Mutex};

        let pipe_reg = self.sql_engine.pipeline_registry();
        let view_reg = self.sql_engine.incremental_view_registry();

        let sink_spec = pipe_reg
            .sink(sink_name)
            .map_err(KrishivError::from)?
            .ok_or_else(|| KrishivError::Runtime {
                message: format!("START PIPELINE: sink '{sink_name}' is not declared"),
            })?;
        // Presence check, not a value: the sink's view must be declared before
        // the pipeline is built. The entry itself is re-read per view below,
        // in dependency order.
        view_reg
            .get(&sink_spec.view)
            .map_err(KrishivError::from)?
            .ok_or_else(|| KrishivError::Runtime {
                message: format!(
                    "START PIPELINE: sink '{sink_name}' references undefined incremental view '{}'",
                    sink_spec.view
                ),
            })?;

        // Resolve each declared source (query → batches, or connector) and feed it.
        let collected: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
        let mut builder = self
            .pipeline(format!("sql::{sink_name}"))
            .mode(crate::PipelineMode::Ivm);
        for (src_name, src_spec) in pipe_reg.sources().map_err(KrishivError::from)? {
            builder = match src_spec {
                krishiv_sql::pipeline_ddl::SourceSpec::Query(q) => {
                    let batches = self
                        .sql_plain(&q)
                        .await?
                        .collect_async()
                        .await?
                        .into_batches();
                    builder.source_memory(src_name, batches)
                }
                krishiv_sql::pipeline_ddl::SourceSpec::Connector(spec) => {
                    let src = crate::pipeline::build_source(&src_name, &spec).await?;
                    builder.source(src_name, crate::pipeline::Ingest::Connector(src))
                }
            };
        }

        // A view may reference upstream incremental views (not just sources), so
        // register the transitive view closure feeding the sink, in dependency
        // order.
        //
        // IVM-AUD-DDL-B2: every view in a pipeline is registered materialized,
        // overriding whatever `CREATE INCREMENTAL VIEW` declared. That is not a
        // stray literal — a non-materialized view keeps no snapshot
        // (`IncrementalView::publish_output` skips it), and both a downstream
        // view and the sink read exactly that snapshot, so an unmaterialized
        // view in a pipeline would feed them nothing. What was wrong was doing
        // it in silence: the flag is parsed, stored, and then contradicted with
        // no word to the user. Say so when it actually overrides a choice.
        let mut ordered: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        collect_pipeline_view_deps(&sink_spec.view, view_reg, &mut ordered, &mut seen)
            .map_err(KrishivError::from)?;
        for vname in &ordered {
            if let Some(entry) = view_reg.get(vname).map_err(KrishivError::from)? {
                if !entry.is_materialized {
                    tracing::info!(
                        view = %vname,
                        pipeline_sink = %sink_spec.view,
                        "view declared without MATERIALIZED is materialized anyway for this \
                         pipeline: downstream views and the sink read its snapshot, which an \
                         unmaterialized view does not keep"
                    );
                }
                builder = builder.view_with_lateness(
                    vname.clone(),
                    entry.body_sql.clone(),
                    true,
                    entry.lateness.clone(),
                    entry.is_recursive,
                );
            }
        }

        // A connector sink writes the output out-of-band; a plain sink collects
        // it in memory to return as a result set.
        let has_connector_sink = sink_spec.connector.is_some();
        builder = match sink_spec.connector {
            Some(spec) => {
                let sink = crate::pipeline::build_sink(sink_name, &spec).await?;
                builder.sink(
                    sink_spec.view.clone(),
                    crate::pipeline::Egress::Connector(sink),
                )
            }
            None => builder.sink_memory(sink_spec.view.clone(), collected.clone()),
        };

        builder.run(crate::RunPolicy::Once).await?;

        // Connector sinks send output to their destination — return an empty ack.
        if has_connector_sink {
            return self.sql_plain("SELECT 1 WHERE FALSE").await;
        }

        // Return the collected output as a queryable result.
        let out = collected
            .lock()
            .map_err(|_| KrishivError::Runtime {
                message: "pipeline result mutex poisoned".into(),
            })?
            .clone();
        if out.is_empty() {
            return self.sql_plain("SELECT 1 WHERE FALSE").await;
        }
        // GAP 1: Register incremental view output as a queryable DataFusion
        // table so `SELECT * FROM <view_name>` works after START PIPELINE.
        // The table persists for the session lifetime and is updated on each
        // pipeline run. Use the backing view name as the table name.
        let view_table_name = sink_spec.view.clone();
        self.sql_engine
            .register_record_batches(&view_table_name, out)
            .await
            .map_err(KrishivError::from)?;
        self.sql_plain(&format!("SELECT * FROM \"{view_table_name}\""))
            .await
    }

    /// Execute SQL after API-key authentication and table-level policy checks.
    ///
    /// Requires [`SessionBuilder::with_auth`]. When [`SessionBuilder::with_policy`]
    /// is configured, every table referenced by the query must pass
    /// [`PolicyHook::check_table_access`].
    pub fn sql_as(&self, api_key: &str, query: impl AsRef<str> + Send) -> Result<DataFrame> {
        block_on(self.sql_as_async(api_key, query))
    }

    /// Async variant of [`Self::sql_as`].
    pub async fn sql_as_async(&self, api_key: &str, query: impl AsRef<str>) -> Result<DataFrame> {
        let auth = self
            .auth
            .as_ref()
            .ok_or_else(|| KrishivError::InvalidConfig {
                message: "sql_as requires SessionBuilder::with_auth".into(),
            })?;
        if auth.authenticate(api_key).is_none() {
            return Err(KrishivError::AccessDenied {
                reason: "invalid API key".into(),
            });
        }
        let query = query.as_ref();
        if let Some(policy) = &self.policy {
            for table in krishiv_sql::referenced_table_names(query).map_err(KrishivError::from)? {
                if !policy.check_table_access(&table) {
                    return Err(KrishivError::AccessDenied {
                        reason: format!("access denied to table: {table}"),
                    });
                }
            }
        }
        self.sql_async(query).await
    }

    /// Shared operation registry for cancellation and progress reporting.
    pub fn operation_registry(&self) -> Arc<krishiv_sql::OperationRegistry> {
        Arc::clone(self.sql_engine.operation_registry())
    }

    /// Shared live-table registry backing `CREATE LIVE TABLE` DDL.
    /// Shared incremental-view registry backing `CREATE INCREMENTAL VIEW` DDL.
    pub fn incremental_view_registry(
        &self,
    ) -> &Arc<krishiv_sql::incremental_view::IncrementalViewRegistry> {
        self.sql_engine.incremental_view_registry()
    }

    // ── Unified compute entry points ──────────────────────────────────────────

    /// Submit a [`CompiledJob`](crate::CompiledJob) to its engine — the single
    /// unified entry point behind the three engines.
    ///
    /// Every front-end (SQL, Python, Rust) compiles to one `CompiledJob` and
    /// reaches the engines the same way: this dispatches by the job's explicit
    /// [`EngineKind`](crate::EngineKind) through [`run_job`](crate::run_job),
    /// with placement-provided sources and sinks. Engine selection is the job's,
    /// never the API surface's.
    ///
    /// `submit` is the **bounded, run-once** entry: it drains the job's
    /// source(s) to end-of-input and returns a terminal
    /// [`JobHandle`](crate::JobHandle) (status `Completed`). A *streaming* job
    /// submitted here therefore runs its window over the bounded input once; for
    /// an unbounded, continuously maintained job use
    /// [`submit_streaming`](Self::submit_streaming) / [`stream`](Self::stream)
    /// (streaming) or [`ivm`](Self::ivm) (incremental).
    ///
    /// The job's `name` is its **durable identity** (it becomes the `JobId`).
    /// For a stateful engine at a durable placement, submitting under a name that
    /// already has a persisted checkpoint *resumes that state* — this is the
    /// restart-resume mechanism, so reuse a name only to continue the same job.
    ///
    /// Placement coverage: embedded runs all three engines in-process; batch runs
    /// at single-node and distributed through this session's `ExecutionRuntime`;
    /// single-node incremental/streaming run in-process with durable checkpoints;
    /// distributed streaming runs the window on executors and distributed
    /// incremental maintains the view through a remote [`ivm`](Self::ivm) job.
    pub async fn submit(&self, job: crate::CompiledJob) -> Result<crate::JobHandle> {
        match self.mode {
            ExecutionMode::Embedded => {
                // The incremental engine emits retractions; its connector sink
                // consolidates them into the net table (append-only file sinks
                // cannot apply a delete). Batch/streaming emit insert-only output.
                let runtime = if job.engine == crate::EngineKind::Incremental {
                    crate::connector_runtime::embedded_consolidating_runtime()
                } else {
                    crate::connector_runtime::embedded_connector_runtime()
                };
                crate::run_job(job, runtime)
                    .await
                    .map_err(KrishivError::from)
            }
            // Batch at single-node / distributed runs through this session's real
            // `ExecutionRuntime` (in-process cluster or remote coordinator) via a
            // placement-injected query executor — the engine code is unchanged.
            ExecutionMode::SingleNode | ExecutionMode::Distributed
                if job.engine == crate::EngineKind::Batch =>
            {
                let placement = krishiv_engine_core::Placement::from(self.mode);
                let runtime = crate::connector_runtime::runtime_backed_engine_runtime(
                    placement,
                    Arc::clone(&self.runtime),
                );
                crate::run_job(job, runtime)
                    .await
                    .map_err(KrishivError::from)
            }
            // Stateful engines (incremental / streaming) at single-node run
            // in-process over connector-backed sources/sinks with an on-disk
            // checkpoint service.
            //
            // IVM-AUD-INT-F13: only the streaming engine uses it. This comment
            // used to say operator state and source offsets survive a restart
            // for both; `IncrementalEngine::run` never reads or writes
            // `rt.checkpoint`, so an incremental job at single-node restarts
            // from zero exactly as it does embedded. See
            // `connector_runtime::durable_engine_runtime`.
            ExecutionMode::SingleNode => {
                // Incremental output consolidates retractions into the net table;
                // streaming output is insert-only.
                let consolidate = job.engine == crate::EngineKind::Incremental;
                let runtime = crate::connector_runtime::durable_engine_runtime(
                    krishiv_engine_core::Placement::SingleNode,
                    self.checkpoint_dir(),
                    consolidate,
                )
                .map_err(KrishivError::from)?;
                crate::run_job(job, runtime)
                    .await
                    .map_err(KrishivError::from)
            }
            // Distributed *streaming*: drain the bounded source locally and run the
            // window on the remote coordinator's executors through the continuous
            // seam (register → push → drain), then write the closed windows to the
            // sink. The windowed compute runs on executors; only I/O is local.
            ExecutionMode::Distributed if job.engine == crate::EngineKind::Streaming => {
                crate::connector_runtime::run_streaming_job_via_runtime(&self.runtime, &job)
                    .await
                    .map_err(KrishivError::from)
            }
            // Distributed *incremental*: maintain the view on the remote coordinator
            // through a mode-aware `IvmJob` (the same one `Session::ivm` returns) —
            // drain the bounded CDC source locally, feed/step the view on the
            // cluster, and write its net snapshot to the sink.
            ExecutionMode::Distributed if job.engine == crate::EngineKind::Incremental => {
                let ivm = self.ivm(&job.name).await?;
                crate::connector_runtime::run_incremental_job_via_ivm(&ivm, &job).await
            }
            // All three engines are routed above for distributed placement; this
            // defensive arm covers any future `EngineKind`.
            ExecutionMode::Distributed => Err(KrishivError::unsupported(format!(
                "submit() does not yet route the distributed {} engine",
                job.engine
            ))),
        }
    }

    /// Resolve the directory durable single-node checkpoints are written to.
    ///
    /// Precedence: the `checkpoint_dir` session property, then the
    /// `KRISHIV_CHECKPOINT_DIR` environment variable, then a stable per-host
    /// default under the system temp directory. Files are keyed by job id, so one
    /// directory safely holds every job's latest checkpoint.
    fn checkpoint_dir(&self) -> PathBuf {
        if let Some(dir) = self.get_config("checkpoint_dir") {
            return PathBuf::from(dir);
        }
        if let Ok(dir) = std::env::var("KRISHIV_CHECKPOINT_DIR") {
            return PathBuf::from(dir);
        }
        std::env::temp_dir().join("krishiv-checkpoints")
    }

    /// Compile a SQL pipeline script to a [`CompiledJob`](crate::CompiledJob)
    /// and [`submit`](Self::submit) it — the SQL front-end's unified entry.
    ///
    /// The script is a sequence of `CREATE SOURCE` / `CREATE SINK` statements
    /// (see [`compile_sql_job`](crate::compile_sql_job)); the engine is inferred
    /// from the declared connectors and whether the transform is windowed, then
    /// dispatched the same way as every other front-end. Example:
    ///
    /// ```sql
    /// CREATE SOURCE orders FROM parquet(path='/in.parquet');
    /// CREATE SOURCE summary AS SELECT k, SUM(v) AS total FROM orders GROUP BY k;
    /// CREATE SINK out FROM summary INTO parquet(path='/out.parquet');
    /// ```
    pub async fn submit_sql(&self, sql: &str) -> Result<crate::JobHandle> {
        let job = self.compile_sql_job(sql)?;
        self.submit(job).await
    }

    /// Submit a continuous, unbounded **streaming** job and return a stoppable
    /// [`RunningJob`](crate::RunningJob).
    ///
    /// Unlike [`submit`](Self::submit) — which drains a bounded source once and
    /// returns — this keeps draining the job's source on a background task,
    /// emitting closed windows and checkpointing periodically, until
    /// [`RunningJob::stop`](crate::RunningJob::stop) is called. Embedded
    /// placement is wired today; single-node and distributed continuous
    /// streaming is the cluster follow-up.
    pub fn submit_streaming(&self, job: crate::CompiledJob) -> Result<crate::RunningJob> {
        // H-22 (audit): enforce table-access policy on streaming SQL the
        // same way `submit` / `sql_async` do. Without this, a deny rule
        // on a Kafka source table is bypassed by routing the job through
        // `submit_streaming` instead of `submit`.
        if let Some(policy) = &self.policy {
            match krishiv_sql::referenced_table_names(&job.query) {
                Ok(tables) => {
                    for table in &tables {
                        if !policy.check_table_access(table) {
                            return Err(KrishivError::AccessDenied {
                                reason: format!("access denied to table: {table}"),
                            });
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        query = %job.query,
                        "failed to extract table references for streaming policy check"
                    );
                    return Err(KrishivError::AccessDenied {
                        reason: format!("access denied: could not verify streaming policy ({e})"),
                    });
                }
            }
        }
        match self.mode {
            ExecutionMode::Embedded => {
                let runtime = crate::connector_runtime::embedded_connector_runtime();
                crate::spawn_streaming_job(job, runtime).map_err(KrishivError::from)
            }
            // Single-node: run the continuous loop in-process but checkpoint
            // durably to disk, so a restart resumes from the persisted state.
            ExecutionMode::SingleNode => {
                // Streaming output is insert-only: no changelog consolidation.
                let runtime = crate::connector_runtime::durable_engine_runtime(
                    krishiv_engine_core::Placement::SingleNode,
                    self.checkpoint_dir(),
                    false,
                )
                .map_err(KrishivError::from)?;
                crate::spawn_streaming_job(job, runtime).map_err(KrishivError::from)
            }
            // Distributed: the subtasks own the source, so this is the
            // run-loop engine — not the cycle model, whose input is hand-fed
            // by the client and which therefore cannot express "the engine
            // reads this Kafka topic", which is exactly what a CompiledJob says.
            ExecutionMode::Distributed => block_on(self.submit_streaming_distributed(job)),
        }
    }

    /// Register `job` as a parallel run-loop continuous job on the coordinator
    /// and return a handle that supervises it.
    ///
    /// This used to be an error pointing at `Session::stream`, which is a
    /// different contract: `stream` hands the client a push/drain pair and the
    /// client feeds the input. A [`CompiledJob`](crate::CompiledJob) names its
    /// own sources, so the honest distributed shape is the one where the
    /// subtasks open those sources themselves — the run-loop engine with
    /// `ContinuousRegistrySource` descriptors.
    async fn submit_streaming_distributed(
        &self,
        job: crate::CompiledJob,
    ) -> Result<crate::RunningJob> {
        use krishiv_engine_core::{JobHandle, JobStatus};

        let url = self
            .coordinator_http_url()
            .ok_or_else(|| {
                KrishivError::unsupported(
                    "distributed streaming needs a coordinator URL; call with_coordinator() or \
                     Session::from_env with KRISHIV_COORDINATOR_URL set",
                )
            })?
            .to_owned();

        // Task #147: the SAME routing ladder the embedded engine and the
        // bench harness use — pipeline, then join, then window, then
        // stateless — so a job's class is decided once, by shape, and the
        // failing class's own compiler produces the error the user sees.
        let stream_spec: krishiv_plan::stream_task::StreamingTaskSpec =
            if krishiv_sql::streaming_pipeline_plan::looks_like_streaming_pipeline(&job.query) {
                krishiv_plan::stream_task::StreamingTaskSpec::Pipeline(Box::new(
                    krishiv_sql::streaming_pipeline_plan::compile_streaming_pipeline_sql(
                        &job.query,
                    )
                    .map_err(|e| KrishivError::unsupported(e.to_string()))?
                    .spec,
                ))
            } else if krishiv_sql::streaming_join_plan::looks_like_streaming_join(&job.query) {
                krishiv_plan::stream_task::StreamingTaskSpec::Join(Box::new(
                    krishiv_sql::streaming_join_plan::compile_streaming_join_sql(&job.query)
                        .map_err(|e| KrishivError::unsupported(e.to_string()))?
                        .spec,
                ))
            } else if krishiv_sql::streaming_tvf::find_window_tvf(&job.query).is_some() {
                krishiv_plan::stream_task::StreamingTaskSpec::Window(Box::new(
                    krishiv_sql::streaming_window_plan::compile_streaming_window_sql(&job.query)
                        .map_err(|error| {
                            KrishivError::unsupported(format!(
                                "distributed streaming window query did not compile: {error}"
                            ))
                        })?
                        .spec,
                ))
            } else {
                let source = job.sources.first().ok_or_else(|| {
                    KrishivError::unsupported(
                        "a stateless distributed job needs a source to name its input table",
                    )
                })?;
                krishiv_plan::stream_task::StreamingTaskSpec::Stateless(Box::new(
                    krishiv_plan::stream_task::StatelessQuerySpec {
                        sql: job.query.clone(),
                        source: source.name.clone(),
                        side_tables: vec![],
                    },
                ))
            };

        // Every source must be one the *subtasks* can open. A bounded source
        // has no place in an unbounded run-loop, and refusing it here is better
        // than registering a job that reads its input once and then idles
        // forever looking healthy.
        //
        // The same reasoning applies to sinks, and this path does not carry
        // them. `ContinuousRegisterOptions` has no sink field, so a job with a
        // declared sink registered cleanly, ran, reported Running, and wrote
        // its output only into the cluster's egress ring — where it is dropped
        // past the cap. The identical job submitted embedded opens the sink and
        // writes to it. Nothing said the sink had been discarded.
        //
        // Refusing is the smaller half of the fix and is deliberately the half
        // taken here: the coordinator and executor HAVE honoured registry sinks
        // since #197 (`ContinuousRegisterRequest.sink`, and run_loop.rs opens a
        // `registry-sink:` contract), so the capability exists and only this
        // client leg cannot express it. Wiring it through is the better answer
        // and is recorded as follow-up; until then the user gets a message
        // naming the sink instead of a job that quietly writes nowhere.
        if let Some(sink) = job.sinks.first() {
            return Err(KrishivError::unsupported(format!(
                "distributed continuous streaming does not yet carry this job's sink                  '{}' ({} -> {}): the registration wire has no sink field, so the job's                  output would stay on the cluster's egress buffer — and be dropped past                  its cap — while the job reported Running. Use submit() for a bounded                  run, or register the sink on the coordinator directly, which has                  supported registry sinks since #197.",
                sink.view, sink.connector, sink.uri
            )));
        }

        let mut sources = Vec::with_capacity(job.sources.len());
        for source in &job.sources {
            if source.is_bounded {
                return Err(KrishivError::unsupported(format!(
                    "source '{}' ({}) is bounded, so it cannot drive a distributed continuous \
                     job: the run-loop would drain it once and then idle forever while reporting \
                     Running. Use submit() for a bounded run.",
                    source.name, source.connector
                )));
            }
            sources.push(krishiv_scheduler::ContinuousRegistrySource {
                kind: source.connector.clone(),
                table: source.name.clone(),
                // Reuse the embedded path's locator mapping rather than a
                // second copy: whatever `connector` means when this job runs
                // in-process, it must mean the same on an executor.
                config: crate::connector_runtime::connector_properties_for_source(source),
            });
        }
        if sources.is_empty() {
            return Err(KrishivError::unsupported(
                "a distributed continuous job needs at least one unbounded source for its \
                 subtasks to own; a job with no sources has nothing to read",
            ));
        }

        // Per-class parallelism: windows and stateless scale with sources;
        // joins with the larger side; pipelines are pinned to 1 (stage
        // re-keying — the coordinator refuses anything else by name).
        let parallelism = match &stream_spec {
            krishiv_plan::stream_task::StreamingTaskSpec::Pipeline(_) => 1,
            krishiv_plan::stream_task::StreamingTaskSpec::Join(j) => {
                let left = job
                    .sources
                    .iter()
                    .filter(|s| s.name == j.left_source)
                    .count();
                let right = job
                    .sources
                    .iter()
                    .filter(|s| s.name == j.right_source)
                    .count();
                left.max(right).max(1) as u32
            }
            _ => sources.len().max(1) as u32,
        };
        let mut options = krishiv_runtime::ContinuousRegisterOptions::run_loop(parallelism);
        options.sources = sources;
        let checkpoint_dir = self.checkpoint_dir();
        options =
            options.with_checkpointing(30_000, format!("file://{}", checkpoint_dir.display()));

        krishiv_runtime::execute_coordinator_continuous_register_task(
            &url,
            &job.name,
            &stream_spec,
            &options,
        )
        .await
        .map_err(KrishivError::from)?;

        let handle = JobHandle::from_name(&job.name, JobStatus::Running)
            .map_err(|e| KrishivError::unsupported(e.to_string()))?;
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let job_name = job.name.clone();
        let task = tokio::spawn(async move {
            // Supervise: wait for the stop signal, then tear the job down on
            // the coordinator and confirm it is really gone before reporting
            // Completed.
            while !*stop_rx.borrow() {
                if stop_rx.changed().await.is_err() {
                    break;
                }
            }
            krishiv_runtime::execute_coordinator_cancel_job(&url, &job_name)
                .await
                .map_err(|e| {
                    krishiv_engine_core::EngineError::Runtime(format!(
                        "cancelling distributed continuous job '{job_name}': {e}"
                    ))
                })?;
            JobHandle::from_name(&job_name, JobStatus::Completed)
        });
        Ok(crate::RunningJob::supervised(handle, stop_tx, task))
    }

    /// Run a one-shot batch SQL query, returning a [`DataFrame`].
    ///
    /// This is an alias for [`sql`](Self::sql) that names the execution mode at
    /// the call site, symmetric with [`ivm`](Self::ivm) and [`stream`](Self::stream).
    pub fn batch(&self, query: impl AsRef<str> + Send) -> Result<DataFrame> {
        self.sql(query)
    }

    /// Create or attach to an incremental-view-maintenance job by name.
    ///
    /// Mode-aware: a session built for distributed execution (with a coordinator
    /// URL) returns a remote job; otherwise an in-process embedded job. The
    /// returned [`IvmJob`](crate::IvmJob) exposes the same `feed`/`step`/
    /// `snapshot`/`checkpoint` surface in both modes.
    ///
    /// # This job auto-partitions; `DataFrame::to_incremental` does not
    ///
    /// IVM-AUD-API-A3. A job created here shards its flow by key when its
    /// **first** view is a single-column `GROUP BY` over a routable key type,
    /// into `KRISHIV_IVM_SHARDS` shards. `DataFrame::to_incremental` pins its
    /// job to a single flow instead, because a partitioned flow never cascades
    /// a base view's output to derived views and that surface has to support
    /// the view-DAG. Same session, same mode, same SQL, different fan-out and
    /// different composability — pick this one for a single aggregate at scale
    /// and `to_incremental` when views build on views.
    ///
    /// The two also collide by name: an unpartitioned job cannot be created
    /// under a name a partitioned job already holds, and vice versa. That is
    /// refused rather than silently reinterpreted (IVM-AUD-INT-F16).
    ///
    /// # `SingleNode` gets the embedded job, not the daemon's
    ///
    /// IVM-AUD-API-A2 / INT-F14. A `SingleNode` session has a coordinator and
    /// routes [`submit`](Self::submit) through it with on-disk checkpoints, but
    /// IVM is not routed there: the arm below treats `SingleNode` exactly like
    /// `Embedded`, so the job lives in this process's registry, the local
    /// daemon cannot see or list it, and nothing about it survives the process.
    /// Same session, same mode: `submit` is daemon-backed and `ivm` is not.
    ///
    /// Two further consequences worth knowing before you rely on it: the
    /// coordinator's `/api/v1/ivm/*` surface (`/stats`, `/snap`, `DELETE`) does
    /// not reach this job, and neither does `krishiv ivm run --mode single-node`
    /// as a way to attach to it. Whether SingleNode IVM *should* route to the
    /// daemon is an open decision, recorded in
    /// `docs/implementation/ivm-audit-register.md`; until it is taken, this doc
    /// is the whole of the contract.
    /// Auto-partitions: if the job's first view is key-shardable it is spread
    /// across shards. Fast for a single view, and **cannot host a view-DAG** —
    /// a partitioned flow never cascades a base view's output into a derived
    /// view. Use [`ivm_unpartitioned`](Self::ivm_unpartitioned) when the job
    /// will gain derived views; `DataFrame::to_incremental` picks that one for
    /// exactly this reason.
    pub async fn ivm(&self, name: &str) -> Result<crate::IvmJob> {
        self.ivm_with_shape(name, true).await
    }

    /// Like [`ivm`](Self::ivm) but pinned to a single flow, so the job can host
    /// a view-DAG (`Session::view` + `to_incremental`, where a derived view
    /// reads the base view's full output).
    ///
    /// This is the same pin `DataFrame::to_incremental` uses. It exists as a
    /// named `Session` entry point because the choice is a capability rather
    /// than a tuning knob, and because an error message elsewhere directed
    /// callers here before the method existed (IVM-AUD-DUP-2).
    pub async fn ivm_unpartitioned(&self, name: &str) -> Result<crate::IvmJob> {
        self.ivm_with_shape(name, false).await
    }

    async fn ivm_with_shape(&self, name: &str, partitioned: bool) -> Result<crate::IvmJob> {
        crate::IvmJob::for_mode(
            self.mode,
            self.ivm_http_url(),
            Some(&self.ivm_registry),
            name,
            partitioned,
        )
        .await
    }

    /// Read an incremental view as a [`DataFrame`] — the Rust view-DAG entry
    /// point.
    ///
    /// Two uses, both of which need the view's rows registered under its name
    /// so the query can be planned against it:
    ///
    /// * **compose** — `session.view(&iv)?.filter(..)?.to_incremental("d")`
    ///   co-registers the derived view into `iv`'s job, so a feed to the base
    ///   cascades to it in the same tick;
    /// * **read** — `.collect()` returns the view's rows *as of this call*. The
    ///   DataFrame carries that snapshot; it is not a live handle.
    ///
    /// IVM-AUD-API-D2: only Python had this. `DataFrame::with_ivm_parent` is
    /// `pub`, but on its own it tags a DataFrame that has nothing to plan
    /// against — a Rust user had to hand-register a table named after the base
    /// view, with the base view's schema, before the derived SQL would resolve,
    /// and nothing said so. Getting it subtly wrong (registering the storage
    /// schema rather than the declared one) types the derived view differently
    /// before and after the base's first tick.
    ///
    /// # Cost
    ///
    /// Every call materializes the base view's **whole** snapshot and casts it
    /// — O(rows in the view) in time and memory. The compose path does not need
    /// those rows (only the schema is load-bearing once the derived view is
    /// registered into the base's job), but this function cannot tell the two
    /// uses apart, because the caller picks afterwards. Hoist it out of loops.
    ///
    /// A session table that already carries the view's name is refused rather
    /// than replaced: the derived view's SQL could not tell the two apart. The
    /// registration lasts only as long as planning.
    pub fn view(&self, iv: &crate::IncrementalDataFrame) -> Result<DataFrame> {
        let name = iv.name();
        if self.table_exists(name)? {
            return Err(KrishivError::unsupported(format!(
                "cannot read incremental view '{name}' as a DataFrame: this session already has                  a table named '{name}', and the derived view's SQL cannot distinguish them.                  Rename the view (to_incremental(name)) or drop/rename the table."
            )));
        }
        // The table always carries the view's DECLARED output schema, whether
        // or not it has rows yet, so a derived view is typed identically before
        // and after the base has ticked. The engine materializes snapshots in
        // equivalent storage types (`Utf8` where the plan says `Utf8View`), so
        // the rows are cast onto the declared schema; a cast that fails is a
        // real divergence and is raised, not papered over with an empty table.
        let schema = iv.output_schema();
        let mut batches = vec![RecordBatch::new_empty(schema.clone())];
        if let Some(snapshot) = block_on(iv.snapshot())? {
            let columns = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let column = snapshot.column(i);
                    arrow::compute::cast(column, field.data_type()).map_err(|e| {
                        KrishivError::unsupported(format!(
                            "incremental view '{name}' materialized column '{}' as {:?}, which                              does not fit its declared output type {:?}: {e}",
                            field.name(),
                            column.data_type(),
                            field.data_type()
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            batches.push(
                RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
                    KrishivError::unsupported(format!(
                        "incremental view '{name}' snapshot does not fit its declared output                          schema: {e}"
                    ))
                })?,
            );
        }
        self.register_record_batches(name, batches)?;
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let planned = self.sql(format!("SELECT * FROM {quoted}"));
        // Deregister whether or not planning succeeded: the registration must
        // never outlive the call, or it shadows a later real table of that name.
        self.deregister_table(name)?;
        Ok(planned?.with_ivm_parent(iv.shared_job()))
    }

    /// Resolve the coordinator **HTTP** base for management endpoints (IVM lives
    /// at `/api/v1/ivm/*`). Prefer the explicit HTTP URL; fall back to the Flight
    /// URL for single-port/local setups where they coincide.
    pub(crate) fn ivm_http_url(&self) -> Option<&str> {
        self.coordinator_http_url
            .as_deref()
            .or(self.coordinator_url.as_deref())
    }

    /// Submit a continuous windowed streaming job and return a [`StreamJob`]
    /// handle for pushing input and draining output.
    ///
    /// Returns a [`StreamJob::Embedded`](crate::StreamJob::Embedded) in
    /// embedded / single-node mode (push + drain against the in-process
    /// cluster) or the coordinator over Flight in distributed mode — either
    /// way the unified [`crate::StreamingJob`] handle, so the caller never
    /// branches on placement.
    pub fn stream(
        &self,
        name: impl Into<String> + Send,
        spec: LocalWindowExecutionSpec,
    ) -> Result<crate::StreamingJob> {
        block_on(self.stream_async(name, spec))
    }

    /// Asynchronously submit a continuous windowed streaming job.
    ///
    /// Prefer this over [`Self::stream`] from async code: the distributed
    /// branch performs a real coordinator RPC round-trip via `block_on`,
    /// which cannot be called from a thread already driving a Tokio runtime
    /// without hopping to a fresh OS thread.
    pub async fn stream_async(
        &self,
        name: impl Into<String>,
        spec: LocalWindowExecutionSpec,
    ) -> Result<crate::StreamingJob> {
        self.stream_with_options_async(name, spec, &Default::default())
            .await
    }

    /// Submit a continuous windowed streaming job, choosing its distributed
    /// execution model.
    ///
    /// [`stream`](Self::stream) takes the coordinator's defaults, which are the
    /// single-subtask cycle model — one task, input hand-fed from this client,
    /// no barrier checkpointing. Pass
    /// [`ContinuousRegisterOptions::run_loop`](krishiv_runtime::ContinuousRegisterOptions::run_loop)
    /// to get the parallel, key-grouped, barrier-checkpointed run-loop engine,
    /// whose subtasks own their own sources.
    ///
    /// Options are ignored outside distributed mode: embedded and single-node
    /// run the in-process cluster, which has no run-loop implementation. Asking
    /// for one there is an error rather than a silent downgrade.
    pub async fn stream_with_options_async(
        &self,
        name: impl Into<String>,
        spec: LocalWindowExecutionSpec,
        options: &krishiv_runtime::ContinuousRegisterOptions,
    ) -> Result<crate::StreamingJob> {
        let name = name.into();
        match self.mode {
            ExecutionMode::Distributed => {
                // The management endpoints are HTTP; `coordinator_grpc_url()`
                // was being handed to a function that treats its argument as an
                // HTTP base, which breaks any deployment whose gRPC and HTTP
                // authorities differ.
                let url = self.coordinator_http_url().ok_or_else(|| {
                    KrishivError::unsupported(
                        "distributed streaming needs a coordinator URL; \
                         call with_coordinator() or Session::from_env with KRISHIV_COORDINATOR_URL set",
                    )
                })?;
                // Convert the runtime LocalWindowExecutionSpec to the plan-crate
                // WindowExecutionSpec that RemoteStreamingJob::create expects.
                // The conversion is lossless — both are the same windowed-
                // aggregation spec represented at different layers.
                let plan_spec = spec.to_plan_spec();
                let remote = krishiv_runtime::RemoteStreamingJob::create_with_options(
                    url, &plan_spec, &name, options,
                )
                .await
                .map_err(KrishivError::from)?;
                self.remember_stream_job(&name, None, spec);
                Ok(crate::StreamingJob::from_remote(remote))
            }
            ExecutionMode::Embedded | ExecutionMode::SingleNode => {
                // Fail closed rather than ignore. The in-process cluster has no
                // run-loop implementation, so accepting `mode: "run-loop"` here
                // and quietly running the single-executor loop would be the
                // exact silent-downgrade this option exists to end.
                if options.mode.is_some() || options.parallelism.is_some_and(|p| p > 1) {
                    return Err(KrishivError::unsupported(format!(
                        "continuous run-loop options are only available in distributed mode; \
                         this session is {}. The in-process cluster runs a single-subtask \
                         continuous loop and cannot honour mode/parallelism.",
                        self.mode
                    )));
                }
                let job_id = self.submit_stream_job(name, spec)?;
                Ok(crate::StreamingJob::from_session_job(self.clone(), job_id))
            }
        }
    }

    /// Start building a declarative pipeline (`source → transform → sink`).
    ///
    /// The returned [`PipelineBuilder`](crate::PipelineBuilder) compiles down to
    /// the imperative core (`ivm`/`stream`/`batch`) — it adds connectors and a
    /// driver loop, not a second engine. There is no trigger stage: boundedness,
    /// watermark, and change-events drive each mode; the optional
    /// [`RunPolicy`](crate::RunPolicy) only coalesces input.
    ///
    /// A named pipeline's IVM job **persists across runs** (the registry is keyed
    /// by name), so repeated `run()` calls feed new input incrementally. Use
    /// [`reset_ivm_job`](Self::reset_ivm_job) or `Pipeline::refresh` to start
    /// fresh ("full refresh").
    pub fn pipeline(&self, name: impl Into<String>) -> crate::PipelineBuilder {
        crate::pipeline::PipelineBuilder::new(self.clone(), name)
    }

    /// Drop a persisted embedded IVM job by name so the next `ivm`/pipeline run
    /// starts from fresh, empty state (the "full refresh" primitive). Returns
    /// `true` if a job existed.
    pub fn reset_ivm_job(&self, name: &str) -> bool {
        self.ivm_registry.delete(name)
    }

    /// Execute a SQL query and feed its result as insertions into an IVM job,
    /// then step the view. Returns the step summary.
    ///
    /// This is the one-call bridge from batch SQL to incremental maintenance:
    ///
    /// ```rust,ignore
    /// let summary = session
    ///     .feed_ivm_from_sql("revenue_job", "orders",
    ///         "SELECT region, amount FROM staging")
    ///     .await?;
    /// ```
    pub async fn feed_ivm_from_sql(
        &self,
        ivm_job_name: &str,
        source_name: &str,
        query: impl AsRef<str> + Send,
    ) -> Result<StepReport> {
        let df = self.sql_async(query).await?;
        let delta = df.collect_as_delta_batch()?;
        let job = self.ivm(ivm_job_name).await?;
        job.feed(source_name, &delta).await?;
        let step = job.step().await?;
        Ok(step)
    }

    /// Execute a SQL query with a wall-clock timeout.
    ///
    /// Returns `KrishivError::Runtime` if the query does not produce a result
    /// within `timeout_ms` milliseconds.
    pub async fn sql_with_timeout_async(
        &self,
        query: impl AsRef<str> + Send,
        timeout_ms: u64,
    ) -> Result<DataFrame> {
        let timeout = std::time::Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout, self.sql_async(query))
            .await
            .map_err(|_| KrishivError::Runtime {
                message: format!("SQL query timed out after {timeout_ms}ms"),
            })?
    }

    /// Synchronous variant of [`sql_with_timeout_async`].
    pub fn sql_with_timeout(
        &self,
        query: impl AsRef<str> + Send,
        timeout_ms: u64,
    ) -> Result<DataFrame> {
        let query = query.as_ref().to_owned();
        block_on(self.sql_with_timeout_async(query, timeout_ms))
    }

    /// Execute SQL on the local `SqlEngine` only (embedded / single-node path).
    ///
    /// Never routes to a remote Flight endpoint, even in distributed mode.
    pub fn execute_local(&self, query: impl AsRef<str> + Send) -> Result<DataFrame> {
        block_on(self.execute_local_async(query))
    }

    /// Async variant of [`Self::execute_local`].
    ///
    /// Always executes via the local `SqlEngine` (DataFusion) regardless of
    /// session mode. Returns a DataFrame with pre-collected results so that
    /// `.collect()` returns immediately without remote routing.
    pub async fn execute_local_async(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        let query = query.as_ref();
        // Enforce table-access policy.
        if let Some(policy) = &self.policy
            && let Ok(tables) = krishiv_sql::referenced_table_names(query)
        {
            for table in &tables {
                if !policy.check_table_access(table) {
                    return Err(KrishivError::AccessDenied {
                        reason: format!("access denied to table: {table}"),
                    });
                }
            }
        }
        let sql_df = self
            .sql_engine
            .sql(query)
            .await
            .map_err(|e| KrishivError::Runtime {
                message: e.to_string(),
            })?;
        let batches = sql_df.collect().await.map_err(|e| KrishivError::Runtime {
            message: e.to_string(),
        })?;
        Ok(DataFrame::from_batches(
            self.mode,
            batches,
            self.jobs.clone(),
            self.next_job_id.clone(),
            self.runtime.clone(),
            self.registered_parquet.clone(),
        ))
    }

    /// Execute SQL through the session [`ExecutionRuntime`] (remote when configured).
    pub fn execute_remote(&self, query: impl AsRef<str> + Send) -> Result<DataFrame> {
        block_on(self.execute_remote_async(query))
    }

    /// Async variant of [`Self::execute_remote`].
    pub async fn execute_remote_async(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        if self.coordinator_url.is_none() {
            return Err(KrishivError::unsupported(
                "execute_remote requires SessionBuilder::with_coordinator(flight_url)",
            ));
        }
        if self.mode == ExecutionMode::Embedded {
            return Err(KrishivError::unsupported(
                "execute_remote is not valid in embedded mode; use execute_local",
            ));
        }
        if self.mode == ExecutionMode::Distributed && !self.runtime.uses_remote_execution() {
            return Err(KrishivError::unsupported(
                "distributed remote execution requires SessionBuilder::with_remote_execution(true)",
            ));
        }
        // Inline scalar SQL functions, then prepend any Python UDF directives so
        // they ship to the executors (which run them via a python worker).
        let expanded = self.expand_scalar_sql(query.as_ref())?;
        let expanded = self.prepend_python_udfs(&expanded);
        let query = expanded.as_str();
        if let Some(policy) = &self.policy
            && let Ok(tables) = krishiv_sql::referenced_table_names(query)
        {
            for table in &tables {
                if !policy.check_table_access(table) {
                    return Err(KrishivError::AccessDenied {
                        reason: format!("access denied to table: {table}"),
                    });
                }
            }
        }
        let tables = self
            .registered_parquet
            .iter()
            .map(|entry| entry.value().to_registration(entry.key()))
            .collect::<Vec<_>>();
        let is_streaming = self.sql_engine.is_streaming_query(query).unwrap_or(false);
        let batches =
            runtime_collect_batch_sql(Arc::clone(&self.runtime), query, &tables, is_streaming)
                .await?;
        Ok(DataFrame::from_batches(
            self.mode,
            batches,
            self.jobs.clone(),
            self.next_job_id.clone(),
            self.runtime.clone(),
            self.registered_parquet.clone(),
        ))
    }

    /// Read checkpointed keyed state as a table — the **State Processor API**
    /// (Spark's State Data Source / Flink's State Processor API).
    ///
    /// Opens the RocksDB state backend directory at `path` (a checkpoint /
    /// savepoint state dir), scans the `(operator_id, state_name)` namespace, and
    /// returns a DataFrame with columns `key` / `value` (raw bytes) plus
    /// `key_utf8` / `value_utf8` (UTF-8 decode where the bytes are valid text) —
    /// so state can be inspected, debugged, or migrated offline.
    pub fn read_state(
        &self,
        path: impl AsRef<Path>,
        operator_id: &str,
        state_name: &str,
    ) -> Result<DataFrame> {
        use krishiv_state::{Namespace, RocksDbStateBackend, StateReader};
        let backend =
            RocksDbStateBackend::open(path.as_ref()).map_err(|e| KrishivError::Runtime {
                message: format!("open state backend at {}: {e}", path.as_ref().display()),
            })?;
        let entries = StateReader::new(&backend)
            .entries(&Namespace::new(operator_id, state_name))
            .map_err(|e| KrishivError::Runtime {
                message: format!("read state ({operator_id}/{state_name}): {e}"),
            })?;
        Ok(self.dataframe_from_batches(vec![state_entries_to_batch(&entries)?]))
    }

    /// Create a DataFrame by reading a local Parquet path directly.
    pub fn read_parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame> {
        let path = path.as_ref().to_path_buf();
        block_on(self.read_parquet_async(path))
    }

    /// Read a Delta Lake table directory.
    ///
    /// **Local-only (R18)**: Reads from a local `_delta_log/*.json` directory.
    /// S3 paths and `delta-rs` integration are not yet implemented in this release.
    pub async fn read_delta_async(
        &self,
        path: impl AsRef<str>,
        version: Option<i64>,
    ) -> Result<DataFrame> {
        let sql_dataframe = self.sql_engine.read_delta(path, version).await?;
        Ok(self.dataframe_from_sql(sql_dataframe).with_force_local())
    }

    /// Read a Hudi table directory.
    ///
    /// **Local-only (R18)**: Reads local Copy-on-Write Parquet files.
    /// Remote Hudi catalogs and S3 paths are not yet supported.
    pub async fn read_hudi_async(
        &self,
        path: impl AsRef<str>,
        query_type: krishiv_connectors::lakehouse::HudiQueryType,
        begin_instant: Option<&str>,
    ) -> Result<DataFrame> {
        let sql_dataframe = self
            .sql_engine
            .read_hudi(path, query_type, begin_instant)
            .await?;
        Ok(self.dataframe_from_sql(sql_dataframe).with_force_local())
    }

    /// Append a DataFrame into a local Hudi Copy-On-Write table.
    ///
    /// **Local-only (R18)**: Writes to a local CoW Parquet directory.
    /// S3, object-store URIs, and remote Iceberg catalogs are not supported.
    /// The `path` argument must be a local filesystem path.
    pub async fn write_hudi_append_async(
        &self,
        path: impl AsRef<std::path::Path>,
        dataframe: &DataFrame,
    ) -> Result<krishiv_connectors::lakehouse::HudiWriteResult> {
        let batches = dataframe.collect_async().await?.into_batches();
        let path = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let writer = krishiv_connectors::lakehouse::HudiCowWriter::open(&path);
            let mut total = krishiv_connectors::lakehouse::HudiWriteResult {
                instant: String::new(),
                rows_inserted: 0,
                rows_updated: 0,
                snapshot_rows: 0,
            };
            for batch in batches {
                let result = writer.append(batch).map_err(|e| KrishivError::Runtime {
                    message: e.to_string(),
                })?;
                total.instant = result.instant;
                total.rows_inserted += result.rows_inserted;
                total.rows_updated += result.rows_updated;
                total.snapshot_rows += result.snapshot_rows;
            }
            Ok(total)
        })
        .await
        .map_err(|e| KrishivError::Runtime {
            message: format!("spawn_blocking join error: {e}"),
        })?
    }

    /// Upsert a DataFrame into a local Hudi Copy-On-Write table by key column.
    ///
    /// **Local-only (R18)**: Writes to a local CoW Parquet directory by key column.
    /// S3, object-store URIs, and remote Iceberg catalogs are not supported.
    /// The `path` argument must be a local filesystem path.
    pub async fn write_hudi_upsert_async(
        &self,
        path: impl AsRef<std::path::Path>,
        key_column: &str,
        dataframe: &DataFrame,
    ) -> Result<krishiv_connectors::lakehouse::HudiWriteResult> {
        let batches = dataframe.collect_async().await?.into_batches();
        let path = path.as_ref().to_path_buf();
        let key_column_str = key_column.to_string();
        tokio::task::spawn_blocking(move || {
            let writer = krishiv_connectors::lakehouse::HudiCowWriter::open(&path);
            let mut total = krishiv_connectors::lakehouse::HudiWriteResult {
                instant: String::new(),
                rows_inserted: 0,
                rows_updated: 0,
                snapshot_rows: 0,
            };
            for batch in batches {
                let result =
                    writer
                        .upsert(&key_column_str, batch)
                        .map_err(|e| KrishivError::Runtime {
                            message: e.to_string(),
                        })?;
                total.instant = result.instant;
                total.rows_inserted += result.rows_inserted;
                total.rows_updated += result.rows_updated;
                total.snapshot_rows += result.snapshot_rows;
            }
            Ok(total)
        })
        .await
        .map_err(|e| KrishivError::Runtime {
            message: format!("spawn_blocking join error: {e}"),
        })?
    }

    /// Asynchronously create a DataFrame by reading a local Parquet path directly.
    pub async fn read_parquet_async(&self, path: impl AsRef<Path>) -> Result<DataFrame> {
        let path = path.as_ref();
        let table = parquet_scan_table_name(path);
        self.register_parquet_async(&table, path).await?;
        self.sql_async(format!("SELECT * FROM {table}")).await
    }

    /// Create a bounded local memory stream.
    pub fn memory_stream(
        &self,
        name: impl Into<String>,
        batches: Vec<StreamBatch>,
    ) -> Result<Stream> {
        let name = name.into();
        let record_batches: Vec<RecordBatch> = batches.iter().map(|b| b.batch().clone()).collect();
        self.register_memory_stream(name.clone(), record_batches)?;
        Ok(Stream::for_session(
            name,
            StreamMode::Bounded,
            batches,
            self.mode,
            self.coordinator_url.clone(),
            self.state_ttl.map(|c| c.ttl_ms()),
            self.runtime.clone(),
        ))
    }

    /// Create a schema-bound unbounded local memory stream.
    ///
    /// Use [`Stream::try_push_batch`] or [`Stream::push_batch_async`] to ingest
    /// batches, then [`Stream::close_input`] to terminate the SQL stream.
    pub fn unbounded_memory_stream(
        &self,
        name: impl Into<String>,
        schema: SchemaRef,
    ) -> Result<Stream> {
        crate::window::ensure_alpha_api("unbounded_memory_stream")?;
        let name = name.into();
        let input = self.register_unbounded_input(&name, schema, None)?;
        Ok(Stream::for_unbounded_session(
            name,
            input,
            self.mode,
            self.coordinator_url.clone(),
            self.state_ttl.map(|c| c.ttl_ms()),
            self.runtime.clone(),
        ))
    }

    /// Create an unbounded local memory stream with a specific queue capacity.
    pub fn unbounded_memory_stream_with_capacity(
        &self,
        name: impl Into<String>,
        schema: SchemaRef,
        capacity: usize,
    ) -> Result<Stream> {
        crate::window::ensure_alpha_api("unbounded_memory_stream_with_capacity")?;
        let name = name.into();
        let input = self.register_unbounded_input(&name, schema, Some(capacity))?;
        Ok(Stream::for_unbounded_session(
            name,
            input,
            self.mode,
            self.coordinator_url.clone(),
            self.state_ttl.map(|c| c.ttl_ms()),
            self.runtime.clone(),
        ))
    }

    // ── Catalog API ──────────────────────────────────────────────────────────

    /// List registered table names.
    pub fn list_tables(&self) -> Result<Vec<String>> {
        Ok(self
            .sql_engine
            .registered_table_names()
            .into_iter()
            .map(|name| {
                name.strip_prefix("_krishiv_parquet_")
                    .unwrap_or(&name)
                    .to_string()
            })
            .collect())
    }

    /// Check if a table exists in the session.
    pub fn table_exists(&self, name: &str) -> Result<bool> {
        let names = self.list_tables()?;
        Ok(names
            .iter()
            .any(|n| n == name || format!("_krishiv_parquet_{name}") == *n))
    }

    /// Create a SQL view in the current session.
    pub fn create_view(&self, name: &str, query: &str) -> Result<()> {
        // Quote the identifier to prevent SQL injection.
        let safe_name = quote_identifier(name);
        let sql = format!("CREATE VIEW {safe_name} AS {query}");
        let df = self.sql(&sql)?;
        // CREATE VIEW returns no rows; we just need to execute it.
        let _ = krishiv_common::async_util::block_on(df.collect_async())?;
        Ok(())
    }

    /// Drop a table or view from the session.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let safe_name = quote_identifier(name);
        let drop_table_sql = format!("DROP TABLE {safe_name}");
        match self.execute_ddl(&drop_table_sql) {
            Ok(()) => Ok(()),
            Err(table_error) => {
                let drop_view_sql = format!("DROP VIEW {safe_name}");
                self.execute_ddl(&drop_view_sql)
                    .map_err(|view_error| KrishivError::Runtime {
                        message: format!(
                            "failed to drop table or view '{name}': \
                             DROP TABLE failed with {table_error}; \
                             DROP VIEW failed with {view_error}"
                        ),
                    })
            }
        }
    }

    // ── File read API ────────────────────────────────────────────────────────

    /// Create a DataFrame by reading a local CSV file.
    pub fn read_csv(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame> {
        let path = path.as_ref().to_path_buf();
        krishiv_common::async_util::block_on(self.read_csv_async(path))
    }

    /// Asynchronously create a DataFrame by reading a local CSV file.
    pub async fn read_csv_async(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame> {
        self.read_csv_with_options_async(path, true, b',').await
    }

    pub async fn read_csv_with_options_async(
        &self,
        path: impl AsRef<std::path::Path>,
        has_header: bool,
        delimiter: u8,
    ) -> Result<DataFrame> {
        let opts = krishiv_sql::CsvReaderOptions {
            has_header: Some(has_header),
            delimiter: Some(delimiter as char),
        };
        let sql_dataframe = self
            .sql_engine
            .read_csv_with_options(path, &opts)
            .await
            .map_err(KrishivError::from)?;
        Ok(self.dataframe_from_sql(sql_dataframe))
    }

    /// Create a DataFrame by reading a local JSON/NDJSON file.
    pub fn read_json(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame> {
        let path = path.as_ref().to_path_buf();
        krishiv_common::async_util::block_on(self.read_json_async(path))
    }

    /// Asynchronously create a DataFrame by reading a local JSON/NDJSON file.
    pub async fn read_json_async(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame> {
        let path = path.as_ref();
        let sql_dataframe = self
            .sql_engine
            .read_json(path)
            .await
            .map_err(KrishivError::from)?;
        Ok(self.dataframe_from_sql(sql_dataframe))
    }

    /// Create a DataFrame by reading a local CSV file with typed options.
    pub fn read_csv_with_options(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        opts: krishiv_sql::CsvReaderOptions,
    ) -> Result<DataFrame> {
        krishiv_common::async_util::block_on(async move {
            let sql_dataframe = self
                .sql_engine
                .read_csv_with_options(path, &opts)
                .await
                .map_err(KrishivError::from)?;
            Ok(self.dataframe_from_sql(sql_dataframe))
        })
    }

    /// Create a DataFrame by reading a local Parquet file with typed options.
    pub fn read_parquet_with_options(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        opts: krishiv_sql::ParquetReaderOptions,
    ) -> Result<DataFrame> {
        krishiv_common::async_util::block_on(self.read_parquet_with_options_async(path, opts))
    }

    /// Asynchronously create a DataFrame by reading a local Parquet file with typed options.
    ///
    /// Prefer this over [`Self::read_parquet_with_options`] from async code:
    /// the sync version performs real file/schema I/O via `block_on`, which
    /// cannot be called from a thread already driving a Tokio runtime
    /// without hopping to a fresh OS thread.
    pub async fn read_parquet_with_options_async(
        &self,
        path: impl AsRef<std::path::Path>,
        opts: krishiv_sql::ParquetReaderOptions,
    ) -> Result<DataFrame> {
        let sql_dataframe = self
            .sql_engine
            .read_parquet_with_options(path, &opts)
            .await
            .map_err(KrishivError::from)?;
        Ok(self.dataframe_from_sql(sql_dataframe))
    }

    /// Register in-memory record batches as a named table in this session.
    ///
    /// Used by [`DataFrame::cache`] to materialize query results into memory.
    ///
    /// Works in every execution mode. An embedded session keeps the batches in
    /// process; a session that executes remotely also spills them to a
    /// temporary parquet file so the table travels with the query and resolves
    /// on the cluster. The spill lives as long as the session and is removed
    /// when the last clone of it drops.
    pub fn register_record_batches(
        &self,
        name: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<()> {
        block_on(self.register_record_batches_async(name, batches))
    }

    /// Asynchronously register in-memory Arrow batches as a queryable table.
    ///
    /// Phase 61 (#15 async completeness): the async twin of
    /// [`register_record_batches`](Self::register_record_batches). Prefer this
    /// from async code — the sync form blocks the current thread on it, which
    /// from inside a Tokio runtime forces a thread hop.
    pub async fn register_record_batches_async(
        &self,
        name: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<()> {
        // Audit: this used to register on the local `SqlEngine` only. In a
        // remote session that plans locally and executes on a coordinator,
        // `sql()` therefore succeeded and `collect()` failed at the cluster with
        // an unresolved table — while its sibling `register_parquet` registers
        // both locally and in `registered_parquet` (the set shipped with the
        // query) and works in every mode. Two registration methods that read as
        // interchangeable and were not.
        //
        // A remote session now also spills the batches to parquet and registers
        // the path, so the table travels the same road a `register_parquet`
        // table does: the Flight client inlines it as Arrow IPC, which means it
        // reaches executors in other pods with no shared filesystem. Embedded
        // sessions skip the spill entirely — the local registration is all they
        // ever consult, and writing a file for them would be pure cost.
        if self.runtime.uses_remote_execution() {
            let path = self.spill_table_to_parquet(name, &batches)?;
            self.registered_parquet.insert(
                name.to_owned(),
                RegisteredParquet::new(path, &[] as &[String]),
            );
        }
        self.sql_engine
            .register_record_batches(name, batches)
            .await
            .map_err(KrishivError::from)
    }

    /// Write `batches` to a parquet file under this session's spill directory
    /// and return its path. Creates the directory on first use.
    fn spill_table_to_parquet(
        &self,
        name: &str,
        batches: &[arrow::record_batch::RecordBatch],
    ) -> Result<PathBuf> {
        let Some(first) = batches.first() else {
            return Err(KrishivError::unsupported(format!(
                "cannot register table '{name}' from an empty batch list in a remote session: \
                 there is no schema to ship to the cluster"
            )));
        };
        let dir = self.ensure_table_spill_dir()?;
        // One file per table name; re-registering the same name overwrites it,
        // matching the local registry's replace-in-place semantics.
        let safe: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let path = dir.join(format!("{safe}.parquet"));
        let file = std::fs::File::create(&path).map_err(|e| KrishivError::Runtime {
            message: format!("create spill parquet '{}': {e}", path.display()),
        })?;
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, first.schema(), None).map_err(|e| {
                KrishivError::Runtime {
                    message: format!("create spill parquet writer for '{name}': {e}"),
                }
            })?;
        for batch in batches {
            writer.write(batch).map_err(|e| KrishivError::Runtime {
                message: format!("write spill parquet batch for '{name}': {e}"),
            })?;
        }
        writer.close().map_err(|e| KrishivError::Runtime {
            message: format!("close spill parquet writer for '{name}': {e}"),
        })?;
        Ok(path)
    }

    /// This session's spill directory, created on first use. The guard is
    /// shared across clones so the directory is removed when the last one drops.
    fn ensure_table_spill_dir(&self) -> Result<PathBuf> {
        let mut slot = self
            .table_spill_dir
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = slot.as_ref() {
            return Ok(existing.dir.clone());
        }
        // Unique per session: the pid alone collides across sessions in one
        // process, which would let one session's teardown delete another's
        // registered tables.
        let unique = SESSION_SPILL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "krishiv-session-tables-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).map_err(|e| KrishivError::Runtime {
            message: format!("create session spill dir '{}': {e}", dir.display()),
        })?;
        *slot = Some(Arc::new(SessionSpillDir { dir: dir.clone() }));
        Ok(dir)
    }

    /// Deregister (drop) a named table from this session.
    pub fn deregister_table(&self, name: &str) -> Result<()> {
        self.sql_engine
            .deregister_table(name)
            .map_err(KrishivError::from)
    }

    /// Create a streaming data reader rooted at this session.
    pub fn read_stream(&self) -> crate::streaming_builder::DataStreamReader {
        crate::streaming_builder::DataStreamReader::new(self.clone())
    }

    /// Crate-internal: wrap a list of pre-collected batches as a `DataFrame`.
    pub(crate) fn create_dataframe_from_batches(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<crate::DataFrame> {
        Ok(crate::DataFrame::from_batches(
            self.mode,
            batches,
            self.jobs.clone(),
            self.next_job_id.clone(),
            self.runtime.clone(),
            self.registered_parquet.clone(),
        ))
    }

    pub(crate) fn execute_ddl(&self, sql: &str) -> Result<()> {
        let df = self.sql(sql)?;
        let _ = krishiv_common::async_util::block_on(df.collect_async())?;
        Ok(())
    }
}

fn parquet_scan_table_name(path: &Path) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("scan");
    let sanitized: String = stem
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    // Include a short hash of the full path to prevent collisions when files
    // in different directories share the same stem.
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    let path_hash = hasher.finish();
    format!("_krishiv_parquet_{sanitized}_{path_hash:x}")
}

/// Build the state-as-a-table RecordBatch for [`Session::read_state`]: raw
/// `key`/`value` bytes plus best-effort `key_utf8`/`value_utf8` decodes.
fn state_entries_to_batch(entries: &[(Vec<u8>, Vec<u8>)]) -> Result<RecordBatch> {
    use arrow::array::{BinaryBuilder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};

    let mut key_b = BinaryBuilder::new();
    let mut val_b = BinaryBuilder::new();
    let mut key_s = StringBuilder::new();
    let mut val_s = StringBuilder::new();
    for (k, v) in entries {
        key_b.append_value(k);
        val_b.append_value(v);
        match std::str::from_utf8(k) {
            Ok(s) => key_s.append_value(s),
            Err(_) => key_s.append_null(),
        }
        match std::str::from_utf8(v) {
            Ok(s) => val_s.append_value(s),
            Err(_) => val_s.append_null(),
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, false),
        Field::new("key_utf8", DataType::Utf8, true),
        Field::new("value_utf8", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(key_b.finish()),
            Arc::new(val_b.finish()),
            Arc::new(key_s.finish()),
            Arc::new(val_s.finish()),
        ],
    )
    .map_err(|e| KrishivError::Runtime {
        message: format!("build state table: {e}"),
    })
}

pub(crate) async fn runtime_collect_batch_sql(
    runtime: Arc<dyn ExecutionRuntime>,
    query: &str,
    tables: &[BatchTableRegistration],
    is_streaming: bool,
) -> Result<Vec<RecordBatch>> {
    // Use the async variant: remote runtimes drive the Flight call directly
    // without a spawn_blocking hop; in-process runtimes off-load DataFusion
    // work to the blocking pool internally.
    runtime
        .collect_batch_sql_async(query, tables, is_streaming)
        .await
        .map_err(KrishivError::from)
}

fn submitted_sql_state_to_job_state(state: SubmittedSqlJobState) -> JobState {
    match state {
        SubmittedSqlJobState::Pending => JobState::Pending,
        SubmittedSqlJobState::Running => JobState::Running,
        SubmittedSqlJobState::Succeeded => JobState::Succeeded,
        SubmittedSqlJobState::Failed => JobState::Failed,
        SubmittedSqlJobState::Cancelled => JobState::Cancelled,
    }
}

use krishiv_common::sql_util::quote_identifier;

/// Collect the transitive closure of incremental views that `view` depends on,
/// in dependency order (upstream views before downstream). A token in a view's
/// SQL that resolves to a registered incremental view is treated as a dependency;
/// everything else (sources, columns, keywords) is ignored.
fn collect_pipeline_view_deps(
    view: &str,
    reg: &krishiv_sql::incremental_view::IncrementalViewRegistry,
    ordered: &mut Vec<String>,
    seen: &mut std::collections::HashSet<String>,
) -> krishiv_sql::SqlResult<()> {
    if seen.contains(view) {
        return Ok(());
    }
    seen.insert(view.to_string());
    if let Some(entry) = reg.get(view)? {
        for token in entry
            .body_sql
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .filter(|t| !t.is_empty())
        {
            if token != view && reg.get(token)?.is_some() {
                collect_pipeline_view_deps(token, reg, ordered, seen)?;
            }
        }
        ordered.push(view.to_string());
    }
    Ok(())
}

/// The table named in the first `FROM` clause, for binding a stateless
/// streaming query's pushes to its input. Deliberately simple: streaming
/// stateless queries read one source table by construction.
fn stateless_source_table(sql: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let idx = lower.find(" from ")?;
    let rest = sql[idx + 6..].trim_start();
    let table: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!table.is_empty()).then_some(table)
}

#[cfg(test)]
mod coordinator_url_tests {
    use super::*;

    #[test]
    fn ivm_http_url_prefers_explicit_http_then_falls_back_to_flight() {
        // Distributed IVM management lives on the coordinator HTTP port, which is
        // distinct from the Flight data port. When set explicitly, it wins.
        let s = SessionBuilder::new()
            .with_coordinator("http://coord:2003")
            .with_coordinator_http("http://coord:2002")
            .build()
            .expect("session builds");
        assert_eq!(s.ivm_http_url(), Some("http://coord:2002"));
        assert_eq!(s.coordinator_http_url(), Some("http://coord:2002"));

        // Without an explicit HTTP URL, fall back to the Flight URL (single-port
        // / local setups where they coincide).
        let s = SessionBuilder::new()
            .with_coordinator("http://coord:2003")
            .build()
            .expect("session builds");
        assert_eq!(s.ivm_http_url(), Some("http://coord:2003"));
        assert_eq!(s.coordinator_http_url(), Some("http://coord:2003"));
    }
}

#[cfg(test)]
mod state_reader_tests {
    use super::*;

    /// State Processor API: write keyed state to a RocksDB backend, then read it
    /// back offline as a table via `Session::read_state`.
    #[test]
    fn read_state_returns_checkpointed_state_as_a_table() {
        use krishiv_state::{Namespace, RocksDbStateBackend, StateBackend};

        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("state");
        let ns = Namespace::new("op1", "count");
        // Write state, then drop the backend so RocksDB flushes and releases the
        // directory lock before read_state re-opens it.
        {
            let mut backend = RocksDbStateBackend::open(&dir).expect("open");
            backend
                .put(&ns, b"alice".to_vec(), b"3".to_vec())
                .expect("put alice");
            backend
                .put(&ns, b"bob".to_vec(), b"7".to_vec())
                .expect("put bob");
        }

        let session = SessionBuilder::new().build().expect("session");
        let df = session
            .read_state(&dir, "op1", "count")
            .expect("read_state");
        let batches = df.collect().expect("collect").into_batches();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "two state entries");
        let batch = batches.first().expect("one batch");
        assert_eq!(batch.num_columns(), 4, "key,value,key_utf8,value_utf8");
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["key", "value", "key_utf8", "value_utf8"]);

        // Read out the utf8-decoded keys/values and check the mapping.
        use arrow::array::StringArray;
        let keys = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 keys");
        let vals = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 vals");
        let mut got: Vec<(String, String)> = (0..batch.num_rows())
            .map(|i| (keys.value(i).to_string(), vals.value(i).to_string()))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("alice".to_string(), "3".to_string()),
                ("bob".to_string(), "7".to_string())
            ]
        );
    }
}

#[cfg(test)]
mod udf_registration_tests {
    use super::*;
    use krishiv_plan::udf::MultiplyScalarUdf;

    #[test]
    fn durable_profile_rejects_registration_without_mutating_registry() {
        let session = SessionBuilder::new().build().expect("session should build");
        let udf = Arc::new(MultiplyScalarUdf::new("double", "x", 2));

        let error = session
            .register_scalar_udf_for_policy(
                udf,
                krishiv_common::NativeScalarUdfPolicy::from_decision(
                    krishiv_common::DurabilityProfile::SingleNodeDurable,
                    true,
                ),
            )
            .expect_err("durable profile must reject native scalar UDFs");

        assert!(matches!(
            error,
            KrishivError::InvalidConfig { message }
                if message.contains("single-node-durable")
        ));
        assert!(session.scalar_udf_names().is_empty());
    }

    #[test]
    fn dev_profile_registers_and_synchronizes_scalar_udf() {
        let session = SessionBuilder::new().build().expect("session should build");
        let udf = Arc::new(MultiplyScalarUdf::new("double", "x", 2));

        session
            .register_scalar_udf_for_policy(
                udf,
                krishiv_common::NativeScalarUdfPolicy::from_decision(
                    krishiv_common::DurabilityProfile::DevLocal,
                    false,
                ),
            )
            .expect("dev-local scalar UDF registration should succeed");

        assert_eq!(session.scalar_udf_names(), vec!["double".to_string()]);
    }

    #[test]
    fn empty_scalar_udf_name_is_rejected_without_mutation() {
        let session = SessionBuilder::new().build().expect("session should build");
        let udf = Arc::new(MultiplyScalarUdf::new("   ", "x", 2));

        let error = session
            .register_scalar_udf_for_policy(
                udf,
                krishiv_common::NativeScalarUdfPolicy::from_decision(
                    krishiv_common::DurabilityProfile::DevLocal,
                    false,
                ),
            )
            .expect_err("empty scalar UDF name must be rejected");

        assert!(matches!(
            error,
            KrishivError::InvalidConfig { message }
                if message.contains("must not be empty")
        ));
        assert!(session.scalar_udf_names().is_empty());
    }
}

#[cfg(test)]
mod remote_session_parity_tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let arr: Int64Array = vals.iter().copied().collect();
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("batch")
    }

    /// A Distributed session that needs no live coordinator: nothing here
    /// executes, it only checks what the session *would* ship.
    fn remote_session() -> Session {
        Session::builder()
            .with_coordinator("http://coord.invalid:50051")
            .build()
            .expect("distributed session")
    }

    fn embedded_session() -> Session {
        Session::builder()
            .with_execution_mode(ExecutionMode::Embedded)
            .build()
            .expect("embedded session")
    }

    /// A Python UDF registered on a remote session must ride along with the
    /// query text that `session.sql(..)` produces.
    ///
    /// Before the fix, `prepend_python_udfs` had exactly one caller —
    /// `execute_remote_async` — so `session.sql(q).collect()` shipped the bare
    /// query and executors failed with "unknown function", while
    /// `session.execute_remote(q)` on the same session worked. Two entry points
    /// that read as interchangeable, one of which silently dropped the UDF.
    #[test]
    fn remote_sql_dataframe_ships_registered_python_udfs() {
        let session = remote_session();
        session.register_python_udf_bytes("triple", b"fake-pickle", &["Int64".into()], "Int64");

        let df = session.sql("SELECT 1 AS v").expect("sql");
        let shipped = df.query_for_remote().expect("a query to ship");

        assert!(
            shipped.contains("register-python-udf") && shipped.contains("triple"),
            "the query shipped to the cluster must carry the UDF directive, \
             or the executor cannot reconstruct the function; got: {shipped}"
        );
    }

    /// The embedded path resolves UDFs in process and never ships the text, so
    /// it must stay byte-identical to what the user wrote. This is the other
    /// half of the fix: the directive is submission-only, not a planning input.
    #[test]
    fn embedded_sql_dataframe_query_is_unchanged() {
        let session = embedded_session();
        session.register_python_udf_bytes("triple", b"fake-pickle", &["Int64".into()], "Int64");

        let df = session.sql("SELECT 1 AS v").expect("sql");
        let query = df.query_for_remote().expect("a query");

        assert!(
            !query.contains("register-python-udf"),
            "embedded execution never ships the query; prepending directives \
             there only corrupts diagnostics; got: {query}"
        );
    }

    /// In a remote session, an in-memory table must become something the
    /// cluster can actually read.
    ///
    /// Before the fix this registered on the local `SqlEngine` only, so the
    /// query planned locally and then failed at the coordinator with an
    /// unresolved table — while the sibling `register_parquet` registered in
    /// both places and worked in every mode.
    #[test]
    fn remote_register_record_batches_is_shipped_to_the_cluster() {
        let session = remote_session();
        session
            .register_record_batches("nums", vec![batch(&[1, 2, 3])])
            .expect("register");

        let registration = session
            .registered_parquet
            .get("nums")
            .map(|entry| entry.value().clone())
            .expect("an in-memory table registered remotely must be shipped");
        assert!(
            registration.path.exists(),
            "the spilled parquet must exist on disk at {}",
            registration.path.display()
        );
    }

    /// A transformed DataFrame must still execute in a remote session.
    ///
    /// Audit: every transform routes through `with_new_ops`, which drops
    /// `sql_query`, and remote execution used to require it — so `select`,
    /// `filter`, `join`, `group_by` and the rest were embedded-only in both the
    /// Rust and Python surfaces. A program that worked locally stopped working
    /// the moment its session pointed at a cluster. The plan is now rendered
    /// back to SQL through the DataFusion unparser.
    #[test]
    fn transformed_dataframe_is_shippable_in_a_remote_session() {
        let session = remote_session();
        session
            .register_record_batches("nums", vec![batch(&[1, 2, 3])])
            .expect("register");

        let filtered = session
            .sql("SELECT v FROM nums")
            .expect("sql")
            .filter("v > 1")
            .expect("filter");

        let shipped = filtered
            .query_for_remote()
            .expect("a transformed DataFrame must be shippable");
        assert!(
            shipped.to_ascii_uppercase().contains("WHERE"),
            "the unparsed SQL must carry the filter, or the cluster would run \
             the unfiltered query and return the wrong rows; got: {shipped}"
        );
    }

    /// The UDF prelude must survive a transform.
    ///
    /// It is held apart from `sql_query` precisely because a transform discards
    /// that field; baking the directives into it would lose them here.
    #[test]
    fn udf_prelude_survives_a_transform() {
        let session = remote_session();
        session.register_python_udf_bytes("triple", b"fake-pickle", &["Int64".into()], "Int64");
        session
            .register_record_batches("nums", vec![batch(&[1, 2, 3])])
            .expect("register");

        let filtered = session
            .sql("SELECT v FROM nums")
            .expect("sql")
            .filter("v > 1")
            .expect("filter");

        let shipped = filtered.query_for_remote().expect("shippable");
        assert!(
            shipped.contains("register-python-udf"),
            "a transform must not strip the UDF directives; got: {shipped}"
        );
    }

    /// `submit_streaming` in Distributed mode used to be a flat error telling
    /// the caller to use `Session::stream` — a different contract, in which the
    /// CLIENT feeds the input. A `CompiledJob` names its own sources, so the
    /// honest distributed shape is the run-loop engine, whose subtasks open
    /// those sources themselves. These guards run before any coordinator RPC,
    /// so they need no live cluster.
    #[test]
    fn distributed_streaming_refuses_a_bounded_source_instead_of_idling_on_it() {
        let session = remote_session();
        let job = crate::CompiledJob::new(
            "bounded-stream",
            "SELECT k, COUNT(*) FROM TUMBLE(TABLE src, DESCRIPTOR(ts), 60000) GROUP BY k",
            vec![crate::SourceSpec::bounded(
                "src",
                "parquet",
                "/tmp/x.parquet",
            )],
            vec![],
            true,
        );

        let error = session
            .submit_streaming(job)
            .expect_err("a bounded source cannot drive an unbounded run-loop");
        let message = error.to_string();
        assert!(
            message.contains("bounded") && message.contains("idle"),
            "the refusal must say WHY a bounded source is wrong here — the job would \
             read it once and then look healthy forever; got: {message}"
        );
    }

    /// A declared sink is refused rather than silently discarded.
    ///
    /// The registration wire has no sink field, so this path used to register
    /// the job, run it, report Running, and write the output only into the
    /// cluster's egress ring — where it is dropped past the cap. The identical
    /// job submitted embedded opens the sink and writes to it. Nothing anywhere
    /// said the sink had been dropped.
    ///
    /// The message must name the sink and the alternative, because "unsupported"
    /// alone turns a silent wrong answer into a dead end. The coordinator has
    /// honoured registry sinks since #197, so the capability exists — only this
    /// client leg cannot express it, and the error says so.
    #[test]
    fn distributed_streaming_refuses_a_declared_sink_instead_of_dropping_it() {
        let session = remote_session();
        let job = crate::CompiledJob::new(
            "sinking-stream",
            "SELECT k, COUNT(*) FROM TUMBLE(TABLE src, DESCRIPTOR(ts), 60000) GROUP BY k",
            vec![crate::SourceSpec::unbounded("src", "kafka", "events")],
            vec![crate::SinkSpec::new("out", "parquet", "/tmp/out.parquet")],
            true,
        );

        let error = session
            .submit_streaming(job)
            .expect_err("a declared sink must be refused, not silently discarded");
        let message = error.to_string();
        assert!(
            message.contains("parquet") && message.contains("/tmp/out.parquet"),
            "the refusal must name the sink whose output would vanish; got: {message}"
        );
        assert!(
            message.contains("egress") || message.contains("coordinator"),
            "and must say where the output would have gone, or how to get it there; \
             got: {message}"
        );
    }

    /// A job with no sources has nothing for its subtasks to read. Registering
    /// it would produce a run-loop that idles forever reporting Running.
    #[test]
    fn distributed_streaming_refuses_a_job_with_no_sources() {
        let session = remote_session();
        let job = crate::CompiledJob::new(
            "sourceless",
            "SELECT k, COUNT(*) FROM TUMBLE(TABLE src, DESCRIPTOR(ts), 60000) GROUP BY k",
            vec![],
            vec![],
            true,
        );

        let message = session
            .submit_streaming(job)
            .expect_err("a sourceless continuous job must be refused")
            .to_string();
        assert!(
            message.contains("at least one unbounded source"),
            "the refusal must name what is missing: {message}"
        );
    }

    /// The positive half, and the one that actually proves the feature exists:
    /// a valid windowed job over an unbounded source must get PAST every local
    /// guard and attempt the coordinator RPC.
    ///
    /// Before this, the Distributed arm was a flat `unsupported` for every
    /// input — so a test that only checked refusals would have passed against
    /// the old code too. Here the session points at an unroutable host, so
    /// reaching the network is exactly what "the guards let it through" looks
    /// like: the error must be a transport failure, never `unsupported`.
    #[test]
    fn a_valid_windowed_job_over_an_unbounded_source_reaches_the_coordinator() {
        let session = remote_session();
        let job = crate::CompiledJob::new(
            "live-stream",
            "SELECT k, COUNT(*) FROM TUMBLE(TABLE src, DESCRIPTOR(ts), 60000) GROUP BY k",
            vec![crate::SourceSpec::unbounded("src", "kafka", "events")],
            vec![],
            true,
        );

        let error = session
            .submit_streaming(job)
            .expect_err("coord.invalid does not resolve, so the RPC must fail");
        assert!(
            !matches!(error, KrishivError::Unsupported { .. }),
            "a well-formed distributed streaming job must no longer be refused as \
             unsupported — it must be attempted; got: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("continuous-register") || message.contains("transport"),
            "the failure must come from the registration RPC, which means every local \
             guard passed: {message}"
        );
    }

    /// The distributed path must ship the SAME connector properties the
    /// embedded path resolves. Two mappings would be free to drift, and drift
    /// here means a job that reads the wrong data rather than one that errors.
    #[test]
    fn distributed_source_properties_match_what_the_embedded_path_resolves() {
        let spec = crate::SourceSpec::unbounded("events", "csv", "/data/events.csv");
        let props = crate::connector_runtime::connector_properties_for_source(&spec);
        assert_eq!(
            props.get("path").map(String::as_str),
            Some("/data/events.csv"),
            "a path-addressed connector's uri must resolve to the same `path` \
             property the embedded driver reads; got {props:?}"
        );

        // And the mapping's deliberate refusal survives: an endpoint-addressed
        // connector gets no invented locator key.
        let kafka = crate::SourceSpec::unbounded("events", "kafka", "some-topic");
        let kafka_props = crate::connector_runtime::connector_properties_for_source(&kafka);
        assert!(
            !kafka_props.contains_key("path"),
            "kafka carries its locator in options; inventing `path` for it would be a \
             guess shipped to an executor: {kafka_props:?}"
        );
    }

    /// Embedded sessions must not pay for a spill they never read.
    #[test]
    fn embedded_register_record_batches_writes_no_spill() {
        let session = embedded_session();
        session
            .register_record_batches("nums", vec![batch(&[1, 2, 3])])
            .expect("register");

        assert!(
            session.registered_parquet.get("nums").is_none(),
            "embedded execution resolves the table in process; spilling it to \
             parquet is pure cost"
        );
    }
}
