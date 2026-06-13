use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_common::async_util::block_on;
use krishiv_plan::governance::{AuthProvider, PolicyHook};
use krishiv_plan::udf::{AggregateUdf, ScalarUdf, TableUdf, UdfRegistry};
use krishiv_runtime::{
    BatchTableRegistration, ExecutionPlacement, ExecutionRuntime, InProcessCluster, JobStatus,
    LocalJobRegistry, LocalWindowExecutionSpec, RuntimeMode, build_execution_runtime,
};
use krishiv_sql::{ContinuousTableInput, SqlEngine};

use crate::dataframe::DataFrame;
use crate::error::{KrishivError, Result};
use crate::stream::Stream;
use crate::types::{DeploymentTarget, ExecutionMode, StreamBatch, StreamMode};
use crate::window::StateTtlConfig;

/// Builder for Krishiv sessions.
#[derive(Clone)]
pub struct SessionBuilder {
    mode: ExecutionMode,
    deployment_target: DeploymentTarget,
    auth: Option<Arc<dyn AuthProvider>>,
    policy: Option<Arc<dyn PolicyHook>>,
    coordinator_url: Option<String>,
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
            local_cluster_grpc: None,
            state_ttl: None,
            target_parallelism: None,
            remote_execution: remote_execution_from_env(),
            in_process_cluster: None,
            remote_execution_explicit: false,
            shuffle_partitions: None,
            config: std::collections::BTreeMap::new(),
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
        let coordinator_url = std::env::var("KRISHIV_COORDINATOR_URL")
            .or_else(|_| std::env::var("KRISHIV_COORDINATOR"))
            .ok();

        let mut builder = SessionBuilder::new();

        match mode_raw.trim().to_ascii_lowercase().as_str() {
            // No mode: infer from coordinator URL presence.
            "" => {
                if let Some(url) = coordinator_url {
                    builder = builder.with_local_cluster(url);
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

        if let Ok(val) = std::env::var("KRIVISH_SHUFFLE_PARTITIONS") {
            let n: u32 = val.trim().parse().map_err(|_| {
                KrishivError::unsupported(format!(
                    "KRIVISH_SHUFFLE_PARTITIONS must be a positive integer, got '{val}'"
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
            std::thread::available_parallelism().unwrap_or(std::num::NonZeroUsize::new(1).unwrap())
        });
        let sql_engine = SqlEngine::new()
            .with_target_parallelism(parallelism)
            .with_udf_registry(Arc::clone(&udf_registry))
            .with_shuffle_partitions(self.shuffle_partitions);
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
            coordinator_grpc_url: self.local_cluster_grpc,
            state_ttl: self.state_ttl,
            memory_streams: Arc::new(DashMap::new()),
            udf_registry,
            local_cluster,
            runtime,
            registered_parquet: Arc::new(DashMap::new()),
            stream_jobs: Arc::new(DashMap::new()),
            unbounded_streams: Arc::new(DashMap::new()),
            config: Arc::new(DashMap::from_iter(self.config)),
        })
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
    coordinator_grpc_url: Option<String>,
    state_ttl: Option<StateTtlConfig>,
    memory_streams: Arc<DashMap<String, Vec<RecordBatch>>>,
    udf_registry: Arc<RwLock<UdfRegistry>>,
    local_cluster: Arc<InProcessCluster>,
    runtime: Arc<dyn ExecutionRuntime>,
    registered_parquet: Arc<DashMap<String, PathBuf>>,
    stream_jobs: Arc<DashMap<String, LocalWindowExecutionSpec>>,
    unbounded_streams: Arc<DashMap<String, Arc<ContinuousTableInput>>>,
    config: Arc<DashMap<String, String>>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("mode", &self.mode)
            .field("sql_engine", &self.sql_engine)
            .finish_non_exhaustive()
    }
}

impl Session {
    /// Start building a session.
    pub fn builder() -> SessionBuilder {
        SessionBuilder::new()
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
        self.stream_jobs.insert(name.clone(), spec);
        Ok(name)
    }

    pub fn push_stream_job_input(&self, job_id: &str, batches: Vec<RecordBatch>) -> Result<()> {
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
        self.jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .snapshot()
    }

    /// Optional control-plane gRPC endpoint associated with this session.
    pub fn coordinator_grpc_url(&self) -> Option<&str> {
        self.coordinator_grpc_url.as_deref()
    }

    /// Shared UDF registry for this session.
    pub fn udf_registry(&self) -> Arc<RwLock<UdfRegistry>> {
        Arc::clone(&self.udf_registry)
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
        self.udf_registry
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .scalar_names()
            .into_iter()
            .map(str::to_owned)
            .collect()
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
        self.udf_registry
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .aggregate_names()
            .into_iter()
            .map(str::to_owned)
            .collect()
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
        self.udf_registry
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .table_names()
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    /// Register a local Parquet path as a SQL table.
    pub fn register_parquet(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let table_name = table_name.as_ref().to_owned();
        let path = path.as_ref().to_path_buf();
        block_on(async {
            self.sql_engine
                .register_parquet(&table_name, &path)
                .await
                .map_err(KrishivError::from)
        })?;
        self.registered_parquet.insert(table_name.clone(), path);
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
        if self.runtime.uses_remote() {
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
        self.registered_parquet.insert(table_name, path);
        Ok(())
    }

    fn dataframe_from_sql(
        &self,
        sql_dataframe: impl krishiv_sql::KrishivDataFrameOps + 'static,
    ) -> DataFrame {
        let sql_query = sql_dataframe.query().map(str::to_owned);
        DataFrame::from_sql_dataframe(
            self.mode,
            sql_dataframe,
            sql_query,
            self.jobs.clone(),
            self.next_job_id.clone(),
            self.coordinator_url.clone(),
            self.runtime.clone(),
            self.registered_parquet.clone(),
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
    pub fn sql(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        block_on(self.sql_async(query))
    }

    /// Asynchronously create a DataFrame from a SQL query.
    pub async fn sql_async(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        let query = query.as_ref().to_owned();
        let sql_dataframe = self.sql_engine.sql(&query).await?;
        Ok(self.dataframe_from_sql(sql_dataframe))
    }

    /// Execute SQL on the local `SqlEngine` only (embedded / single-node path).
    ///
    /// Never routes to a remote Flight endpoint, even in distributed mode.
    pub fn execute_local(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        block_on(self.execute_local_async(query))
    }

    /// Async variant of [`Self::execute_local`].
    ///
    /// Always executes via the local `SqlEngine` (DataFusion) regardless of
    /// session mode. Returns a DataFrame with pre-collected results so that
    /// `.collect()` returns immediately without remote routing.
    pub async fn execute_local_async(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        let sql_df =
            self.sql_engine
                .sql(query.as_ref())
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
    pub fn execute_remote(&self, query: impl AsRef<str>) -> Result<DataFrame> {
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
        let query = query.as_ref();
        let tables = self
            .registered_parquet
            .iter()
            .map(|entry| BatchTableRegistration::new(entry.key().clone(), entry.value().clone()))
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
        let df = self.sql("SHOW TABLES")?;
        let batches = krishiv_common::async_util::block_on(df.collect_async())?.into_batches();
        let mut tables = Vec::new();
        for batch in &batches {
            let idx = batch
                .schema()
                .index_of("TableName")
                .or_else(|_| batch.schema().index_of("table_name"))
                .map_err(|_| KrishivError::Runtime {
                    message: "SHOW TABLES result missing table_name column".into(),
                })?;
            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| KrishivError::Runtime {
                    message: "SHOW TABLES table_name is not a string column".into(),
                })?;
            for i in 0..col.len() {
                if let Some(name) = col.value(i).strip_prefix("_krishiv_parquet_") {
                    tables.push(name.to_string());
                } else {
                    tables.push(col.value(i).to_string());
                }
            }
        }
        tables.sort();
        tables.dedup();
        Ok(tables)
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
        let sql = format!("CREATE VIEW {name} AS {query}");
        let df = self.sql(&sql)?;
        // CREATE VIEW returns no rows; we just need to execute it.
        let _ = krishiv_common::async_util::block_on(df.collect_async())?;
        Ok(())
    }

    /// Drop a table or view from the session.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let sql = format!("DROP TABLE {name}");
        let df = self.sql(&sql)?;
        let _ = krishiv_common::async_util::block_on(df.collect_async())?;
        Ok(())
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
        path: impl AsRef<std::path::Path>,
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
        path: impl AsRef<std::path::Path>,
        opts: krishiv_sql::ParquetReaderOptions,
    ) -> Result<DataFrame> {
        krishiv_common::async_util::block_on(async move {
            let sql_dataframe = self
                .sql_engine
                .read_parquet_with_options(path, &opts)
                .await
                .map_err(KrishivError::from)?;
            Ok(self.dataframe_from_sql(sql_dataframe))
        })
    }

    /// Register in-memory record batches as a named table in this session.
    ///
    /// Used by [`DataFrame::cache`] to materialize query results into memory.
    pub fn register_record_batches(
        &self,
        name: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<()> {
        krishiv_common::async_util::block_on(
            self.sql_engine
                .register_record_batches(name, batches),
        )
        .map_err(KrishivError::from)
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
}

fn parquet_scan_table_name(path: &Path) -> String {
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("scan");
    let sanitized: String = stem
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("_krishiv_parquet_{sanitized}")
}

pub(crate) async fn runtime_collect_batch_sql(
    runtime: Arc<dyn ExecutionRuntime>,
    query: &str,
    tables: &[BatchTableRegistration],
    is_streaming: bool,
) -> Result<Vec<RecordBatch>> {
    let query = query.to_owned();
    let tables = tables.to_vec();
    tokio::task::spawn_blocking(move || runtime.collect_batch_sql(&query, &tables, is_streaming))
        .await
        .map_err(|e| KrishivError::Runtime {
            message: format!("runtime collect task failed: {e}"),
        })?
        .map_err(KrishivError::from)
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
