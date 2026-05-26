use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use arrow::record_batch::RecordBatch;
use krishiv_async_util::block_on;
use krishiv_governance::{AuthProvider, PolicyHook};
use krishiv_plan::ExecutionKind;
use krishiv_runtime::{
    BatchTableRegistration, ExecutionRuntime, InProcessCluster, JobStatus, LocalJobRegistry,
    LocalWindowExecutionSpec, RuntimeMode, build_execution_runtime,
};
use krishiv_sql::SqlEngine;
use krishiv_sql_policy::PolicyEnforcingSqlEngine;
use krishiv_udf::{ScalarUdf, UdfRegistry};

use crate::dataframe::DataFrame;
use crate::error::{KrishivError, Result};
use crate::stream::Stream;
use crate::types::{ExecutionMode, StreamBatch, StreamMode};
use crate::window::StateTtlConfig;

/// Builder for Krishiv sessions.
#[derive(Clone)]
pub struct SessionBuilder {
    mode: ExecutionMode,
    auth: Option<Arc<dyn AuthProvider>>,
    policy: Option<Arc<dyn PolicyHook>>,
    coordinator_url: Option<String>,
    local_cluster_grpc: Option<String>,
    state_ttl: Option<StateTtlConfig>,
    /// When true, route data-plane work to the remote Flight endpoint (no local fallback).
    remote_execution: bool,
    /// Reuse an existing in-process cluster (continuous stream registry, coordinator bridge).
    in_process_cluster: Option<Arc<InProcessCluster>>,
    /// Whether `with_remote_execution` was called explicitly (B2): controls
    /// whether `build()` flips Distributed mode to `remote_execution = true`
    /// automatically.  Tests and integrations that want a local fallback in
    /// Distributed mode set this via `with_remote_execution(false)`.
    remote_execution_explicit: bool,
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
            .field("remote_execution", &self.remote_execution)
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
            auth: None,
            policy: None,
            coordinator_url: None,
            local_cluster_grpc: None,
            state_ttl: None,
            remote_execution: remote_execution_from_env(),
            in_process_cluster: None,
            remote_execution_explicit: false,
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

pub(crate) fn shared_embedded_runtime() -> Arc<dyn ExecutionRuntime> {
    static RUNTIME: OnceLock<Arc<dyn ExecutionRuntime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            let cluster =
                Arc::new(InProcessCluster::new().expect("shared embedded in-process cluster"));
            build_execution_runtime(RuntimeMode::Embedded, cluster, None, None, false)
        })
        .clone()
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
        self.local_cluster_grpc = None;
        self.coordinator_url = Some(flight_url.into());
        self.mode = ExecutionMode::SingleNode;
        self
    }

    /// When true, batch and streaming data-plane work is routed to the remote Flight
    /// endpoint instead of the session-embedded in-process cluster.  When false in
    /// Distributed mode, the session falls back to an in-process cluster — useful
    /// for integration tests but never the default (see B2).
    #[must_use]
    pub fn with_remote_execution(mut self, enabled: bool) -> Self {
        self.remote_execution = enabled;
        self.remote_execution_explicit = true;
        self
    }

    /// gRPC coordinator address (control plane), separate from the Flight SQL URL.
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

    /// Build a session.
    pub fn build(self) -> Result<Session> {
        let udf_registry = Arc::new(RwLock::new(UdfRegistry::new()));
        let sql_engine = SqlEngine::new().with_udf_registry(Arc::clone(&udf_registry));
        let policy_engine = match (self.auth, self.policy) {
            (Some(auth), Some(policy)) => Some(PolicyEnforcingSqlEngine::new(
                sql_engine.clone(),
                auth,
                policy,
            )),
            _ => None,
        };
        let local_cluster = match self.in_process_cluster {
            Some(cluster) => cluster,
            None => Arc::new(InProcessCluster::new().map_err(|e| KrishivError::Runtime {
                message: e.to_string(),
            })?),
        };

        // B2: Distributed sessions default to true remote execution.  An
        // explicit `with_remote_execution(false)` (or `KRISHIV_REMOTE_EXEC=0`)
        // still keeps the local fallback for integration tests.
        let remote_execution = if self.remote_execution_explicit
            || remote_execution_from_env_opt().is_some()
        {
            self.remote_execution
        } else {
            matches!(self.mode, ExecutionMode::Distributed)
        };

        if matches!(self.mode, ExecutionMode::Distributed) && self.coordinator_url.is_none() {
            return Err(KrishivError::unsupported(
                "Distributed mode requires SessionBuilder::with_coordinator(<flight_url>); \
                 otherwise use Embedded or SingleNode",
            ));
        }

        let runtime = build_execution_runtime(
            execution_mode_to_runtime_mode(self.mode),
            Arc::clone(&local_cluster),
            self.coordinator_url.clone(),
            self.local_cluster_grpc.clone(),
            remote_execution,
        );
        Ok(Session {
            mode: self.mode,
            sql_engine,
            policy_engine,
            jobs: Arc::new(Mutex::new(LocalJobRegistry::default())),
            next_job_id: Arc::new(AtomicU64::new(1)),
            coordinator_url: self.coordinator_url,
            coordinator_grpc_url: self.local_cluster_grpc,
            state_ttl: self.state_ttl,
            memory_streams: Arc::new(RwLock::new(HashMap::new())),
            udf_registry,
            local_cluster,
            runtime,
            registered_parquet: Arc::new(RwLock::new(HashMap::new())),
            stream_jobs: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

/// User-facing Krishiv session.
#[derive(Clone)]
pub struct Session {
    mode: ExecutionMode,
    sql_engine: SqlEngine,
    policy_engine: Option<PolicyEnforcingSqlEngine>,
    jobs: Arc<Mutex<LocalJobRegistry>>,
    next_job_id: Arc<AtomicU64>,
    pub(crate) coordinator_url: Option<String>,
    coordinator_grpc_url: Option<String>,
    state_ttl: Option<StateTtlConfig>,
    memory_streams: Arc<RwLock<HashMap<String, Vec<RecordBatch>>>>,
    udf_registry: Arc<RwLock<UdfRegistry>>,
    local_cluster: Arc<InProcessCluster>,
    runtime: Arc<dyn ExecutionRuntime>,
    registered_parquet: Arc<RwLock<HashMap<String, PathBuf>>>,
    stream_jobs: Arc<RwLock<HashMap<String, LocalWindowExecutionSpec>>>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("mode", &self.mode)
            .field("sql_engine", &self.sql_engine)
            .field(
                "policy_engine",
                &self
                    .policy_engine
                    .as_ref()
                    .map(|_| "<PolicyEnforcingSqlEngine>"),
            )
            .finish_non_exhaustive()
    }
}

impl Session {
    /// Start building a session.
    pub fn builder() -> SessionBuilder {
        SessionBuilder::new()
    }

    /// Current execution mode.
    pub fn mode(&self) -> ExecutionMode {
        self.mode
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

    /// Returns an error if the session was built for a deployment mode whose
    /// routing does not match the runtime — for example a Distributed session
    /// whose runtime is silently using the in-process fallback (B2 guard).
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
        let plan = krishiv_plan::PhysicalPlan::new(
            format!("stream:continuous:{name}"),
            ExecutionKind::Streaming,
        );
        self.runtime
            .accept_plan(&plan)
            .map_err(KrishivError::from)?;
        self.runtime
            .register_continuous_stream(&name, &spec)
            .map_err(KrishivError::from)?;
        self.stream_jobs
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.clone(), spec);
        Ok(name)
    }

    /// Push input batches to a continuous streaming job.
    pub fn push_stream_job_input(&self, job_id: &str, batches: Vec<RecordBatch>) -> Result<()> {
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
        self.memory_streams
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.into(), batches);
        Ok(())
    }

    /// Resolve batches previously registered with [`register_memory_stream`].
    pub fn memory_stream_batches(&self, name: &str) -> Option<Vec<RecordBatch>> {
        self.memory_streams
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
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
    pub fn register_scalar_udf(&self, udf: Arc<dyn ScalarUdf>) {
        self.udf_registry
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .register_scalar(udf);
        let _ = block_on(self.sql_engine.sync_scalar_udfs());
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
        self.registered_parquet
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(table_name, path);
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
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(table_name, path);
        Ok(())
    }

    fn dataframe_from_sql(&self, sql_dataframe: krishiv_sql::SqlDataFrame) -> DataFrame {
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

    /// Create a DataFrame from a SQL query.
    pub fn sql(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        block_on(self.sql_async(query))
    }

    /// Asynchronously create a DataFrame from a SQL query.
    pub async fn sql_async(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        if self.policy_engine.is_some() {
            return Err(KrishivError::AccessDenied {
                reason: "session has a policy engine configured; use sql_as() to execute SQL with an authenticated principal".into(),
            });
        }
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
    pub async fn execute_local_async(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        if self.policy_engine.is_some() {
            return Err(KrishivError::AccessDenied {
                reason:
                    "session has a policy engine configured; use sql_as() for authorized execution"
                        .into(),
            });
        }
        let sql_dataframe = self.sql_engine.sql(query.as_ref()).await?;
        Ok(self.dataframe_from_sql(sql_dataframe))
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
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(table, path)| BatchTableRegistration::new(table.clone(), path.clone()))
            .collect::<Vec<_>>();
        let batches = runtime_collect_batch_sql(Arc::clone(&self.runtime), query, &tables).await?;
        Ok(DataFrame::from_batches(
            self.mode,
            batches,
            self.jobs.clone(),
            self.next_job_id.clone(),
            self.runtime.clone(),
            self.registered_parquet.clone(),
        ))
    }

    /// Execute SQL authenticated as the principal identified by `api_key`.
    ///
    /// Applies the configured [`PolicyHook`]: denies access to prohibited tables
    /// and masks columns per the masking rules before returning results.
    /// Returns [`KrishivError::AccessDenied`] if the session has no policy engine or
    /// if authentication fails.
    pub async fn sql_as(&self, api_key: &str, query: impl AsRef<str>) -> Result<DataFrame> {
        let engine = self
            .policy_engine
            .as_ref()
            .ok_or_else(|| KrishivError::AccessDenied {
                reason: "session was not built with an AuthProvider and PolicyHook".into(),
            })?;
        let principal = engine
            .authenticate(api_key)
            .map_err(|e| KrishivError::AccessDenied {
                reason: e.to_string(),
            })?;
        let query_str = query.as_ref();
        let effective_sql = engine
            .prepare_authorized_query(&principal, query_str)
            .map_err(KrishivError::from)?;
        let tables = self
            .registered_parquet
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(table, path)| BatchTableRegistration::new(table.clone(), path.clone()))
            .collect::<Vec<_>>();
        let batches =
            runtime_collect_batch_sql(Arc::clone(&self.runtime), &effective_sql, &tables).await?;
        let masked = engine
            .mask_result_batches(&principal, query_str, batches)
            .map_err(KrishivError::from)?;
        Ok(DataFrame::from_batches(
            self.mode,
            masked,
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

    /// Read a Delta Lake table directory (R18).
    pub async fn read_delta_async(
        &self,
        path: impl AsRef<str>,
        version: Option<i64>,
    ) -> Result<DataFrame> {
        let sql_dataframe = self.sql_engine.read_delta(path, version).await?;
        Ok(self.dataframe_from_sql(sql_dataframe))
    }

    /// Read a Hudi table directory (R18).
    pub async fn read_hudi_async(
        &self,
        path: impl AsRef<str>,
        query_type: krishiv_lakehouse::HudiQueryType,
        begin_instant: Option<&str>,
    ) -> Result<DataFrame> {
        let sql_dataframe = self
            .sql_engine
            .read_hudi(path, query_type, begin_instant)
            .await?;
        Ok(self.dataframe_from_sql(sql_dataframe))
    }

    /// Asynchronously create a DataFrame by reading a local Parquet path directly.
    pub async fn read_parquet_async(&self, path: impl AsRef<Path>) -> Result<DataFrame> {
        let path = path.as_ref();
        let table = parquet_scan_table_name(path);
        self.register_parquet_async(&table, path).await?;
        self.sql_async(format!("SELECT * FROM {table}")).await
    }

    /// Create a bounded local memory stream.
    pub fn memory_stream(&self, name: impl Into<String>, batches: Vec<StreamBatch>) -> Stream {
        let name = name.into();
        let record_batches: Vec<RecordBatch> = batches.iter().map(|b| b.batch().clone()).collect();
        self.register_memory_stream(name.clone(), record_batches)
            .expect("memory stream registration");
        Stream::for_session(
            name,
            StreamMode::Bounded,
            batches,
            self.mode,
            self.coordinator_url.clone(),
            self.state_ttl.map(|c| c.ttl_ms()),
            self.runtime.clone(),
        )
    }

    /// Create an unbounded local memory stream placeholder.
    pub fn unbounded_memory_stream(&self, name: impl Into<String>) -> Stream {
        Stream::for_session(
            name,
            StreamMode::Unbounded,
            Vec::new(),
            self.mode,
            self.coordinator_url.clone(),
            self.state_ttl.map(|c| c.ttl_ms()),
            self.runtime.clone(),
        )
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
) -> Result<Vec<RecordBatch>> {
    let query = query.to_owned();
    let tables = tables.to_vec();
    tokio::task::spawn_blocking(move || runtime.collect_batch_sql(&query, &tables))
        .await
        .map_err(|e| KrishivError::Runtime {
            message: format!("runtime collect task failed: {e}"),
        })?
        .map_err(KrishivError::from)
}
