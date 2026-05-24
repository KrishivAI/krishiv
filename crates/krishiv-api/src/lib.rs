#![forbid(unsafe_code)]

//! Public Rust API for Krishiv R1.
//!
//! This crate owns the long-term user-facing Rust API. DataFusion is used under
//! the hood through `krishiv-sql`, while Arrow record batches are exposed as the
//! public data interchange shape.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use krishiv_async_util::block_on;
use krishiv_governance::{AuthProvider, PolicyHook};
use krishiv_plan::{ExecutionKind, LogicalPlan, PhysicalPlan};
use krishiv_runtime::{
    build_execution_runtime, BatchTableRegistration, ExecutionRuntime, InProcessCluster, JobId,
    JobState, RuntimeMode,
};
use krishiv_sql::{SqlDataFrame, SqlEngine};
use krishiv_sql_policy::PolicyEnforcingSqlEngine;

pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub use arrow::record_batch::RecordBatch;
pub use krishiv_plan::{LogicalPlan as KrishivLogicalPlan, PhysicalPlan as KrishivPhysicalPlan};
pub use krishiv_exec::{AggExpr, AggFunction};
pub use krishiv_runtime::{
    ClusterEndpoints, InProcessStreamingRuntime, LocalWindowExecutionSpec, LocalWindowKind,
    execute_windowed_stream, is_streaming_plan,
};
pub use krishiv_runtime::{JobStatus, LocalJobRegistry};
pub use krishiv_state::TtlConfig;
pub use krishiv_udf::{ScalarUdf, UdfError, UdfRegistry};

/// API result alias.
pub type Result<T> = std::result::Result<T, KrishivError>;

/// Public API errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KrishivError {
    /// A requested capability is not available in the current release.
    Unsupported { feature: String },
    /// User-provided configuration is invalid.
    InvalidConfig { message: String },
    /// Runtime error surfaced through the public API.
    Runtime { message: String },
    /// Access denied by auth or policy.
    AccessDenied { reason: String },
}

impl KrishivError {
    /// Create an unsupported-feature error.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported {
            feature: feature.into(),
        }
    }
}

impl fmt::Display for KrishivError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { feature } => write!(f, "unsupported Krishiv feature: {feature}"),
            Self::InvalidConfig { message } => write!(f, "invalid Krishiv config: {message}"),
            Self::Runtime { message } => write!(f, "Krishiv runtime error: {message}"),
            Self::AccessDenied { reason } => write!(f, "access denied: {reason}"),
        }
    }
}

impl Error for KrishivError {}

impl From<krishiv_runtime::RuntimeError> for KrishivError {
    fn from(value: krishiv_runtime::RuntimeError) -> Self {
        Self::Runtime {
            message: value.to_string(),
        }
    }
}

impl From<krishiv_sql::SqlError> for KrishivError {
    fn from(value: krishiv_sql::SqlError) -> Self {
        match value {
            krishiv_sql::SqlError::AccessDenied { reason } => Self::AccessDenied { reason },
            other => Self::Runtime {
                message: other.to_string(),
            },
        }
    }
}

/// Execution mode selected for a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// In-process execution for embedding Krishiv in a Rust application.
    Embedded,
    /// Single-node execution through the local Krishiv runtime.
    SingleNode,
    /// Reserved for the R2 Kubernetes/distributed runtime.
    Distributed,
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Embedded => f.write_str("embedded"),
            Self::SingleNode => f.write_str("single-node"),
            Self::Distributed => f.write_str("distributed"),
        }
    }
}

/// Query result wrapper around Arrow record batches.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    batches: Vec<RecordBatch>,
}

impl QueryResult {
    /// Create a query result from Arrow batches.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    /// Result batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Total row count across all batches.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Format the result as an ASCII table for CLI and tests.
    pub fn pretty(&self) -> Result<String> {
        krishiv_sql::pretty_batches(&self.batches).map_err(Into::into)
    }
}

/// Stream batch wrapper.
#[derive(Debug, Clone)]
pub struct StreamBatch {
    sequence: u64,
    batch: RecordBatch,
}

impl StreamBatch {
    /// Create a stream batch.
    pub fn new(sequence: u64, batch: RecordBatch) -> Self {
        Self { sequence, batch }
    }

    /// Sequence number in the local stream.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Record batch payload.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }
}

/// R1 local stream mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMode {
    /// Bounded stream backed by known in-memory batches.
    Bounded,
    /// Unbounded stream placeholder for future local streaming tests.
    Unbounded,
}

impl fmt::Display for StreamMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bounded => f.write_str("bounded"),
            Self::Unbounded => f.write_str("unbounded"),
        }
    }
}

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
        }
    }
}

fn remote_execution_from_env() -> bool {
    std::env::var("KRISHIV_REMOTE_EXEC")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn shared_embedded_runtime() -> Arc<dyn ExecutionRuntime> {
    static RUNTIME: OnceLock<Arc<dyn ExecutionRuntime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            let cluster = Arc::new(
                InProcessCluster::new().expect("shared embedded in-process cluster"),
            );
            build_execution_runtime(RuntimeMode::Embedded, cluster, None, false)
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
    /// endpoint instead of the session-embedded in-process cluster.
    #[must_use]
    pub fn with_remote_execution(mut self, enabled: bool) -> Self {
        self.remote_execution = enabled;
        self
    }

    /// Attach streaming operator state TTL (wired to `krishiv-state` backends).
    #[must_use]
    pub fn with_state_ttl(mut self, ttl: StateTtlConfig) -> Self {
        self.state_ttl = Some(ttl);
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
        let local_cluster = Arc::new(
            InProcessCluster::new().map_err(|e| KrishivError::Runtime {
                message: e.to_string(),
            })?,
        );
        let runtime = build_execution_runtime(
            execution_mode_to_runtime_mode(self.mode),
            Arc::clone(&local_cluster),
            self.coordinator_url.clone(),
            self.remote_execution,
        );
        Ok(Session {
            mode: self.mode,
            sql_engine,
            policy_engine,
            jobs: Arc::new(Mutex::new(LocalJobRegistry::default())),
            next_job_id: Arc::new(AtomicU64::new(1)),
            coordinator_url: self.coordinator_url,
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
    coordinator_url: Option<String>,
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
    pub fn state_ttl_config(&self) -> Option<TtlConfig> {
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

    /// Submit a continuous streaming job (unbounded sources). Returns a handle id.
    pub fn submit_stream_job(
        &self,
        name: impl Into<String>,
        spec: LocalWindowExecutionSpec,
    ) -> Result<String> {
        let name = name.into();
        let plan = PhysicalPlan::new(format!("stream:continuous:{name}"), ExecutionKind::Streaming);
        self.runtime.accept_plan(&plan).map_err(KrishivError::from)?;
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
    pub fn push_stream_job_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
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

    fn dataframe_from_sql(&self, sql_dataframe: SqlDataFrame) -> DataFrame {
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
        let batches = runtime_collect_batch_sql(
            Arc::clone(&self.runtime),
            &effective_sql,
            &tables,
        )
        .await?;
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
        let record_batches: Vec<RecordBatch> =
            batches.iter().map(|b| b.batch().clone()).collect();
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

/// DataFrame API backed by DataFusion for R1 local execution.
#[derive(Clone)]
pub struct DataFrame {
    logical_plan: LogicalPlan,
    sql_dataframe: Option<SqlDataFrame>,
    sql_query: Option<String>,
    /// Pre-collected batches — set when the DataFrame is constructed from
    /// already-executed results (e.g. [`Session::sql_as`]).
    pre_collected: Option<Vec<RecordBatch>>,
    mode: ExecutionMode,
    jobs: Arc<Mutex<LocalJobRegistry>>,
    next_job_id: Arc<AtomicU64>,
    coordinator_url: Option<String>,
    runtime: Arc<dyn ExecutionRuntime>,
    registered_parquet: Arc<RwLock<HashMap<String, PathBuf>>>,
}

impl fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFrame")
            .field("logical_plan", &self.logical_plan)
            .field("mode", &self.mode)
            .field("has_sql_query", &self.sql_query.is_some())
            .field("pre_collected", &self.pre_collected.as_ref().map(|b| b.len()))
            .finish_non_exhaustive()
    }
}

impl DataFrame {
    /// Create a logical-only DataFrame.
    pub fn new(logical_plan: LogicalPlan) -> Self {
        Self {
            logical_plan,
            sql_dataframe: None,
            sql_query: None,
            pre_collected: None,
            mode: ExecutionMode::Embedded,
            jobs: Arc::new(Mutex::new(LocalJobRegistry::default())),
            next_job_id: Arc::new(AtomicU64::new(1)),
            coordinator_url: None,
            runtime: shared_embedded_runtime(),
            registered_parquet: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn from_sql_dataframe(
        mode: ExecutionMode,
        sql_dataframe: SqlDataFrame,
        sql_query: Option<String>,
        jobs: Arc<Mutex<LocalJobRegistry>>,
        next_job_id: Arc<AtomicU64>,
        coordinator_url: Option<String>,
        runtime: Arc<dyn ExecutionRuntime>,
        registered_parquet: Arc<RwLock<HashMap<String, PathBuf>>>,
    ) -> Self {
        let logical_plan = sql_dataframe.krishiv_logical_plan();
        Self {
            logical_plan,
            sql_dataframe: Some(sql_dataframe),
            sql_query,
            pre_collected: None,
            mode,
            jobs,
            next_job_id,
            coordinator_url,
            runtime,
            registered_parquet,
        }
    }

    /// Construct a [`DataFrame`] from a pre-collected list of record batches.
    ///
    /// Used by [`Session::sql_as`] to wrap the results of a policy-enforced query.
    pub(crate) fn from_batches(
        mode: ExecutionMode,
        batches: Vec<RecordBatch>,
        jobs: Arc<Mutex<LocalJobRegistry>>,
        next_job_id: Arc<AtomicU64>,
        runtime: Arc<dyn ExecutionRuntime>,
        registered_parquet: Arc<RwLock<HashMap<String, PathBuf>>>,
    ) -> Self {
        let logical_plan = LogicalPlan::new("policy-enforced-query", ExecutionKind::Batch);
        Self {
            logical_plan,
            sql_dataframe: None,
            sql_query: None,
            pre_collected: Some(batches),
            mode,
            jobs,
            next_job_id,
            coordinator_url: None,
            runtime,
            registered_parquet,
        }
    }
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    /// Explain the current plan.
    pub fn explain(&self) -> Result<String> {
        block_on(self.explain_async())
    }

    /// Asynchronously explain the current plan.
    pub async fn explain_async(&self) -> Result<String> {
        if let Some(query) = self.sql_query.as_deref() {
            return self.runtime.explain_sql(query).map_err(KrishivError::from);
        }
        match &self.sql_dataframe {
            Some(dataframe) => dataframe.explain().await.map_err(Into::into),
            None => Ok(self.logical_plan.describe()),
        }
    }

    /// Explain the Krishiv logical wrapper only.
    pub fn explain_logical(&self) -> String {
        match &self.sql_dataframe {
            Some(dataframe) => dataframe.explain_logical(),
            None => self.logical_plan.describe(),
        }
    }

    /// Collect results.
    pub fn collect(&self) -> Result<QueryResult> {
        block_on(self.collect_async())
    }

    /// Asynchronously collect results.
    pub async fn collect_async(&self) -> Result<QueryResult> {
        let job_id = self.start_job("local-dataframe");
        self.update_job(&job_id, "local-dataframe", JobState::Running);

        if let Some(batches) = &self.pre_collected {
            self.update_job(&job_id, "local-dataframe", JobState::Succeeded);
            return Ok(QueryResult::new(batches.clone()));
        }

        let result = if let Some(query) = self.sql_query.as_deref() {
            let tables = self
                .registered_parquet
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .map(|(table, path)| BatchTableRegistration::new(table.clone(), path.clone()))
                .collect::<Vec<_>>();
            runtime_collect_batch_sql(Arc::clone(&self.runtime), query, &tables)
                .await
                .map(QueryResult::new)
        } else {
            self.runtime
                .accept_plan(&PhysicalPlan::new(
                    self.logical_plan.name(),
                    self.logical_plan.kind(),
                ))
                .map_err(KrishivError::from)?;
            match &self.sql_dataframe {
                Some(dataframe) => dataframe
                    .collect()
                    .await
                    .map(QueryResult::new)
                    .map_err(Into::into),
                None => Err(KrishivError::unsupported(
                    "logical-only DataFrame cannot be collected",
                )),
            }
        };

        match &result {
            Ok(_) => self.update_job(&job_id, "local-dataframe", JobState::Succeeded),
            Err(_) => self.update_job(&job_id, "local-dataframe", JobState::Failed),
        }

        result
    }

    fn start_job(&self, name: &str) -> JobId {
        let id = JobId::new(format!(
            "local-{}",
            self.next_job_id.fetch_add(1, Ordering::SeqCst)
        ));
        self.update_job(&id, name, JobState::Pending);
        id
    }

    fn update_job(&self, id: &JobId, name: &str, state: JobState) {
        if let Ok(mut jobs) = self.jobs.lock() {
            jobs.upsert(JobStatus::new(id.clone(), name, state));
        }
    }
}

/// Stream API for R1 local memory streams.
#[derive(Clone)]
pub struct Stream {
    name: String,
    mode: StreamMode,
    execution_mode: ExecutionMode,
    coordinator_url: Option<String>,
    state_ttl_ms: Option<u64>,
    batches: Vec<StreamBatch>,
    runtime: Arc<dyn ExecutionRuntime>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("name", &self.name)
            .field("mode", &self.mode)
            .field("execution_mode", &self.execution_mode)
            .field("batch_count", &self.batches.len())
            .finish_non_exhaustive()
    }
}

impl Stream {
    /// Create a stream with an explicit execution mode.
    ///
    /// Prefer [`Session::memory_stream`] so the stream inherits the session mode.
    pub fn new(
        name: impl Into<String>,
        mode: StreamMode,
        batches: Vec<StreamBatch>,
        execution_mode: ExecutionMode,
    ) -> Self {
        Self::for_session(
            name,
            mode,
            batches,
            execution_mode,
            None,
            None,
            shared_embedded_runtime(),
        )
    }

    fn for_session(
        name: impl Into<String>,
        mode: StreamMode,
        batches: Vec<StreamBatch>,
        execution_mode: ExecutionMode,
        coordinator_url: Option<String>,
        state_ttl_ms: Option<u64>,
        runtime: Arc<dyn ExecutionRuntime>,
    ) -> Self {
        Self {
            name: name.into(),
            mode,
            execution_mode,
            coordinator_url,
            state_ttl_ms,
            batches,
            runtime,
        }
    }

    /// Stream name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stream mode.
    pub fn mode(&self) -> StreamMode {
        self.mode
    }

    /// Whether this stream is bounded.
    pub fn is_bounded(&self) -> bool {
        self.mode == StreamMode::Bounded
    }

    /// Borrow local batches.
    pub fn batches(&self) -> &[StreamBatch] {
        &self.batches
    }

    /// Collect bounded in-memory stream batches.
    pub fn collect_bounded(&self) -> Result<Vec<StreamBatch>> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream collection requires a streaming runtime",
            ));
        }

        let plan = PhysicalPlan::new(&self.name, ExecutionKind::Streaming);
        self.runtime.accept_plan(&plan)?;
        Ok(self.batches.clone())
    }

    /// Execution mode for this stream.
    pub fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode
    }

    /// Map local stream batches.
    pub fn map_batches(&self, mut f: impl FnMut(&StreamBatch) -> StreamBatch) -> Result<Stream> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream mapping requires a streaming runtime",
            ));
        }

        let plan = PhysicalPlan::new(format!("{}:map", self.name), ExecutionKind::Streaming);
        self.runtime.accept_plan(&plan)?;

        Ok(Self::for_session(
            self.name.clone(),
            self.mode,
            self.batches.iter().map(&mut f).collect(),
            self.execution_mode,
            self.coordinator_url.clone(),
            self.state_ttl_ms,
            self.runtime.clone(),
        ))
    }

    /// Filter local stream batches.
    pub fn filter_batches(&self, mut f: impl FnMut(&StreamBatch) -> bool) -> Result<Stream> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream filtering requires a streaming runtime",
            ));
        }

        let plan = PhysicalPlan::new(format!("{}:filter", self.name), ExecutionKind::Streaming);
        self.runtime.accept_plan(&plan)?;

        Ok(Self::for_session(
            self.name.clone(),
            self.mode,
            self.batches
                .iter()
                .filter(|batch| f(batch))
                .cloned()
                .collect(),
            self.execution_mode,
            self.coordinator_url.clone(),
            self.state_ttl_ms,
            self.runtime.clone(),
        ))
    }

    /// Key the stream by `column`, returning a [`KeyedStream`] that supports
    /// event-time windowing and stateful aggregation.
    ///
    /// `key_by` is the entry point for the R5.1 stateful streaming API.
    /// The same key always routes to the same executor task for the job
    /// lifetime (keyed-distribution stability contract).
    pub fn key_by(self, column: impl Into<String>) -> KeyedStream {
        KeyedStream {
            key_column: column.into(),
            event_time_column: None,
            watermark_spec: None,
            multi_source_watermark: None,
            inner: self,
        }
    }
}

// ── Streaming API ─────────────────────────────────────────────────────────────

/// Watermark configuration for event-time streaming.
///
/// A fixed-lag watermark declares that no event with `event_time < max_seen − lag`
/// will ever arrive.  This is the only watermark strategy in R5.1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkSpec {
    lag_ms: u64,
}

impl WatermarkSpec {
    /// Create a fixed-lag watermark with the given allowed lateness in milliseconds.
    pub fn fixed_lag_ms(lag_ms: u64) -> Self {
        Self { lag_ms }
    }

    /// Allowed lateness in milliseconds.
    pub fn lag_ms(&self) -> u64 {
        self.lag_ms
    }
}

/// A stream keyed by a column value.
///
/// Created by [`Stream::key_by`].  Use the builder methods to configure
/// event-time extraction, watermarking, and windowing before submitting to a
/// distributed runtime.
#[derive(Debug, Clone)]
pub struct KeyedStream {
    inner: Stream,
    key_column: String,
    event_time_column: Option<String>,
    watermark_spec: Option<WatermarkSpec>,
    multi_source_watermark: Option<MultiSourceWatermarkSpec>,
}

impl KeyedStream {
    /// Assign event time from `column` (must be `Int64` milliseconds since epoch).
    #[must_use]
    pub fn with_event_time(mut self, column: impl Into<String>) -> Self {
        self.event_time_column = Some(column.into());
        self
    }

    /// Configure the watermark strategy for late-event handling.
    #[must_use]
    pub fn watermark(mut self, spec: WatermarkSpec) -> Self {
        self.watermark_spec = Some(spec);
        self
    }

    /// Configure multi-source watermark reconciliation (R5.2).
    #[must_use]
    pub fn with_multi_source_watermark(mut self, spec: MultiSourceWatermarkSpec) -> Self {
        self.multi_source_watermark = Some(spec);
        self
    }

    /// Multi-source watermark configuration, if set.
    pub fn multi_source_watermark(&self) -> Option<&MultiSourceWatermarkSpec> {
        self.multi_source_watermark.as_ref()
    }

    /// Create a tumbling event-time window of `window_size_ms` milliseconds.
    pub fn tumbling_window(self, window_size_ms: u64) -> WindowedStream {
        WindowedStream {
            keyed: self,
            window_size_ms,
        }
    }

    /// The column used to key the stream.
    pub fn key_column(&self) -> &str {
        &self.key_column
    }

    /// The event-time column, if configured.
    pub fn event_time_column(&self) -> Option<&str> {
        self.event_time_column.as_deref()
    }

    /// The watermark configuration, if set.
    pub fn watermark_spec(&self) -> Option<&WatermarkSpec> {
        self.watermark_spec.as_ref()
    }

    /// The inner stream.
    pub fn inner(&self) -> &Stream {
        &self.inner
    }
}

/// A keyed stream with a tumbling window applied.
///
/// Windowed stream descriptor; call [`WindowedStream::collect`] to execute locally
/// in embedded or single-node mode.
#[derive(Debug, Clone)]
pub struct WindowedStream {
    keyed: KeyedStream,
    window_size_ms: u64,
}

impl WindowedStream {
    /// Key column name.
    pub fn key_column(&self) -> &str {
        self.keyed.key_column()
    }

    /// Event-time column name.
    pub fn event_time_column(&self) -> Option<&str> {
        self.keyed.event_time_column()
    }

    /// Watermark lag in milliseconds (0 if not configured).
    pub fn watermark_lag_ms(&self) -> u64 {
        self.keyed.watermark_spec().map_or(0, WatermarkSpec::lag_ms)
    }

    /// Window size in milliseconds.
    pub fn window_size_ms(&self) -> u64 {
        self.window_size_ms
    }

    /// The underlying keyed stream.
    pub fn keyed_stream(&self) -> &KeyedStream {
        &self.keyed
    }

    /// Execute the tumbling window and collect output batches (embedded / single-node).
    pub fn collect(&self) -> Result<Vec<StreamBatch>> {
        self.collect_with_aggs(LocalWindowExecutionSpec::default_count_agg())
    }

    /// Execute the tumbling window with custom aggregate expressions.
    pub fn collect_with_aggs(&self, agg_exprs: Vec<AggExpr>) -> Result<Vec<StreamBatch>> {
        let spec = build_tumbling_spec(&self.keyed, self.window_size_ms, agg_exprs)?;
        execute_windowed_inner(&self.keyed.inner, spec)
    }
}

fn event_time_column_for_keyed(keyed: &KeyedStream) -> Result<String> {
    keyed.event_time_column.clone().ok_or_else(|| {
        KrishivError::unsupported(
            "windowed stream execution requires with_event_time() before collect",
        )
    })
}

fn apply_multi_source_watermark(
    keyed: &KeyedStream,
    spec: &mut LocalWindowExecutionSpec,
) {
    if let Some(ms) = keyed.multi_source_watermark() {
        spec.source_watermark_lags = ms
            .source_specs()
            .iter()
            .map(|(id, ws)| (id.clone(), ws.lag_ms()))
            .collect();
        spec.source_id_column = Some(String::from("source_id"));
    }
}

fn build_tumbling_spec(
    keyed: &KeyedStream,
    window_size_ms: u64,
    agg_exprs: Vec<AggExpr>,
) -> Result<LocalWindowExecutionSpec> {
    let event_time = event_time_column_for_keyed(keyed)?;
    let lag = keyed.watermark_spec().map(WatermarkSpec::lag_ms).unwrap_or(0);
    let mut spec = LocalWindowExecutionSpec {
        key_column: keyed.key_column.clone(),
        event_time_column: event_time,
        watermark_lag_ms: lag,
        window_kind: LocalWindowKind::Tumbling,
        window_size_ms,
        agg_exprs,
        state_ttl_ms: keyed.inner.state_ttl_ms,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
    };
    apply_multi_source_watermark(keyed, &mut spec);
    Ok(spec)
}

fn execute_windowed_inner(
    stream: &Stream,
    spec: LocalWindowExecutionSpec,
) -> Result<Vec<StreamBatch>> {
    if !stream.is_bounded() {
        return Err(KrishivError::unsupported(
            "unbounded stream window execution requires Session::submit_stream_job",
        ));
    }
    let plan_name = krishiv_runtime::fragment_from_local_spec(&spec);
    stream
        .runtime
        .accept_plan(&PhysicalPlan::new(plan_name, ExecutionKind::Streaming))
        .map_err(KrishivError::from)?;
    let input: Vec<RecordBatch> = stream
        .batches
        .iter()
        .map(|b| b.batch().clone())
        .collect();
    let output = stream
        .runtime
        .collect_bounded_window(stream.name(), input, &spec)
        .map_err(KrishivError::from)?;
    Ok(output
        .into_iter()
        .enumerate()
        .map(|(seq, batch)| StreamBatch::new(seq as u64, batch))
        .collect())
}

// ── R5.2 Streaming API ────────────────────────────────────────────────────────

/// Multi-source watermark configuration (R5.2).
///
/// Each source can have its own fixed-lag watermark.  The effective watermark
/// across all sources is the minimum, so a stalled source blocks all windows.
#[derive(Debug, Clone, Default)]
pub struct MultiSourceWatermarkSpec {
    source_specs: std::collections::HashMap<String, WatermarkSpec>,
}

impl MultiSourceWatermarkSpec {
    /// Create an empty multi-source spec.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a watermark spec for `source_id`.
    #[must_use]
    pub fn source(mut self, source_id: impl Into<String>, spec: WatermarkSpec) -> Self {
        self.source_specs.insert(source_id.into(), spec);
        self
    }

    /// The configured per-source specs.
    pub fn source_specs(&self) -> &std::collections::HashMap<String, WatermarkSpec> {
        &self.source_specs
    }
}

/// State TTL (time-to-live) configuration for streaming operators (R5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateTtlConfig {
    ttl_ms: u64,
}

impl StateTtlConfig {
    /// Create a TTL config with the given duration in milliseconds.
    pub fn new(ttl_ms: u64) -> Self {
        Self { ttl_ms }
    }

    /// TTL duration in milliseconds.
    pub fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    /// Convert to `krishiv-state` [`TtlConfig`] for state backends.
    pub fn to_ttl_config(self) -> TtlConfig {
        TtlConfig::new(self.ttl_ms)
    }
}

/// A keyed stream with a sliding window applied (R5.2).
#[derive(Debug, Clone)]
pub struct SlidingWindowedStream {
    keyed: KeyedStream,
    /// Total window duration in milliseconds.
    window_size_ms: u64,
    /// Slide step in milliseconds.
    slide_ms: u64,
}

impl SlidingWindowedStream {
    /// Key column name.
    pub fn key_column(&self) -> &str {
        self.keyed.key_column()
    }

    /// Event-time column name.
    pub fn event_time_column(&self) -> Option<&str> {
        self.keyed.event_time_column()
    }

    /// Watermark lag in milliseconds.
    pub fn watermark_lag_ms(&self) -> u64 {
        self.keyed.watermark_spec().map_or(0, WatermarkSpec::lag_ms)
    }

    /// Total window size in milliseconds.
    pub fn window_size_ms(&self) -> u64 {
        self.window_size_ms
    }

    /// Slide step in milliseconds.
    pub fn slide_ms(&self) -> u64 {
        self.slide_ms
    }
}

/// A keyed stream with a session window applied (R5.2).
#[derive(Debug, Clone)]
pub struct SessionWindowedStream {
    keyed: KeyedStream,
    /// Inactivity gap that closes a session in milliseconds.
    session_gap_ms: u64,
}

impl SessionWindowedStream {
    /// Key column name.
    pub fn key_column(&self) -> &str {
        self.keyed.key_column()
    }

    /// Event-time column name.
    pub fn event_time_column(&self) -> Option<&str> {
        self.keyed.event_time_column()
    }

    /// Inactivity gap in milliseconds.
    pub fn session_gap_ms(&self) -> u64 {
        self.session_gap_ms
    }

    /// Execute the session window and collect output batches.
    pub fn collect(&self) -> Result<Vec<StreamBatch>> {
        self.collect_with_aggs(LocalWindowExecutionSpec::default_count_agg())
    }

    /// Execute with custom aggregates.
    pub fn collect_with_aggs(&self, agg_exprs: Vec<AggExpr>) -> Result<Vec<StreamBatch>> {
        let event_time = event_time_column_for_keyed(&self.keyed)?;
        let lag = self
            .keyed
            .watermark_spec()
            .map(WatermarkSpec::lag_ms)
            .unwrap_or(0);
        let mut spec = LocalWindowExecutionSpec {
            key_column: self.keyed.key_column.clone(),
            event_time_column: event_time,
            watermark_lag_ms: lag,
            window_kind: LocalWindowKind::Session {
                gap_ms: self.session_gap_ms,
            },
            window_size_ms: self.session_gap_ms,
            agg_exprs,
            state_ttl_ms: self.keyed.inner.state_ttl_ms,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };
        apply_multi_source_watermark(&self.keyed, &mut spec);
        execute_windowed_inner(&self.keyed.inner, spec)
    }
}

impl SlidingWindowedStream {
    /// Execute the sliding window and collect output batches.
    pub fn collect(&self) -> Result<Vec<StreamBatch>> {
        self.collect_with_aggs(LocalWindowExecutionSpec::default_count_agg())
    }

    /// Execute with custom aggregates.
    pub fn collect_with_aggs(&self, agg_exprs: Vec<AggExpr>) -> Result<Vec<StreamBatch>> {
        let event_time = event_time_column_for_keyed(&self.keyed)?;
        let lag = self
            .keyed
            .watermark_spec()
            .map(WatermarkSpec::lag_ms)
            .unwrap_or(0);
        let mut spec = LocalWindowExecutionSpec {
            key_column: self.keyed.key_column.clone(),
            event_time_column: event_time,
            watermark_lag_ms: lag,
            window_kind: LocalWindowKind::Sliding {
                slide_ms: self.slide_ms,
            },
            window_size_ms: self.window_size_ms,
            agg_exprs,
            state_ttl_ms: self.keyed.inner.state_ttl_ms,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };
        apply_multi_source_watermark(&self.keyed, &mut spec);
        execute_windowed_inner(&self.keyed.inner, spec)
    }
}

impl KeyedStream {
    /// Create a sliding event-time window of total size `window_size_ms` advancing
    /// by `slide_ms` (R5.2).
    pub fn sliding_window(self, window_size_ms: u64, slide_ms: u64) -> SlidingWindowedStream {
        SlidingWindowedStream {
            keyed: self,
            window_size_ms,
            slide_ms,
        }
    }

    /// Create a session window that closes after `session_gap_ms` of inactivity (R5.2).
    pub fn session_window(self, session_gap_ms: u64) -> SessionWindowedStream {
        SessionWindowedStream {
            keyed: self,
            session_gap_ms,
        }
    }
}

fn parquet_scan_table_name(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("scan");
    let sanitized: String = stem
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("_krishiv_parquet_{sanitized}")
}

async fn runtime_collect_batch_sql(
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

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    use super::{
        DataType, ExecutionMode, Field, KrishivError, LocalWindowKind, RecordBatch, Schema,
        Session, SessionBuilder, StreamBatch,
    };
    use std::collections::HashMap;

    // ── P0.3 regression: block_on must reuse the current runtime ────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn block_on_does_not_panic_inside_tokio_runtime() {
        // P0.3: Calling block_on (the sync wrapper) from within an existing
        // multi-thread Tokio runtime must NOT panic.  The previous implementation
        // called `Runtime::new().block_on(f)` which panics with "Cannot start a
        // runtime from within a runtime".  The fix uses `block_in_place(||
        // handle.block_on(f))` which is safe on multi-thread runtimes.  We need
        // `flavor = "multi_thread"` here because `block_in_place` is not
        // supported on the default current_thread runtime used by `#[tokio::test]`.
        let session = Session::builder()
            .build()
            .expect("SessionBuilder must succeed");
        // sql() calls block_on internally; this must not panic.
        let result = session.sql("SELECT 1 AS v");
        assert!(
            result.is_ok(),
            "block_on panicked inside Tokio runtime: {result:?}"
        );
    }

    // ── P0.1: SessionBuilder::build uses a single shared SqlEngine ───────────

    #[tokio::test]
    async fn session_builder_policy_engine_shares_sql_engine_context() {
        // P0.1: When a session is built with auth + policy, the PolicyEnforcingSqlEngine
        // must share the same underlying SessionContext as sql_engine so that
        // tables registered on the session are visible to policy-enforced queries.
        let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
            "key-ptr".into(),
            "alice".into(),
            Role::Reader,
        )]));
        let session = SessionBuilder::new()
            .with_auth(auth)
            .with_policy(Arc::new(AllowAllPolicy))
            .build()
            .unwrap();

        // Register a table via the sql_engine path.
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);
        session
            .register_parquet_async("people", &parquet_path)
            .await
            .unwrap();

        // The policy engine must see the same table (shared context).
        let df = session
            .sql_as("key-ptr", "SELECT count(*) AS n FROM people")
            .await
            .expect("policy engine should see tables registered on the shared sql_engine");
        let result = df.collect_async().await.unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn session_builder_defaults_to_embedded() {
        let session = match Session::builder().build() {
            Ok(session) => session,
            Err(error) => panic!("unexpected API error: {error}"),
        };

        assert_eq!(session.mode(), ExecutionMode::Embedded);
    }

    #[test]
    fn session_builder_accepts_single_node() {
        let session = match Session::builder()
            .with_execution_mode(ExecutionMode::SingleNode)
            .build()
        {
            Ok(session) => session,
            Err(error) => panic!("unexpected API error: {error}"),
        };

        assert_eq!(session.mode(), ExecutionMode::SingleNode);
    }

    #[test]
    fn sql_collects_literal_query() {
        let session = match Session::builder().build() {
            Ok(session) => session,
            Err(error) => panic!("unexpected API error: {error}"),
        };

        let dataframe = match session.sql("select 1 as value") {
            Ok(dataframe) => dataframe,
            Err(error) => panic!("unexpected API error: {error}"),
        };
        let result = match dataframe.collect() {
            Ok(result) => result,
            Err(error) => panic!("unexpected collect error: {error}"),
        };

        assert_eq!(result.row_count(), 1);
        assert!(result.pretty().unwrap_or_default().contains("value"));
        assert_eq!(session.jobs().len(), 1);
        assert_eq!(
            session.jobs()[0].state(),
            krishiv_runtime::JobState::Succeeded
        );
    }

    #[test]
    fn embedded_and_single_node_sql_over_parquet_match() {
        let temp = match tempdir() {
            Ok(temp) => temp,
            Err(error) => panic!("unexpected tempdir error: {error}"),
        };
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);

        let embedded = Session::builder()
            .with_execution_mode(ExecutionMode::Embedded)
            .build()
            .unwrap_or_else(|error| panic!("unexpected API error: {error}"));
        let single_node = Session::builder()
            .with_execution_mode(ExecutionMode::SingleNode)
            .build()
            .unwrap_or_else(|error| panic!("unexpected API error: {error}"));

        embedded
            .register_parquet("people", &parquet_path)
            .unwrap_or_else(|error| panic!("unexpected register error: {error}"));
        single_node
            .register_parquet("people", &parquet_path)
            .unwrap_or_else(|error| panic!("unexpected register error: {error}"));

        let query = "select city, count(*) as count from people group by city order by city";
        let embedded_pretty = embedded
            .sql(query)
            .and_then(|dataframe| dataframe.collect())
            .and_then(|result| result.pretty())
            .unwrap_or_else(|error| panic!("unexpected embedded query error: {error}"));
        let single_node_pretty = single_node
            .sql(query)
            .and_then(|dataframe| dataframe.collect())
            .and_then(|result| result.pretty())
            .unwrap_or_else(|error| panic!("unexpected single-node query error: {error}"));

        assert_eq!(embedded_pretty, single_node_pretty);
        assert!(embedded_pretty.contains("London"));
        assert!(embedded_pretty.contains("Paris"));
    }

    #[test]
    fn read_parquet_collects_rows() {
        let temp = tempdir().unwrap_or_else(|error| panic!("unexpected tempdir error: {error}"));
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);
        let session = Session::builder()
            .build()
            .unwrap_or_else(|error| panic!("unexpected API error: {error}"));

        let result = session
            .read_parquet(&parquet_path)
            .and_then(|dataframe| dataframe.collect())
            .unwrap_or_else(|error| panic!("unexpected parquet read error: {error}"));

        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn memory_stream_supports_bounded_map_filter_collect() {
        let session = match Session::builder().build() {
            Ok(session) => session,
            Err(error) => panic!("unexpected API error: {error}"),
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap_or_else(|error| panic!("unexpected record batch error: {error}"));
        let stream = session.memory_stream("numbers", vec![StreamBatch::new(0, batch)]);
        let mapped = stream
            .map_batches(|batch| batch.clone())
            .unwrap_or_else(|error| panic!("unexpected stream map error: {error}"));
        let filtered = mapped
            .filter_batches(|batch| batch.sequence() == 0)
            .unwrap_or_else(|error| panic!("unexpected stream filter error: {error}"));

        assert_eq!(filtered.name(), "numbers");
        assert_eq!(filtered.collect_bounded().unwrap_or_default().len(), 1);
    }

    #[test]
    fn unbounded_memory_stream_rejects_collect() {
        let session = Session::builder()
            .build()
            .unwrap_or_else(|error| panic!("unexpected API error: {error}"));
        let stream = session.unbounded_memory_stream("events");

        assert!(!stream.is_bounded());
        assert!(stream.collect_bounded().is_err());
    }

    // ── Streaming API tests ───────────────────────────────────────────────────

    #[allow(unused_imports)]
    use super::Stream;
    use super::{KeyedStream, WatermarkSpec, WindowedStream};

    #[test]
    fn key_by_returns_keyed_stream_with_correct_column() {
        let session = Session::builder().build().unwrap();
        let stream = session.memory_stream("events", vec![]);
        let keyed: KeyedStream = stream.key_by("user_id");
        assert_eq!(keyed.key_column(), "user_id");
        assert!(keyed.event_time_column().is_none());
        assert!(keyed.watermark_spec().is_none());
    }

    #[test]
    fn keyed_stream_builder_chain() {
        let session = Session::builder().build().unwrap();
        let stream = session.memory_stream("events", vec![]);
        let keyed = stream
            .key_by("user_id")
            .with_event_time("event_ts")
            .watermark(WatermarkSpec::fixed_lag_ms(5000));

        assert_eq!(keyed.key_column(), "user_id");
        assert_eq!(keyed.event_time_column(), Some("event_ts"));
        assert_eq!(keyed.watermark_spec().unwrap().lag_ms(), 5000);
    }

    #[test]
    fn tumbling_window_carries_correct_config() {
        let session = Session::builder().build().unwrap();
        let stream = session.memory_stream("events", vec![]);
        let windowed: WindowedStream = stream
            .key_by("user_id")
            .with_event_time("ts")
            .watermark(WatermarkSpec::fixed_lag_ms(1000))
            .tumbling_window(60_000);

        assert_eq!(windowed.key_column(), "user_id");
        assert_eq!(windowed.event_time_column(), Some("ts"));
        assert_eq!(windowed.watermark_lag_ms(), 1000);
        assert_eq!(windowed.window_size_ms(), 60_000);
    }

    #[test]
    fn tumbling_window_collect_executes_in_embedded_mode() {
        let session = Session::builder().build().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 5_000, 2_000])) as _,
            ],
        )
        .unwrap();
        let stream = session.memory_stream("events", vec![StreamBatch::new(0, batch)]);
        let out = stream
            .key_by("user_id")
            .with_event_time("ts")
            .watermark(WatermarkSpec::fixed_lag_ms(0))
            .tumbling_window(10_000)
            .collect()
            .expect("window collect");
        assert!(!out.is_empty());
    }

    #[test]
    fn sliding_window_collect_via_unified_runtime() {
        let session = Session::builder().build().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 5_000, 2_000])) as _,
            ],
        )
        .unwrap();
        let stream = session.memory_stream("events", vec![StreamBatch::new(0, batch)]);
        let out = stream
            .key_by("user_id")
            .with_event_time("ts")
            .sliding_window(10_000, 5_000)
            .collect()
            .expect("sliding collect");
        assert!(!out.is_empty());
    }

    #[test]
    fn session_window_collect_via_unified_runtime() {
        let session = Session::builder().build().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 8_000])) as _,
            ],
        )
        .unwrap();
        let stream = session.memory_stream("events", vec![StreamBatch::new(0, batch)]);
        let out = stream
            .key_by("user_id")
            .with_event_time("ts")
            .session_window(5_000)
            .collect()
            .expect("session collect");
        assert!(!out.is_empty());
    }

    #[test]
    fn session_reuses_coordinator_across_window_collects() {
        let session = Session::builder().build().unwrap();
        let ptr_before = session
            .local_cluster()
            .streaming_runtime()
            .coordinator_instance_id();
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_000])) as _,
            ],
        )
        .unwrap();
        for _ in 0..2 {
            let stream = session.memory_stream("events", vec![StreamBatch::new(0, batch.clone())]);
            let _ = stream
                .key_by("user_id")
                .with_event_time("ts")
                .tumbling_window(10_000)
                .collect()
                .expect("collect");
        }
        let ptr_after = session
            .local_cluster()
            .streaming_runtime()
            .coordinator_instance_id();
        assert_eq!(ptr_before, ptr_after);
    }

    #[test]
    fn watermark_spec_lag_ms_roundtrip() {
        let spec = WatermarkSpec::fixed_lag_ms(30_000);
        assert_eq!(spec.lag_ms(), 30_000);
    }

    use super::{
        MultiSourceWatermarkSpec, SessionWindowedStream, SlidingWindowedStream, StateTtlConfig,
    };

    #[test]
    fn multi_source_watermark_spec_roundtrip() {
        let spec = MultiSourceWatermarkSpec::new()
            .source("src-a", WatermarkSpec::fixed_lag_ms(1000))
            .source("src-b", WatermarkSpec::fixed_lag_ms(2000));
        assert_eq!(spec.source_specs().len(), 2);
        assert_eq!(spec.source_specs()["src-a"].lag_ms(), 1000);
        assert_eq!(spec.source_specs()["src-b"].lag_ms(), 2000);
    }

    #[test]
    fn state_ttl_config_roundtrip() {
        let cfg = StateTtlConfig::new(5_000);
        assert_eq!(cfg.ttl_ms(), 5_000);
    }

    #[test]
    fn sliding_window_api_builder() {
        let session = Session::builder().build().unwrap();
        let stream = session.unbounded_memory_stream("events");
        let sliding: SlidingWindowedStream = stream
            .key_by("user_id")
            .with_event_time("ts")
            .watermark(WatermarkSpec::fixed_lag_ms(500))
            .sliding_window(2_000, 500);
        assert_eq!(sliding.key_column(), "user_id");
        assert_eq!(sliding.event_time_column(), Some("ts"));
        assert_eq!(sliding.watermark_lag_ms(), 500);
        assert_eq!(sliding.window_size_ms(), 2_000);
        assert_eq!(sliding.slide_ms(), 500);
    }

    #[test]
    fn session_window_api_builder() {
        let session = Session::builder().build().unwrap();
        let stream = session.unbounded_memory_stream("events");
        let sess: SessionWindowedStream = stream
            .key_by("device_id")
            .with_event_time("ts")
            .session_window(30_000);
        assert_eq!(sess.key_column(), "device_id");
        assert_eq!(sess.event_time_column(), Some("ts"));
        assert_eq!(sess.session_gap_ms(), 30_000);
    }

    fn write_people_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("city", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["London", "Paris", "London"])),
            ],
        )
        .unwrap_or_else(|error| panic!("unexpected record batch error: {error}"));
        let file = File::create(path)
            .unwrap_or_else(|error| panic!("unexpected parquet file error: {error}"));
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .unwrap_or_else(|error| panic!("unexpected parquet writer error: {error}"));
        writer
            .write(&batch)
            .unwrap_or_else(|error| panic!("unexpected parquet write error: {error}"));
        writer
            .close()
            .unwrap_or_else(|error| panic!("unexpected parquet close error: {error}"));
    }

    // ── sql_as tests ─────────────────────────────────────────────────────────────

    use krishiv_governance::{MaskingRule, PolicyHook, Principal, Role, StaticApiKeyAuthProvider};

    struct AllowAllPolicy;
    impl PolicyHook for AllowAllPolicy {
        fn check_table_access(&self, _p: &Principal, _table: &str) -> bool {
            true
        }
        fn column_masking_rule(
            &self,
            _p: &Principal,
            _table: &str,
            _col: &str,
        ) -> Option<MaskingRule> {
            None
        }
    }

    #[tokio::test]
    async fn session_sql_as_with_valid_key_executes_query() {
        let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
            "key123".into(),
            "alice".into(),
            Role::Reader,
        )]));
        let session = SessionBuilder::new()
            .with_auth(auth)
            .with_policy(Arc::new(AllowAllPolicy))
            .build()
            .unwrap();
        let df = session.sql_as("key123", "SELECT 42 AS v").await.unwrap();
        let result = df.collect_async().await.unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[tokio::test]
    async fn session_sql_as_with_invalid_key_returns_access_denied() {
        let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
            "key123".into(),
            "alice".into(),
            Role::Reader,
        )]));
        let session = SessionBuilder::new()
            .with_auth(auth)
            .with_policy(Arc::new(AllowAllPolicy))
            .build()
            .unwrap();
        let result = session.sql_as("wrong_key", "SELECT 1").await;
        assert!(matches!(result, Err(KrishivError::AccessDenied { .. })));
    }

    #[tokio::test]
    async fn session_without_policy_sql_as_returns_access_denied() {
        let session = SessionBuilder::new().build().unwrap();
        let result = session.sql_as("any_key", "SELECT 1").await;
        assert!(matches!(result, Err(KrishivError::AccessDenied { .. })));
    }

    #[tokio::test]
    async fn session_sql_as_can_read_registered_session_tables() {
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);
        let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
            "key123".into(),
            "alice".into(),
            Role::Reader,
        )]));
        let session = SessionBuilder::new()
            .with_auth(auth)
            .with_policy(Arc::new(AllowAllPolicy))
            .build()
            .unwrap();

        session
            .register_parquet_async("people", &parquet_path)
            .await
            .unwrap();
        let df = session
            .sql_as("key123", "SELECT city FROM people ORDER BY city")
            .await
            .unwrap();
        let result = df.collect_async().await.unwrap();

        assert_eq!(result.row_count(), 3);
    }

    // ── GAP-RT-05: sql() / sql_async() fail-closed when policy engine is set ───

    #[tokio::test(flavor = "multi_thread")]
    async fn session_sql_async_fails_when_policy_configured() {
        let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
            "key-rt05".into(),
            "alice".into(),
            Role::Reader,
        )]));
        let session = SessionBuilder::new()
            .with_auth(auth)
            .with_policy(Arc::new(AllowAllPolicy))
            .build()
            .unwrap();
        let result = session.sql("SELECT 1");
        assert!(
            matches!(result, Err(KrishivError::AccessDenied { .. })),
            "expected AccessDenied but got: {result:?}"
        );
    }

    // ── S6.1: SessionBuilder::with_coordinator ────────────────────────────────

    #[test]
    fn with_coordinator_sets_distributed_mode() {
        let session = Session::builder()
            .with_coordinator("http://coord:50051")
            .build()
            .unwrap();
        assert_eq!(session.mode(), ExecutionMode::Distributed);
    }

    #[test]
    fn session_register_scalar_udf() {
        use std::sync::Arc;

        use krishiv_udf::MultiplyScalarUdf;

        let session = SessionBuilder::new().build().unwrap();
        assert!(session.scalar_udf_names().is_empty());

        let udf = Arc::new(MultiplyScalarUdf::new("double", "x", 2));
        session.register_scalar_udf(udf);
        let names = session.scalar_udf_names();
        assert_eq!(names, vec!["double".to_string()]);

        let registry = session.udf_registry();
        let guard = registry.read().unwrap();
        let loaded = guard
            .get_scalar("double")
            .expect("udf should be registered");
        assert_eq!(loaded.name(), "double");
    }

    #[test]
    fn with_coordinator_stores_url_accessible_via_sql() {
        // Building a distributed session must not fail.
        let session = Session::builder()
            .with_coordinator("http://coord:50051")
            .build()
            .unwrap();
        assert_eq!(
            session.coordinator_url.as_deref(),
            Some("http://coord:50051")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distributed_session_sql_collects_via_local_coordinator() {
        let session = Session::builder()
            .with_coordinator("http://127.0.0.1:50051")
            .build()
            .unwrap();
        let df = session.sql_async("SELECT 3 AS n").await.unwrap();
        let result = df.collect_async().await.unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distributed_window_collect_via_local_cluster() {
        let session = Session::builder()
            .with_local_cluster("http://127.0.0.1:50051")
            .build()
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_000])) as _,
            ],
        )
        .unwrap();
        let stream = session.memory_stream("events", vec![StreamBatch::new(0, batch)]);
        let out = stream
            .key_by("user_id")
            .with_event_time("ts")
            .tumbling_window(10_000)
            .collect()
            .expect("distributed window collect");
        assert!(!out.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distributed_read_parquet_collects_via_coordinator() {
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);
        let session = Session::builder()
            .with_coordinator("http://127.0.0.1:50051")
            .build()
            .unwrap();
        let df = session.read_parquet_async(&parquet_path).await.unwrap();
        let result = df.collect_async().await.unwrap();
        assert_eq!(result.row_count(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn continuous_stream_job_poll_drains_via_coordinator() {
        use krishiv_runtime::LocalWindowExecutionSpec;

        let session = Session::builder().build().unwrap();
        let spec = LocalWindowExecutionSpec {
            key_column: "user_id".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };
        session.submit_stream_job("events", spec).expect("submit");
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_000])) as _,
            ],
        )
        .unwrap();
        session
            .push_stream_job_input("events", vec![batch])
            .expect("push");
        let _ = session.poll_stream_job("events").await.expect("poll");
    }

    #[test]
    fn multi_source_watermark_window_collect_with_source_column() {
        let session = Session::builder().build().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("source_id", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_000])) as _,
                Arc::new(StringArray::from(vec!["src-a"])) as _,
            ],
        )
        .unwrap();
        let stream = session.memory_stream("events", vec![StreamBatch::new(0, batch)]);
        let out = stream
            .key_by("user_id")
            .with_event_time("ts")
            .with_multi_source_watermark(
                MultiSourceWatermarkSpec::new()
                    .source("src-a", WatermarkSpec::fixed_lag_ms(0))
                    .source("src-b", WatermarkSpec::fixed_lag_ms(0)),
            )
            .tumbling_window(10_000)
            .collect()
            .expect("multi-source collect");
        assert!(!out.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_execution_without_fallback_uses_flight_server() {
        use std::net::SocketAddr;

        use krishiv_flight_sql::make_flight_sql_server;
        use tonic::transport::Server;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(make_flight_sql_server())
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });

        let url = format!("http://{addr}");
        let session = Session::builder()
            .with_coordinator(&url)
            .with_remote_execution(true)
            .build()
            .unwrap();
        assert!(session.execution_runtime().uses_remote_execution());
        let df = session.sql_async("SELECT 99 AS n").await.unwrap();
        let result = df.collect_async().await.unwrap();
        assert_eq!(result.row_count(), 1);
        server.abort();
    }
}
