#![forbid(unsafe_code)]

//! Public Rust API for Krishiv R1.
//!
//! This crate owns the long-term user-facing Rust API. DataFusion is used under
//! the hood through `krishiv-sql`, while Arrow record batches are exposed as the
//! public data interchange shape.

use std::error::Error;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use krishiv_async_util::block_on;
use krishiv_governance::{AuthProvider, PolicyHook};
use krishiv_plan::{ExecutionKind, LogicalPlan, PhysicalPlan};
use krishiv_runtime::{
    DistributedBackend, EmbeddedBackend, ExecutionBackend, JobId, JobState, SingleNodeBackend,
};
use krishiv_sql::{SqlDataFrame, SqlEngine};
use krishiv_sql_policy::PolicyEnforcingSqlEngine;

pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub use arrow::record_batch::RecordBatch;
pub use krishiv_plan::{LogicalPlan as KrishivLogicalPlan, PhysicalPlan as KrishivPhysicalPlan};
pub use krishiv_runtime::{JobStatus, LocalJobRegistry};

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
}

impl fmt::Debug for SessionBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionBuilder")
            .field("mode", &self.mode)
            .field("auth", &self.auth.as_ref().map(|_| "<AuthProvider>"))
            .field("policy", &self.policy.as_ref().map(|_| "<PolicyHook>"))
            .field("coordinator_url", &self.coordinator_url)
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
        }
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

    /// Build a session.
    pub fn build(self) -> Result<Session> {
        let sql_engine = SqlEngine::new();
        let policy_engine = match (self.auth, self.policy) {
            (Some(auth), Some(policy)) => Some(PolicyEnforcingSqlEngine::new(
                sql_engine.clone(),
                auth,
                policy,
            )),
            _ => None,
        };
        Ok(Session {
            mode: self.mode,
            sql_engine,
            policy_engine,
            jobs: Arc::new(Mutex::new(LocalJobRegistry::default())),
            next_job_id: Arc::new(AtomicU64::new(1)),
            coordinator_url: self.coordinator_url,
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

    /// Known local jobs.
    pub fn jobs(&self) -> Vec<JobStatus> {
        self.jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .snapshot()
    }

    /// Register a local Parquet path as a SQL table.
    pub fn register_parquet(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        ensure_local_mode(self.mode)?;
        let table_name = table_name.as_ref().to_owned();
        let path = path.as_ref().to_path_buf();
        block_on(async {
            self.sql_engine
                .register_parquet(table_name, path)
                .await
                .map_err(Into::into)
        })
    }

    /// Asynchronously register a local Parquet path as a SQL table.
    pub async fn register_parquet_async(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        ensure_local_mode(self.mode)?;
        self.sql_engine
            .register_parquet(table_name, path)
            .await
            .map_err(Into::into)
    }

    /// Create a DataFrame from a SQL query.
    pub fn sql(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        ensure_local_mode(self.mode)?;
        block_on(self.sql_async(query))
    }

    /// Asynchronously create a DataFrame from a SQL query.
    pub async fn sql_async(&self, query: impl AsRef<str>) -> Result<DataFrame> {
        ensure_local_mode(self.mode)?;
        if self.policy_engine.is_some() {
            return Err(KrishivError::AccessDenied {
                reason: "session has a policy engine configured; use sql_as() to execute SQL with an authenticated principal".into(),
            });
        }
        let query = query.as_ref().to_owned();
        let sql_dataframe = self.sql_engine.sql(&query).await?;
        Ok(DataFrame::from_sql_dataframe(
            self.mode,
            sql_dataframe,
            self.jobs.clone(),
            self.next_job_id.clone(),
            self.coordinator_url.clone(),
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
        let batches = engine
            .execute_as(&principal, query_str)
            .await
            .map_err(KrishivError::from)?;
        Ok(DataFrame::from_batches(
            self.mode,
            batches,
            self.jobs.clone(),
            self.next_job_id.clone(),
        ))
    }

    /// Create a DataFrame by reading a local Parquet path directly.
    pub fn read_parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame> {
        ensure_local_mode(self.mode)?;
        let path = path.as_ref().to_path_buf();
        block_on(self.read_parquet_async(path))
    }

    /// Asynchronously create a DataFrame by reading a local Parquet path directly.
    pub async fn read_parquet_async(&self, path: impl AsRef<Path>) -> Result<DataFrame> {
        ensure_local_mode(self.mode)?;
        let sql_dataframe = self.sql_engine.read_parquet(path).await?;
        Ok(DataFrame::from_sql_dataframe(
            self.mode,
            sql_dataframe,
            self.jobs.clone(),
            self.next_job_id.clone(),
            self.coordinator_url.clone(),
        ))
    }

    /// Create a bounded local memory stream.
    pub fn memory_stream(&self, name: impl Into<String>, batches: Vec<StreamBatch>) -> Stream {
        Stream::for_session(name, StreamMode::Bounded, batches, self.mode)
    }

    /// Create an unbounded local memory stream placeholder.
    pub fn unbounded_memory_stream(&self, name: impl Into<String>) -> Stream {
        Stream::for_session(name, StreamMode::Unbounded, Vec::new(), self.mode)
    }
}

/// DataFrame API backed by DataFusion for R1 local execution.
#[derive(Debug, Clone)]
pub struct DataFrame {
    logical_plan: LogicalPlan,
    sql_dataframe: Option<SqlDataFrame>,
    /// Pre-collected batches — set when the DataFrame is constructed from
    /// already-executed results (e.g. [`Session::sql_as`]).
    pre_collected: Option<Vec<RecordBatch>>,
    mode: ExecutionMode,
    jobs: Arc<Mutex<LocalJobRegistry>>,
    next_job_id: Arc<AtomicU64>,
    coordinator_url: Option<String>,
}

impl DataFrame {
    /// Create a logical-only DataFrame.
    pub fn new(logical_plan: LogicalPlan) -> Self {
        Self {
            logical_plan,
            sql_dataframe: None,
            pre_collected: None,
            mode: ExecutionMode::Embedded,
            jobs: Arc::new(Mutex::new(LocalJobRegistry::default())),
            next_job_id: Arc::new(AtomicU64::new(1)),
            coordinator_url: None,
        }
    }

    fn from_sql_dataframe(
        mode: ExecutionMode,
        sql_dataframe: SqlDataFrame,
        jobs: Arc<Mutex<LocalJobRegistry>>,
        next_job_id: Arc<AtomicU64>,
        coordinator_url: Option<String>,
    ) -> Self {
        let logical_plan = sql_dataframe.krishiv_logical_plan();
        Self {
            logical_plan,
            sql_dataframe: Some(sql_dataframe),
            pre_collected: None,
            mode,
            jobs,
            next_job_id,
            coordinator_url,
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
    ) -> Self {
        let logical_plan = LogicalPlan::new("policy-enforced-query", ExecutionKind::Batch);
        Self {
            logical_plan,
            sql_dataframe: None,
            pre_collected: Some(batches),
            mode,
            jobs,
            next_job_id,
            coordinator_url: None,
        }
    }

    /// Borrow the Krishiv logical plan wrapper.
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    /// Explain the current plan.
    pub fn explain(&self) -> Result<String> {
        block_on(self.explain_async())
    }

    /// Asynchronously explain the current plan.
    pub async fn explain_async(&self) -> Result<String> {
        ensure_local_mode(self.mode)?;
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
        ensure_local_mode(self.mode)?;
        let job_id = self.start_job("local-dataframe");
        self.update_job(&job_id, "local-dataframe", JobState::Running);

        // If this DataFrame wraps pre-collected batches (e.g. from sql_as),
        // return them directly without going through DataFusion again.
        if let Some(batches) = &self.pre_collected {
            self.update_job(&job_id, "local-dataframe", JobState::Succeeded);
            return Ok(QueryResult::new(batches.clone()));
        }

        let result = if let Err(error) = accept_plan_with_backend(
            self.mode,
            self.logical_plan.name(),
            self.logical_plan.kind(),
            self.coordinator_url.as_deref(),
        ) {
            Err(error)
        } else {
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
#[derive(Debug, Clone)]
pub struct Stream {
    name: String,
    mode: StreamMode,
    execution_mode: ExecutionMode,
    batches: Vec<StreamBatch>,
}

impl Stream {
    /// Create a stream.
    pub fn new(name: impl Into<String>, mode: StreamMode, batches: Vec<StreamBatch>) -> Self {
        Self::for_session(name, mode, batches, ExecutionMode::Embedded)
    }

    fn for_session(
        name: impl Into<String>,
        mode: StreamMode,
        batches: Vec<StreamBatch>,
        execution_mode: ExecutionMode,
    ) -> Self {
        Self {
            name: name.into(),
            mode,
            execution_mode,
            batches,
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
        ensure_local_mode(self.execution_mode)?;
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream collection requires a streaming runtime",
            ));
        }

        accept_plan_with_backend(
            self.execution_mode,
            &self.name,
            ExecutionKind::Streaming,
            None,
        )?;
        Ok(self.batches.clone())
    }

    /// Map local stream batches.
    pub fn map_batches(&self, mut f: impl FnMut(&StreamBatch) -> StreamBatch) -> Result<Stream> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream mapping requires a streaming runtime",
            ));
        }

        ensure_local_mode(self.execution_mode)?;

        Ok(Self::for_session(
            self.name.clone(),
            self.mode,
            self.batches.iter().map(&mut f).collect(),
            self.execution_mode,
        ))
    }

    /// Filter local stream batches.
    pub fn filter_batches(&self, mut f: impl FnMut(&StreamBatch) -> bool) -> Result<Stream> {
        ensure_local_mode(self.execution_mode)?;
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream filtering requires a streaming runtime",
            ));
        }

        Ok(Self::for_session(
            self.name.clone(),
            self.mode,
            self.batches
                .iter()
                .filter(|batch| f(batch))
                .cloned()
                .collect(),
            self.execution_mode,
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
/// This is a descriptor type — no execution happens until the stream is
/// submitted to a distributed runtime.
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

// All execution modes are now supported; this is a no-op retained to keep
// call sites that guard metadata-only operations readable.
#[allow(dead_code)]
fn ensure_local_mode(_mode: ExecutionMode) -> Result<()> {
    Ok(())
}

fn accept_plan_with_backend(
    mode: ExecutionMode,
    plan_name: &str,
    kind: ExecutionKind,
    coordinator_url: Option<&str>,
) -> Result<()> {
    let physical_plan = PhysicalPlan::new(plan_name, kind);

    match mode {
        ExecutionMode::Embedded => {
            let mut backend = EmbeddedBackend;
            backend.execute(&physical_plan)?;
        }
        ExecutionMode::SingleNode => {
            let mut backend = SingleNodeBackend;
            backend.execute(&physical_plan)?;
        }
        ExecutionMode::Distributed => {
            let url = coordinator_url.ok_or_else(|| {
                KrishivError::unsupported(
                    "distributed mode requires a coordinator URL; use SessionBuilder::with_coordinator",
                )
            })?;
            let mut backend = DistributedBackend::new(url);
            backend.execute(&physical_plan)?;
        }
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    use super::{
        DataType, ExecutionMode, Field, KrishivError, RecordBatch, Schema, Session, SessionBuilder,
        StreamBatch,
    };

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
}
