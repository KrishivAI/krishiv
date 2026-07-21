//! Unified execution runtime across Embedded (in-process), SingleNode (local daemon), and Distributed modes.

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use krishiv_plan::PhysicalPlan;

use crate::in_process::BatchSqlTable;
use crate::in_process_cluster::InProcessCluster;
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{EmbeddedBackend, ExecutionBackend, ExecutionReport, RuntimeError, RuntimeResult};

/// Local cluster connection endpoints for SingleNode / Distributed clients.
#[derive(Debug, Clone, Default)]
pub struct ClusterEndpoints {
    /// Coordinator gRPC address (e.g. `http://127.0.0.1:9090`).
    pub grpc_url: Option<String>,
    /// Arrow Flight SQL address for batch result fetch.
    pub flight_url: Option<String>,
}

impl ClusterEndpoints {
    pub fn loopback_default() -> Self {
        Self {
            grpc_url: Some(String::from("http://127.0.0.1:9090")),
            flight_url: Some(String::from("http://127.0.0.1:50051")),
        }
    }
}

/// Deployment mode for runtime implementations (mirrors `krishiv_api::ExecutionMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMode {
    Embedded,
    SingleNode,
    Distributed,
}

/// Explicit placement decision for runtime work.
///
/// `RuntimeMode` describes the user-visible execution mode. `ExecutionPlacement`
/// describes where data-plane work is actually allowed to run. Keeping them
/// separate prevents distributed sessions from silently falling back to local
/// execution when a coordinator endpoint is missing or disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPlacement {
    /// Run coordinator/executor work in the current process.
    LocalInProcess,
    /// Route single-node work to a local daemon over Flight/gRPC.
    SingleNodeDaemon,
    /// Require a remote cluster endpoint; never use local fallback.
    RemoteClusterRequired,
}

/// Parquet table forwarded to executor SQL tasks during batch collect.
#[derive(Debug, Clone)]
pub struct BatchTableRegistration {
    pub table_name: String,
    pub path: PathBuf,
}

impl BatchTableRegistration {
    pub fn new(table_name: impl Into<String>, path: PathBuf) -> Self {
        Self {
            table_name: table_name.into(),
            path,
        }
    }
}

/// Unified runtime API for batch plan acceptance and bounded streaming collect.
pub trait ExecutionRuntime: Send + Sync {
    /// Execution mode label for telemetry.
    fn mode(&self) -> RuntimeMode;

    /// Concrete placement used by this runtime.
    fn placement(&self) -> ExecutionPlacement;

    /// Whether data-plane work is routed to a remote Flight endpoint (no local fallback).
    fn uses_remote_execution(&self) -> bool {
        !matches!(self.placement(), ExecutionPlacement::LocalInProcess)
    }

    /// Accept or dispatch a physical plan (batch or streaming).
    fn accept_plan(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport>;

    /// Execute a bounded windowed pipeline and return output batches.
    fn collect_bounded_window(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>>;

    /// Execute a bounded windowed pipeline and return output batches together
    /// with the max watermark observed across output batches (C8).
    ///
    /// The default implementation calls `collect_bounded_window` and returns
    /// `None` for the watermark. Override in distributed runtimes to propagate
    /// the executor's watermark back to the caller for global alignment.
    fn collect_bounded_window_with_watermark(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<(Vec<RecordBatch>, Option<i64>)> {
        self.collect_bounded_window(topic, input_batches, spec)
            .map(|batches| (batches, None))
    }

    /// Execute batch SQL through coordinator/Flight and return all result batches.
    fn collect_batch_sql(
        &self,
        query: &str,
        tables: &[BatchTableRegistration],
        is_streaming: bool,
    ) -> RuntimeResult<Vec<RecordBatch>>;

    /// Async variant of [`Self::collect_batch_sql`].
    ///
    /// Returns a boxed future so callers holding `Arc<dyn ExecutionRuntime>` can
    /// await without an outer `spawn_blocking` wrapper.  In-process runtimes
    /// off-load DataFusion work onto the blocking pool; remote runtimes produce a
    /// genuinely async future that drives the Flight/gRPC call directly,
    /// eliminating the extra thread hop that the sync path requires.
    fn collect_batch_sql_async<'a>(
        &'a self,
        query: &'a str,
        tables: &'a [BatchTableRegistration],
        is_streaming: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = RuntimeResult<Vec<RecordBatch>>> + Send + 'a>,
    >;

    /// Execute a batch SQL write whose result is committed through a sink
    /// output contract (Phase 2.3 distributed writes) instead of being
    /// collected back to the client.
    ///
    /// `sink_contract` is a full output contract description such as
    /// `object-parquet-sink:<base>:<dest>:mode=overwrite:partition_by=c`.
    /// Sink tasks stage part files under `<dest>/_staging/<job_id>/` and the
    /// coordinator publishes them atomically when the job succeeds, so the
    /// destination never exposes partial output.
    ///
    /// The default implementation rejects the call; runtimes that can submit
    /// sink jobs (in-process cluster, Flight `batch_sql_sink` action) override it.
    fn collect_batch_sql_sink(
        &self,
        _query: &str,
        _tables: &[BatchTableRegistration],
        _sink_contract: &str,
    ) -> RuntimeResult<()> {
        Err(RuntimeError::unsupported(
            "this execution runtime does not support distributed sink writes",
        ))
    }

    /// Explain a SQL query (plan metadata only).
    fn explain_sql(&self, query: &str) -> RuntimeResult<String>;

    /// Register a continuous streaming job (long-running operator).
    fn register_continuous_stream(
        &self,
        job_id: &str,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<()>;

    /// Push input batches to a continuous streaming job.
    fn push_continuous_stream_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> RuntimeResult<()>;

    /// Drain newly emitted batches from a continuous streaming job.
    fn drain_continuous_stream(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>>;

    /// Optional remote Flight URL (distributed / single-node daemon).
    fn flight_url(&self) -> Option<&str> {
        None
    }

    /// Optional coordinator gRPC URL used by CLI/operator control-plane integrations.
    fn coordinator_grpc_url(&self) -> Option<&str> {
        None
    }

    /// Forward a Kafka source registration to the remote coordinator.
    ///
    /// The default no-op is appropriate for in-process runtimes where the caller
    /// has already registered the source on the local `SqlEngine`.
    /// Remote runtimes override this to send a `RegisterKafkaSource` Flight action.
    #[cfg(feature = "kafka")]
    fn register_kafka_source(
        &self,
        _name: &str,
        _schema_ipc_b64: &str,
        _bootstrap_servers: &str,
        _topic: &str,
        _group_id: &str,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

/// Compute the event-time watermark from a slice of batches.
///
/// Returns `max(event_time_column) - lag_ms`, or `None` when the column is
/// absent or all batches are empty.  Matches the logic used by the executor's
/// streaming fragment (`compute_input_watermark`) so embedded and distributed
/// modes produce the same watermark for the same inputs.
fn compute_watermark_ms(batches: &[RecordBatch], event_time_col: &str, lag_ms: u64) -> Option<i64> {
    use arrow::array::{
        Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};

    let mut max_ts: Option<i64> = None;
    for batch in batches {
        let col_idx = batch.schema().index_of(event_time_col).ok()?;
        let col = batch.column(col_idx);
        let col_max: Option<i64> = match col.data_type() {
            DataType::Int64 => col.as_any().downcast_ref::<Int64Array>().and_then(|arr| {
                (0..arr.len())
                    .filter(|&i| arr.is_valid(i))
                    .map(|i| arr.value(i))
                    .max()
            }),
            DataType::Timestamp(TimeUnit::Millisecond, _) => col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .and_then(|arr| {
                    (0..arr.len())
                        .filter(|&i| arr.is_valid(i))
                        .map(|i| arr.value(i))
                        .max()
                }),
            DataType::Timestamp(TimeUnit::Microsecond, _) => col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .and_then(|arr| {
                    (0..arr.len())
                        .filter(|&i| arr.is_valid(i))
                        .map(|i| arr.value(i) / 1_000)
                        .max()
                }),
            DataType::Timestamp(TimeUnit::Second, _) => col
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .and_then(|arr| {
                    (0..arr.len())
                        .filter(|&i| arr.is_valid(i))
                        .map(|i| arr.value(i) * 1_000)
                        .max()
                }),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .and_then(|arr| {
                    (0..arr.len())
                        .filter(|&i| arr.is_valid(i))
                        .map(|i| arr.value(i) / 1_000_000)
                        .max()
                }),
            _ => None,
        };
        if let Some(ts) = col_max {
            max_ts = Some(max_ts.unwrap_or(i64::MIN).max(ts));
        }
    }
    max_ts.map(|ts| ts.saturating_sub(i64::try_from(lag_ms).unwrap_or(i64::MAX)))
}

fn tables_to_batch_sql(tables: &[BatchTableRegistration]) -> Vec<BatchSqlTable> {
    // Skip the allocation entirely for the common empty-table case (most
    // queries in embedded mode don't register parquet tables).
    if tables.is_empty() {
        return Vec::new();
    }
    tables
        .iter()
        .map(|t| BatchSqlTable {
            table_name: t.table_name.clone(),
            path: t.path.clone(),
            ipc_b64: String::new(),
        })
        .collect()
}

/// Like [`tables_to_batch_sql`], but also reads each table's parquet file into
/// base64 Arrow-IPC and ships it in-band.
///
/// Used by the distributed remote path: executors run in separate pods and do
/// not share the client's filesystem, so the table data must travel with the
/// request. If a file cannot be read (e.g. it lives on a filesystem the
/// coordinator *does* share), the entry degrades to path-based resolution.
fn tables_to_batch_sql_inline(tables: &[BatchTableRegistration]) -> Vec<BatchSqlTable> {
    tables
        .iter()
        .map(|t| {
            let ipc_b64 = match crate::flight_protocol::parquet_file_to_ipc_b64(&t.path) {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(
                        table = %t.table_name,
                        path = %t.path.display(),
                        error = %e,
                        "could not inline parquet table; falling back to path-based resolution"
                    );
                    String::new()
                }
            };
            BatchSqlTable {
                table_name: t.table_name.clone(),
                path: t.path.clone(),
                ipc_b64,
            }
        })
        .collect()
}

/// In-process cluster runtime for Embedded mode.
///
/// Neither backend carries per-call state, so a `Mutex<...>` is unnecessary —
/// removing it eliminates the serialization point that previously blocked
/// concurrent SQL submissions from the same session (C3).
pub struct InProcessExecutionRuntime {
    mode: RuntimeMode,
    cluster: Arc<InProcessCluster>,
}

impl InProcessExecutionRuntime {
    pub fn embedded(cluster: Arc<InProcessCluster>) -> Self {
        Self {
            mode: RuntimeMode::Embedded,
            cluster,
        }
    }
}

impl ExecutionRuntime for InProcessExecutionRuntime {
    fn mode(&self) -> RuntimeMode {
        self.mode
    }

    fn placement(&self) -> ExecutionPlacement {
        ExecutionPlacement::LocalInProcess
    }

    fn accept_plan(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        match self.mode {
            RuntimeMode::Embedded => {
                let backend = EmbeddedBackend::default();
                backend.execute(plan)
            }
            RuntimeMode::SingleNode | RuntimeMode::Distributed => Err(RuntimeError::unsupported(
                "in-process runtime does not serve distributed or single-node daemon mode",
            )),
        }
    }

    fn collect_bounded_window(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        self.cluster
            .collect_bounded_window(topic, input_batches, spec)
    }

    fn collect_bounded_window_with_watermark(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<(Vec<RecordBatch>, Option<i64>)> {
        let watermark = compute_watermark_ms(
            &input_batches,
            &spec.event_time_column,
            spec.watermark_lag_ms,
        );
        let batches = self
            .cluster
            .collect_bounded_window(topic, input_batches, spec)?;
        Ok((batches, watermark))
    }

    fn collect_batch_sql(
        &self,
        query: &str,
        tables: &[BatchTableRegistration],
        is_streaming: bool,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        self.cluster
            .collect_batch_sql(query, &tables_to_batch_sql(tables), is_streaming)
    }

    fn collect_batch_sql_async<'a>(
        &'a self,
        query: &'a str,
        tables: &'a [BatchTableRegistration],
        is_streaming: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = RuntimeResult<Vec<RecordBatch>>> + Send + 'a>,
    > {
        // DataFusion execution is CPU-bound and blocking; off-load it to the
        // blocking thread pool so we don't stall a tokio worker.  We clone the
        // Arc (cheap) to satisfy the 'static bound required by spawn_blocking.
        let cluster = Arc::clone(&self.cluster);
        let query = query.to_owned();
        let tables = tables_to_batch_sql(tables);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                cluster.collect_batch_sql(&query, &tables, is_streaming)
            })
            .await
            .map_err(|e| RuntimeError::transport(format!("in-process task failed: {e}")))?
        })
    }

    fn collect_batch_sql_sink(
        &self,
        query: &str,
        tables: &[BatchTableRegistration],
        sink_contract: &str,
    ) -> RuntimeResult<()> {
        self.cluster
            .execute_batch_sql_sink(query, &tables_to_batch_sql(tables), sink_contract)
    }

    fn explain_sql(&self, query: &str) -> RuntimeResult<String> {
        krishiv_sql::explain_sql(query).map_err(|e| RuntimeError::transport(e.to_string()))
    }

    fn register_continuous_stream(
        &self,
        job_id: &str,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<()> {
        self.cluster.register_continuous_job(job_id, spec)
    }

    fn push_continuous_stream_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> RuntimeResult<()> {
        self.cluster.push_continuous_input(job_id, batches)
    }

    fn drain_continuous_stream(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        self.cluster.drain_continuous_job(job_id)
    }
}

/// Runtime that routes all data-plane work to a Flight/gRPC endpoint.
pub struct RemoteExecutionRuntime {
    pool: crate::flight_client::FlightClientPool,
    coordinator_grpc_url: Option<String>,
    session_mode: RuntimeMode,
    placement: ExecutionPlacement,
}

impl RemoteExecutionRuntime {
    pub fn new(
        flight_url: impl Into<String>,
        session_mode: RuntimeMode,
        coordinator_grpc_url: Option<String>,
        placement: ExecutionPlacement,
    ) -> crate::RuntimeResult<Self> {
        Ok(Self {
            pool: crate::flight_client::FlightClientPool::new(flight_url)?,
            coordinator_grpc_url,
            session_mode,
            placement,
        })
    }

    /// Start background health checks for the Flight client pool.
    /// Call this after construction when running inside a Tokio runtime.
    pub async fn start_health_checks(&self) {
        self.pool.start_health_checks().await;
    }

    /// Eagerly spawn health checks as a background Tokio task when a runtime is
    /// already available (e.g. from within a `#[tokio::main]` or any async
    /// call-chain). When called from a sync context with no ambient runtime
    /// this is a no-op: the pool lazily starts the same background loop on its
    /// first async use (`FlightClientPool::ensure_health_checks`), so the
    /// checks are never silently dropped — this method only front-loads them.
    pub fn spawn_health_checks(&self) {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let pool = self.pool.clone();
                handle.spawn(async move { pool.start_health_checks().await });
            }
            Err(_) => {
                tracing::debug!(
                    "Flight client health checks deferred to first use: \
                     spawn_health_checks called outside a Tokio runtime context"
                );
            }
        }
    }
}

/// Return true only for the dedicated tonic `Unimplemented` transport status.
fn is_server_unimplemented(e: &RuntimeError) -> bool {
    matches!(e, RuntimeError::ServerUnimplemented { .. })
}

/// Return true when `do_action`'s response was rejected for being too large
/// (server `resource_exhausted`, or the client's own response buffer
/// overflowing) — the caller should retry through the streaming
/// `execute()`/`do_get` transport, which has no equivalent single-message cap.
/// Unlike the `is_server_unimplemented` fallback, this is not a version- or
/// capability-compatibility downgrade, so callers apply it unconditionally
/// rather than gating it behind `allow_remote_sql_comment_fallback`.
fn is_result_too_large(e: &RuntimeError) -> bool {
    matches!(e, RuntimeError::ResultTooLarge { .. })
}

fn allow_remote_sql_comment_fallback() -> bool {
    krishiv_common::allows_remote_sql_comment_fallback()
}

impl ExecutionRuntime for RemoteExecutionRuntime {
    fn mode(&self) -> RuntimeMode {
        self.session_mode
    }

    fn placement(&self) -> ExecutionPlacement {
        self.placement
    }

    fn accept_plan(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        use krishiv_plan::ExecutionKind;
        // Streaming plans must use collect_bounded_window or the continuous
        // stream APIs, not accept_plan. Silently returning success here would
        // hide the fact that no execution happened on the remote cluster.
        if plan.kind() == ExecutionKind::Streaming {
            let job_id = plan.name();
            if job_id.trim().is_empty() {
                return Err(RuntimeError::plan_rejected(
                    "streaming plan name must not be empty",
                ));
            }
            let spec = crate::plan::streaming_spec_from_plan(plan)?;
            self.register_continuous_stream(job_id, &spec)?;
            return Ok(ExecutionReport::new(
                "distributed",
                plan.name(),
                plan.kind(),
                true,
            ));
        }
        use crate::flight_action::{ExecutePlanBody, KrishivFlightAction};
        use krishiv_common::async_util::block_on;
        let body = ExecutePlanBody::from_plan(plan)?;
        let action = KrishivFlightAction::ExecutePlan(body);
        block_on(async {
            match self.pool.do_action(&action).await {
                Ok(_) => Ok(()),
                Err(e) if is_server_unimplemented(&e) => {
                    if !allow_remote_sql_comment_fallback() {
                        return Err(e);
                    }
                    let sql = crate::flight_client::plan_to_sql(plan);
                    self.pool.execute_sql(&sql).await.map(|_| ())
                }
                Err(e) => Err(e),
            }
        })?;
        Ok(ExecutionReport::new(
            "distributed",
            plan.name(),
            plan.kind(),
            true,
        ))
    }

    fn collect_bounded_window(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        self.collect_bounded_window_with_watermark(topic, input_batches, spec)
            .map(|(batches, _wm)| batches)
    }

    fn collect_bounded_window_with_watermark(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<(Vec<RecordBatch>, Option<i64>)> {
        use crate::flight_action::{BoundedWindowBody, KrishivFlightAction, encode_batches};
        use crate::flight_client::decode_ipc_response;
        use crate::flight_protocol::encode_bounded_window;
        use krishiv_common::async_util::block_on;

        // The active coordinator owns partitioning and executor placement.
        // Flight pool alternates are failover coordinators, not data shards.
        let batches_b64 = encode_batches(&input_batches)?;
        let action = KrishivFlightAction::BoundedWindow(BoundedWindowBody {
            topic: topic.to_string(),
            spec: spec.to_plan_spec(),
            batches_b64,
            response_watermark_ms: None,
        });
        block_on(async {
            match self.pool.do_action(&action).await {
                Ok(body) => {
                    let watermark = serde_json::from_slice::<BoundedWindowBody>(&body)
                        .ok()
                        .and_then(|r| r.response_watermark_ms);
                    let batches = decode_ipc_response(&body)?;
                    Ok((batches, watermark))
                }
                Err(e) if is_result_too_large(&e) => {
                    let sql = encode_bounded_window(topic, spec, &input_batches)?;
                    let batches = self
                        .pool
                        .execute_sql_capped(&sql, crate::flight_client::client_max_result_bytes())
                        .await?;
                    Ok((batches, None))
                }
                Err(e) if is_server_unimplemented(&e) => {
                    if !allow_remote_sql_comment_fallback() {
                        return Err(e);
                    }
                    let sql = encode_bounded_window(topic, spec, &input_batches)?;
                    let batches = self.pool.execute_sql(&sql).await?;
                    Ok((batches, None))
                }
                Err(e) => Err(e),
            }
        })
    }

    fn collect_batch_sql(
        &self,
        query: &str,
        tables: &[BatchTableRegistration],
        is_streaming: bool,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        use krishiv_common::async_util::block_on;
        block_on(self.collect_batch_sql_async(query, tables, is_streaming))
    }

    fn collect_batch_sql_async<'a>(
        &'a self,
        query: &'a str,
        tables: &'a [BatchTableRegistration],
        is_streaming: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = RuntimeResult<Vec<RecordBatch>>> + Send + 'a>,
    > {
        use crate::flight_action::{BatchSqlBody, KrishivFlightAction};
        use crate::flight_client::decode_ipc_response;
        use crate::flight_protocol::encode_batch_sql;

        Box::pin(async move {
            // Build the typed action; clone tables into the action body so we
            // still have them for the SQL-comment fallback path below. Inline the
            // parquet data so executors in other pods need no shared filesystem.
            let batch_tables = tables_to_batch_sql_inline(tables);
            let action = KrishivFlightAction::BatchSql(BatchSqlBody {
                query: query.to_owned(),
                tables: batch_tables.clone(),
                is_streaming,
            });
            match self.pool.do_action(&action).await {
                Ok(body) => decode_ipc_response(&body),
                Err(e) if is_result_too_large(&e) => {
                    // Not a compatibility downgrade (unlike the Unimplemented
                    // arm below) — do_action's single-message transport is
                    // structurally the wrong tool for this result regardless
                    // of server version, so retry unconditionally through the
                    // streaming do_get transport instead.
                    let mut sql = encode_batch_sql(query, &batch_tables);
                    if is_streaming {
                        sql = format!("-- krishiv:streaming=true\n{sql}");
                    }
                    self.pool
                        .execute_sql_capped(&sql, crate::flight_client::client_max_result_bytes())
                        .await
                }
                Err(e) if is_server_unimplemented(&e) => {
                    if !allow_remote_sql_comment_fallback() {
                        return Err(e);
                    }
                    let mut sql = encode_batch_sql(query, &batch_tables);
                    if is_streaming {
                        sql = format!("-- krishiv:streaming=true\n{sql}");
                    }
                    self.pool.execute_sql(&sql).await
                }
                Err(e) => Err(e),
            }
        })
    }

    fn collect_batch_sql_sink(
        &self,
        query: &str,
        tables: &[BatchTableRegistration],
        sink_contract: &str,
    ) -> RuntimeResult<()> {
        use crate::flight_action::{BatchSqlSinkBody, KrishivFlightAction};
        use krishiv_common::async_util::block_on;
        // No SQL-comment fallback for sink writes: the staged commit protocol
        // requires a server that understands the typed action, otherwise the
        // write would silently degrade to an uncommitted path.
        let action = KrishivFlightAction::BatchSqlSink(BatchSqlSinkBody {
            query: query.to_owned(),
            tables: tables_to_batch_sql_inline(tables),
            sink_contract: sink_contract.to_owned(),
        });
        block_on(async { self.pool.do_action(&action).await.map(|_| ()) })
    }

    fn explain_sql(&self, query: &str) -> RuntimeResult<String> {
        use crate::flight_client::flight_explain_from_batches;
        use crate::flight_protocol::encode_explain_sql;
        use krishiv_common::async_util::block_on;
        let sql = encode_explain_sql(query);
        let batches = block_on(self.pool.execute_sql(&sql))?;
        Ok(flight_explain_from_batches(&batches))
    }

    fn register_continuous_stream(
        &self,
        job_id: &str,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<()> {
        use crate::flight_action::{ContinuousRegisterBody, KrishivFlightAction};
        use crate::flight_protocol::encode_continuous_register;
        use krishiv_common::async_util::block_on;
        let action = KrishivFlightAction::ContinuousRegister(ContinuousRegisterBody {
            job_id: job_id.to_string(),
            spec: spec.to_plan_spec(),
        });
        block_on(async {
            match self.pool.do_action(&action).await {
                Ok(_) => Ok(()),
                Err(e) if is_server_unimplemented(&e) => {
                    if !allow_remote_sql_comment_fallback() {
                        return Err(e);
                    }
                    let sql = encode_continuous_register(job_id, spec)?;
                    self.pool.execute_sql(&sql).await.map(|_| ())
                }
                Err(e) => Err(e),
            }
        })
    }

    fn push_continuous_stream_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> RuntimeResult<()> {
        use crate::flight_action::{ContinuousPushBody, KrishivFlightAction, encode_batches};
        use crate::flight_protocol::encode_continuous_push;
        use krishiv_common::async_util::block_on;
        let batches_b64 = encode_batches(&batches)?;
        let action = KrishivFlightAction::ContinuousPush(ContinuousPushBody {
            job_id: job_id.to_string(),
            batches_b64,
        });
        block_on(async {
            match self.pool.do_action(&action).await {
                Ok(_) => Ok(()),
                Err(e) if is_server_unimplemented(&e) => {
                    if !allow_remote_sql_comment_fallback() {
                        return Err(e);
                    }
                    let sql = encode_continuous_push(job_id, &batches)?;
                    self.pool.execute_sql(&sql).await.map(|_| ())
                }
                Err(e) => Err(e),
            }
        })
    }

    fn drain_continuous_stream(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        use crate::flight_action::{ContinuousDrainBody, KrishivFlightAction};
        use crate::flight_client::decode_ipc_response;
        use crate::flight_protocol::encode_continuous_drain;
        use krishiv_common::async_util::block_on;
        let action = KrishivFlightAction::ContinuousDrain(ContinuousDrainBody {
            job_id: job_id.to_string(),
        });
        block_on(async {
            match self.pool.do_action(&action).await {
                Ok(body) => decode_ipc_response(&body),
                Err(e) if is_result_too_large(&e) => {
                    let sql = encode_continuous_drain(job_id);
                    self.pool
                        .execute_sql_capped(&sql, crate::flight_client::client_max_result_bytes())
                        .await
                }
                Err(e) if is_server_unimplemented(&e) => {
                    if !allow_remote_sql_comment_fallback() {
                        return Err(e);
                    }
                    let sql = encode_continuous_drain(job_id);
                    self.pool.execute_sql(&sql).await
                }
                Err(e) => Err(e),
            }
        })
    }

    fn flight_url(&self) -> Option<&str> {
        Some(self.pool.flight_url())
    }

    fn coordinator_grpc_url(&self) -> Option<&str> {
        self.coordinator_grpc_url.as_deref()
    }

    #[cfg(feature = "kafka")]
    fn register_kafka_source(
        &self,
        name: &str,
        schema_ipc_b64: &str,
        bootstrap_servers: &str,
        topic: &str,
        group_id: &str,
    ) -> RuntimeResult<()> {
        use crate::flight_action::{KrishivFlightAction, RegisterKafkaSourceBody};
        use krishiv_common::async_util::block_on;
        let action = KrishivFlightAction::RegisterKafkaSource(RegisterKafkaSourceBody {
            name: name.to_owned(),
            schema_ipc_b64: schema_ipc_b64.to_owned(),
            bootstrap_servers: bootstrap_servers.to_owned(),
            topic: topic.to_owned(),
            group_id: group_id.to_owned(),
        });
        block_on(async { self.pool.do_action(&action).await.map(|_| ()) })
    }
}

/// Build the appropriate runtime for a session configuration.
///
/// `in_process_cluster` is required for Embedded mode.
/// For SingleNode mode, only `SingleNodeDaemon` placement is supported
/// (requires a `coordinator_flight_url`). `in_process_cluster` is ignored
/// (but can be `None`) for SingleNodeDaemon and Distributed placements.
pub fn build_execution_runtime(
    mode: RuntimeMode,
    in_process_cluster: Option<Arc<InProcessCluster>>,
    coordinator_flight_url: Option<String>,
    coordinator_grpc_url: Option<String>,
    placement: ExecutionPlacement,
) -> RuntimeResult<Arc<dyn ExecutionRuntime>> {
    match (mode, placement) {
        (RuntimeMode::Embedded, ExecutionPlacement::LocalInProcess) => {
            let cluster = in_process_cluster.clone().ok_or_else(|| {
                RuntimeError::unsupported("Embedded mode requires an InProcessCluster")
            })?;
            Ok(Arc::new(InProcessExecutionRuntime::embedded(cluster)))
        }
        (RuntimeMode::SingleNode, ExecutionPlacement::SingleNodeDaemon) => {
            let url = coordinator_flight_url.ok_or_else(|| {
                RuntimeError::unsupported(
                    "SingleNodeDaemon placement requires a local Flight SQL coordinator URL",
                )
            })?;
            let rt = RemoteExecutionRuntime::new(
                url,
                RuntimeMode::SingleNode,
                coordinator_grpc_url,
                ExecutionPlacement::SingleNodeDaemon,
            )?;
            rt.spawn_health_checks();
            Ok(Arc::new(rt))
        }
        (RuntimeMode::Distributed, ExecutionPlacement::RemoteClusterRequired) => {
            let url = coordinator_flight_url.ok_or_else(|| {
                RuntimeError::unsupported(
                    "Distributed placement requires an explicit remote Flight SQL coordinator URL",
                )
            })?;
            let rt = RemoteExecutionRuntime::new(
                url,
                RuntimeMode::Distributed,
                coordinator_grpc_url,
                ExecutionPlacement::RemoteClusterRequired,
            )?;
            rt.spawn_health_checks();
            Ok(Arc::new(rt))
        }
        (RuntimeMode::Embedded, _) => Err(RuntimeError::unsupported(
            "Embedded mode only supports LocalInProcess placement",
        )),
        (RuntimeMode::SingleNode, _) => Err(RuntimeError::unsupported(
            "SingleNode mode requires SingleNodeDaemon placement with a coordinator Flight URL; \
             for in-process execution use Embedded mode",
        )),
        (RuntimeMode::Distributed, _) => Err(RuntimeError::unsupported(
            "Distributed mode cannot use local fallback; use RemoteClusterRequired placement",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        BatchTableRegistration, ClusterEndpoints, ExecutionPlacement, InProcessExecutionRuntime,
        RuntimeMode, build_execution_runtime, tables_to_batch_sql, tables_to_batch_sql_inline,
    };
    use crate::ExecutionRuntime;
    use crate::InProcessCluster;
    use krishiv_plan::{ExecutionKind, PhysicalPlan};

    #[test]
    fn inline_batch_tables_ship_parquet_data_in_band() {
        // The distributed remote path must inline each parquet table as Arrow IPC
        // so executors in other pods need no shared filesystem. Path-only shipping
        // (tables_to_batch_sql) leaves ipc_b64 empty; the inline variant fills it
        // with bytes that decode back to the original rows.
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use base64::Engine as _;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        {
            let file = std::fs::File::create(&path).unwrap();
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let regs = vec![BatchTableRegistration::new("t", path.clone())];

        // Path-only: no data travels with the request.
        let path_only = tables_to_batch_sql(&regs);
        assert_eq!(path_only.len(), 1);
        assert!(
            path_only[0].ipc_b64.is_empty(),
            "path-based shipping carries no inline data"
        );

        // Inline: the parquet is read and shipped as IPC.
        let inline = tables_to_batch_sql_inline(&regs);
        assert_eq!(inline.len(), 1);
        assert_eq!(inline[0].table_name, "t");
        assert!(
            !inline[0].ipc_b64.is_empty(),
            "inline shipping must embed the parquet data as IPC"
        );

        // The inlined bytes decode back to the original three rows.
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(inline[0].ipc_b64.as_bytes())
            .unwrap();
        let reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 3, "decoded inline IPC must hold all original rows");
    }

    #[test]
    fn distributed_runtime_preserves_flight_and_grpc_urls() {
        let runtime = build_execution_runtime(
            RuntimeMode::Distributed,
            None,
            Some(String::from("http://127.0.0.1:50051")),
            Some(String::from("http://127.0.0.1:9090")),
            ExecutionPlacement::RemoteClusterRequired,
        )
        .expect("distributed runtime");

        assert_eq!(
            runtime.flight_url(),
            Some("http://127.0.0.1:50051"),
            "flight URL should be preserved for distributed sessions"
        );
        assert_eq!(
            runtime.coordinator_grpc_url(),
            Some("http://127.0.0.1:9090"),
            "gRPC coordinator URL should be preserved for distributed sessions"
        );
    }

    #[test]
    fn deployment_conformance_embedded_single_node_daemon_and_distributed_fake() {
        let embedded_cluster = Arc::new(InProcessCluster::new().unwrap());
        let embedded = build_execution_runtime(
            RuntimeMode::Embedded,
            Some(embedded_cluster),
            None,
            None,
            ExecutionPlacement::LocalInProcess,
        )
        .expect("embedded runtime");
        assert_eq!(embedded.mode(), RuntimeMode::Embedded);
        assert_eq!(embedded.placement(), ExecutionPlacement::LocalInProcess);
        assert!(!embedded.uses_remote_execution());
        assert_eq!(
            embedded
                .collect_batch_sql("SELECT 1 AS n", &[], false)
                .unwrap()[0]
                .num_rows(),
            1
        );

        let single_node_daemon = build_execution_runtime(
            RuntimeMode::SingleNode,
            None,
            Some("http://127.0.0.1:50051".into()),
            Some("http://127.0.0.1:9090".into()),
            ExecutionPlacement::SingleNodeDaemon,
        )
        .expect("single-node daemon runtime");
        assert_eq!(single_node_daemon.mode(), RuntimeMode::SingleNode);
        assert_eq!(
            single_node_daemon.placement(),
            ExecutionPlacement::SingleNodeDaemon
        );
        assert!(single_node_daemon.uses_remote_execution());
        assert_eq!(
            single_node_daemon.flight_url(),
            Some("http://127.0.0.1:50051")
        );
        assert_eq!(
            single_node_daemon.coordinator_grpc_url(),
            Some("http://127.0.0.1:9090")
        );

        let distributed_fake = build_execution_runtime(
            RuntimeMode::Distributed,
            None,
            Some("http://distributed.example.invalid:50051".into()),
            Some("http://distributed.example.invalid:9090".into()),
            ExecutionPlacement::RemoteClusterRequired,
        )
        .expect("distributed fake endpoint runtime");
        assert_eq!(distributed_fake.mode(), RuntimeMode::Distributed);
        assert_eq!(
            distributed_fake.placement(),
            ExecutionPlacement::RemoteClusterRequired
        );
        assert!(distributed_fake.uses_remote_execution());
        assert_eq!(
            distributed_fake.flight_url(),
            Some("http://distributed.example.invalid:50051")
        );
        assert_eq!(
            distributed_fake.coordinator_grpc_url(),
            Some("http://distributed.example.invalid:9090")
        );
    }

    #[test]
    fn deployment_conformance_rejects_invalid_runtime_placements() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let invalid_cases = [
            (
                RuntimeMode::Embedded,
                ExecutionPlacement::SingleNodeDaemon,
                Some(cluster.clone()),
                Some("http://127.0.0.1:50051".to_owned()),
            ),
            // SingleNode with LocalInProcess is no longer valid — must use Embedded mode
            (
                RuntimeMode::SingleNode,
                ExecutionPlacement::LocalInProcess,
                Some(cluster.clone()),
                None,
            ),
            (
                RuntimeMode::SingleNode,
                ExecutionPlacement::RemoteClusterRequired,
                Some(cluster),
                Some("http://127.0.0.1:50051".to_owned()),
            ),
            (
                RuntimeMode::Distributed,
                ExecutionPlacement::LocalInProcess,
                None,
                Some("http://127.0.0.1:50051".to_owned()),
            ),
        ];

        for (mode, placement, cluster, flight_url) in invalid_cases {
            let err = match build_execution_runtime(mode, cluster, flight_url, None, placement) {
                Ok(_) => panic!("invalid placement {mode:?}/{placement:?} must be rejected"),
                Err(err) => err,
            };
            assert!(
                matches!(err, crate::RuntimeError::Unsupported { .. }),
                "expected Unsupported for {mode:?}/{placement:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn cluster_endpoints_default() {
        let ep = ClusterEndpoints::default();
        assert!(ep.grpc_url.is_none());
        assert!(ep.flight_url.is_none());
    }

    #[test]
    fn cluster_endpoints_loopback_default() {
        let ep = ClusterEndpoints::loopback_default();
        assert_eq!(ep.grpc_url.as_deref(), Some("http://127.0.0.1:9090"));
        assert_eq!(ep.flight_url.as_deref(), Some("http://127.0.0.1:50051"));
    }

    #[test]
    fn cluster_endpoints_clone() {
        let ep = ClusterEndpoints::loopback_default();
        let ep2 = ep.clone();
        assert_eq!(ep2.grpc_url, ep.grpc_url);
        assert_eq!(ep2.flight_url, ep.flight_url);
    }

    #[test]
    fn batch_table_registration_new() {
        let reg = BatchTableRegistration::new("my_table", "/data/t.parquet".into());
        assert_eq!(reg.table_name, "my_table");
        assert_eq!(reg.path, std::path::PathBuf::from("/data/t.parquet"));
    }

    #[test]
    fn batch_table_registration_clone() {
        let reg = BatchTableRegistration::new("t", "/t.parquet".into());
        let reg2 = reg.clone();
        assert_eq!(reg2.table_name, "t");
    }

    #[test]
    fn runtime_mode_debug_clone_eq() {
        let m1 = RuntimeMode::Embedded;
        let m2 = m1;
        assert_eq!(m1, m2);
        assert_eq!(format!("{:?}", m1), "Embedded");
    }

    #[test]
    fn embedded_runtime_mode() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        assert_eq!(rt.mode(), RuntimeMode::Embedded);
    }

    #[test]
    fn embedded_runtime_accepts_plan() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        let plan = PhysicalPlan::new("test-plan", ExecutionKind::Batch);
        let report = rt.accept_plan(&plan).unwrap();
        assert!(report.accepted());
        assert_eq!(report.backend(), "embedded");
    }

    #[test]
    fn embedded_runtime_collect_batch_sql() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        let batches = rt.collect_batch_sql("SELECT 1 AS n", &[], false).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_runtime_collect_batch_sql_async() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt: Arc<dyn ExecutionRuntime> = Arc::new(InProcessExecutionRuntime::embedded(cluster));
        let batches = rt
            .collect_batch_sql_async("SELECT 42 AS n", &[], false)
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[test]
    fn embedded_runtime_explain_sql() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        let result = rt.explain_sql("SELECT 1").unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn embedded_runtime_continuous_lifecycle() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        let spec = crate::LocalWindowExecutionSpec {
            key_column_type: String::from("utf8"),
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: crate::LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            allowed_lateness_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        };
        rt.register_continuous_stream("j1", &spec).unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])) as _,
                Arc::new(arrow::array::Int64Array::from(vec![1_000])) as _,
            ],
        )
        .unwrap();
        rt.push_continuous_stream_input("j1", vec![batch]).unwrap();
        let _ = rt.drain_continuous_stream("j1").unwrap();
    }

    #[test]
    fn embedded_runtime_flight_url_none() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        assert!(rt.flight_url().is_none());
    }

    #[test]
    fn embedded_runtime_coordinator_grpc_url_none() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        assert!(rt.coordinator_grpc_url().is_none());
    }

    #[test]
    fn build_runtime_embedded() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(
            RuntimeMode::Embedded,
            Some(cluster),
            None,
            None,
            ExecutionPlacement::LocalInProcess,
        )
        .expect("embedded runtime");
        assert_eq!(rt.mode(), RuntimeMode::Embedded);
        assert_eq!(rt.placement(), ExecutionPlacement::LocalInProcess);
    }

    #[test]
    fn build_runtime_single_node_daemon_requires_flight_url() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let err = match build_execution_runtime(
            RuntimeMode::SingleNode,
            Some(cluster),
            None,
            None,
            ExecutionPlacement::SingleNodeDaemon,
        ) {
            Ok(_) => panic!("single-node daemon without Flight URL should fail"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            crate::RuntimeError::unsupported(
                "SingleNodeDaemon placement requires a local Flight SQL coordinator URL",
            )
        );
    }

    #[test]
    fn build_runtime_single_node_daemon_with_flight_url() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(
            RuntimeMode::SingleNode,
            Some(cluster),
            Some("http://127.0.0.1:50051".into()),
            None,
            ExecutionPlacement::SingleNodeDaemon,
        )
        .expect("single-node daemon runtime");
        assert_eq!(rt.mode(), RuntimeMode::SingleNode);
        assert_eq!(rt.placement(), ExecutionPlacement::SingleNodeDaemon);
        assert!(rt.uses_remote_execution());
        assert_eq!(rt.flight_url(), Some("http://127.0.0.1:50051"));
    }

    #[test]
    fn build_runtime_distributed_requires_explicit_flight_url() {
        let err = match build_execution_runtime(
            RuntimeMode::Distributed,
            None,
            None,
            None,
            ExecutionPlacement::RemoteClusterRequired,
        ) {
            Ok(_) => panic!("distributed runtime without Flight URL should fail"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            crate::RuntimeError::unsupported(
                "Distributed placement requires an explicit remote Flight SQL coordinator URL",
            )
        );
    }

    #[test]
    fn build_runtime_distributed_with_custom_flight_url() {
        let rt = build_execution_runtime(
            RuntimeMode::Distributed,
            None,
            Some("http://remote:50051".into()),
            None,
            ExecutionPlacement::RemoteClusterRequired,
        )
        .expect("distributed runtime");
        assert_eq!(rt.mode(), RuntimeMode::Distributed);
        assert_eq!(rt.placement(), ExecutionPlacement::RemoteClusterRequired);
        assert_eq!(rt.flight_url(), Some("http://remote:50051"));
    }

    #[test]
    fn build_runtime_distributed_remote_execution() {
        let rt = build_execution_runtime(
            RuntimeMode::Distributed,
            None,
            Some("http://remote:50051".into()),
            None,
            ExecutionPlacement::RemoteClusterRequired,
        )
        .expect("distributed runtime");
        assert!(rt.uses_remote_execution());
    }

    #[test]
    fn build_runtime_distributed_rejects_local_fallback() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let err = match build_execution_runtime(
            RuntimeMode::Distributed,
            Some(cluster),
            Some("http://remote:50051".into()),
            None,
            ExecutionPlacement::LocalInProcess,
        ) {
            Ok(_) => panic!("distributed local fallback should fail"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            crate::RuntimeError::unsupported(
                "Distributed mode cannot use local fallback; use RemoteClusterRequired placement",
            )
        );
    }

    // ── Durability profile smoke tests ────────────────────────────────────────

    #[test]
    fn dev_local_profile_batch_sql_returns_results() {
        // dev-local is the default in-memory profile used by InProcessCluster.
        // Verifies that batch SQL works end-to-end under the default durability profile.
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        let batches = rt
            .collect_batch_sql("SELECT 42 AS answer", &[], false)
            .unwrap();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42, "batch SQL must return correct result");
    }

    #[test]
    fn dev_local_profile_continuous_double_drain_does_not_panic() {
        // Verifies that draining a continuous job twice (second drain is idempotent)
        // does not panic or produce stale results under dev-local.
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        let spec = crate::LocalWindowExecutionSpec {
            key_column_type: String::from("utf8"),
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: crate::LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            allowed_lateness_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        };
        rt.register_continuous_stream("durable-j1", &spec).unwrap();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])) as _,
                Arc::new(arrow::array::Int64Array::from(vec![1_000_i64, 2_000])) as _,
            ],
        )
        .unwrap();

        rt.push_continuous_stream_input("durable-j1", vec![batch])
            .unwrap();
        let first_drain = rt.drain_continuous_stream("durable-j1").unwrap();

        // Second drain with no new input must return empty (results consumed once).
        let second_drain = rt.drain_continuous_stream("durable-j1").unwrap();
        let _ = first_drain; // first drain result is not asserted (may be empty for in-window data)
        assert!(
            second_drain.is_empty(),
            "second drain with no new input must be empty under dev-local"
        );
    }

    #[test]
    fn plan_execution_kind_batch() {
        let plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        assert_eq!(plan.kind(), ExecutionKind::Batch);
    }

    #[test]
    fn plan_execution_kind_streaming() {
        let plan = PhysicalPlan::new("test", ExecutionKind::Streaming);
        assert_eq!(plan.kind(), ExecutionKind::Streaming);
    }

    #[test]
    fn distributed_runtime_flight_url_preserved() {
        let rt = build_execution_runtime(
            RuntimeMode::Distributed,
            None,
            Some("http://custom:50051".into()),
            Some("http://custom:9090".into()),
            ExecutionPlacement::RemoteClusterRequired,
        )
        .expect("distributed runtime");
        assert_eq!(rt.flight_url(), Some("http://custom:50051"));
        assert_eq!(rt.coordinator_grpc_url(), Some("http://custom:9090"));
    }

    // ── is_server_unimplemented guard ─────────────────────────────────────────

    use super::is_server_unimplemented;

    #[test]
    fn fallback_triggered_on_tonic_unimplemented_status() {
        // The ServerUnimplemented variant is emitted by do_action when tonic
        // returns Code::Unimplemented — this is the only error that triggers fallback.
        let err = crate::RuntimeError::ServerUnimplemented {
            message: "action not yet supported".into(),
        };
        assert!(
            is_server_unimplemented(&err),
            "ServerUnimplemented variant must trigger the SQL fallback"
        );
    }

    #[test]
    fn fallback_not_triggered_on_word_unimplemented_in_message() {
        // A schema error or user message containing "Unimplemented" as a word
        // must NOT trigger the fallback — only tonic status prefix matches.
        let err = crate::RuntimeError::Transport {
            message: "schema column 'Unimplemented' type is not supported".into(),
        };
        assert!(
            !is_server_unimplemented(&err),
            "non-tonic error containing 'Unimplemented' must not trigger fallback"
        );
    }

    #[test]
    fn fallback_not_triggered_on_auth_error() {
        let err = crate::RuntimeError::Transport {
            message: "status: Unauthenticated, message: API key required".into(),
        };
        assert!(
            !is_server_unimplemented(&err),
            "auth error must not trigger Unimplemented fallback"
        );
    }

    #[test]
    fn fallback_triggered_on_status_code_format() {
        // A Transport error whose message contains the old tonic status string
        // must NOT trigger fallback — only the dedicated variant does.
        // This protects against coincidental string matches (user error messages,
        // schema errors) that previously caused silent incorrect fallback.
        let err = crate::RuntimeError::Transport {
            message: "Status { code: Unimplemented, message: \"not yet\" }".into(),
        };
        assert!(
            !is_server_unimplemented(&err),
            "Transport error with Unimplemented text must not trigger fallback; use ServerUnimplemented"
        );
    }

    // ── is_result_too_large guard ─────────────────────────────────────────────

    use super::is_result_too_large;

    #[test]
    fn stream_fallback_triggered_on_result_too_large() {
        // Emitted by do_action for both the server's resource_exhausted status
        // and the client's own response-buffer overflow — either one should
        // route the query through the streaming do_get transport instead.
        let err = crate::RuntimeError::ResultTooLarge {
            message: "Flight action result (999) exceeds maximum (100)".into(),
        };
        assert!(
            is_result_too_large(&err),
            "ResultTooLarge variant must trigger the streaming fallback"
        );
    }

    #[test]
    fn stream_fallback_not_triggered_on_word_large_in_message() {
        // A schema or user-facing error mentioning "large" in prose must not
        // trigger the fallback — only the dedicated variant does.
        let err = crate::RuntimeError::Transport {
            message: "column 'large_value' has an unsupported type".into(),
        };
        assert!(
            !is_result_too_large(&err),
            "non-dedicated error containing 'large' must not trigger the streaming fallback"
        );
    }

    #[test]
    fn stream_fallback_and_unimplemented_fallback_are_independent() {
        let unimplemented = crate::RuntimeError::ServerUnimplemented {
            message: "action not yet supported".into(),
        };
        assert!(!is_result_too_large(&unimplemented));
        let too_large = crate::RuntimeError::ResultTooLarge {
            message: "too big".into(),
        };
        assert!(!is_server_unimplemented(&too_large));
    }

    #[test]
    fn remote_runtime_streaming_plan_registers_continuous_stream() {
        let rt = build_execution_runtime(
            RuntimeMode::Distributed,
            None,
            Some("http://fake.invalid:50051".into()),
            None,
            ExecutionPlacement::RemoteClusterRequired,
        )
        .expect("distributed runtime");
        let plan = PhysicalPlan::new("stream-plan", ExecutionKind::Streaming);
        let err = rt.accept_plan(&plan).unwrap_err();
        assert!(
            !matches!(err, crate::RuntimeError::Unsupported { .. }),
            "streaming plan dispatch should attempt remote continuous register, got: {err:?}"
        );
    }

    // ── G3: In-process streaming plan delegation ─────────────────────────────

    #[test]
    fn in_process_embedded_accepts_streaming_plan() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        let plan = PhysicalPlan::new("stream-plan", ExecutionKind::Streaming);
        let report = rt
            .accept_plan(&plan)
            .expect("embedded accept_plan should delegate streaming plans");
        assert_eq!(report.backend(), "single-node"); // Embedded mode delegates to SingleNode
    }

    // ── G1: typed BatchSql action carries is_streaming flag ──────────────────

    #[test]
    fn remote_collect_batch_sql_uses_typed_action() {
        use crate::flight_action::{BatchSqlBody, KrishivFlightAction};
        // Verify the typed action correctly carries is_streaming.
        let body = BatchSqlBody {
            query: "SELECT * FROM events".to_owned(),
            tables: vec![],
            is_streaming: true,
        };
        let action = KrishivFlightAction::BatchSql(body);
        assert_eq!(action.action_type(), "krishiv.v1.batch_sql");
        match &action {
            KrishivFlightAction::BatchSql(b) => {
                assert!(b.is_streaming);
                assert_eq!(b.query, "SELECT * FROM events");
            }
            other => panic!("expected BatchSql, got {other:?}"),
        }
        // Verify batch (non-streaming) action.
        let batch_body = BatchSqlBody {
            query: "SELECT 1".to_owned(),
            tables: vec![],
            is_streaming: false,
        };
        let batch_action = KrishivFlightAction::BatchSql(batch_body);
        match &batch_action {
            KrishivFlightAction::BatchSql(b) => assert!(!b.is_streaming),
            other => panic!("expected BatchSql, got {other:?}"),
        }
    }

    #[test]
    fn batch_sql_action_round_trips() {
        use crate::flight_action::{BatchSqlBody, KrishivFlightAction};
        let action = KrishivFlightAction::BatchSql(BatchSqlBody {
            query: "SELECT id FROM t".to_owned(),
            tables: vec![],
            is_streaming: true,
        });
        let bytes = action.to_action_body().expect("encode");
        let decoded = KrishivFlightAction::from_action_body(&bytes).expect("decode");
        assert_eq!(action, decoded);
    }
}
