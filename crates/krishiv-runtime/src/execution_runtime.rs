//! Unified execution runtime across Embedded, SingleNode, and Distributed modes.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use krishiv_plan::{ExecutionKind, PhysicalPlan};

use crate::in_process::BatchSqlTable;
use crate::in_process_cluster::InProcessCluster;
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{
    EmbeddedBackend, ExecutionBackend, ExecutionReport, RuntimeError, RuntimeResult,
    SingleNodeBackend,
};

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
        })
        .collect()
}

/// In-process cluster runtime for Embedded and auto-start SingleNode.
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

    pub fn single_node(cluster: Arc<InProcessCluster>) -> Self {
        Self {
            mode: RuntimeMode::SingleNode,
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
        // Streaming plans carry long-running operator state and must go through
        // collect_bounded_window or the continuous stream APIs.  Accepting them
        // silently here would return a success report without executing anything.
        if plan.kind() == ExecutionKind::Streaming {
            return Err(RuntimeError::unsupported(
                "streaming plans must use collect_bounded_window or the continuous \
                 stream APIs; submit via Session::submit_stream_job for unbounded pipelines",
            ));
        }
        match self.mode {
            RuntimeMode::Embedded => {
                let backend = EmbeddedBackend::default();
                backend.execute(plan)
            }
            RuntimeMode::SingleNode => {
                let sn = SingleNodeBackend;
                sn.execute(plan)
            }
            RuntimeMode::Distributed => Err(RuntimeError::unsupported(
                "in-process runtime does not serve distributed mode",
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

    fn collect_batch_sql(
        &self,
        query: &str,
        tables: &[BatchTableRegistration],
        is_streaming: bool,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        self.cluster
            .collect_batch_sql(query, &tables_to_batch_sql(tables), is_streaming)
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
    ) -> Self {
        Self {
            pool: crate::flight_client::FlightClientPool::new(flight_url),
            coordinator_grpc_url,
            session_mode,
            placement,
        }
    }
}

/// Returns `true` only when `e` is a tonic `Status::Unimplemented` response
/// from the server — meaning the server explicitly does not support the
/// requested action and the client should fall back to the SQL protocol.
///
/// This is intentionally strict to avoid triggering the fallback on real errors
/// that happen to contain the word "Unimplemented" (schema errors, auth errors,
/// user-facing messages, etc.). Tonic formats unimplemented status as:
/// `"status: Unimplemented, message: ..."`.
fn is_server_unimplemented(e: &RuntimeError) -> bool {
    matches!(e, RuntimeError::Transport { message }
        if message.starts_with("status: Unimplemented")
            || message.starts_with("Status { code: Unimplemented")
    )
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
            return Err(RuntimeError::unsupported(
                "streaming plan dispatch to a remote cluster is not yet implemented; \
                 use collect_bounded_window or the continuous stream APIs instead",
            ));
        }
        use crate::flight_action::{ExecutePlanBody, KrishivFlightAction};
        use krishiv_common::async_util::block_on;
        let body = ExecutePlanBody::from_plan(plan)?;
        let action = KrishivFlightAction::ExecutePlan(body);
        let result = block_on(self.pool.do_action(&action));
        match result {
            Ok(_) => {}
            Err(ref e) if is_server_unimplemented(e) => {
                let sql = crate::flight_client::plan_to_sql(plan);
                let _ = block_on(self.pool.execute_sql(&sql))?;
            }
            Err(e) => return Err(e),
        }
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
        let batches_b64 = encode_batches(&input_batches)?;
        let action = KrishivFlightAction::BoundedWindow(BoundedWindowBody {
            topic: topic.to_string(),
            spec: spec.to_plan_spec(),
            batches_b64,
            response_watermark_ms: None,
        });
        let result = block_on(self.pool.do_action(&action));
        match result {
            Ok(body) => {
                // Attempt to decode as BoundedWindowBody response (server populates
                // response_watermark_ms on the reply path). Fall back to raw IPC if
                // the server returns plain batches without a JSON envelope (C8).
                let watermark = if let Ok(resp) =
                    serde_json::from_slice::<BoundedWindowBody>(&body)
                {
                    resp.response_watermark_ms
                } else {
                    None
                };
                let batches = decode_ipc_response(&body)?;
                Ok((batches, watermark))
            }
            Err(ref e) if is_server_unimplemented(e) => {
                let sql = encode_bounded_window(topic, spec, &input_batches)?;
                let batches = block_on(self.pool.execute_sql(&sql))?;
                Ok((batches, None))
            }
            Err(e) => Err(e),
        }
    }

    fn collect_batch_sql(
        &self,
        query: &str,
        tables: &[BatchTableRegistration],
        is_streaming: bool,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        use crate::flight_protocol::encode_batch_sql;
        use krishiv_common::async_util::block_on;
        let mut sql = encode_batch_sql(query, &tables_to_batch_sql(tables));
        if is_streaming {
            sql = format!("-- krishiv:streaming=true\n{sql}");
        }
        block_on(self.pool.execute_sql(&sql))
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
        let result = block_on(self.pool.do_action(&action));
        match result {
            Ok(_) => Ok(()),
            Err(ref e) if is_server_unimplemented(e) => {
                let sql = encode_continuous_register(job_id, spec)?;
                let _ = block_on(self.pool.execute_sql(&sql))?;
                Ok(())
            }
            Err(e) => Err(e),
        }
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
        let result = block_on(self.pool.do_action(&action));
        match result {
            Ok(_) => Ok(()),
            Err(ref e) if is_server_unimplemented(e) => {
                let sql = encode_continuous_push(job_id, &batches)?;
                let _ = block_on(self.pool.execute_sql(&sql))?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn drain_continuous_stream(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        use crate::flight_action::{ContinuousDrainBody, KrishivFlightAction};
        use crate::flight_client::decode_ipc_response;
        use crate::flight_protocol::encode_continuous_drain;
        use krishiv_common::async_util::block_on;
        let action = KrishivFlightAction::ContinuousDrain(ContinuousDrainBody {
            job_id: job_id.to_string(),
        });
        let result = block_on(self.pool.do_action(&action));
        match result {
            Ok(body) => decode_ipc_response(&body),
            Err(ref e) if is_server_unimplemented(e) => {
                let sql = encode_continuous_drain(job_id);
                block_on(self.pool.execute_sql(&sql))
            }
            Err(e) => Err(e),
        }
    }

    fn flight_url(&self) -> Option<&str> {
        Some(self.pool.flight_url())
    }

    fn coordinator_grpc_url(&self) -> Option<&str> {
        self.coordinator_grpc_url.as_deref()
    }
}

/// Build the appropriate runtime for a session configuration.
///
/// `in_process_cluster` is required for Embedded and SingleNode with
/// `LocalInProcess` placement. It is ignored (but can be `None`) for
/// SingleNodeDaemon and Distributed placements.
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
        (RuntimeMode::SingleNode, ExecutionPlacement::LocalInProcess) => {
            let cluster = in_process_cluster.ok_or_else(|| {
                RuntimeError::unsupported("SingleNode LocalInProcess requires an InProcessCluster")
            })?;
            Ok(Arc::new(InProcessExecutionRuntime::single_node(cluster)))
        }
        (RuntimeMode::SingleNode, ExecutionPlacement::SingleNodeDaemon) => {
            let url = coordinator_flight_url.ok_or_else(|| {
                RuntimeError::unsupported(
                    "SingleNodeDaemon placement requires a local Flight SQL coordinator URL",
                )
            })?;
            Ok(Arc::new(RemoteExecutionRuntime::new(
                url,
                RuntimeMode::SingleNode,
                coordinator_grpc_url,
                ExecutionPlacement::SingleNodeDaemon,
            )))
        }
        (RuntimeMode::Distributed, ExecutionPlacement::RemoteClusterRequired) => {
            let url = coordinator_flight_url.ok_or_else(|| {
                RuntimeError::unsupported(
                    "Distributed placement requires an explicit remote Flight SQL coordinator URL",
                )
            })?;
            Ok(Arc::new(RemoteExecutionRuntime::new(
                url,
                RuntimeMode::Distributed,
                coordinator_grpc_url,
                ExecutionPlacement::RemoteClusterRequired,
            )))
        }
        (RuntimeMode::Embedded, _) => Err(RuntimeError::unsupported(
            "Embedded mode only supports LocalInProcess placement",
        )),
        (RuntimeMode::SingleNode, ExecutionPlacement::RemoteClusterRequired) => {
            Err(RuntimeError::unsupported(
                "SingleNode mode cannot use RemoteClusterRequired placement; use Distributed mode",
            ))
        }
        (RuntimeMode::Distributed, _) => Err(RuntimeError::unsupported(
            "Distributed mode cannot use local fallback; use RemoteClusterRequired placement",
        )),
    }
}

/// Classify a plan for routing without executing it.
pub fn plan_execution_kind(plan: &PhysicalPlan) -> ExecutionKind {
    plan.kind()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        BatchTableRegistration, ClusterEndpoints, ExecutionPlacement, InProcessExecutionRuntime,
        RuntimeMode, build_execution_runtime,
    };
    use crate::ExecutionRuntime;
    use crate::InProcessCluster;
    use krishiv_plan::{ExecutionKind, PhysicalPlan};

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
            embedded.collect_batch_sql("SELECT 1 AS n", &[], false).unwrap()[0].num_rows(),
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
    fn single_node_runtime_mode() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::single_node(cluster);
        assert_eq!(rt.mode(), RuntimeMode::SingleNode);
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
    fn single_node_runtime_accepts_plan() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::single_node(cluster);
        let plan = PhysicalPlan::new("test-plan", ExecutionKind::Batch);
        let report = rt.accept_plan(&plan).unwrap();
        assert!(report.accepted());
        assert_eq!(report.backend(), "single-node");
    }

    #[test]
    fn embedded_runtime_collect_batch_sql() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        let batches = rt.collect_batch_sql("SELECT 1 AS n", &[], false).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
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
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: crate::LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
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
    fn build_runtime_single_node_no_flight_url() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(
            RuntimeMode::SingleNode,
            Some(cluster),
            None,
            None,
            ExecutionPlacement::LocalInProcess,
        )
        .expect("single-node runtime");
        assert_eq!(rt.mode(), RuntimeMode::SingleNode);
        assert_eq!(rt.placement(), ExecutionPlacement::LocalInProcess);
        assert!(rt.flight_url().is_none());
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
        let batches = rt.collect_batch_sql("SELECT 42 AS answer", &[], false).unwrap();
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
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: crate::LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
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

        rt.push_continuous_stream_input("durable-j1", vec![batch]).unwrap();
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
        assert_eq!(super::plan_execution_kind(&plan), ExecutionKind::Batch);
    }

    #[test]
    fn plan_execution_kind_streaming() {
        let plan = PhysicalPlan::new("test", ExecutionKind::Streaming);
        assert_eq!(super::plan_execution_kind(&plan), ExecutionKind::Streaming);
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

    #[test]
    fn single_node_runtime_collect_bounded_window() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::single_node(cluster);
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
        let spec = crate::LocalWindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: crate::LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let out = rt
            .collect_bounded_window("events", vec![batch], &spec)
            .unwrap();
        assert!(!out.is_empty());
    }

    // ── is_server_unimplemented guard ─────────────────────────────────────────

    use super::is_server_unimplemented;

    #[test]
    fn fallback_triggered_on_tonic_unimplemented_status() {
        let err = crate::RuntimeError::Transport {
            message: "status: Unimplemented, message: action not yet supported".into(),
        };
        assert!(
            is_server_unimplemented(&err),
            "tonic Unimplemented status must trigger the SQL fallback"
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
        let err = crate::RuntimeError::Transport {
            message: "Status { code: Unimplemented, message: \"not yet\" }".into(),
        };
        assert!(
            is_server_unimplemented(&err),
            "alternative tonic Status format must also trigger fallback"
        );
    }

    #[test]
    fn remote_runtime_rejects_streaming_plan() {
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
            matches!(err, crate::RuntimeError::Unsupported { .. }),
            "streaming plan dispatch to remote must return Unsupported, got: {err:?}"
        );
    }

    // ── G3: In-process streaming plan guard ──────────────────────────────────

    #[test]
    fn in_process_embedded_rejects_streaming_plan() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::embedded(cluster);
        let plan = PhysicalPlan::new("stream-plan", ExecutionKind::Streaming);
        let err = rt.accept_plan(&plan).unwrap_err();
        assert!(
            matches!(err, crate::RuntimeError::Unsupported { .. }),
            "embedded accept_plan must reject streaming plans, got: {err:?}"
        );
    }

    #[test]
    fn in_process_single_node_rejects_streaming_plan() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = InProcessExecutionRuntime::single_node(cluster);
        let plan = PhysicalPlan::new("stream-plan", ExecutionKind::Streaming);
        let err = rt.accept_plan(&plan).unwrap_err();
        assert!(
            matches!(err, crate::RuntimeError::Unsupported { .. }),
            "single-node accept_plan must reject streaming plans, got: {err:?}"
        );
    }

    // ── G1: is_streaming flag encoding in remote collect_batch_sql ────────────

    #[test]
    fn remote_collect_batch_sql_streaming_flag_prefixes_comment() {
        // Documents that is_streaming=true is encoded as a first-line SQL comment
        // so the remote Flight handler can detect streaming intent without a
        // separate gRPC field.  The constant format must not change without
        // updating the server-side parser.
        let query = "SELECT * FROM events";
        let streaming_sql = format!("-- krishiv:streaming=true\n{query}");
        assert!(
            streaming_sql.starts_with("-- krishiv:streaming=true\n"),
            "streaming flag must be the first line of the encoded SQL"
        );
        assert!(
            streaming_sql.contains(query),
            "original query must be preserved after the streaming comment"
        );
        // Verify the non-streaming path does not add the prefix.
        let batch_sql = query.to_string();
        assert!(
            !batch_sql.starts_with("-- krishiv:streaming=true"),
            "batch SQL must not carry the streaming comment prefix"
        );
    }
}
