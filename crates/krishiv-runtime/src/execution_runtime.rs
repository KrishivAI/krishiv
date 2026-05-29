//! Unified execution runtime across Embedded, SingleNode, and Distributed modes.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use krishiv_plan::{ExecutionKind, PhysicalPlan};

use crate::in_process::BatchSqlTable;
use crate::in_process_cluster::InProcessCluster;
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{
    DistributedBackend, EmbeddedBackend, ExecutionBackend, ExecutionReport, RuntimeError,
    RuntimeResult, SingleNodeBackend,
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

    /// Whether data-plane work is routed to a remote Flight endpoint (no local fallback).
    fn uses_remote_execution(&self) -> bool {
        false
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

    /// Execute batch SQL through coordinator/Flight and return all result batches.
    fn collect_batch_sql(
        &self,
        query: &str,
        tables: &[BatchTableRegistration],
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

    fn accept_plan(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
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
    ) -> RuntimeResult<Vec<RecordBatch>> {
        self.cluster
            .collect_batch_sql(query, &tables_to_batch_sql(tables))
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

/// Distributed / remote-cluster runtime (Flight SQL + optional in-process fallback for tests).
pub struct RemoteExecutionRuntime {
    flight_url: String,
    coordinator_grpc_url: Option<String>,
    session_mode: RuntimeMode,
    /// When set, bounded streaming also uses the in-process cluster (integration tests).
    local_fallback: Option<Arc<InProcessCluster>>,
}

impl RemoteExecutionRuntime {
    pub fn new(
        flight_url: impl Into<String>,
        session_mode: RuntimeMode,
        coordinator_grpc_url: Option<String>,
    ) -> Self {
        Self {
            flight_url: flight_url.into(),
            coordinator_grpc_url,
            session_mode,
            local_fallback: None,
        }
    }

    pub fn with_local_fallback(mut self, cluster: Arc<InProcessCluster>) -> Self {
        self.local_fallback = Some(cluster);
        self
    }

    fn local_accept_plan(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        let cluster = self.local_fallback.as_ref().ok_or_else(|| {
            RuntimeError::unsupported("plan acceptance requires a local cluster fallback")
        })?;
        let runtime = match self.session_mode {
            RuntimeMode::Embedded => InProcessExecutionRuntime::embedded(Arc::clone(cluster)),
            RuntimeMode::SingleNode | RuntimeMode::Distributed => {
                InProcessExecutionRuntime::single_node(Arc::clone(cluster))
            }
        };
        runtime.accept_plan(plan)
    }
}

impl ExecutionRuntime for RemoteExecutionRuntime {
    fn mode(&self) -> RuntimeMode {
        self.session_mode
    }

    fn uses_remote_execution(&self) -> bool {
        self.local_fallback.is_none()
    }

    fn accept_plan(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        if self.local_fallback.is_some() {
            return self.local_accept_plan(plan);
        }
        let backend = DistributedBackend::new(self.flight_url.clone());
        backend.execute(plan)
    }

    fn collect_bounded_window(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        if let Some(cluster) = &self.local_fallback {
            return cluster.collect_bounded_window(topic, input_batches, spec);
        }
        use krishiv_async_util::block_on;
        block_on(crate::flight_client::execute_remote_bounded_window(
            &self.flight_url,
            topic,
            input_batches,
            spec,
        ))
    }

    fn collect_batch_sql(
        &self,
        query: &str,
        tables: &[BatchTableRegistration],
    ) -> RuntimeResult<Vec<RecordBatch>> {
        if let Some(cluster) = &self.local_fallback {
            return cluster.collect_batch_sql(query, &tables_to_batch_sql(tables));
        }
        use krishiv_async_util::block_on;
        block_on(crate::flight_client::execute_remote_batch_sql(
            &self.flight_url,
            query,
            &tables_to_batch_sql(tables),
        ))
    }

    fn explain_sql(&self, query: &str) -> RuntimeResult<String> {
        if self.local_fallback.is_some() {
            return krishiv_sql::explain_sql(query)
                .map_err(|e| RuntimeError::transport(e.to_string()));
        }
        use krishiv_async_util::block_on;
        block_on(crate::flight_client::execute_remote_explain(
            &self.flight_url,
            query,
        ))
    }

    fn register_continuous_stream(
        &self,
        job_id: &str,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<()> {
        if let Some(cluster) = &self.local_fallback {
            return cluster.register_continuous_job(job_id, spec);
        }
        use krishiv_async_util::block_on;
        block_on(crate::flight_client::execute_remote_continuous_register(
            &self.flight_url,
            job_id,
            spec,
        ))
    }

    fn push_continuous_stream_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> RuntimeResult<()> {
        if let Some(cluster) = &self.local_fallback {
            return cluster.push_continuous_input(job_id, batches);
        }
        use krishiv_async_util::block_on;
        block_on(crate::flight_client::execute_remote_continuous_push(
            &self.flight_url,
            job_id,
            batches,
        ))
    }

    fn drain_continuous_stream(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        if let Some(cluster) = &self.local_fallback {
            return cluster.drain_continuous_job(job_id);
        }
        use krishiv_async_util::block_on;
        block_on(crate::flight_client::execute_remote_continuous_drain(
            &self.flight_url,
            job_id,
        ))
    }

    fn flight_url(&self) -> Option<&str> {
        Some(&self.flight_url)
    }

    fn coordinator_grpc_url(&self) -> Option<&str> {
        self.coordinator_grpc_url.as_deref()
    }
}

/// Build the appropriate runtime for a session configuration.
pub fn build_execution_runtime(
    mode: RuntimeMode,
    cluster: Arc<InProcessCluster>,
    coordinator_flight_url: Option<String>,
    coordinator_grpc_url: Option<String>,
    remote_execution: bool,
) -> Arc<dyn ExecutionRuntime> {
    match mode {
        RuntimeMode::Embedded => {
            Arc::new(InProcessExecutionRuntime::embedded(Arc::clone(&cluster)))
        }
        RuntimeMode::SingleNode => {
            if let Some(url) = coordinator_flight_url {
                let mut remote =
                    RemoteExecutionRuntime::new(url, RuntimeMode::SingleNode, coordinator_grpc_url);
                if !remote_execution {
                    remote = remote.with_local_fallback(Arc::clone(&cluster));
                }
                Arc::new(remote)
            } else {
                Arc::new(InProcessExecutionRuntime::single_node(cluster))
            }
        }
        RuntimeMode::Distributed => {
            let url =
                coordinator_flight_url.unwrap_or_else(|| String::from("http://127.0.0.1:50051"));
            let mut remote =
                RemoteExecutionRuntime::new(url, RuntimeMode::Distributed, coordinator_grpc_url);
            if !remote_execution {
                remote = remote.with_local_fallback(Arc::clone(&cluster));
            }
            Arc::new(remote)
        }
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
        BatchTableRegistration, ClusterEndpoints, InProcessExecutionRuntime, RuntimeMode,
        build_execution_runtime,
    };
    use crate::ExecutionRuntime;
    use crate::InProcessCluster;
    use krishiv_plan::{ExecutionKind, PhysicalPlan};

    #[test]
    fn distributed_runtime_preserves_flight_and_grpc_urls() {
        let cluster = Arc::new(InProcessCluster::new().expect("cluster"));
        let runtime = build_execution_runtime(
            RuntimeMode::Distributed,
            cluster,
            Some(String::from("http://127.0.0.1:50051")),
            Some(String::from("http://127.0.0.1:9090")),
            false,
        );

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
        let batches = rt.collect_batch_sql("SELECT 1 AS n", &[]).unwrap();
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
        let rt = build_execution_runtime(RuntimeMode::Embedded, cluster, None, None, false);
        assert_eq!(rt.mode(), RuntimeMode::Embedded);
    }

    #[test]
    fn build_runtime_single_node_no_flight_url() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(RuntimeMode::SingleNode, cluster, None, None, false);
        assert_eq!(rt.mode(), RuntimeMode::SingleNode);
        assert!(rt.flight_url().is_none());
    }

    #[test]
    fn build_runtime_single_node_with_flight_url_and_fallback() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(
            RuntimeMode::SingleNode,
            cluster,
            Some("http://127.0.0.1:50051".into()),
            None,
            false,
        );
        assert_eq!(rt.flight_url(), Some("http://127.0.0.1:50051"));
    }

    #[test]
    fn build_runtime_distributed_default_flight_url() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(RuntimeMode::Distributed, cluster, None, None, false);
        assert_eq!(rt.flight_url(), Some("http://127.0.0.1:50051"));
    }

    #[test]
    fn build_runtime_distributed_with_custom_flight_url() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(
            RuntimeMode::Distributed,
            cluster,
            Some("http://remote:50051".into()),
            None,
            false,
        );
        assert_eq!(rt.flight_url(), Some("http://remote:50051"));
    }

    #[test]
    fn build_runtime_distributed_remote_execution() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(
            RuntimeMode::Distributed,
            cluster,
            Some("http://remote:50051".into()),
            None,
            true,
        );
        assert!(rt.uses_remote_execution());
    }

    #[test]
    fn build_runtime_distributed_with_fallback() {
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(
            RuntimeMode::Distributed,
            cluster,
            Some("http://remote:50051".into()),
            None,
            false,
        );
        assert!(!rt.uses_remote_execution());
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
        let cluster = Arc::new(InProcessCluster::new().unwrap());
        let rt = build_execution_runtime(
            RuntimeMode::Distributed,
            cluster,
            Some("http://custom:50051".into()),
            Some("http://custom:9090".into()),
            false,
        );
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
}
