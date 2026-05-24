//! Unified execution runtime across Embedded, SingleNode, and Distributed modes.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
pub struct InProcessExecutionRuntime {
    mode: RuntimeMode,
    cluster: Arc<InProcessCluster>,
    backend: Mutex<EmbeddedBackend>,
}

impl InProcessExecutionRuntime {
    pub fn embedded(cluster: Arc<InProcessCluster>) -> Self {
        Self {
            mode: RuntimeMode::Embedded,
            cluster,
            backend: Mutex::new(EmbeddedBackend::default()),
        }
    }

    pub fn single_node(cluster: Arc<InProcessCluster>) -> Self {
        Self {
            mode: RuntimeMode::SingleNode,
            cluster,
            backend: Mutex::new(EmbeddedBackend::default()),
        }
    }
}

impl ExecutionRuntime for InProcessExecutionRuntime {
    fn mode(&self) -> RuntimeMode {
        self.mode
    }

    fn accept_plan(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        match self.mode {
            RuntimeMode::Embedded => self
                .backend
                .lock()
                .map_err(|_| RuntimeError::transport("runtime backend lock poisoned"))?
                .execute(plan),
            RuntimeMode::SingleNode => {
                let mut sn = SingleNodeBackend;
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
    session_mode: RuntimeMode,
    /// When set, bounded streaming also uses the in-process cluster (integration tests).
    local_fallback: Option<Arc<InProcessCluster>>,
}

impl RemoteExecutionRuntime {
    pub fn new(flight_url: impl Into<String>, session_mode: RuntimeMode) -> Self {
        Self {
            flight_url: flight_url.into(),
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
            RuntimeMode::SingleNode => {
                InProcessExecutionRuntime::single_node(Arc::clone(cluster))
            }
            RuntimeMode::Embedded | RuntimeMode::Distributed => {
                InProcessExecutionRuntime::embedded(Arc::clone(cluster))
            }
        };
        runtime.accept_plan(plan)
    }
}

impl ExecutionRuntime for RemoteExecutionRuntime {
    fn mode(&self) -> RuntimeMode {
        self.session_mode
    }

    fn accept_plan(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        if self.local_fallback.is_some() {
            return self.local_accept_plan(plan);
        }
        let mut backend = DistributedBackend::new(self.flight_url.clone());
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
        use krishiv_exec::execute_bounded_window;
        use crate::in_process_cluster::local_spec_to_plan_spec;
        execute_bounded_window(input_batches, &local_spec_to_plan_spec(spec))
            .map_err(|e| RuntimeError::transport(e.to_string()))
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
        block_on(crate::flight_client::execute_remote_sql(
            &self.flight_url,
            query,
        ))
    }

    fn register_continuous_stream(
        &self,
        job_id: &str,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<()> {
        let cluster = self.local_fallback.as_ref().ok_or_else(|| {
            RuntimeError::unsupported(
                "continuous streaming requires a local cluster fallback in distributed mode",
            )
        })?;
        cluster.register_continuous_job(job_id, spec)
    }

    fn push_continuous_stream_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> RuntimeResult<()> {
        let cluster = self.local_fallback.as_ref().ok_or_else(|| {
            RuntimeError::unsupported(
                "continuous streaming requires a local cluster fallback in distributed mode",
            )
        })?;
        cluster.push_continuous_input(job_id, batches)
    }

    fn drain_continuous_stream(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        let cluster = self.local_fallback.as_ref().ok_or_else(|| {
            RuntimeError::unsupported(
                "continuous streaming requires a local cluster fallback in distributed mode",
            )
        })?;
        cluster.drain_continuous_job(job_id)
    }

    fn flight_url(&self) -> Option<&str> {
        Some(&self.flight_url)
    }
}

/// Build the appropriate runtime for a session configuration.
pub fn build_execution_runtime(
    mode: RuntimeMode,
    cluster: Arc<InProcessCluster>,
    coordinator_flight_url: Option<String>,
) -> Arc<dyn ExecutionRuntime> {
    match mode {
        RuntimeMode::Embedded => {
            Arc::new(InProcessExecutionRuntime::embedded(Arc::clone(&cluster)))
        }
        RuntimeMode::SingleNode => {
            if let Some(url) = coordinator_flight_url {
                Arc::new(
                    RemoteExecutionRuntime::new(url, RuntimeMode::SingleNode)
                        .with_local_fallback(Arc::clone(&cluster)),
                )
            } else {
                Arc::new(InProcessExecutionRuntime::single_node(cluster))
            }
        }
        RuntimeMode::Distributed => {
            let url = coordinator_flight_url
                .unwrap_or_else(|| String::from("http://127.0.0.1:50051"));
            Arc::new(
                RemoteExecutionRuntime::new(url, RuntimeMode::Distributed)
                    .with_local_fallback(Arc::clone(&cluster)),
            )
        }
    }
}

/// Classify a plan for routing without executing it.
pub fn plan_execution_kind(plan: &PhysicalPlan) -> ExecutionKind {
    plan.kind()
}
