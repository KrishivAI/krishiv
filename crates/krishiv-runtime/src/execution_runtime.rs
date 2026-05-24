//! Unified execution runtime across Embedded, SingleNode, and Distributed modes.

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_plan::{ExecutionKind, PhysicalPlan};

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

    /// Optional remote Flight URL (distributed / single-node daemon).
    fn flight_url(&self) -> Option<&str> {
        None
    }
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
}

/// Distributed / remote-cluster runtime (Flight SQL + optional in-process fallback for tests).
pub struct RemoteExecutionRuntime {
    flight_url: String,
    /// When set, bounded streaming also uses the in-process cluster (integration tests).
    local_fallback: Option<Arc<InProcessCluster>>,
}

impl RemoteExecutionRuntime {
    pub fn new(flight_url: impl Into<String>) -> Self {
        Self {
            flight_url: flight_url.into(),
            local_fallback: None,
        }
    }

    pub fn with_local_fallback(mut self, cluster: Arc<InProcessCluster>) -> Self {
        self.local_fallback = Some(cluster);
        self
    }
}

impl ExecutionRuntime for RemoteExecutionRuntime {
    fn mode(&self) -> RuntimeMode {
        RuntimeMode::Distributed
    }

    fn accept_plan(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
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
        // Direct operator execution when no remote streaming job API is configured.
        use krishiv_exec::execute_bounded_window;
        use crate::in_process_cluster::local_spec_to_plan_spec;
        execute_bounded_window(input_batches, &local_spec_to_plan_spec(spec))
            .map_err(|e| RuntimeError::transport(e.to_string()))
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
                    RemoteExecutionRuntime::new(url).with_local_fallback(Arc::clone(&cluster)),
                )
            } else {
                Arc::new(InProcessExecutionRuntime::single_node(cluster))
            }
        }
        RuntimeMode::Distributed => {
            let url = coordinator_flight_url
                .unwrap_or_else(|| String::from("http://127.0.0.1:50051"));
            Arc::new(
                RemoteExecutionRuntime::new(url).with_local_fallback(Arc::clone(&cluster)),
            )
        }
    }
}

/// Classify a plan for routing without executing it.
pub fn plan_execution_kind(plan: &PhysicalPlan) -> ExecutionKind {
    plan.kind()
}
