//! Barrier dispatch plan types.

use krishiv_proto::{ExecutorId, FencingToken, JobId, TaskId};

/// One barrier round-trip target for a running task on an executor.
#[derive(Debug, Clone)]
pub struct BarrierDispatchTarget {
    pub executor_id: ExecutorId,
    pub barrier_endpoint: String,
    pub task_id: TaskId,
}

/// Plan for dispatching one checkpoint epoch via BarrierService.
#[derive(Debug, Clone)]
pub struct BarrierDispatchPlan {
    pub job_id: JobId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
    pub targets: Vec<BarrierDispatchTarget>,
}
