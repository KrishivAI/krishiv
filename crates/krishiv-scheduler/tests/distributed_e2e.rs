//! In-process distributed execution smoke tests (WS-0 / GAP-T2).

use krishiv_plan::{ExecutionKind, NodeOp, PhysicalPlan, PlanNode};
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorId, JobId, JobKind, JobSpec, StageId, StageSpec,
    TaskId, TaskSpec,
};
use krishiv_scheduler::{Coordinator, SharedCoordinator};

#[tokio::test]
async fn in_process_batch_job_submits_with_plan_op_lowering() {
    let coord_id = CoordinatorId::try_new("e2e-coord").unwrap();
    let mut coord = Coordinator::active(coord_id);
    let exec_id = ExecutorId::try_new("e2e-exec").unwrap();
    coord
        .register_executor(ExecutorDescriptor::new(
            exec_id,
            krishiv_scheduler::IN_PROCESS_TASK_ENDPOINT,
            2,
        ))
        .unwrap();
    let shared = SharedCoordinator::new(coord);
    let job_id = JobId::try_new("e2e-batch").unwrap();
    let node = PlanNode::new("scan", "parquet", ExecutionKind::Batch).with_op(NodeOp::Scan {
        table: String::from("t"),
        filters: vec![],
    });
    let fragment = krishiv_plan::encode_typed_task_fragment(&node).expect("encode");
    assert!(fragment.contains("sql:"));
    let stage = StageSpec::new(StageId::try_new("s1").unwrap(), "stage")
        .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), fragment));
    let spec = JobSpec::new(job_id.clone(), "e2e", JobKind::Batch).with_stage(stage);
    shared.write().await.submit_job(spec).unwrap();
    assert_eq!(
        shared.read().await.job_snapshot(&job_id).unwrap().job_id(),
        &job_id
    );
}

#[test]
fn in_process_streaming_window_lowers_to_stream_fragment() {
    use krishiv_plan::encode_typed_task_fragment;
    use krishiv_plan::window::WindowExecutionSpec;
    let spec = WindowExecutionSpec::tumbling("user_id", "ts", 1_000);
    let node = PlanNode::new("w", "win", ExecutionKind::Streaming).with_op(NodeOp::Window {
        spec: Box::new(spec),
    });
    let frag = encode_typed_task_fragment(&node).expect("encode");
    assert!(
        frag.contains("stream:spec:v1:"),
        "expected lossless format, got: {frag}"
    );
    let plan = PhysicalPlan::new("stream-plan", ExecutionKind::Streaming).with_node(node);
    assert_eq!(plan.kind(), ExecutionKind::Streaming);
}

#[test]
fn in_process_bridge_endpoint_constant() {
    assert!(krishiv_scheduler::is_in_process_task_endpoint(
        krishiv_scheduler::IN_PROCESS_TASK_ENDPOINT
    ));
}
