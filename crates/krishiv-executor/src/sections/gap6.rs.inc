// ── GAP-6 helpers ─────────────────────────────────────────────────────────

/// A drainer that returns a fixed set of Arrow batches exactly once, then
/// returns an empty vec on every subsequent call.  Used to verify that
/// `stream:loop:` fragments process batches via `ContinuousWindowExecutor`.
struct OneShotDrainer {
    batches: std::sync::Mutex<Option<Vec<arrow::record_batch::RecordBatch>>>,
}

impl OneShotDrainer {
    fn new(batches: Vec<arrow::record_batch::RecordBatch>) -> Arc<Self> {
        Arc::new(Self {
            batches: std::sync::Mutex::new(Some(batches)),
        })
    }
}

impl crate::ContinuousJobDrainer for OneShotDrainer {
    fn drain_job(
        &self,
        _job_id: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        let mut guard = self.batches.lock().unwrap();
        Ok(guard.take().unwrap_or_default())
    }
}

fn make_loop_assignment(fragment: &str) -> ExecutorTaskAssignment {
    let ids = TaskAttemptRef::new(
        JobId::try_new("loop-job-1").unwrap(),
        StageId::try_new("stage-1").unwrap(),
        TaskId::try_new("task-1").unwrap(),
        AttemptId::initial(),
    );
    ExecutorTaskAssignment::new(
        ids,
        ExecutorId::try_new("exec-1").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new(fragment),
        OutputContract::new(
            krishiv_proto::OutputContractKind::InlineRecordBatches,
            "loop output",
        ),
    )
}

fn make_loop_assignment_with_batches(
    fragment: &str,
    batches: &[RecordBatch],
) -> ExecutorTaskAssignment {
    use arrow::ipc::writer::StreamWriter;

    let mut ipc_bytes = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut ipc_bytes, &batches[0].schema()).expect("ipc writer");
        for batch in batches {
            writer.write(batch).expect("ipc batch");
        }
        writer.finish().expect("ipc finish");
    }
    make_loop_assignment(fragment).with_input_partitions(vec![InputPartition::typed(
        "continuous-input",
        InputPartitionDescriptor::InlineIpc {
            table_name: "input".into(),
            ipc_bytes,
        },
    )])
}

fn make_streaming_assignment(fragment: &str, partitions: Vec<&str>) -> ExecutorTaskAssignment {
    let ids = TaskAttemptRef::new(
        JobId::try_new("stream-job-1").unwrap(),
        StageId::try_new("stage-1").unwrap(),
        TaskId::try_new("task-1").unwrap(),
        AttemptId::initial(),
    );
    let assignment = ExecutorTaskAssignment::new(
        ids,
        ExecutorId::try_new("exec-1").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new(fragment),
        OutputContract::new(
            krishiv_proto::OutputContractKind::InlineRecordBatches,
            "streaming output",
        ),
    );
    let input_partitions: Vec<krishiv_proto::InputPartition> = partitions
        .iter()
        .enumerate()
        .map(|(i, desc)| krishiv_proto::InputPartition::new(format!("p{i}"), *desc))
        .collect();
    assignment.with_input_partitions(input_partitions)
}

#[tokio::test]
async fn streaming_tumbling_window_count_produces_correct_output() {
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let assignment = make_streaming_assignment(
        "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
        vec![
            "stream-kafka:events:0:0:key=a,ts=100,val=1|key=b,ts=200,val=1|key=a,ts=300,val=1",
        ],
    );
    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .unwrap();

    assert_eq!(
        output.kind(),
        crate::ExecutorTaskOutputKind::StreamingWindow
    );
    assert_eq!(output.row_count(), 2);
    assert_eq!(output.batch_count(), 2);
}

#[tokio::test]
async fn executor_runs_typed_streaming_fragment_body() {
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let fragment = krishiv_plan::TypedTaskFragment::new(
        krishiv_plan::ExecutionKind::Streaming,
        "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
    )
    .encode()
    .unwrap();
    let assignment = make_streaming_assignment(
        &fragment,
        vec![
            "stream-kafka:events:0:0:key=a,ts=100,val=1|key=b,ts=200,val=1|key=a,ts=300,val=1",
        ],
    );
    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .unwrap();

    assert_eq!(
        output.kind(),
        crate::ExecutorTaskOutputKind::StreamingWindow
    );
    assert_eq!(output.row_count(), 2);
}

#[tokio::test]
async fn streaming_tumbling_window_sum_produces_correct_output() {
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let assignment = make_streaming_assignment(
        "stream:tw:key=key:time=ts:win=1000:lag=0:agg=sum:col=val",
        vec![
            "stream-kafka:events:0:0:key=x,ts=100,val=10|key=x,ts=200,val=20|key=x,ts=300,val=30",
        ],
    );
    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .unwrap();

    assert_eq!(
        output.kind(),
        crate::ExecutorTaskOutputKind::StreamingWindow
    );
    assert_eq!(output.row_count(), 1);
    assert_eq!(output.batch_count(), 1);
}

#[tokio::test]
async fn streaming_fragment_with_multiple_partitions() {
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let assignment = make_streaming_assignment(
        "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
        vec![
            "stream-kafka:events:0:0:key=a,ts=100,val=0",
            "stream-kafka:events:1:0:key=a,ts=200,val=0|key=b,ts=300,val=0",
        ],
    );
    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .unwrap();
    assert_eq!(output.row_count(), 2);
}

#[tokio::test]
async fn streaming_fragment_invalid_fragment_returns_error() {
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let assignment = make_streaming_assignment(
        "stream:unknown-operator",
        vec!["stream-kafka:t:0:0:key=a,ts=100,val=1"],
    );
    let err = runner
        .execute_streaming_fragment(&assignment)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::ExecutorError::InvalidAssignment { .. }),
        "expected InvalidAssignment, got {err}"
    );
}

#[tokio::test]
async fn streaming_fragment_routes_data_through_operator_queue() {
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let assignment = make_streaming_assignment(
        "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
        vec![
            "stream-kafka:events:0:0:key=a,ts=100,val=1|key=b,ts=200,val=1|key=a,ts=300,val=1",
        ],
    );
    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .unwrap();

    assert_eq!(
        output.kind(),
        crate::ExecutorTaskOutputKind::StreamingWindow
    );
    assert_eq!(output.row_count(), 2);
    assert_eq!(output.batch_count(), 2);
}

#[test]
fn barrier_simulator_accepts_increasing_epochs() {
    let mut sim = BarrierSimulator::new();
    sim.process_barrier(1, 1000, 0).unwrap();
    sim.process_barrier(2, 2000, 1).unwrap();
    sim.process_barrier(3, 3000, 0).unwrap();
    assert_eq!(sim.last_committed_epoch(), 3);
    assert_eq!(sim.snapshots().len(), 3);
}

#[test]
fn barrier_simulator_rejects_stale_epoch() {
    let mut sim = BarrierSimulator::new();
    sim.process_barrier(1, 1000, 0).unwrap();
    let err = sim.process_barrier(1, 2000, 0).unwrap_err();
    assert!(
        matches!(err, crate::ExecutorError::InvalidAssignment { .. }),
        "expected InvalidAssignment for stale epoch, got {err}"
    );
    assert_eq!(sim.last_committed_epoch(), 1);
}

#[test]
fn barrier_simulator_rejects_zero_epoch_after_commit() {
    let mut sim = BarrierSimulator::new();
    sim.process_barrier(5, 1000, 0).unwrap();
    let err = sim.process_barrier(0, 2000, 0).unwrap_err();
    assert!(matches!(
        err,
        crate::ExecutorError::InvalidAssignment { .. }
    ));
}

#[test]
fn barrier_snapshot_records_watermark_and_open_windows() {
    let mut sim = BarrierSimulator::new();
    sim.process_barrier(1, 5000, 3).unwrap();
    assert_eq!(
        sim.snapshots()[0],
        BarrierSnapshot {
            epoch: 1,
            watermark_ms: 5000,
            open_windows: 3,
        }
    );
}

#[test]
fn barrier_watermark_monotonicity_enforced_by_operator_not_simulator() {
    let mut sim = BarrierSimulator::new();
    sim.process_barrier(1, 1000, 0).unwrap();
    sim.process_barrier(2, 999, 0).unwrap();
    assert_eq!(sim.snapshots()[1].watermark_ms, 999);
}

#[tokio::test]
async fn deterministic_replay_end_to_end() {
    let fragment = "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count";
    let partition = "stream-kafka:events:0:0:key=a,ts=100,val=0|key=b,ts=150,val=0|key=a,ts=200,val=0\
         |key=c,ts=500,val=0|key=a,ts=800,val=0|key=b,ts=900,val=0";

    let run = || async {
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let assignment = make_streaming_assignment(fragment, vec![partition]);
        runner
            .execute_streaming_fragment(&assignment)
            .await
            .unwrap()
    };

    let run1 = run().await;
    let run2 = run().await;

    assert_eq!(
        run1.row_count(),
        run2.row_count(),
        "replay row_count must match"
    );
    assert_eq!(
        run1.batch_count(),
        run2.batch_count(),
        "replay batch_count must match"
    );
    assert_eq!(
        run1.column_count(),
        run2.column_count(),
        "replay column_count must match"
    );
    assert_eq!(run1.kind(), run2.kind(), "replay output kind must match");
}

#[tokio::test]
async fn streaming_e2e_full_stack_job_stays_running() {
    allow_anonymous_for_tests();
    let executor_id = ExecutorId::try_new("exec-e2e-fs").unwrap();
    let job_id = JobId::try_new("job-e2e-fs-1").unwrap();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-e2e-fs").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared.clone());
    let inbox = ExecutorAssignmentInbox::new();

    {
        let mut coordinator = shared.write().await;
        coordinator
            .register_executor(krishiv_proto::ExecutorDescriptor::new(
                executor_id.clone(),
                "pod-e2e",
                1,
            ))
            .unwrap();
        let job = JobSpec::new(job_id.clone(), "e2e streaming", JobKind::Streaming).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "stream-stage").with_task(
                TaskSpec::new(
                    TaskId::try_new("task-1").unwrap(),
                    "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
                ),
            ),
        );
        coordinator.submit_job(job).unwrap();
        coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
    }

    let (stage_id, task_id, attempt, lease) = {
        let coordinator = shared.read().await;
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let stage_id = detail.stages()[0].stage_id().clone();
        let task_id = detail.stages()[0].tasks()[0].task_id().clone();
        let attempt = detail.stages()[0].tasks()[0].attempt();
        let lease = coordinator.executor_snapshots()[0].lease_generation();
        (stage_id, task_id, attempt, lease)
    };

    let ids = TaskAttemptRef::new(
        job_id.clone(),
        stage_id,
        task_id,
        AttemptId::try_new(attempt).unwrap(),
    );
    let assignment = ExecutorTaskAssignment::new(
        ids,
        executor_id,
        lease,
        PlanFragment::new("stream:tw:key=key:time=ts:win=1000:lag=0:agg=count"),
        OutputContract::new(OutputContractKind::InlineRecordBatches, "streaming output"),
    )
    .with_input_partitions(vec![InputPartition::new(
        "p0",
        "stream-kafka:events:0:0:key=a,ts=100,val=1|key=b,ts=200,val=1|key=a,ts=300,val=1",
    )]);
    inbox.push(assignment).unwrap();

    let runner = ExecutorTaskRunner::new(inbox.clone());
    let report = runner.run_next_with(&service).await.unwrap().unwrap();

    assert_eq!(
        report.output().kind(),
        ExecutorTaskOutputKind::StreamingWindow
    );
    assert_eq!(
        report.output().row_count(),
        2,
        "a and b each get one window row"
    );
    assert!(inbox.is_empty().unwrap());

    let state = shared.read().await.job_snapshot(&job_id).unwrap().state();
    assert_eq!(
        state,
        JobState::Running,
        "streaming job must remain Running even after all tasks report terminal output"
    );
}

#[tokio::test]
async fn streaming_e2e_coordinator_reattach_preserves_watermark() {
    let executor_id = ExecutorId::try_new("exec-e2e-ra").unwrap();
    let job_id = JobId::try_new("job-e2e-ra-1").unwrap();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-e2e-ra").unwrap(),
    ));

    {
        let mut coordinator = shared.write().await;
        coordinator
            .register_executor(krishiv_proto::ExecutorDescriptor::new(
                executor_id.clone(),
                "pod-ra",
                1,
            ))
            .unwrap();
        let job = JobSpec::new(job_id.clone(), "ra streaming", JobKind::Streaming).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "stream-stage").with_task(
                TaskSpec::new(
                    TaskId::try_new("task-1").unwrap(),
                    "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
                ),
            ),
        );
        coordinator.submit_job(job).unwrap();
        coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
    }

    let task_id = {
        let coordinator = shared.read().await;
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        detail.stages()[0].tasks()[0].task_id().clone()
    };

    {
        let mut coordinator = shared.write().await;
        let mut store = InMemoryMetadataStore::default();
        coordinator.persist_jobs_to_store(&mut store).unwrap();
        coordinator.recover_from_store(&mut store).unwrap();
    }

    // Read the lease AFTER recover_from_store: the recovery path calls
    // advance_clock(heartbeat_timeout_ticks) to evict stale executors, which
    // marks the registered executor Lost and bumps its lease generation.
    // The heartbeat must use the current (post-recovery) lease.
    let lease = shared.read().await.executor_snapshots()[0].lease_generation();

    let reported_watermark_ms: i64 = 7_500;
    let reported_offset = b"events:0:offset-99".to_vec();
    {
        let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
            .with_lease_generation(lease)
            .with_streaming_task_states(vec![StreamingTaskState::new(
                task_id.clone(),
                reported_watermark_ms,
                reported_offset.clone(),
            )]);
        shared.write().await.executor_heartbeat(heartbeat).unwrap();
    }

    let coordinator = shared.read().await;
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task = &detail.stages()[0].tasks()[0];
    assert_eq!(
        task.last_watermark_ms(),
        Some(reported_watermark_ms),
        "coordinator must record executor-reported watermark on re-attach"
    );
    assert_eq!(
        task.last_source_offset(),
        Some(reported_offset.as_slice()),
        "coordinator must record executor-reported source offset on re-attach"
    );

    assert_eq!(
        coordinator.job_snapshot(&job_id).unwrap().state(),
        JobState::Running,
        "job must remain Running after coordinator re-attach"
    );
    drop(coordinator);

    assert_eq!(
        shared.read().await.job_snapshots().len(),
        1,
        "coordinator must not create a duplicate job on re-attach"
    );
}

use krishiv_proto::InitiateCheckpointRequest;
use krishiv_state::checkpoint::{CheckpointStorage, LocalFsCheckpointStorage, snapshot_path};
use krishiv_state::{RocksDbStateBackend, StateBackend};

use crate::runner::TaskRunner;

#[derive(Debug, Default)]
struct FailingCheckpointStorage;

#[tonic::async_trait]
impl CheckpointStorage for FailingCheckpointStorage {
    async fn write_bytes_async(
        &self,
        _path: &str,
        _data: &[u8],
    ) -> krishiv_state::checkpoint::CheckpointResult<()> {
        Err(krishiv_state::checkpoint::CheckpointError::Storage {
            message: "injected write failure".to_string(),
        })
    }

    async fn read_bytes_async(
        &self,
        _path: &str,
    ) -> krishiv_state::checkpoint::CheckpointResult<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn list_dir_async(
        &self,
        _prefix: &str,
    ) -> krishiv_state::checkpoint::CheckpointResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn delete_prefix_async(
        &self,
        _prefix: &str,
    ) -> krishiv_state::checkpoint::CheckpointResult<()> {
        Ok(())
    }

    fn write_bytes(
        &self,
        _path: &str,
        _data: &[u8],
    ) -> krishiv_state::checkpoint::CheckpointResult<()> {
        Err(krishiv_state::checkpoint::CheckpointError::Storage {
            message: "injected write failure".to_string(),
        })
    }

    fn read_bytes(
        &self,
        _path: &str,
    ) -> krishiv_state::checkpoint::CheckpointResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn list_dir(
        &self,
        _prefix: &str,
    ) -> krishiv_state::checkpoint::CheckpointResult<Vec<String>> {
        Ok(Vec::new())
    }

    fn delete_prefix(&self, _prefix: &str) -> krishiv_state::checkpoint::CheckpointResult<()> {
        Ok(())
    }
}

#[test]
fn executor_checkpoint_takes_state_snapshot_and_writes_to_storage() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let task_id = TaskId::try_new("task-cp-1").unwrap();
    let job_id = JobId::try_new("job-cp-1").unwrap();
    let mut runner = TaskRunner::new(task_id.clone());

    let mut backend = RocksDbStateBackend::ephemeral().unwrap();
    let ns = krishiv_state::Namespace::new("operator-task-cp-1", "my-state");
    backend
        .put(&ns, b"key1".to_vec(), b"value1".to_vec())
        .unwrap();
    let state = crate::runner::CheckpointStateHandle::from_backend(backend);

    let req = InitiateCheckpointRequest {
        job_id: job_id.clone(),
        epoch: 1,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };

    let ack = runner
        .handle_initiate_checkpoint(req, &state, &storage)
        .unwrap();

    assert_eq!(ack.epoch, 1, "ack epoch must match request");
    assert_eq!(ack.task_id, task_id);
    assert!(
        ack.snapshot_path.is_some(),
        "state backend produced snapshot"
    );
    assert_eq!(runner.last_acked_epoch, 1);

    let expected_path = snapshot_path("job-cp-1", 1, "operator-task-cp-1", "task-cp-1");
    let data = storage.read_bytes(&expected_path).unwrap();
    assert!(data.is_some(), "snapshot file must be written to storage");
}

#[test]
fn executor_checkpoint_ack_includes_snapshot_path() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let task_id = TaskId::try_new("task-cp-path").unwrap();
    let job_id = JobId::try_new("job-cp-path").unwrap();
    let mut runner = TaskRunner::new(task_id.clone());

    let mut backend_with_state = RocksDbStateBackend::ephemeral().unwrap();
    let ns = krishiv_state::Namespace::new("operator-task-cp-path", "data");
    backend_with_state
        .put(&ns, b"k".to_vec(), b"v".to_vec())
        .unwrap();
    let state = crate::runner::CheckpointStateHandle::from_backend(backend_with_state);

    let req = InitiateCheckpointRequest {
        job_id: job_id.clone(),
        epoch: 2,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };
    let ack = runner
        .handle_initiate_checkpoint(req, &state, &storage)
        .unwrap();
    assert!(
        ack.snapshot_path.is_some(),
        "ack must include snapshot_path when state backend produced snapshot bytes"
    );

    let expected_path =
        snapshot_path("job-cp-path", 2, "operator-task-cp-path", "task-cp-path");
    let data = storage.read_bytes(&expected_path).unwrap();
    assert!(
        data.is_some(),
        "snapshot file must be written at the expected path"
    );
}

#[test]
fn executor_checkpoint_ack_includes_source_offset() {
    use krishiv_connectors::kafka::KafkaOffset;

    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let task_id = TaskId::try_new("task-cp-offset").unwrap();
    let job_id = JobId::try_new("job-cp-offset").unwrap();
    let generic_offset = krishiv_proto::CheckpointSourceOffset {
        partition_id: krishiv_proto::PartitionId::try_new("registry-events-0").unwrap(),
        offset: 3,
        encoded_offset: 3_u64.to_le_bytes().to_vec(),
    };
    let mut runner = TaskRunner::new(task_id.clone())
        .with_source_offsets(vec![generic_offset.clone()])
        .with_kafka_source_offsets(vec![
            KafkaOffset {
                topic: "events".into(),
                partition: 0,
                offset: 42,
            },
            KafkaOffset {
                topic: "events".into(),
                partition: 1,
                offset: 7,
            },
        ]);
    let backend = crate::runner::CheckpointStateHandle::from_backend(
        RocksDbStateBackend::ephemeral().unwrap(),
    );

    let req = InitiateCheckpointRequest {
        job_id: job_id.clone(),
        epoch: 1,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };
    let ack = runner
        .handle_initiate_checkpoint(req, &backend, &storage)
        .unwrap();
    assert_eq!(ack.source_offsets.len(), 3);
    assert_eq!(ack.source_offsets[0], generic_offset);
    assert_eq!(ack.source_offsets[1].offset, 42);
    assert_eq!(ack.source_offsets[2].offset, 7);

    let mut runner2 = TaskRunner::new(TaskId::try_new("task-cp-nooffset").unwrap());
    let req2 = InitiateCheckpointRequest {
        job_id: job_id.clone(),
        epoch: 1,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };
    let ack2 = runner2
        .handle_initiate_checkpoint(req2, &backend, &storage)
        .unwrap();
    assert!(
        ack2.source_offsets.is_empty(),
        "non-Kafka task must have no source offsets"
    );
}

#[test]
fn executor_rejects_stale_checkpoint_epoch() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let task_id = TaskId::try_new("task-cp-stale").unwrap();
    let job_id = JobId::try_new("job-cp-stale").unwrap();
    let mut runner = TaskRunner::new(task_id.clone());
    let backend = crate::runner::CheckpointStateHandle::from_backend(
        RocksDbStateBackend::ephemeral().unwrap(),
    );

    let req5 = InitiateCheckpointRequest {
        job_id: job_id.clone(),
        epoch: 5,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };
    let ack5 = runner
        .handle_initiate_checkpoint(req5, &backend, &storage)
        .unwrap();
    assert_eq!(ack5.epoch, 5, "first ack must be for epoch 5");
    assert_eq!(runner.last_acked_epoch, 5);

    let req3 = InitiateCheckpointRequest {
        job_id: job_id.clone(),
        epoch: 3,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };
    let stale_ack = runner
        .handle_initiate_checkpoint(req3, &backend, &storage)
        .unwrap();
    assert_eq!(
        stale_ack.epoch, 5,
        "stale ack epoch must be last_acked_epoch (5), not the stale request epoch (3)"
    );
    assert_eq!(
        runner.last_acked_epoch, 5,
        "last_acked_epoch must not change on stale rejection"
    );
}

#[test]
fn executor_checkpoint_write_failure_does_not_ack_epoch() {
    let storage = FailingCheckpointStorage;
    let job_id = JobId::try_new("job-cp-write-fail").unwrap();
    let mut runner = TaskRunner::new(TaskId::try_new("task-cp-write-fail").unwrap());

    let mut backend = RocksDbStateBackend::ephemeral().unwrap();
    let ns = krishiv_state::Namespace::new("operator-task-cp-write-fail", "state");
    backend.put(&ns, b"k".to_vec(), b"v".to_vec()).unwrap();
    let state = crate::runner::CheckpointStateHandle::from_backend(backend);

    let req = InitiateCheckpointRequest {
        job_id,
        epoch: 9,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };

    let error = runner
        .handle_initiate_checkpoint(req, &state, &storage)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("checkpoint snapshot write failed"),
        "unexpected error: {error}"
    );
    assert_eq!(runner.last_acked_epoch, 0);
}

#[tokio::test]
async fn checkpoint_ack_delivered() {
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct RecordingCoordinator {
        acks: Arc<Mutex<Vec<CheckpointAckRequest>>>,
    }

    #[tonic::async_trait]
    impl CoordinatorExecutorService for RecordingCoordinator {
        async fn push_task_result(
            &self,
            request: tonic::Request<krishiv_proto::services::TaskResultChunkStream>,
        ) -> Result<tonic::Response<krishiv_proto::PushTaskResultResponse>, tonic::Status> {
            use futures::StreamExt as _;
            let mut stream = request.into_inner();
            while let Some(chunk) = stream.next().await {
                chunk?;
            }
            Ok(tonic::Response::new(
                krishiv_proto::PushTaskResultResponse::new(
                    krishiv_proto::TransportDisposition::Accepted,
                ),
            ))
        }

        async fn register_executor(
            &self,
            request: tonic::Request<RegisterExecutorRequest>,
        ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
            let request = request.into_inner();
            Ok(tonic::Response::new(RegisterExecutorResponse::new(
                request.descriptor().executor_id().clone(),
                LeaseGeneration::initial(),
                TransportDisposition::Accepted,
            )))
        }

        async fn deregister_executor(
            &self,
            _request: tonic::Request<DeregisterExecutorRequest>,
        ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn executor_heartbeat(
            &self,
            request: tonic::Request<ExecutorHeartbeatRequest>,
        ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
            Ok(tonic::Response::new(ExecutorHeartbeatResponse::new(
                request.into_inner().lease_generation(),
                TransportDisposition::Accepted,
            )))
        }

        async fn task_status(
            &self,
            _request: tonic::Request<TaskStatusRequest>,
        ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(TaskStatusResponse::new(
                TransportDisposition::Accepted,
            )))
        }

        async fn checkpoint_ack(
            &self,
            request: tonic::Request<CheckpointAckRequest>,
        ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
            self.acks.lock().unwrap().push(request.into_inner());
            Ok(tonic::Response::new(CheckpointAckResponse::Accepted))
        }
    }

    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let backend = RocksDbStateBackend::ephemeral().unwrap();
    let coordinator = RecordingCoordinator::default();
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let assignment = ExecutorTaskAssignment::new(
        TaskAttemptRef::new(
            JobId::try_new("job-ack").unwrap(),
            StageId::try_new("stage-ack").unwrap(),
            TaskId::try_new("task-ack").unwrap(),
            AttemptId::initial(),
        ),
        ExecutorId::try_new("exec-ack").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new("sql: select 1"),
        OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
    );
    let req = InitiateCheckpointRequest {
        job_id: JobId::try_new("job-ack").unwrap(),
        epoch: 3,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };

    let response = runner
        .initiate_checkpoint_and_deliver_ack(
            &assignment,
            req,
            crate::runner::CheckpointStateHandle::from_backend(backend),
            Arc::new(storage),
            coordinator.clone(),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(response, CheckpointAckResponse::Accepted);
    let acks = coordinator.acks.lock().unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].epoch, 3);
    assert_eq!(acks[0].task_id.as_str(), "task-ack");
}

#[tokio::test]
async fn checkpoint_ack_delivery_retries_transient_failure() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct FlakyCoordinator {
        calls: Arc<AtomicUsize>,
        acks: Arc<Mutex<Vec<CheckpointAckRequest>>>,
    }

    #[tonic::async_trait]
    impl CoordinatorExecutorService for FlakyCoordinator {
        async fn push_task_result(
            &self,
            request: tonic::Request<krishiv_proto::services::TaskResultChunkStream>,
        ) -> Result<tonic::Response<krishiv_proto::PushTaskResultResponse>, tonic::Status> {
            use futures::StreamExt as _;
            let mut stream = request.into_inner();
            while let Some(chunk) = stream.next().await {
                chunk?;
            }
            Ok(tonic::Response::new(
                krishiv_proto::PushTaskResultResponse::new(
                    krishiv_proto::TransportDisposition::Accepted,
                ),
            ))
        }

        async fn register_executor(
            &self,
            request: tonic::Request<RegisterExecutorRequest>,
        ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
            let request = request.into_inner();
            Ok(tonic::Response::new(RegisterExecutorResponse::new(
                request.descriptor().executor_id().clone(),
                LeaseGeneration::initial(),
                TransportDisposition::Accepted,
            )))
        }

        async fn deregister_executor(
            &self,
            _request: tonic::Request<DeregisterExecutorRequest>,
        ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn executor_heartbeat(
            &self,
            request: tonic::Request<ExecutorHeartbeatRequest>,
        ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
            Ok(tonic::Response::new(ExecutorHeartbeatResponse::new(
                request.into_inner().lease_generation(),
                TransportDisposition::Accepted,
            )))
        }

        async fn task_status(
            &self,
            _request: tonic::Request<TaskStatusRequest>,
        ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(TaskStatusResponse::new(
                TransportDisposition::Accepted,
            )))
        }

        async fn checkpoint_ack(
            &self,
            request: tonic::Request<CheckpointAckRequest>,
        ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Err(tonic::Status::unavailable(
                    "temporary coordinator ack failure",
                ));
            }
            self.acks.lock().unwrap().push(request.into_inner());
            Ok(tonic::Response::new(CheckpointAckResponse::Accepted))
        }
    }

    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let backend = RocksDbStateBackend::ephemeral().unwrap();
    let coordinator = FlakyCoordinator::default();
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let assignment = ExecutorTaskAssignment::new(
        TaskAttemptRef::new(
            JobId::try_new("job-ack-retry").unwrap(),
            StageId::try_new("stage-ack-retry").unwrap(),
            TaskId::try_new("task-ack-retry").unwrap(),
            AttemptId::initial(),
        ),
        ExecutorId::try_new("exec-ack-retry").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new("sql: select 1"),
        OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
    );
    let req = InitiateCheckpointRequest {
        job_id: JobId::try_new("job-ack-retry").unwrap(),
        epoch: 4,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };

    let response = runner
        .initiate_checkpoint_and_deliver_ack(
            &assignment,
            req,
            crate::runner::CheckpointStateHandle::from_backend(backend),
            Arc::new(storage),
            coordinator.clone(),
            Vec::new(),
        )
        .await
        .unwrap();

    assert_eq!(response, CheckpointAckResponse::Accepted);
    assert_eq!(coordinator.calls.load(Ordering::SeqCst), 2);
    let acks = coordinator.acks.lock().unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].epoch, 4);
    assert_eq!(acks[0].task_id.as_str(), "task-ack-retry");
}

#[tokio::test]
async fn checkpoint_fanout_uses_running_attempts_without_preexisting_task_runner() {
    use dashmap::DashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct RecordingCoordinator {
        acks: Arc<Mutex<Vec<CheckpointAckRequest>>>,
    }

    #[tonic::async_trait]
    impl CoordinatorExecutorService for RecordingCoordinator {
        async fn push_task_result(
            &self,
            request: tonic::Request<krishiv_proto::services::TaskResultChunkStream>,
        ) -> Result<tonic::Response<krishiv_proto::PushTaskResultResponse>, tonic::Status> {
            use futures::StreamExt as _;
            let mut stream = request.into_inner();
            while let Some(chunk) = stream.next().await {
                chunk?;
            }
            Ok(tonic::Response::new(
                krishiv_proto::PushTaskResultResponse::new(
                    krishiv_proto::TransportDisposition::Accepted,
                ),
            ))
        }

        async fn register_executor(
            &self,
            request: tonic::Request<RegisterExecutorRequest>,
        ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
            let request = request.into_inner();
            Ok(tonic::Response::new(RegisterExecutorResponse::new(
                request.descriptor().executor_id().clone(),
                LeaseGeneration::initial(),
                TransportDisposition::Accepted,
            )))
        }

        async fn deregister_executor(
            &self,
            _request: tonic::Request<DeregisterExecutorRequest>,
        ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn executor_heartbeat(
            &self,
            request: tonic::Request<ExecutorHeartbeatRequest>,
        ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
            Ok(tonic::Response::new(ExecutorHeartbeatResponse::new(
                request.into_inner().lease_generation(),
                TransportDisposition::Accepted,
            )))
        }

        async fn task_status(
            &self,
            _request: tonic::Request<TaskStatusRequest>,
        ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(TaskStatusResponse::new(
                TransportDisposition::Accepted,
            )))
        }

        async fn checkpoint_ack(
            &self,
            request: tonic::Request<CheckpointAckRequest>,
        ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
            self.acks.lock().unwrap().push(request.into_inner());
            Ok(tonic::Response::new(CheckpointAckResponse::Accepted))
        }
    }

    let job_id = JobId::try_new("job-fanout").unwrap();
    let task_id = TaskId::try_new("task-fanout").unwrap();
    let attempt = TaskAttemptRef::new(
        job_id.clone(),
        StageId::try_new("stage-fanout").unwrap(),
        task_id.clone(),
        AttemptId::initial(),
    );
    let running_attempts = Arc::new(DashMap::new());
    running_attempts.insert(task_id.as_str().to_string(), attempt);

    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let backend = RocksDbStateBackend::ephemeral().unwrap();
    let coordinator = RecordingCoordinator::default();
    let exec_id = ExecutorId::try_new("exec-checkpoint-fanout").unwrap();
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_executor_id(exec_id)
        .with_running_attempts(running_attempts);
    let req = InitiateCheckpointRequest {
        job_id,
        epoch: 7,
        fencing_token: FencingToken::initial(),
        alignment: CheckpointAlignment::default(),
    };

    runner
        .initiate_checkpoint_for_job(
            &req,
            crate::runner::CheckpointStateHandle::from_backend(backend),
            Arc::new(storage) as Arc<dyn CheckpointStorage>,
            coordinator.clone(),
        )
        .await
        .unwrap();

    let acks = coordinator.acks.lock().unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].epoch, 7);
    assert_eq!(acks[0].task_id, task_id);
    assert_eq!(runner.checkpoint_runners.len(), 1);
}

#[tokio::test]
async fn heartbeat_includes_running_attempts() {
    use krishiv_proto::StageId;

    let runtime = ExecutorRuntime::new(
        ExecutorConfig::new("exec-hb", "pod-a", 1, "http://coordinator").unwrap(),
    );
    let attempt = TaskAttemptRef::new(
        JobId::try_new("job-hb").unwrap(),
        StageId::try_new("stage-hb").unwrap(),
        TaskId::try_new("task-hb").unwrap(),
        AttemptId::initial(),
    );
    let heartbeat = runtime
        .heartbeat_request()
        .with_running_attempts(vec![attempt]);
    assert_eq!(heartbeat.running_attempts().len(), 1);
    assert_eq!(
        heartbeat.running_attempts()[0].task_id().as_str(),
        "task-hb"
    );
}

use krishiv_proto::{ShuffleReadConfig, ShuffleWriteConfig};
use krishiv_shuffle::{InMemoryShuffleStore, PartitionId, ShufflePartition, ShuffleStore};

fn shuffle_assignment_helper(
    job_id: &str,
    stage_id: &str,
    task_id: &str,
    fragment: &str,
) -> ExecutorTaskAssignment {
    ExecutorTaskAssignment::new(
        TaskAttemptRef::new(
            JobId::try_new(job_id).unwrap(),
            StageId::try_new(stage_id).unwrap(),
            TaskId::try_new(task_id).unwrap(),
            AttemptId::initial(),
        ),
        ExecutorId::try_new("exec-shuffle-1").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new(fragment),
        OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
    )
}

#[tokio::test]
async fn test_shuffle_write_task_partitions_output() {
    let temp = tempdir().unwrap();
    let parquet_path = temp.path().join("data.parquet");
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5, 6])) as _,
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])) as _,
            ],
        )
        .unwrap();
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let store = Arc::new(InMemoryShuffleStore::new());
    let inbox = ExecutorAssignmentInbox::new();
    let backend = Arc::new(krishiv_shuffle::ShuffleBackend::InMemory(Arc::clone(
        &store,
    )));
    let runner = ExecutorTaskRunner::new(inbox.clone()).with_inmem_shuffle(backend);

    let num_partitions = 3usize;
    let write_cfg = ShuffleWriteConfig {
        stage_id: StageId::try_new("stage-sw").unwrap(),
        num_partitions,
        key_columns: vec![String::from("id")],
        lease_token: 1,
    };

    let assignment = shuffle_assignment_helper(
        "job-sw",
        "stage-sw",
        "task-sw-1",
        "sql: select id, name from data",
    )
    .with_input_partitions(vec![InputPartition::new(
        "data-part-1",
        format!("local-parquet:data:{}", parquet_path.display()),
    )])
    .with_shuffle_write(write_cfg);

    let service = AcceptingCoordinatorService;
    let report = runner
        .run_assignment_with(assignment, &service)
        .await
        .unwrap();

    assert_eq!(report.output().kind(), ExecutorTaskOutputKind::ShuffleWrite);
    assert_eq!(report.output().row_count(), 6);
    assert_eq!(report.output().shuffle_partitions().len(), num_partitions);

    let mut total_stored_rows = 0usize;
    for p in 0..num_partitions {
        let id = PartitionId {
            job_id: String::from("job-sw"),
            stage_id: String::from("stage-sw"),
            partition: p as u32,
        };
        if let Some(partition) = store.read_partition(&id).await.unwrap() {
            total_stored_rows += partition
                .batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>();
        }
    }
    assert_eq!(total_stored_rows, 6);
}

#[tokio::test]
async fn test_shuffle_read_task_returns_batches() {
    let store = Arc::new(InMemoryShuffleStore::new());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10i64, 20, 30])) as _,
            Arc::new(StringArray::from(vec!["x", "y", "z"])) as _,
        ],
    )
    .unwrap();
    let id = PartitionId {
        job_id: String::from("job-sr"),
        stage_id: String::from("stage-sr"),
        partition: 1,
    };
    store
        .write_partition(
            ShufflePartition {
                id,
                schema,
                batches: vec![batch],
            },
            42,
        )
        .await
        .unwrap();

    let inbox = ExecutorAssignmentInbox::new();
    let backend = Arc::new(krishiv_shuffle::ShuffleBackend::InMemory(Arc::clone(
        &store,
    )));
    let runner = ExecutorTaskRunner::new(inbox.clone()).with_inmem_shuffle(backend);

    let read_cfg = ShuffleReadConfig {
        stage_id: StageId::try_new("stage-sr").unwrap(),
        partition_id: 1,
        lease_token: 42,
    };

    let assignment =
        shuffle_assignment_helper("job-sr", "stage-sr", "task-sr-1", "shuffle-read")
            .with_shuffle_read(read_cfg);

    let service = AcceptingCoordinatorService;
    let report = runner
        .run_assignment_with(assignment, &service)
        .await
        .unwrap();

    assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
    assert_eq!(report.output().row_count(), 3);
    assert_eq!(report.output().column_count(), 2);
}
