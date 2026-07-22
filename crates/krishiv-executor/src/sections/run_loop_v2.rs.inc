// ── Phase 55: promoted run-loop (`stream:rloop:`) tests ──────────────────

use crate::fragment::run_loop::{STREAM_RLOOP_PREFIX, rloop_key_group_range};

const RLOOP_WINDOW_SPEC: &str = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
/// Endpoint sentinel that marks every peer as co-located with the runner so
/// the keyed exchange short-circuits through the shared input map.
const RLOOP_LOCAL_ENDPOINT: &str = "http://local-test-endpoint";

fn make_rloop_assignment(
    job_id: &str,
    subtask: usize,
    parallelism: usize,
    peers_partition: Option<&str>,
) -> ExecutorTaskAssignment {
    let ids = TaskAttemptRef::new(
        JobId::try_new(job_id).unwrap(),
        StageId::try_new("stage-streaming").unwrap(),
        TaskId::try_new(format!("task-streaming-{subtask}")).unwrap(),
        AttemptId::initial(),
    );
    let fragment = format!(
        "{STREAM_RLOOP_PREFIX}{job_id}|{subtask}/{parallelism}|{RLOOP_WINDOW_SPEC}"
    );
    let mut assignment = ExecutorTaskAssignment::new(
        ids,
        ExecutorId::try_new("exec-1").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new(fragment).with_streaming(true),
        OutputContract::new(
            krishiv_proto::OutputContractKind::InlineRecordBatches,
            "rloop output",
        ),
    )
    .with_key_group_range(rloop_key_group_range(subtask, parallelism))
    .with_requires_reattach(true);
    if let Some(peers) = peers_partition {
        assignment =
            assignment.with_input_partitions(vec![krishiv_proto::InputPartition::new(
                "stream-peers",
                peers,
            )]);
    }
    assignment
}

fn rloop_peers_partition(job_parallelism: usize) -> String {
    let entries: Vec<String> = (0..job_parallelism)
        .map(|i| format!("{i}=task-streaming-{i}@{RLOOP_LOCAL_ENDPOINT}"))
        .collect();
    format!("stream-peers:{}", entries.join(";"))
}

/// Drive one run-loop job at `parallelism` on a single shared runner: push
/// `input` batches under the job key, wait for `expected_rows` egress rows,
/// cancel every subtask, and return the collected output rows as
/// `(key, count)` pairs sorted by key.
async fn run_rloop_job_to_completion(
    job_id: &str,
    parallelism: usize,
    inputs: Vec<RecordBatch>,
    expected_rows: usize,
) -> Vec<(String, i64)> {
    use arrow::array::{Array, Int64Array, StringArray};

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_own_task_endpoint(RLOOP_LOCAL_ENDPOINT);
    let peers = rloop_peers_partition(parallelism);

    let mut handles = Vec::new();
    for subtask in 0..parallelism {
        let assignment = make_rloop_assignment(job_id, subtask, parallelism, Some(&peers));
        let task_runner = runner.clone();
        handles.push(tokio::spawn(async move {
            task_runner.execute_streaming_fragment(&assignment).await
        }));
    }
    // Give every subtask a beat to register its input notify + egress buffer.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    runner
        .continuous_inputs
        .entry(job_id.to_owned())
        .or_default()
        .extend(inputs);
    runner.notify_continuous_input(job_id);

    // Await the expected egress rows (leave collected batches in place).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let rows: usize = runner
            .continuous_outputs
            .get(job_id)
            .map(|entry| entry.iter().map(|b| b.num_rows()).sum())
            .unwrap_or(0);
        if rows >= expected_rows {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "run-loop job {job_id} produced {rows}/{expected_rows} rows before timeout"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Stop every subtask; the loop observes the tombstone and exits.
    for subtask in 0..parallelism {
        let task_id = TaskId::try_new(format!("task-streaming-{subtask}")).unwrap();
        runner
            .inbox()
            .cancel_task(&JobId::try_new(job_id).unwrap(), &task_id)
            .unwrap();
        runner.notify_continuous_input(&format!("{job_id}#task-streaming-{subtask}"));
    }
    runner.notify_continuous_input(job_id);
    for handle in handles {
        let output = handle
            .await
            .expect("run-loop task must not panic")
            .expect("run-loop task must exit cleanly on cancel");
        assert_eq!(
            output.kind(),
            ExecutorTaskOutputKind::Cancelled,
            "a cancelled run-loop reports Cancelled"
        );
    }

    let batches: Vec<RecordBatch> = runner
        .continuous_outputs
        .get(job_id)
        .map(|entry| entry.value().clone())
        .unwrap_or_default();
    let mut rows: Vec<(String, i64)> = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(batch.schema().index_of("key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        let count_idx = batch.num_columns() - 1;
        let counts = batch
            .column(count_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        for i in 0..batch.num_rows() {
            if !keys.is_null(i) {
                rows.push((keys.value(i).to_owned(), counts.value(i)));
            }
        }
    }
    rows.sort();
    rows
}

/// Phase 55 exit gate: a continuous windowed job at parallelism 3 produces
/// exactly the results of the parallelism-1 run under keyed input — the keyed
/// exchange routes every row to its owning subtask and each subtask closes
/// its windows independently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_loop_parallel_three_matches_parallel_one() {
    // 60 keys, two events each in window [0, 10_000), then a closing batch
    // at ts=25_000 for every key so ALL subtasks' watermarks advance.
    let keys: Vec<String> = (0..60).map(|i| format!("user-{i:02}")).collect();
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let make_inputs = || {
        let in_window_a =
            krishiv_common::test_fixtures::make_test_key_ts_batch(key_refs.clone(), vec![100; 60])
                .unwrap();
        let in_window_b = krishiv_common::test_fixtures::make_test_key_ts_batch(
            key_refs.clone(),
            vec![5_000; 60],
        )
        .unwrap();
        let closing = krishiv_common::test_fixtures::make_test_key_ts_batch(
            key_refs.clone(),
            vec![25_000; 60],
        )
        .unwrap();
        vec![in_window_a, in_window_b, closing]
    };

    let single = run_rloop_job_to_completion("rloop-eq-p1", 1, make_inputs(), 60).await;
    let parallel = run_rloop_job_to_completion("rloop-eq-p3", 3, make_inputs(), 60).await;

    assert_eq!(single.len(), 60, "parallelism-1 closes one window per key");
    assert_eq!(
        single, parallel,
        "parallelism-3 output must equal parallelism-1 output under keyed input"
    );
    assert!(
        single.iter().all(|(_, count)| *count == 2),
        "each key aggregates both in-window events exactly once: {single:?}"
    );

    // Exit-gate latency instrument: the µs-bucket ingest→emit histogram must
    // have recorded observations with p99 ≤ 100 ms for the in-process loop.
    let p99 = krishiv_metrics::global_metrics()
        .stream_record_latency_quantile("rloop-eq-p3", 0.99)
        .expect("run-loop must record ingest→emit latency");
    assert!(
        p99 <= 0.1,
        "in-engine ingest→emit p99 must be ≤ 100 ms, got {p99}s"
    );
}

/// ST-4 distributed: a quiet source still closes session windows via the
/// run-loop's idle tick (previously the tick's only caller was embedded).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_loop_idle_tick_closes_session_windows_distributed() {
    let job_id = "rloop-idle-session";
    let session_spec = "stream:ses:key=key:time=ts:win=10000:lag=0:gap=100:agg=count";
    let fragment = format!("{STREAM_RLOOP_PREFIX}{job_id}|0/1|{session_spec}");
    let ids = TaskAttemptRef::new(
        JobId::try_new(job_id).unwrap(),
        StageId::try_new("stage-streaming").unwrap(),
        TaskId::try_new("task-streaming-0").unwrap(),
        AttemptId::initial(),
    );
    let assignment = ExecutorTaskAssignment::new(
        ids,
        ExecutorId::try_new("exec-1").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new(fragment).with_streaming(true),
        OutputContract::new(
            krishiv_proto::OutputContractKind::InlineRecordBatches,
            "rloop output",
        ),
    )
    .with_key_group_range(rloop_key_group_range(0, 1))
    .with_requires_reattach(true);

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_own_task_endpoint(RLOOP_LOCAL_ENDPOINT);
    let task_runner = runner.clone();
    let handle = tokio::spawn(async move {
        task_runner.execute_streaming_fragment(&assignment).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // One session's events, then silence: only the idle tick (wall-clock ≫
    // event time) can close the session.
    let batch =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["s", "s"], vec![100, 150])
            .unwrap();
    runner
        .continuous_inputs
        .entry(job_id.to_owned())
        .or_default()
        .push(batch);
    runner.notify_continuous_input(job_id);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let rows: usize = runner
            .continuous_outputs
            .get(job_id)
            .map(|entry| entry.iter().map(|b| b.num_rows()).sum())
            .unwrap_or(0);
        if rows > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "idle tick must close the session window without further input"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let task_id = TaskId::try_new("task-streaming-0").unwrap();
    runner
        .inbox()
        .cancel_task(&JobId::try_new(job_id).unwrap(), &task_id)
        .unwrap();
    runner.notify_continuous_input(&format!("{job_id}#task-streaming-0"));
    let output = handle.await.unwrap().unwrap();
    assert_eq!(output.kind(), ExecutorTaskOutputKind::Cancelled);
}

/// Phase 55: barrier snapshots resolve per-task bound state — a run-loop
/// subtask's checkpoint captures ITS window executor, not a job singleton
/// (the H-6 fix carried through the checkpoint path).
#[tokio::test]
async fn checkpoint_state_for_task_resolves_bound_subtask_state() {
    use crate::runner::{CheckpointStateHandle, TaskStateBinding};

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let spec = krishiv_plan::window::WindowExecutionSpec::tumbling("key", "ts", 10_000);
    let exec_a = std::sync::Arc::new(std::sync::Mutex::new(
        krishiv_dataflow::ContinuousWindowExecutor::new(spec.clone()).unwrap(),
    ));
    let exec_b = std::sync::Arc::new(std::sync::Mutex::new(
        krishiv_dataflow::ContinuousWindowExecutor::new(spec).unwrap(),
    ));
    runner.loop_executors.insert("job-x#0".into(), exec_a.clone());
    runner.loop_executors.insert("job-x#1".into(), exec_b.clone());
    runner
        .task_state_bindings
        .insert("task-streaming-0".into(), TaskStateBinding::Window("job-x#0".into()));
    runner
        .task_state_bindings
        .insert("task-streaming-1".into(), TaskStateBinding::Window("job-x#1".into()));

    let fallback = CheckpointStateHandle::from_backend(
        krishiv_state::InMemoryStateBackend::default(),
    );
    let resolved_a =
        runner.checkpoint_state_for_task("job-x", "task-streaming-0", fallback.clone());
    let resolved_b =
        runner.checkpoint_state_for_task("job-x", "task-streaming-1", fallback.clone());
    match (&resolved_a, &resolved_b) {
        (
            CheckpointStateHandle::ContinuousWindow(a),
            CheckpointStateHandle::ContinuousWindow(b),
        ) => {
            assert!(std::sync::Arc::ptr_eq(a, &exec_a), "task 0 binds executor A");
            assert!(std::sync::Arc::ptr_eq(b, &exec_b), "task 1 binds executor B");
            assert!(!std::sync::Arc::ptr_eq(a, b), "subtasks never share state");
        }
        other => panic!("expected ContinuousWindow handles, got {other:?}"),
    }
    // Unbound tasks fall back to the job-level lookup, then the backend.
    let unbound = runner.checkpoint_state_for_task("job-x", "task-other", fallback);
    assert!(matches!(unbound, CheckpointStateHandle::Backend(_)));
}

/// G5 / platform #88 exit wording: a two-input streaming join retains state
/// across cycles and survives checkpoint → kill → restore with exact
/// pre-kill accumulations.
#[tokio::test]
async fn window_join_retains_state_across_cycles_and_restores() {
    use krishiv_dataflow::WatermarkWindowJoinSpec;

    let spec = WatermarkWindowJoinSpec {
        time_column: "ts".into(),
        left_key_column: "key".into(),
        right_key_column: "key".into(),
        window_ms: 5_000,
    };
    let fragment = format!("window-join:{}", serde_json::to_string(&spec).unwrap());
    let job = "join-persist-job";

    let left = krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["k"], vec![1_000])
        .unwrap();
    let right = krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["k"], vec![1_500])
        .unwrap();

    let make_assignment = |partitions: Vec<krishiv_proto::InputPartition>| {
        let ids = TaskAttemptRef::new(
            JobId::try_new(job).unwrap(),
            StageId::try_new("stage-join").unwrap(),
            TaskId::try_new("task-join").unwrap(),
            AttemptId::initial(),
        );
        ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new(&fragment).with_streaming(true),
            OutputContract::new(
                krishiv_proto::OutputContractKind::InlineRecordBatches,
                "join output",
            ),
        )
        .with_input_partitions(partitions)
    };
    let ipc_partition = |index: usize, batch: &RecordBatch| {
        use arrow::ipc::writer::StreamWriter;
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
            writer.write(batch).unwrap();
            writer.finish().unwrap();
        }
        krishiv_proto::InputPartition::typed(
            format!("p{index}"),
            krishiv_proto::InputPartitionDescriptor::InlineIpc {
                table_name: format!("input-{index}"),
                ipc_bytes: bytes,
            },
        )
    };

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());

    // Cycle 1: left side only — nothing matches yet, but the row must be
    // buffered (the historical implementation rebuilt the operator per cycle
    // and lost it by construction).
    let out1 = runner
        .execute_streaming_fragment(&make_assignment(vec![ipc_partition(0, &left)]))
        .await
        .unwrap();
    assert_eq!(out1.row_count(), 0, "no join partner yet");
    let checkpoint = out1
        .state_snapshot()
        .expect("cycle output carries the operator snapshot")
        .to_vec();

    // Cycle 2 on the SAME runner: the right row joins the retained left row.
    // (Even partition indices are the left stream, so pad with an empty
    // left slot by sending the right batch at an odd index.)
    let out2 = runner
        .execute_streaming_fragment(&make_assignment(vec![
            ipc_partition(0, &left.slice(0, 0)),
            ipc_partition(1, &right),
        ]))
        .await
        .unwrap();
    assert!(
        out2.row_count() > 0,
        "right row must join the left row retained from cycle 1"
    );

    // Kill: a FRESH runner (fresh process) restores the checkpoint taken
    // after cycle 1 and reproduces the same join from pre-kill state.
    let restored_runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    restored_runner.pending_restores.insert(
        job.to_owned(),
        crate::runner::RestoredJobCheckpoint {
            epoch: 1,
            fencing_token: 1,
            snapshots: vec![checkpoint],
        },
    );
    let out3 = restored_runner
        .execute_streaming_fragment(&make_assignment(vec![
            ipc_partition(0, &left.slice(0, 0)),
            ipc_partition(1, &right),
        ]))
        .await
        .unwrap();
    assert!(
        out3.row_count() > 0,
        "restored join state must reproduce the pre-kill accumulation"
    );
}

/// Barrier drain without a wired context is a no-op (the CLI slot loop stays
/// the only drainer outside the run-loop runtime).
#[tokio::test]
async fn drain_barriers_without_context_is_noop() {
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    assert_eq!(runner.drain_barriers_via_context().await, 0);
}

/// Phase 59 cancel-latency cert, streaming surface. The batch surface was
/// live-proven at 0.03–0.23s (scripts/phase59_cancel_cert.sh); the run-loop
/// checks its cancel tombstone at the top of every iteration but that
/// transition had never been *timed*. This measures cancel→exit latency on a
/// loop that is actively draining input (not idle-waking) and holds it to the
/// certified ≤2s bound with heavy margin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_loop_cancel_latency_stays_inside_the_two_second_bound() {
    let job_id = "rloop-cancel-latency";
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_own_task_endpoint(RLOOP_LOCAL_ENDPOINT);
    let peers = rloop_peers_partition(1);
    let assignment = make_rloop_assignment(job_id, 0, 1, Some(&peers));
    let task_runner = runner.clone();
    let handle =
        tokio::spawn(async move { task_runner.execute_streaming_fragment(&assignment).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Keep the loop busy: push fresh batches every 5 ms until the task exits,
    // so the measurement covers a working loop, not an idle notify wake-up.
    let feeder_runner = runner.clone();
    let feeder_job = job_id.to_owned();
    let feeder = tokio::spawn(async move {
        let mut ts = 0i64;
        loop {
            let batch = krishiv_common::test_fixtures::make_test_key_ts_batch(
                vec!["k1", "k2"],
                vec![ts, ts + 1],
            )
            .unwrap();
            feeder_runner
                .continuous_inputs
                .entry(feeder_job.clone())
                .or_default()
                .push(batch);
            feeder_runner.notify_continuous_input(&feeder_job);
            ts += 10;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    });

    // Let it chew on real input before cancelling.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let started = std::time::Instant::now();
    let task_id = TaskId::try_new("task-streaming-0").unwrap();
    runner
        .inbox()
        .cancel_task(&JobId::try_new(job_id).unwrap(), &task_id)
        .unwrap();
    runner.notify_continuous_input(&format!("{job_id}#task-streaming-0"));
    runner.notify_continuous_input(job_id);

    let output = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
        .await
        .expect("run-loop must exit after cancel, not hang")
        .expect("run-loop task must not panic")
        .expect("run-loop task must exit cleanly on cancel");
    let latency = started.elapsed();
    feeder.abort();

    assert_eq!(output.kind(), ExecutorTaskOutputKind::Cancelled);
    assert!(
        latency < std::time::Duration::from_secs(2),
        "streaming cancel latency exceeded the certified 2s bound: {latency:?}"
    );
    eprintln!("phase59 streaming cancel->exit latency: {latency:?}");
}
