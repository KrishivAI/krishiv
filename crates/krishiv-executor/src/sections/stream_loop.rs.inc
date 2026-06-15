// ── GAP-6: stream:loop: tests ────────────────────────────────────────────

/// Verify that `stream:loop:` invokes `ContinuousWindowExecutor` and emits
/// closed-window batches after the watermark crosses the window boundary.
#[tokio::test]
async fn stream_loop_emits_window_via_continuous_executor() {
    use std::sync::Arc;

    // Build an input batch: events at t=100 and t=12_000 ms in a 10s window.
    // With lag=0 the second event advances the watermark past the first window.
    let batch = krishiv_common::test_fixtures::make_test_key_ts_batch(
        vec!["a", "a"],
        vec![100_i64, 12_000_i64],
    );

    let drainer = OneShotDrainer::new(vec![batch]);
    let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
    let fragment = format!("stream:loop:loop-job-1|{window_spec}");

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_continuous_drainer(drainer as Arc<dyn crate::ContinuousJobDrainer>);

    let assignment = make_loop_assignment(&fragment);
    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .expect("stream:loop execution should succeed");

    assert_eq!(
        output.kind(),
        ExecutorTaskOutputKind::StreamingWindow,
        "stream:loop should emit StreamingWindow output"
    );
    // The first tumbling window [0, 10000) must be emitted when the watermark
    // advances past 10000 due to the event at ts=12000.
    assert!(
        output.row_count() > 0,
        "stream:loop should emit at least one window row"
    );
}

/// Verify that `stream:loop:` reuses the same `ContinuousWindowExecutor`
/// across multiple drain cycles (state is accumulated).
#[tokio::test]
async fn stream_loop_reuses_executor_state_across_drains() {
    use std::sync::Arc;

    // First drain: single event in [0, 10000).  Window not yet emitted.
    let batch1 =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![500_i64]);

    // Second drain: event that advances watermark past 10000, closing the window.
    let batch2 =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![15_000_i64]);

    // The runner is CLONED between drains to simulate a re-used runner; its
    // `loop_executors` Arc is shared so state survives.
    let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
    let fragment = format!("stream:loop:loop-job-state|{window_spec}");

    // Drainer 1: returns batch1 then empty.
    struct SeqDrainer {
        inner: std::sync::Mutex<std::collections::VecDeque<Vec<RecordBatch>>>,
    }
    impl crate::ContinuousJobDrainer for SeqDrainer {
        fn drain_job(&self, _job_id: &str) -> Result<Vec<RecordBatch>, String> {
            Ok(self.inner.lock().unwrap().pop_front().unwrap_or_default())
        }
    }
    let drainer = Arc::new(SeqDrainer {
        inner: std::sync::Mutex::new(std::collections::VecDeque::from(vec![
            vec![batch1],
            vec![batch2],
        ])),
    });

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_continuous_drainer(drainer as Arc<dyn crate::ContinuousJobDrainer>);

    // First drain: no window should be emitted yet.
    let out1 = runner
        .execute_streaming_fragment(&make_loop_assignment(&fragment))
        .await
        .expect("drain 1 ok");
    assert_eq!(out1.row_count(), 0, "no window emitted on first drain");

    // Second drain: reuse the same runner (shared loop_executors); window
    // should now be emitted.
    let out2 = runner
        .execute_streaming_fragment(&make_loop_assignment(&fragment))
        .await
        .expect("drain 2 ok");
    assert!(
        out2.row_count() > 0,
        "window must be emitted on second drain after watermark advances"
    );
}

/// Distributed stream:loop cycles consume coordinator-delivered InlineIpc
/// partitions when no local drainer is attached.
#[tokio::test]
async fn stream_loop_without_drainer_processes_inline_input() {
    let window_spec = krishiv_plan::window::encode_window_execution_spec(
        &krishiv_plan::window::WindowExecutionSpec::tumbling("key", "ts", 10_000),
    )
    .unwrap();
    let fragment = format!("stream:loop:no-drainer-job|{window_spec}");
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let batch = krishiv_common::test_fixtures::make_test_key_ts_batch(
        vec!["a", "a"],
        vec![100_i64, 12_000_i64],
    );
    let assignment = make_loop_assignment_with_batches(&fragment, &[batch]);
    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .expect("inline continuous cycle must execute");
    assert!(
        output.row_count() > 0,
        "inline cycle must execute the registered window instead of echoing input"
    );
}

#[tokio::test]
async fn legacy_stream_continuous_fragment_is_rejected() {
    let fragment = "stream:continuous:legacy-job";
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let batch = krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![100_i64]);
    let assignment = make_loop_assignment_with_batches(fragment, &[batch]);

    let error = runner
        .execute_streaming_fragment(&assignment)
        .await
        .expect_err("legacy fragment must not echo unprocessed input");

    assert!(
        matches!(error, crate::ExecutorError::InvalidAssignment { .. }),
        "legacy fragment must fail as an invalid assignment: {error}"
    );
}

// ── R7.2 source throttle wiring ──────────────────────────────────────────

/// Verify that the `SourceThrottleTable` on `ExecutorTaskRunner` is shared
/// across runner clones (simulating the heartbeat loop writing, runner tasks
/// reading) and that `apply` + `active_limit` behave correctly.
#[test]
fn source_throttle_table_shared_across_runner_clones() {
    use krishiv_proto::HeartbeatThrottleCommand;

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let clone = runner.clone();

    // Simulate what the heartbeat loop does when it receives ThrottleDecisions.
    let cmds = vec![
        HeartbeatThrottleCommand {
            source_id: "src-kafka-0".into(),
            rows_per_second: Some(1000),
        },
        HeartbeatThrottleCommand {
            source_id: "src-kafka-1".into(),
            rows_per_second: None, // clear / unlimited
        },
    ];
    for tc in &cmds {
        runner
            .source_throttle_limits
            .apply(&tc.source_id, tc.rows_per_second);
    }

    // Reads through the clone (task runner side) must observe the updates.
    assert_eq!(
        clone.source_throttle_limits.active_limit("src-kafka-0"),
        Some(1000)
    );
    assert_eq!(
        clone.source_throttle_limits.active_limit("src-kafka-1"),
        None
    );
    assert_eq!(
        clone.source_throttle_limits.active_limit("src-kafka-99"),
        None
    );

    // check_and_log should not panic regardless of whether a limit is active.
    clone.source_throttle_limits.check_and_log("src-kafka-0");
    clone.source_throttle_limits.check_and_log("src-kafka-1");
    clone.source_throttle_limits.check_and_log("src-kafka-99");
}

