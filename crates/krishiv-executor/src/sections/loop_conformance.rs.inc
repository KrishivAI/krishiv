// ── Cross-loop conformance: the executor-side arms ───────────────────────
//
// The other two arms live in `crates/krishiv-api/src/streaming_conformance.rs`
// (the embedded engine and the runtime seam). All four read the same fixtures
// and the same expectations from `krishiv_dataflow::streaming_corpus`, which is
// the deepest crate both sides depend on.
//
// What these arms establish: neither executor-resident loop performs an
// end-of-stream flush. The cycle fragment's entire vocabulary is `drain` plus
// `snapshot`; the run-loop deliberately refuses to force-flush an unbounded job
// on stop. So both emit exactly the windows the watermark closed on its own —
// `CorpusEntry::expected_without_flush` — and not the windows a correct bounded
// run must produce.
//
// Every assertion below therefore describes TODAY's behaviour under a
// `BUG(...)` marker. That is deliberate: asserting the broken value pins the
// defect's exact blast radius, where asserting `cycle != embedded` would pass
// vacuously the day both loops break the same way — which is precisely the
// failure mode that let dd47d50 and 8756b41 happen weeks apart.

use krishiv_dataflow::streaming_corpus::{
    AGG_INPUT, AGG_OUTPUT, CORPUS, CorpusEntry, KEY_COLUMN, TIME_COLUMN, WINDOW_SIZE_MS,
    sorted_expectation, totals_from_batches,
};

/// The corpus query as a `WindowExecutionSpec`: tumbling, keyed, `SUM(amount)`.
///
/// Encoded rather than hand-written as a `stream:tw:` string so the aggregate is
/// unambiguous — the ASCII form in `RLOOP_WINDOW_SPEC` only expresses `count`,
/// which would silently measure a different query than the other two arms run.
fn corpus_window_spec() -> String {
    use krishiv_plan::window::{WindowAgg, WindowAggKind, WindowExecutionSpec};

    let mut spec = WindowExecutionSpec::tumbling(
        KEY_COLUMN,
        TIME_COLUMN,
        u64::try_from(WINDOW_SIZE_MS).expect("corpus window size is positive"),
    );
    spec.watermark_lag_ms = 0;
    spec.agg_exprs = vec![WindowAgg {
        kind: WindowAggKind::Sum,
        input_column: AGG_INPUT.to_owned(),
        output_column: AGG_OUTPUT.to_owned(),
        filter: None,
    }];
    krishiv_plan::window::encode_window_execution_spec(&spec).expect("encode corpus window spec")
}

/// Drive one corpus entry through the cycle fragment (`stream:loop:`).
///
/// One invocation, the whole fixture delivered as an inline IPC partition —
/// which is what a coordinator sends. The cycle task exists only for the
/// duration of this call, so whatever it has not emitted by the time it returns
/// is lost.
async fn via_cycle_fragment(entry: &CorpusEntry) -> Vec<(String, i64)> {
    let fragment = format!("stream:loop:cycle-{}|{}", entry.name, corpus_window_spec());
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let assignment = make_loop_assignment_with_batches(&fragment, &[entry.batch().expect("corpus batch")]);

    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .unwrap_or_else(|e| panic!("[{}] cycle fragment failed: {e}", entry.name));

    totals_from_batches(output.record_batches())
        .unwrap_or_else(|e| panic!("[{}] cycle output unreadable: {e}", entry.name))
}

/// Drive one corpus entry through the run-loop fragment (`stream:rloop:`).
///
/// Cancellation is the run-loop's only exit, so this pushes the fixture, gives
/// the loop a bounded settle window, then cancels. It deliberately does NOT
/// wait for an expected row count the way `run_rloop_job_to_completion` does:
/// two of the three fixtures are expected to produce nothing at all today, and
/// a row-count wait would turn that into a ten-second timeout instead of the
/// assertion it should be.
async fn via_run_loop_fragment(entry: &CorpusEntry) -> Vec<(String, i64)> {
    let job_id = &format!("rloop-{}", entry.name);
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_own_task_endpoint(RLOOP_LOCAL_ENDPOINT);
    let peers = rloop_peers_partition(1);

    let ids = TaskAttemptRef::new(
        JobId::try_new(job_id.as_str()).unwrap(),
        StageId::try_new("stage-streaming").unwrap(),
        TaskId::try_new("task-streaming-0").unwrap(),
        AttemptId::initial(),
    );
    let fragment = format!(
        "{STREAM_RLOOP_PREFIX}{job_id}|0/1|{}",
        corpus_window_spec()
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
    .with_requires_reattach(true)
    .with_input_partitions(vec![krishiv_proto::InputPartition::new(
        "stream-peers",
        peers.as_str(),
    )]);

    let task_runner = runner.clone();
    let handle = tokio::spawn(async move {
        task_runner
            .execute_streaming_fragment(&assignment)
            .await
    });

    // Let the subtask register its input notify and egress buffer before the
    // push — otherwise the notify lands before anyone is parked on it.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    runner
        .continuous_inputs
        .entry(job_id.clone())
        .or_default()
        .push(entry.batch().expect("corpus batch"));
    runner.notify_continuous_input(job_id);

    // Bounded settle: long enough for the loop to consume the push and emit
    // whatever the watermark closed, short enough that a fixture producing
    // nothing does not stall the suite.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    runner
        .inbox()
        .cancel_task(
            &JobId::try_new(job_id.as_str()).unwrap(),
            &TaskId::try_new("task-streaming-0").unwrap(),
        )
        .unwrap();
    runner.notify_continuous_input(&format!("{job_id}#task-streaming-0"));
    runner.notify_continuous_input(job_id);

    let output = handle
        .await
        .expect("run-loop task must not panic")
        .unwrap_or_else(|e| panic!("[{}] run-loop fragment failed: {e}", entry.name));
    assert_eq!(
        output.kind(),
        ExecutorTaskOutputKind::Cancelled,
        "[{}] a cancelled run-loop reports Cancelled",
        entry.name
    );

    let batches: Vec<RecordBatch> = runner
        .continuous_outputs
        .get(job_id.as_str())
        .map(|entry| entry.value().clone())
        .unwrap_or_default();
    totals_from_batches(&batches)
        .unwrap_or_else(|e| panic!("[{}] run-loop output unreadable: {e}", entry.name))
}

/// Neither executor-resident loop closes a window the watermark never passed.
///
/// Asserts equality with `expected_without_flush`, not merely "fewer rows than
/// expected". The `closed_window_plus_trailing_window` fixture is what makes
/// that precise: it must emit exactly one row — the window the watermark DID
/// close. An arm emitting zero there is a loop that never ran; an arm emitting
/// two has grown a flush. Both are interesting, and a looser assertion would
/// report neither.
#[tokio::test]
async fn executor_loops_emit_only_what_the_watermark_closed() {
    for entry in CORPUS {
        let cycle = via_cycle_fragment(entry).await;
        let run_loop = via_run_loop_fragment(entry).await;
        let without_flush = sorted_expectation(entry.expected_without_flush);
        let correct = sorted_expectation(entry.expected);

        // BUG(eos-flush-missing-on-both-distributed-loops): the cycle fragment
        // only ever calls `drain` and `snapshot` — there is no flush anywhere in
        // `fragment/streaming.rs`. Step 5 gives it one via a `stream-eos:`
        // directive and flips this to `correct`.
        assert_eq!(
            cycle, without_flush,
            "[{}] cycle fragment should emit only watermark-closed windows today",
            entry.name
        );

        // BUG(eos-flush-missing-on-both-distributed-loops): the run-loop
        // deliberately refuses to force-flush on an unbounded stop
        // (run_loop.rs:1112-1141), which is correct for an unbounded job and
        // wrong for a bounded one. It cannot currently receive a bounded job —
        // session.rs rejects bounded sources for it — so this arm pins the
        // behaviour rather than calling it a defect.
        assert_eq!(
            run_loop, without_flush,
            "[{}] run-loop fragment should emit only watermark-closed windows today",
            entry.name
        );

        // The gap this harness exists to close. Stated as an assertion so the
        // day a flush lands on either loop, this line fails and forces the
        // BUG markers above to be revisited rather than left stale.
        if entry.flush_is_observable() {
            assert_ne!(
                cycle, correct,
                "[{}] cycle now emits the trailing window — the flush landed, so the \
                 BUG markers in this file are stale and must be flipped",
                entry.name
            );
        }
    }
}

/// Drive one corpus entry through the cycle WITH an end-of-stream directive.
///
/// Same fragment, same fixture, one extra input partition — which is exactly
/// what `flush_continuous_stream_coordinated` schedules as a job's final cycle.
async fn via_cycle_fragment_at_end_of_stream(entry: &CorpusEntry) -> Vec<(String, i64)> {
    let fragment = format!("stream:loop:eos-{}|{}", entry.name, corpus_window_spec());
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let assignment = make_loop_assignment_with_batches(
        &fragment,
        &[entry.batch().expect("corpus batch")],
    );
    // Append the directive alongside the inline input the helper built.
    let mut partitions = assignment.input_partitions().to_vec();
    partitions.push(krishiv_proto::InputPartition::new("stream-eos", "stream-eos:1"));
    let assignment = assignment.with_input_partitions(partitions);

    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .unwrap_or_else(|e| panic!("[{}] cycle eos fragment failed: {e}", entry.name));

    totals_from_batches(output.record_batches())
        .unwrap_or_else(|e| panic!("[{}] cycle eos output unreadable: {e}", entry.name))
}

/// A cycle told the stream has ended closes every window it holds open.
///
/// This is the executor half of the coordinator-side fix, and it is provable
/// with no cluster: the directive is an input partition, and delivering one by
/// hand exercises the same read the scheduled final cycle triggers.
///
/// Deleting the `read_end_of_stream_directive` branch in
/// `execute_streaming_fragment` turns this red — while
/// `executor_loops_emit_only_what_the_watermark_closed` stays green, because
/// that one drives the cycle WITHOUT the directive. That red/green split is the
/// proof the flush is reached by the directive and not by something incidental.
#[tokio::test]
async fn a_cycle_told_the_stream_ended_closes_its_open_windows() {
    for entry in CORPUS {
        let without = via_cycle_fragment(entry).await;
        let with = via_cycle_fragment_at_end_of_stream(entry).await;

        assert_eq!(
            with,
            sorted_expectation(entry.expected),
            "[{}] a cycle at end-of-stream must emit every window a correct \
             bounded run produces",
            entry.name
        );

        if entry.flush_is_observable() {
            assert_ne!(
                with, without,
                "[{}] the directive must change the outcome, or this test is \
                 passing for a reason unrelated to the flush",
                entry.name
            );
        }
    }
}

/// The two executor loops agree with each other, whatever they do.
///
/// Separate from the assertion above on purpose: that one pins them to a
/// specific value, this one pins them to each other. A change that moves both
/// loops together passes the second and fails the first, which is the signal
/// that a behaviour changed deliberately; a change that moves only one fails
/// both, which is the signal that they drifted.
#[tokio::test]
async fn the_two_executor_loops_agree_with_each_other() {
    for entry in CORPUS {
        let cycle = via_cycle_fragment(entry).await;
        let run_loop = via_run_loop_fragment(entry).await;
        assert_eq!(
            cycle, run_loop,
            "[{}] the cycle and run-loop fragments closed different windows",
            entry.name
        );
    }
}
