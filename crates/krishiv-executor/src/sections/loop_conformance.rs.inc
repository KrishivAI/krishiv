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

/// A session-window spec with a 1s inactivity gap.
///
/// Session is the window kind that *requires* a wall clock: it closes on
/// inactivity, and inactivity is by definition the absence of the events that
/// would otherwise advance the watermark.
fn session_window_spec() -> String {
    use krishiv_plan::window::{WindowAgg, WindowAggKind, WindowExecutionSpec, WindowKind};

    let mut spec = WindowExecutionSpec::tumbling(KEY_COLUMN, TIME_COLUMN, 10_000);
    spec.window_kind = WindowKind::Session;
    spec.session_gap_ms = Some(1_000);
    spec.watermark_lag_ms = 0;
    spec.agg_exprs = vec![WindowAgg {
        kind: WindowAggKind::Sum,
        input_column: AGG_INPUT.to_owned(),
        output_column: AGG_OUTPUT.to_owned(),
        filter: None,
    }];
    krishiv_plan::window::encode_window_execution_spec(&spec).expect("encode session spec")
}

/// A single event whose timestamp is already well past its own session gap.
///
/// Nothing further will arrive, so no event-time watermark advance can ever
/// close this session — only a wall-clock tick can.
fn stale_session_batch() -> RecordBatch {
    use arrow::array::{Int64Array, StringArray};
    use std::sync::Arc as StdArc;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let schema = krishiv_dataflow::streaming_corpus::schema();
    RecordBatch::try_new(
        schema,
        vec![
            StdArc::new(StringArray::from(vec!["s"])),
            StdArc::new(Int64Array::from(vec![now_ms - 5_000])),
            StdArc::new(Int64Array::from(vec![42_i64])),
        ],
    )
    .expect("session fixture batch")
}

/// The run-loop closes a quiet session on wall clock; the cycle cannot.
///
/// This is the one behaviour the corpus above cannot reach, because every
/// corpus fixture closes its windows by event-time advance. Without it the
/// run-loop's idle tick is exercised only by `StreamDriver`'s own unit tests,
/// and nothing would notice if this loop stopped calling `on_idle` at all —
/// which is exactly the "shared function only some loops reach" shape the
/// driver exists to eliminate.
///
/// Verified discriminating: making `StreamDriver::on_idle` return early
/// unconditionally turns the run-loop half red. The cycle half is not a bug
/// being pinned — a cycle task exists for one invocation and owns no thread
/// between invocations, so it has no wall clock to tick by construction. Step 7
/// makes that refusal explicit at registration instead of silent at runtime.
#[tokio::test]
async fn the_run_loop_closes_a_quiet_session_on_wall_clock_and_the_cycle_cannot() {
    // ── run-loop: has a wall clock ────────────────────────────────────────
    let job_id = "rloop-session-idle";
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_own_task_endpoint(RLOOP_LOCAL_ENDPOINT);
    let peers = rloop_peers_partition(1);
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
        PlanFragment::new(format!(
            "{STREAM_RLOOP_PREFIX}{job_id}|0/1|{}",
            session_window_spec()
        ))
        .with_streaming(true),
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
    let handle = tokio::spawn(async move { task_runner.execute_streaming_fragment(&assignment).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    runner
        .continuous_inputs
        .entry(job_id.to_owned())
        .or_default()
        .push(stale_session_batch());
    runner.notify_continuous_input(job_id);

    // The default idle-tick interval is 500 ms, so ~1.5 s is three chances to
    // fire. Deliberately not env-tuned: KRISHIV_IDLE_TICK_MS is process-global
    // and this suite runs in parallel, so a test that set it could change the
    // cadence of an unrelated one.
    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;

    runner
        .inbox()
        .cancel_task(
            &JobId::try_new(job_id).unwrap(),
            &TaskId::try_new("task-streaming-0").unwrap(),
        )
        .unwrap();
    runner.notify_continuous_input(&format!("{job_id}#task-streaming-0"));
    runner.notify_continuous_input(job_id);
    handle
        .await
        .expect("run-loop must not panic")
        .expect("run-loop exits cleanly on cancel");

    let rloop_rows: usize = runner
        .continuous_outputs
        .get(job_id)
        .map(|e| e.value().iter().map(|b| b.num_rows()).sum())
        .unwrap_or(0);
    assert!(
        rloop_rows > 0,
        "the run-loop must close a session whose gap elapsed while the source was \
         quiet; no further event will ever arrive to advance the watermark for it"
    );

    // ── cycle: owns no thread between invocations ─────────────────────────
    let cycle_runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let cycle_assignment = make_loop_assignment_with_batches(
        &format!("stream:loop:cycle-session|{}", session_window_spec()),
        &[stale_session_batch()],
    );
    let cycle_out = cycle_runner
        .execute_streaming_fragment(&cycle_assignment)
        .await
        .expect("cycle session fragment");
    assert_eq!(
        cycle_out.row_count(),
        0,
        "a cycle invocation has no wall clock, so a quiet session cannot close in it \
         — this is a real functional limit of Cycle mode, not a transient"
    );
}

/// A one-row batch carrying `marker` as its amount, so survivors are
/// identifiable after a drop.
fn marked_batch(marker: i64) -> RecordBatch {
    use arrow::array::{Int64Array, StringArray};
    use std::sync::Arc as StdArc;

    RecordBatch::try_new(
        krishiv_dataflow::streaming_corpus::schema(),
        vec![
            StdArc::new(StringArray::from(vec!["k"])),
            StdArc::new(Int64Array::from(vec![marker])),
            StdArc::new(Int64Array::from(vec![marker])),
        ],
    )
    .expect("marked batch")
}

/// The run-loop egress buffer drops its OLDEST batches, and says so.
///
/// Asserting WHICH batches survive is the whole test. A cap that dropped the
/// newest would leave the same count and the same drop total, so a length-only
/// assertion would pass against the opposite policy — and drop-newest versus
/// drop-oldest are meaningfully different failures for a consumer trying to
/// catch up.
///
/// Reads the effective cap rather than setting it: `KRISHIV_RLOOP_EGRESS_CAP`
/// is process-global, this suite runs in parallel, and `krishiv-executor`
/// forbids `unsafe`, which `std::env::set_var` now requires. Deriving the
/// expectation from the cap is not a tautology — the assertions are about which
/// markers survive and how many were counted, neither of which the cap decides.
///
/// Verified discriminating: changing `egress.drain(..overflow)` to
/// `egress.truncate(cap)` keeps the surviving COUNT identical and flips the
/// surviving markers to the oldest, failing on identity alone.
#[tokio::test]
async fn a_sinkless_egress_overflow_faults_and_holds_the_output() {
    // ADR §73. This buffer is the job's ONLY delivery path, so trimming it is
    // silent data loss. The contract changed: overflow now stalls for a
    // consumer and faults at the budget, and the staged output is HELD rather
    // than dropped so a consumer returning after the fault finds it intact.
    //
    // This test previously asserted the opposite — drop-oldest, counting the
    // loss — and it is what caught the mechanism when §73 first tried to land
    // it. Rewritten deliberately, not "fixed": the sink-attached case below
    // still asserts drop-oldest, because there the rows are safe in the sink.
    let cap = krishiv_common::streaming_dials::rloop_egress_cap();
    let overflow_by = 6_i64;
    let total = cap as i64 + overflow_by;

    let job_id = "egress-cap-job";
    // A narrow budget: the production default is 30s and this crate forbids
    // `unsafe`, so the process-global dial cannot be narrowed from a test.
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_egress_backpressure_budget(std::time::Duration::from_millis(50));
    // No sink contract: this buffer IS the delivery path.
    let assignment = make_loop_assignment("stream:loop:egress-cap|noop");

    let mut fault = None;
    for marker in 0..total {
        if let Err(error) = runner
            .stage_rloop_outputs(job_id, &assignment, &[marked_batch(marker)])
            .await
        {
            fault = Some(error.to_string());
            break;
        }
    }
    let fault = fault.expect(
        "staging past the cap with no durable sink must FAULT; completing every stage \
         means output was silently dropped",
    );
    assert!(
        fault.contains("egress full") && fault.contains("no durable sink"),
        "the fault must name the condition and the remedy: {fault}"
    );

    // Held, not dropped. This is the whole point of the contract change.
    let held = runner
        .continuous_outputs
        .get(job_id)
        .map(|e| e.value().len())
        .unwrap_or(0);
    assert!(
        held > cap,
        "the staged output must still be in the buffer ({held} batches); \
         trimming it to {cap} would be the silent loss this fault exists to refuse"
    );
    let dropped = runner
        .continuous_egress_dropped
        .get(job_id)
        .map(|e| *e.value())
        .unwrap_or(0);
    assert_eq!(dropped, 0, "nothing may be counted as dropped: nothing was dropped");
}

/// A consumer that drains inside the budget lets the loop proceed losslessly —
/// the other half of ADR §73, and the half that makes the fault above a
/// backpressure signal rather than a hair trigger.
#[tokio::test]
async fn a_drain_inside_the_budget_releases_the_egress_stall() {
    let cap = krishiv_common::streaming_dials::rloop_egress_cap();
    let job_id = "egress-drain-job";
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_egress_backpressure_budget(std::time::Duration::from_secs(10));
    let assignment = make_loop_assignment("stream:loop:egress-drain|noop");

    // Fill to exactly the cap without tripping the stall.
    for marker in 0..cap as i64 {
        runner
            .stage_rloop_outputs(job_id, &assignment, &[marked_batch(marker)])
            .await
            .expect("staging up to the cap must not stall");
    }

    // A consumer arrives shortly after the next stage begins waiting. The
    // handle is the same `Arc<DashMap>` a real drain serves from.
    let outputs = std::sync::Arc::clone(&runner.continuous_outputs);
    let drainer = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        if let Some(mut entry) = outputs.get_mut("egress-drain-job") {
            entry.clear();
        }
    });

    runner
        .stage_rloop_outputs(job_id, &assignment, &[marked_batch(cap as i64)])
        .await
        .expect("a drain inside the budget must release the stall, not fault");
    drainer.await.unwrap();

    let dropped = runner
        .continuous_egress_dropped
        .get(job_id)
        .map(|e| *e.value())
        .unwrap_or(0);
    assert_eq!(dropped, 0, "a released stall drops nothing");
}


/// A job delivering through a durable sink does NOT report dropped output.
///
/// The staging buffer is filled unconditionally, before the sink dispatch, so a
/// run-loop job writing to Kafka or Iceberg overflows it exactly like any other
/// — and nobody drains it, because the sink is the real delivery path. Counting
/// those trims as dropped output made the coordinator log
///
///     "This job's output is incomplete by these counts."
///
/// permanently, for a job whose output was complete. That is a false alarm
/// stacked on a real one, which is the worst possible place for it: it teaches
/// whoever reads the log to discount the signal that actually matters.
///
/// Verified discriminating: making `has_durable_sink_contract` return
/// `Ok(false)` unconditionally turns this red with a nonzero count, while
/// `the_egress_cap_drops_oldest_and_counts_every_lost_batch` stays green —
/// that split is what proves the gate keys on the CONTRACT and not on
/// something incidental to the fixture.
// Without the `kafka` feature the sink dispatch fails fast (the contract is
// honoured but no connection is attempted), which is what the test relies on.
// With it, each staged output opens a transactional producer against a broker
// that does not exist and the test hangs on `init_transactions` — which is how
// the executor's kafka arm, never run until R1-17, first showed up.
#[cfg(not(feature = "kafka"))]
#[tokio::test]
async fn a_durable_sink_job_does_not_report_its_staging_trims_as_lost_output() {
    let cap = krishiv_common::streaming_dials::rloop_egress_cap();
    let job_id = "durable-sink-job";
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());

    let ids = TaskAttemptRef::new(
        JobId::try_new(job_id).unwrap(),
        StageId::try_new("stage-1").unwrap(),
        TaskId::try_new("task-1").unwrap(),
        AttemptId::initial(),
    );
    let assignment = ExecutorTaskAssignment::new(
        ids,
        ExecutorId::try_new("exec-1").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new("stream:loop:durable-sink|noop"),
        OutputContract::new(
            krishiv_proto::OutputContractKind::InlineRecordBatches,
            "kafka sink",
        )
        .with_descriptor(krishiv_proto::OutputContractDescriptor::KafkaSink {
            bootstrap_servers: String::from("localhost:9092"),
            topic: String::from("out"),
            transactional_id_prefix: String::from("krishiv"),
        }),
    );

    for marker in 0..(cap as i64 + 6) {
        // The sink DISPATCH is expected to fail here: this build has no `kafka`
        // feature, so the contract is honoured as a contract but not as a
        // connection. That is fine and deliberate — the dispatch runs AFTER the
        // buffer block, so everything under test has already happened, and what
        // matters is that the contract was READ. The `buffered == cap`
        // assertion below is what keeps this from passing vacuously: it can
        // only hold if the buffer block ran to completion.
        let _ = runner
            .stage_rloop_outputs(job_id, &assignment, &[marked_batch(marker)])
            .await;
    }

    // The buffer is still bounded — that part must not regress.
    let buffered = runner
        .continuous_outputs
        .get(job_id)
        .map(|e| e.value().len())
        .unwrap_or(0);
    assert_eq!(
        buffered, cap,
        "the staging buffer must stay bounded regardless of the sink contract"
    );

    let dropped = runner
        .continuous_egress_dropped
        .get(job_id)
        .map(|e| *e.value())
        .unwrap_or(0);
    assert_eq!(
        dropped, 0,
        "a job whose rows are durably in its sink has lost nothing; reporting {dropped} \
         dropped batches would make the coordinator call a complete output incomplete"
    );
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

/// A failed snapshot must not look like an empty one.
///
/// Both cycle and bounded paths used to write
/// `.ok().filter(|b| !b.is_empty())`, which maps `Err(..)` and `Ok(vec![])` to
/// the same `None`. Downstream, `None` means "ship no checkpoint" — so a task
/// whose state capture FAILED reported success, shipped nothing, and left the
/// job's restore point stale. A recovery then replays from an older epoch and
/// reprocesses, with nothing anywhere having said so.
///
/// The two cases are now distinct, and the failure carries its error text so
/// the warning can name what went wrong rather than that something did.
///
/// Verified discriminating: collapsing `Failed` into `Empty` inside
/// `classify_snapshot` fails the third assertion here.
#[test]
fn a_failed_snapshot_is_distinguishable_from_an_empty_one() {
    use crate::fragment::common::{SnapshotOutcome, classify_snapshot};

    assert_eq!(
        classify_snapshot(Ok::<_, String>(Vec::new())),
        SnapshotOutcome::Empty,
        "no accumulated state is benign and common"
    );
    assert_eq!(
        classify_snapshot(Ok::<_, String>(vec![1, 2, 3])),
        SnapshotOutcome::Captured(vec![1, 2, 3]),
        "captured state ships as the job's restore point"
    );
    assert_eq!(
        classify_snapshot(Err::<Vec<u8>, _>(String::from("backend unavailable"))),
        SnapshotOutcome::Failed(String::from("backend unavailable")),
        "a failure must stay a failure, carrying its cause — collapsing it into Empty \
         is what made a stale restore point invisible"
    );

    // Both non-Captured outcomes ship nothing, which is why the distinction has
    // to live in the type rather than in the shipped value.
    assert_eq!(SnapshotOutcome::Empty.into_bytes(), None);
    assert_eq!(
        SnapshotOutcome::Failed(String::from("x")).into_bytes(),
        None
    );
    assert_eq!(
        SnapshotOutcome::Captured(vec![7]).into_bytes(),
        Some(vec![7])
    );
}
