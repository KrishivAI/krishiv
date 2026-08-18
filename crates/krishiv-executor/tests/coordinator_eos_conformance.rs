//! The coordinator arm of the cross-loop streaming conformance harness.
//!
//! Every other arm lives in-crate: `krishiv-api`'s `streaming_conformance`
//! drives the embedded engine and the runtime seam, and this crate's
//! `sections/loop_conformance.rs.inc` drives the cycle and run-loop fragments
//! directly. All of them stop short of the placement that actually runs in
//! production for a distributed job: a **coordinator-backed** cluster, where
//! there is no registry entry and no local operator, and the only way to reach
//! the operator is to schedule work.
//!
//! That gap is why step 5 of the streaming rework shipped saying its coordinator
//! fix was "reasoned and unit-tested, not demonstrated". This file demonstrates
//! it.
//!
//! # What it stands up
//!
//! Two real gRPC servers in one process:
//!
//! 1. the **coordinator's** executor-facing server, so the runner can report
//!    task status and results back and the cycle's output lands in the
//!    coordinator's inline result store;
//! 2. the **executor's** task server on a real TCP port, sharing its continuous
//!    state (`loop_executors`, `continuous_inputs`) with a runner.
//!
//! A real port matters: `push_continuous_input_coordinated` and the end-of-
//! stream cycle both reject `is_in_process_task_endpoint`, so the usual
//! `IN_PROCESS_TASK_ENDPOINT` fixture cannot exercise this path at all. That
//! rejection is exactly why no earlier test covered it.
//!
//! # What it proves
//!
//! A bounded windowed job whose events all fall inside one window emits
//! NOTHING from a drain — the watermark never passed the window end — and emits
//! the window only after `flush_continuous_stream_coordinated` schedules a final
//! cycle carrying `stream-eos:`. That is the whole defect and the whole fix,
//! measured end to end rather than in two halves that were each assumed to meet.

// Integration-test crate: helpers run outside `#[test]` fns, so clippy.toml's
// `allow-unwrap-in-tests` does not reach them. A panic is the failure signal here.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use krishiv_dataflow::streaming_corpus::{CORPUS, sorted_expectation, totals_from_batches};
use krishiv_executor::{ExecutorAssignmentInbox, GrpcCoordinatorService};
use krishiv_proto::{CoordinatorId, ExecutorDescriptor, ExecutorId, LeaseGeneration};
use krishiv_scheduler::{ContinuousRegistrationOptions, Coordinator, SharedCoordinator};

/// Build the corpus query as a window spec: tumbling, keyed, `SUM(amount)`.
fn corpus_spec() -> krishiv_plan::window::WindowExecutionSpec {
    use krishiv_dataflow::streaming_corpus::{
        AGG_INPUT, AGG_OUTPUT, KEY_COLUMN, TIME_COLUMN, WINDOW_SIZE_MS,
    };
    use krishiv_plan::window::{WindowAgg, WindowAggKind, WindowExecutionSpec};

    let mut spec = WindowExecutionSpec::tumbling(
        KEY_COLUMN,
        TIME_COLUMN,
        u64::try_from(WINDOW_SIZE_MS).unwrap(),
    );
    spec.watermark_lag_ms = 0;
    spec.agg_exprs = vec![WindowAgg {
        kind: WindowAggKind::Sum,
        input_column: AGG_INPUT.to_owned(),
        output_column: AGG_OUTPUT.to_owned(),
        filter: None,
    }];
    spec
}

fn ipc_bytes(batch: &arrow::record_batch::RecordBatch) -> Vec<u8> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

/// A coordinator plus a dispatchable executor, both on real ports.
struct Rig {
    coordinator: SharedCoordinator,
}

async fn start_rig(name: &str) -> Rig {
    // The coordinator's executor-facing gRPC denies anonymous access by
    // default, which is the right production posture and would otherwise make
    // every task-status report from the runner fail with UNAUTHENTICATED — and
    // a runner that cannot report is indistinguishable from one that never ran.
    // Process-global, which is safe here: this integration binary holds one
    // test and does not share a process with the unit suites.
    let _ = krishiv_scheduler::set_allow_anonymous();

    // ── coordinator's executor-facing gRPC ────────────────────────────────
    let coord_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coord_addr = coord_listener.local_addr().unwrap();

    let coordinator = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new(format!("{name}-coord")).unwrap(),
    ));

    {
        let shared = coordinator.clone();
        tokio::spawn(async move {
            let _ = krishiv_scheduler::serve_coordinator_executor_grpc_with_listener(
                coord_listener,
                shared,
            )
            .await;
        });
    }

    // ── executor's task gRPC, sharing continuous state with a runner ──────
    let exec_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let exec_addr = exec_listener.local_addr().unwrap();

    let inbox = ExecutorAssignmentInbox::new();
    let loop_executors: krishiv_executor::grpc::SharedLoopExecutors = Arc::new(DashMap::new());
    let continuous_inputs: krishiv_executor::grpc::SharedContinuousInputs =
        Arc::new(DashMap::new());

    {
        let inbox = inbox.clone();
        let loop_executors = Arc::clone(&loop_executors);
        let continuous_inputs = Arc::clone(&continuous_inputs);
        tokio::spawn(async move {
            let _ =
                krishiv_executor::transport::serve_executor_task_grpc_with_listener_and_continuous(
                    exec_listener,
                    inbox,
                    loop_executors,
                    continuous_inputs,
                )
                .await;
        });
    }

    // ── the runner that actually executes what the coordinator dispatches ──
    let runner = krishiv_executor::ExecutorTaskRunner::new(inbox)
        .with_shared_loop_executors(Arc::clone(&loop_executors))
        .with_shared_continuous_inputs(Arc::clone(&continuous_inputs));
    let coord_endpoint = format!("http://{coord_addr}");
    tokio::spawn(async move {
        let service = GrpcCoordinatorService::new(coord_endpoint, LeaseGeneration::initial());
        loop {
            match runner.run_next_with(&service).await {
                Ok(Some(_)) => {}
                Ok(None) => tokio::time::sleep(Duration::from_millis(5)).await,
                // Surfaced rather than swallowed: a runner that cannot report
                // looks exactly like one that never ran, and chasing that
                // distinction is what this loop's silence used to cost.
                Err(error) => {
                    eprintln!("[rig] run_next_with failed: {error}");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    });

    coordinator
        .write()
        .await
        .register_executor(
            ExecutorDescriptor::new(
                ExecutorId::try_new(format!("{name}-exec")).unwrap(),
                format!("http://{exec_addr}"),
                4,
            )
            // The registration endpoint and the TASK endpoint are separate
            // fields; assignments dispatch to the latter. Omitting it is what
            // "has no task endpoint for assignment push" means.
            .with_task_endpoint(format!("http://{exec_addr}")),
        )
        .unwrap();

    // Let both servers accept before anything dials them.
    tokio::time::sleep(Duration::from_millis(150)).await;
    Rig { coordinator }
}

/// A coordinator-backed bounded job closes its trailing window, and only does
/// so because of the end-of-stream cycle.
///
/// The drain assertion is what makes this non-vacuous: if it already returned
/// the window, the flush would be proving nothing. It must be empty first.
#[tokio::test]
async fn a_coordinator_backed_job_closes_its_trailing_window_only_after_the_eos_cycle() {
    let entry = CORPUS
        .iter()
        .find(|e| e.name == "closed_window_plus_trailing_window")
        .expect("the partial-loss fixture");
    // Deliberately the PARTIAL-loss fixture, not the all-or-nothing one. Its
    // `expected_without_flush` is non-empty ([("a",30)]), so the drain
    // assertion below can distinguish "the cycle ran and the watermark closed
    // one window" from "the cycle never ran at all". With the all-or-nothing
    // fixture both look like an empty drain and the assertion passes vacuously
    // — which it did on the first attempt at this test.

    let rig = start_rig("eos").await;
    let job = "eos-trailing-job";

    krishiv_scheduler::register_continuous_stream_with_options(
        &rig.coordinator,
        job,
        &corpus_spec(),
        &ContinuousRegistrationOptions::default(),
    )
    .await
    .expect("register a cycle-mode continuous job");

    krishiv_scheduler::push_continuous_input_coordinated(
        &rig.coordinator,
        job,
        ipc_bytes(&entry.batch().expect("corpus batch")),
    )
    .await
    .expect("push the fixture through the coordinator");

    // Give the dispatched cycle time to run and report its result.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let drained = krishiv_scheduler::drain_continuous_stream_coordinated(&rig.coordinator, job)
        .await
        .expect("drain");
    let drained_batches = krishiv_scheduler::decode_inline_record_batches(&drained).unwrap();
    let drained_totals = totals_from_batches(&drained_batches).unwrap();
    assert_eq!(
        drained_totals,
        sorted_expectation(entry.expected_without_flush),
        "a drain must return only what the watermark closed — if this fixture's window \
         already came back here, the flush below would be proving nothing"
    );

    let flushed = krishiv_scheduler::flush_continuous_stream_coordinated(&rig.coordinator, job)
        .await
        .expect("the end-of-stream cycle must complete");
    let flushed_batches = krishiv_scheduler::decode_inline_record_batches(&flushed).unwrap();
    let flushed_totals = totals_from_batches(&flushed_batches).unwrap();

    // The flush returns only what the FLUSH produced: the coordinator's inline
    // result store is consume-once, and the drain above already took the window
    // the watermark closed. So the job's whole answer is the union, and that is
    // what a caller assembles.
    let mut whole_answer = drained_totals.clone();
    whole_answer.extend(flushed_totals.iter().cloned());
    whole_answer.sort();

    assert_eq!(
        flushed_totals,
        sorted_expectation(&[("a", 7)]),
        "the end-of-stream cycle must produce exactly the window the watermark never \
         reached — no more (it must not re-emit what the drain already took) and no \
         less (that omission is the defect)"
    );
    assert_eq!(
        whole_answer,
        sorted_expectation(entry.expected),
        "drain + flush must together be the complete answer; before the end-of-stream \
         cycle existed, a Distributed bounded job returned only the drain half and \
         reported success"
    );
}
