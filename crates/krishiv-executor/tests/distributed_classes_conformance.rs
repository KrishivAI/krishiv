//! Real-gRPC conformance for the classed streaming fragments (task #147):
//! `stream:rjoin:`, `stream:rbatch:`, `stream:rpipe:` executed by a REAL
//! executor runner behind real ports, registered through the class-routed
//! coordinator path, fed by side-tagged pushes, drained through the run-loop
//! egress. The in-process rig deliberately bypasses fragments, so THIS is
//! what pins that the classed fragments actually run.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::field_reassign_with_default
)]

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use krishiv_executor::{ExecutorAssignmentInbox, GrpcCoordinatorService};
use krishiv_proto::{CoordinatorId, ExecutorDescriptor, ExecutorId, LeaseGeneration};
use krishiv_scheduler::{ContinuousRegistrationOptions, Coordinator, SharedCoordinator};

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
    /// The class-state maps shared between the gRPC service and the runner —
    /// exposed so tests can assert cancel retires them.
    class_executors: krishiv_executor::grpc::SharedClassExecutors,
}

async fn start_rig(name: &str) -> Rig {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
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
    let continuous_outputs: krishiv_executor::runner::SharedContinuousOutputs =
        Arc::new(DashMap::new());
    let input_notify: krishiv_executor::runner::SharedContinuousNotify = Arc::new(DashMap::new());
    let connector_sources: krishiv_executor::runner::SharedContinuousConnectorSources =
        Arc::new(DashMap::new());
    let class_executors = krishiv_executor::grpc::SharedClassExecutors::default();

    {
        let inbox = inbox.clone();
        let loop_executors = Arc::clone(&loop_executors);
        let continuous_inputs = Arc::clone(&continuous_inputs);
        let continuous_outputs = Arc::clone(&continuous_outputs);
        let input_notify = Arc::clone(&input_notify);
        let connector_sources = Arc::clone(&connector_sources);
        let class_executors = class_executors.clone();
        tokio::spawn(async move {
            // The run-loop server: shares egress buffers and input notifies
            // with the runner, which is what run-loop drain/push need — the
            // continuous-only server was enough for the cycle-mode eos test
            // but leaves run-loop egress unreachable.
            let _ = krishiv_executor::transport::serve_executor_task_grpc_with_run_loop(
                exec_listener,
                inbox,
                loop_executors,
                continuous_inputs,
                continuous_outputs,
                input_notify,
                connector_sources,
                class_executors,
            )
            .await;
        });
    }

    // ── the runner that actually executes what the coordinator dispatches ──
    let runner = krishiv_executor::ExecutorTaskRunner::new(inbox)
        .with_shared_loop_executors(Arc::clone(&loop_executors))
        .with_shared_continuous_inputs(Arc::clone(&continuous_inputs))
        .with_shared_continuous_outputs(Arc::clone(&continuous_outputs))
        .with_shared_continuous_notify(Arc::clone(&input_notify))
        .with_shared_continuous_connector_sources(Arc::clone(&connector_sources))
        .with_shared_class_executors(class_executors.clone());
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
    Rig {
        coordinator,
        class_executors,
    }
}

async fn wait_drain(
    rig: &Rig,
    job: &str,
    min_rows: usize,
    max_wait: Duration,
) -> Vec<arrow::record_batch::RecordBatch> {
    let deadline = tokio::time::Instant::now() + max_wait;
    let mut all: Vec<arrow::record_batch::RecordBatch> = Vec::new();
    loop {
        let drained = krishiv_scheduler::drain_continuous_stream_coordinated(&rig.coordinator, job)
            .await
            .unwrap_or_default();
        if let Ok(batches) = krishiv_scheduler::decode_inline_record_batches(&drained) {
            all.extend(batches);
        }
        let rows: usize = all.iter().map(|b| b.num_rows()).sum();
        if rows >= min_rows || tokio::time::Instant::now() >= deadline {
            return all;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn i64_col(batch: &arrow::record_batch::RecordBatch, name: &str) -> Vec<i64> {
    let idx = batch.schema().index_of(name).expect(name);
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Int64");
    (0..batch.num_rows()).map(|i| arr.value(i)).collect()
}

fn two_col(
    name_a: &str,
    name_b: &str,
    a: Vec<i64>,
    b: Vec<i64>,
) -> arrow::record_batch::RecordBatch {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new(name_a, DataType::Int64, false),
        Field::new(name_b, DataType::Int64, false),
    ]));
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
    )
    .unwrap()
}

/// stream:rjoin: joins by VALUE across the real wire: bid 1 and 2 match
/// their auctions, the orphan bid 9 does not, and matches emit with both
/// sides' columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rjoin_fragment_joins_across_the_real_wire() {
    use krishiv_plan::stream_join::StreamingJoinSpec;
    use krishiv_plan::stream_task::StreamingTaskSpec;
    let rig = start_rig("rjoin").await;
    let job = "rjoin-conf";
    let task = StreamingTaskSpec::Join(Box::new(StreamingJoinSpec {
        left_source: "bid".into(),
        right_source: "auction".into(),
        time_column: "ts".into(),
        left_key_column: "k".into(),
        right_key_column: "k".into(),
        window_ms: 10_000,
    }));
    let mut options = ContinuousRegistrationOptions::default();
    options.mode = Some("run-loop".into());
    options.parallelism = Some(1);
    krishiv_scheduler::register_continuous_task_with_options(
        &rig.coordinator,
        job,
        &task,
        &options,
    )
    .await
    .expect("classed join registers against a REAL executor");
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Auctions first (buffered), then bids (matching emits on arrival).
    krishiv_scheduler::push_continuous_input_side_coordinated(
        &rig.coordinator,
        job,
        "R",
        ipc_bytes(&two_col("k", "ts", vec![1, 2], vec![1_000, 1_001])),
    )
    .await
    .expect("push right");
    krishiv_scheduler::push_continuous_input_side_coordinated(
        &rig.coordinator,
        job,
        "L",
        ipc_bytes(&two_col(
            "k",
            "ts",
            vec![1, 2, 9],
            vec![1_050, 1_060, 1_070],
        )),
    )
    .await
    .expect("push left");

    let out = wait_drain(&rig, job, 2, Duration::from_secs(10)).await;
    let rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 2,
        "bids 1 and 2 join; orphan bid 9 must not fabricate"
    );
}

/// stream:rbatch: runs the stateless SQL per pushed batch on the real wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rbatch_fragment_runs_stateless_sql_across_the_real_wire() {
    use krishiv_plan::stream_task::{StatelessQuerySpec, StreamingTaskSpec};
    let rig = start_rig("rbatch").await;
    let job = "rbatch-conf";
    let task = StreamingTaskSpec::Stateless(Box::new(StatelessQuerySpec {
        sql: "SELECT v * 2 AS doubled FROM src WHERE v > 10".into(),
        source: "src".into(),
        side_tables: vec![],
    }));
    let mut options = ContinuousRegistrationOptions::default();
    options.mode = Some("run-loop".into());
    options.parallelism = Some(1);
    krishiv_scheduler::register_continuous_task_with_options(
        &rig.coordinator,
        job,
        &task,
        &options,
    )
    .await
    .expect("classed stateless registers");
    tokio::time::sleep(Duration::from_millis(400)).await;

    krishiv_scheduler::push_continuous_input_coordinated(
        &rig.coordinator,
        job,
        ipc_bytes(&two_col("v", "unused", vec![5, 20, 30], vec![0, 0, 0])),
    )
    .await
    .expect("push");

    let out = wait_drain(&rig, job, 2, Duration::from_secs(10)).await;
    let mut doubled: Vec<i64> = out.iter().flat_map(|b| i64_col(b, "doubled")).collect();
    doubled.sort_unstable();
    assert_eq!(doubled, vec![40, 60], "5 filtered out; 20 and 30 doubled");
}

/// stream:rpipe: runs a join→top-1 pipeline (NEXMark Q9's shape) across the
/// real wire: the winning bid per auction emerges only after the watermark
/// closes the stage window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpipe_fragment_runs_a_pipeline_across_the_real_wire() {
    use krishiv_plan::stream_task::StreamingTaskSpec;
    let rig = start_rig("rpipe").await;
    let job = "rpipe-conf";
    let sql = "WITH joined AS (SELECT b.k, b.bidder, b.price FROM bid b \
               JOIN auction a ON b.k = a.id \
               AND b.ts BETWEEN a.ts - 10000 AND a.ts + 10000) \
               SELECT k, bidder, price \
               FROM TUMBLE(TABLE joined, DESCRIPTOR(left_ts), 10000) \
               GROUP BY k, window_start, window_end ORDER BY price DESC LIMIT 1";
    let plan = krishiv_sql::streaming_pipeline_plan::compile_streaming_pipeline_sql(sql)
        .expect("pipeline compiles");
    let task = StreamingTaskSpec::Pipeline(Box::new(plan.spec));
    let mut options = ContinuousRegistrationOptions::default();
    options.mode = Some("run-loop".into());
    options.parallelism = Some(1);
    krishiv_scheduler::register_continuous_task_with_options(
        &rig.coordinator,
        job,
        &task,
        &options,
    )
    .await
    .expect("classed pipeline registers");
    tokio::time::sleep(Duration::from_millis(400)).await;

    fn bids(
        k: Vec<i64>,
        bidder: Vec<i64>,
        price: Vec<i64>,
        ts: Vec<i64>,
    ) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("bidder", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(k)),
                Arc::new(Int64Array::from(bidder)),
                Arc::new(Int64Array::from(price)),
                Arc::new(Int64Array::from(ts)),
            ],
        )
        .unwrap()
    }

    krishiv_scheduler::push_continuous_input_side_coordinated(
        &rig.coordinator,
        job,
        "R",
        ipc_bytes(&two_col("id", "ts", vec![1], vec![1_000])),
    )
    .await
    .expect("auction");
    krishiv_scheduler::push_continuous_input_side_coordinated(
        &rig.coordinator,
        job,
        "L",
        ipc_bytes(&bids(
            vec![1, 1],
            vec![91, 92],
            vec![100, 900],
            vec![1_050, 1_060],
        )),
    )
    .await
    .expect("bids");
    // Advance both sides far past the stage window so the top-1 closes:
    // a matching pair at t=200k moves the min-of-sides watermark.
    krishiv_scheduler::push_continuous_input_side_coordinated(
        &rig.coordinator,
        job,
        "R",
        ipc_bytes(&two_col("id", "ts", vec![7], vec![200_000])),
    )
    .await
    .expect("advance right");
    krishiv_scheduler::push_continuous_input_side_coordinated(
        &rig.coordinator,
        job,
        "L",
        ipc_bytes(&bids(vec![7], vec![99], vec![1], vec![200_000])),
    )
    .await
    .expect("advance left");

    let out = wait_drain(&rig, job, 1, Duration::from_secs(10)).await;
    let winners: Vec<(i64, i64)> = out
        .iter()
        .filter(|b| b.num_rows() > 0)
        .flat_map(|b| {
            let ks = i64_col(b, "k");
            let ps = i64_col(b, "price");
            ks.into_iter().zip(ps).collect::<Vec<_>>()
        })
        .collect();
    assert!(
        winners.contains(&(1, 900)),
        "auction 1's winning bid (900) must emerge from the distributed pipeline: {winners:?}"
    );
}

/// Deregister must actually STOP the run-loop fragment and free the runner
/// slot it occupies. The rig's runner is a single sequential slot — exactly
/// the production shape scaled down: on the 3-node k3s rig every executor ran
/// `slots` jobs and then never another, because cancel dropped the job's
/// state maps but the loop never exited (forget_job purges the cancel
/// tombstone before the loop can observe it). Job B here only ever runs if
/// job A's loop exits on deregister.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deregister_stops_the_loop_and_frees_the_slot_for_the_next_job() {
    use krishiv_plan::stream_task::{StatelessQuerySpec, StreamingTaskSpec};
    let rig = start_rig("slotfree").await;
    let stateless_task = || {
        StreamingTaskSpec::Stateless(Box::new(StatelessQuerySpec {
            sql: "SELECT v * 2 AS doubled FROM src WHERE v > 10".into(),
            source: "src".into(),
            side_tables: vec![],
        }))
    };
    let mut options = ContinuousRegistrationOptions::default();
    options.mode = Some("run-loop".into());
    options.parallelism = Some(1);

    // Job A occupies the rig's only runner slot and demonstrably runs.
    krishiv_scheduler::register_continuous_task_with_options(
        &rig.coordinator,
        "slot-a",
        &stateless_task(),
        &options,
    )
    .await
    .expect("job A registers");
    tokio::time::sleep(Duration::from_millis(400)).await;
    krishiv_scheduler::push_continuous_input_coordinated(
        &rig.coordinator,
        "slot-a",
        ipc_bytes(&two_col("v", "unused", vec![20, 30], vec![0, 0])),
    )
    .await
    .expect("push A");
    let out_a = wait_drain(&rig, "slot-a", 2, Duration::from_secs(10)).await;
    assert_eq!(
        out_a.iter().map(|b| b.num_rows()).sum::<usize>(),
        2,
        "job A's loop must be running before the deregister is meaningful"
    );

    // Deregister job A — the teardown the HTTP DELETE performs.
    rig.coordinator
        .write()
        .await
        .push_cancel_job(&krishiv_proto::JobId::try_new("slot-a").unwrap())
        .await
        .expect("cancel job A");

    // Cancel retires the classed state, not just windows.
    let leftover: Vec<String> = rig
        .class_executors
        .stateless
        .iter()
        .map(|e| e.key().clone())
        .filter(|k| k.starts_with("slot-a#"))
        .collect();
    assert!(
        leftover.is_empty(),
        "cancel must retire job A's stateless executors, found {leftover:?}"
    );

    // Job B can only run if job A's loop exited and freed the slot.
    krishiv_scheduler::register_continuous_task_with_options(
        &rig.coordinator,
        "slot-b",
        &stateless_task(),
        &options,
    )
    .await
    .expect("job B registers");
    tokio::time::sleep(Duration::from_millis(400)).await;
    krishiv_scheduler::push_continuous_input_coordinated(
        &rig.coordinator,
        "slot-b",
        ipc_bytes(&two_col("v", "unused", vec![50], vec![0])),
    )
    .await
    .expect("push B");
    let out_b = wait_drain(&rig, "slot-b", 1, Duration::from_secs(10)).await;
    let doubled: Vec<i64> = out_b.iter().flat_map(|b| i64_col(b, "doubled")).collect();
    assert_eq!(
        doubled,
        vec![100],
        "job B never ran: job A's cancelled loop is still holding the runner slot"
    );
}
