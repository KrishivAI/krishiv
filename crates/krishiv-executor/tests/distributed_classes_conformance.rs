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
    // One sequential runner slot — the shape the slot-freeing test relies on.
    start_rig_with_loops(name, 1).await
}

/// Rig with `loops` concurrent runner slots (task #149 fix 10: parallel
/// pipeline subtasks need one slot each to run simultaneously).
async fn start_rig_with_loops(name: &str, loops: usize) -> Rig {
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
    let egress_notify: krishiv_executor::runner::SharedContinuousNotify = Arc::new(DashMap::new());
    let continuous_busy: krishiv_executor::grpc::SharedContinuousBusy = Arc::new(DashMap::new());

    {
        let inbox = inbox.clone();
        let loop_executors = Arc::clone(&loop_executors);
        let continuous_inputs = Arc::clone(&continuous_inputs);
        let continuous_outputs = Arc::clone(&continuous_outputs);
        let input_notify = Arc::clone(&input_notify);
        let connector_sources = Arc::clone(&connector_sources);
        let class_executors = class_executors.clone();
        let egress_notify = Arc::clone(&egress_notify);
        let continuous_busy = Arc::clone(&continuous_busy);
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
                egress_notify,
                continuous_busy,
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
        .with_shared_class_executors(class_executors.clone())
        .with_shared_egress_notify(Arc::clone(&egress_notify))
        .with_shared_continuous_busy(Arc::clone(&continuous_busy));
    let coord_endpoint = format!("http://{coord_addr}");
    let runner = Arc::new(runner);
    for _ in 0..loops.max(1) {
        let runner = Arc::clone(&runner);
        let coord_endpoint = coord_endpoint.clone();
        tokio::spawn(async move {
            let service = GrpcCoordinatorService::new(coord_endpoint, LeaseGeneration::initial());
            loop {
                match runner.run_next_with(&service).await {
                    Ok(Some(_)) => {}
                    Ok(None) => tokio::time::sleep(Duration::from_millis(5)).await,
                    // Surfaced rather than swallowed: a runner that cannot
                    // report looks exactly like one that never ran, and
                    // chasing that distinction is what this loop's silence
                    // used to cost.
                    Err(error) => {
                        eprintln!("[rig] run_next_with failed: {error}");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        });
    }

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

/// stream:rloop must coerce PUSHED input exactly as it coerces owned-split
/// reads. NEXMark's `price` is u64; the aggregate pre-downcast refuses
/// unsigned columns, and the pushed path used to skip the coercion the
/// owned-split path applies — every window fragment aggregating a pushed
/// unsigned column died with "unsupported column type for pre-downcast:
/// UInt64" on its first batch (q7/q16 on the live k3s run).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rloop_window_aggregates_a_pushed_unsigned_column() {
    use krishiv_plan::stream_task::StreamingTaskSpec;
    use krishiv_plan::window::{WindowAgg, WindowAggKind, WindowExecutionSpec};
    let rig = start_rig("upush").await;
    let job = "upush-w";
    let spec = WindowExecutionSpec {
        agg_exprs: vec![WindowAgg {
            kind: WindowAggKind::Max,
            input_column: "price".into(),
            output_column: "max_price".into(),
            filter: None,
        }],
        watermark_lag_ms: 0,
        window_size_ms: 10_000,
        ..WindowExecutionSpec::tumbling("k", "ts", 10_000)
    };
    let task = StreamingTaskSpec::Window(Box::new(spec));
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
    .expect("window job registers");
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Two windows of data; the second window's rows advance the watermark past
    // the first window's end so it closes and emits.
    let batch = {
        use arrow::array::{Int64Array, StringArray, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::UInt64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "a"])),
                Arc::new(Int64Array::from(vec![1_000, 2_000, 15_000])),
                Arc::new(UInt64Array::from(vec![7u64, 900u64, 3u64])),
            ],
        )
        .unwrap()
    };
    krishiv_scheduler::push_continuous_input_coordinated(&rig.coordinator, job, ipc_bytes(&batch))
        .await
        .expect("push u64 rows");

    let out = wait_drain(&rig, job, 1, Duration::from_secs(10)).await;
    let max: Vec<i64> = out.iter().flat_map(|b| i64_col(b, "max_price")).collect();
    assert_eq!(
        max,
        vec![900],
        "the first window must close and report MAX over the coerced u64 column"
    );
}

/// A bounded stream whose whole event-time span fits inside ONE window can
/// never close it from data alone — the final window's own events are what
/// set the watermark. The end-of-stream directive (continuous-flush) is how
/// a bounded producer gets those windows out: every subtask flushes its open
/// state into egress, and the next drain collects it. Without the directive
/// the drain below returns nothing forever (observed live: NEXMark q9/q4
/// emitted zero rows across the whole benchmark).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eos_flush_emits_windows_the_data_cannot_close() {
    use krishiv_plan::stream_task::StreamingTaskSpec;
    use krishiv_plan::window::{WindowAgg, WindowAggKind, WindowExecutionSpec};
    let rig = start_rig("eosflush").await;
    let job = "eos-w";
    let mut spec = WindowExecutionSpec::tumbling("k", "ts", 60_000);
    spec.agg_exprs = vec![WindowAgg {
        kind: WindowAggKind::Max,
        input_column: "v".into(),
        output_column: "max_v".into(),
        filter: None,
    }];
    spec.watermark_lag_ms = 0;
    let task = StreamingTaskSpec::Window(Box::new(spec));
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
    .expect("window job registers");
    tokio::time::sleep(Duration::from_millis(400)).await;

    // All rows inside one 60s window: no later event can ever close it.
    krishiv_scheduler::push_continuous_input_coordinated(
        &rig.coordinator,
        job,
        ipc_bytes(&{
            use arrow::array::{Int64Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ]));
            arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["a", "a", "a"])),
                    Arc::new(Int64Array::from(vec![1_000, 2_000, 3_000])),
                    Arc::new(Int64Array::from(vec![7, 900, 3])),
                ],
            )
            .unwrap()
        }),
    )
    .await
    .expect("push");
    // Give the loop a moment to consume, then prove the window stays open.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let before = wait_drain(&rig, job, 1, Duration::from_secs(2)).await;
    assert_eq!(
        before.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "the single open window must NOT emit before end-of-stream is declared"
    );

    krishiv_scheduler::flush_continuous_stream_coordinated(&rig.coordinator, job)
        .await
        .expect("run-loop EOS flush");
    let after = wait_drain(&rig, job, 1, Duration::from_secs(10)).await;
    let max_v: Vec<i64> = after.iter().flat_map(|b| i64_col(b, "max_v")).collect();
    assert_eq!(
        max_v,
        vec![900],
        "the flush must emit the open window's aggregate"
    );
}

/// A join-keyed pipeline runs at parallelism 2 and loses nothing (task #149
/// fix 10): the coordinator round-robins side pushes across subtasks, so
/// most rows land on a subtask that does NOT own their key — the new rpipe
/// keyed exchange must re-route them, or those keys never join and their
/// winners vanish. (This is exactly the wrong-answer mode the old blanket
/// parallelism-1 refusal guarded against.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_pipeline_exchanges_keys_between_subtasks() {
    use krishiv_plan::stream_task::StreamingTaskSpec;
    let rig = start_rig_with_loops("ppipe", 3).await;
    let job = "ppipe-j";
    let mut stage =
        krishiv_plan::window::WindowExecutionSpec::tumbling("left_k", "left_ts", 10_000);
    stage.watermark_lag_ms = 20_000;
    stage.agg_exprs = vec![krishiv_plan::window::WindowAgg {
        kind: krishiv_plan::window::WindowAggKind::Count,
        input_column: String::new(),
        output_column: "n".into(),
        filter: None,
    }];
    let spec = krishiv_plan::stream_join::StreamingPipelineSpec {
        join: krishiv_plan::stream_join::StreamingJoinSpec {
            left_source: "l".into(),
            right_source: "r".into(),
            time_column: "ts".into(),
            left_key_column: "k".into(),
            right_key_column: "k".into(),
            window_ms: 10_000,
        },
        stages: vec![stage],
    };
    assert!(
        spec.parallel_unsafe_reason().is_none(),
        "this pipeline groups by the join key and must be parallel-safe"
    );
    let task = StreamingTaskSpec::Pipeline(Box::new(spec));
    let mut options = ContinuousRegistrationOptions::default();
    options.mode = Some("run-loop".into());
    options.parallelism = Some(2);
    krishiv_scheduler::register_continuous_task_with_options(
        &rig.coordinator,
        job,
        &task,
        &options,
    )
    .await
    .expect("parallel join-keyed pipeline registers");
    tokio::time::sleep(Duration::from_millis(500)).await;

    fn keyed(keys: &[&str], ts: &[i64]) -> arrow::record_batch::RecordBatch {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(ts.to_vec())),
            ],
        )
        .unwrap()
    }
    let keys: Vec<&str> = vec!["a", "b", "c", "d", "e", "f"];
    // One chunk per key per side, so round-robin spreads keys across BOTH
    // subtasks on both sides — without the exchange, misrouted keys never
    // meet their partner.
    for key in &keys {
        krishiv_scheduler::push_continuous_input_side_coordinated(
            &rig.coordinator,
            job,
            "L",
            ipc_bytes(&keyed(&[key], &[1_000])),
        )
        .await
        .expect("push L");
        krishiv_scheduler::push_continuous_input_side_coordinated(
            &rig.coordinator,
            job,
            "R",
            ipc_bytes(&keyed(&[key], &[2_000])),
        )
        .await
        .expect("push R");
    }
    // Let the loops route + join, then declare EOS and collect.
    tokio::time::sleep(Duration::from_millis(800)).await;
    krishiv_scheduler::flush_continuous_stream_coordinated(&rig.coordinator, job)
        .await
        .expect("EOS flush");
    let out = wait_drain(&rig, job, keys.len(), Duration::from_secs(10)).await;
    let rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows,
        keys.len(),
        "every key must produce its window row; missing keys mean the \
         exchange failed to co-locate them"
    );
}

/// The q4 shape at parallelism > 1 (task #149 fix 10, extended): stage 0
/// groups by (join key, cat) — co-located — and stage 1 groups by `cat`
/// alone, which requires the re-key exchange at the split point plus the
/// pre-stage EOS flush leg. Ground truth is the SAME job at parallelism 1;
/// the parallel run must produce the identical per-cat aggregate rows.
/// Without the `#S` exchange (or without the prestage flush round), cat
/// groups fragment across subtasks and the row sets diverge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_pipeline_rekeys_stage_input_and_matches_parallelism_one() {
    let rig = start_rig_with_loops("spipe", 4).await;

    fn q4_spec() -> krishiv_plan::stream_join::StreamingPipelineSpec {
        // Mirror the SQL compiler's multi-column GROUP BY encoding: ONE
        // synthetic "__krishiv_key" column carrying the length-prefixed
        // composite, with the parts listed for output expansion.
        let mut stage0 =
            krishiv_plan::window::WindowExecutionSpec::tumbling("__krishiv_key", "left_ts", 10_000);
        stage0.watermark_lag_ms = 20_000;
        stage0.key_parts = vec![
            krishiv_plan::window::KeyPart {
                name: "left_k".into(),
                type_tag: "auto".into(),
            },
            krishiv_plan::window::KeyPart {
                name: "cat".into(),
                type_tag: "auto".into(),
            },
        ];
        stage0.derived_columns = vec![krishiv_plan::window::DerivedColumn {
            name: "__krishiv_key".into(),
            expr: krishiv_plan::window::WindowScalarExpr::CompositeKey(vec![
                "left_k".into(),
                "cat".into(),
            ]),
        }];
        stage0.agg_exprs = vec![krishiv_plan::window::WindowAgg {
            kind: krishiv_plan::window::WindowAggKind::Count,
            input_column: String::new(),
            output_column: "n".into(),
            filter: None,
        }];
        let mut stage1 =
            krishiv_plan::window::WindowExecutionSpec::tumbling("cat", "window_start_ms", 10_000);
        stage1.watermark_lag_ms = 20_000;
        stage1.agg_exprs = vec![krishiv_plan::window::WindowAgg {
            kind: krishiv_plan::window::WindowAggKind::Count,
            input_column: String::new(),
            output_column: "groups".into(),
            filter: None,
        }];
        krishiv_plan::stream_join::StreamingPipelineSpec {
            join: krishiv_plan::stream_join::StreamingJoinSpec {
                left_source: "l".into(),
                right_source: "r".into(),
                time_column: "ts".into(),
                left_key_column: "k".into(),
                right_key_column: "k".into(),
                window_ms: 10_000,
            },
            stages: vec![stage0, stage1],
        }
    }
    assert_eq!(
        q4_spec().parallel_plan(),
        Ok(Some((1, "cat".into()))),
        "the fixture must be the split shape this test exists for"
    );

    fn left_batch(k: &str, cat: &str, ts: i64) -> arrow::record_batch::RecordBatch {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("cat", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![k])),
                Arc::new(StringArray::from(vec![cat])),
                Arc::new(Int64Array::from(vec![ts])),
            ],
        )
        .unwrap()
    }
    fn right_batch(k: &str, ts: i64) -> arrow::record_batch::RecordBatch {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![k])),
                Arc::new(Int64Array::from(vec![ts])),
            ],
        )
        .unwrap()
    }

    // 6 join keys spread over 3 cats: key-group hashing scatters both the
    // join keys AND the cats across subtasks, so cat groups only reunite if
    // the `#S` exchange runs.
    let data: Vec<(&str, &str)> = vec![
        ("a", "c1"),
        ("b", "c2"),
        ("c", "c3"),
        ("d", "c1"),
        ("e", "c2"),
        ("f", "c3"),
    ];

    async fn run_job(
        rig: &Rig,
        job: &str,
        parallelism: u32,
        data: &[(&str, &str)],
    ) -> Vec<(String, i64)> {
        use krishiv_plan::stream_task::StreamingTaskSpec;
        let task = StreamingTaskSpec::Pipeline(Box::new(q4_spec()));
        let mut options = ContinuousRegistrationOptions::default();
        options.mode = Some("run-loop".into());
        options.parallelism = Some(parallelism);
        krishiv_scheduler::register_continuous_task_with_options(
            &rig.coordinator,
            job,
            &task,
            &options,
        )
        .await
        .expect("split pipeline registers");
        tokio::time::sleep(Duration::from_millis(500)).await;
        for (k, cat) in data {
            krishiv_scheduler::push_continuous_input_side_coordinated(
                &rig.coordinator,
                job,
                "L",
                ipc_bytes(&left_batch(k, cat, 1_000)),
            )
            .await
            .expect("push L");
            krishiv_scheduler::push_continuous_input_side_coordinated(
                &rig.coordinator,
                job,
                "R",
                ipc_bytes(&right_batch(k, 2_000)),
            )
            .await
            .expect("push R");
        }
        tokio::time::sleep(Duration::from_millis(800)).await;
        krishiv_scheduler::flush_continuous_stream_coordinated(&rig.coordinator, job)
            .await
            .expect("EOS flush incl. prestage round");
        let out = wait_drain(rig, job, 3, Duration::from_secs(10)).await;
        // The RAW row multiset, not a per-cat sum: COUNT is decomposable, so
        // summing fragments would equal the truth even when the exchange
        // never ran and each cat's group split across subtasks. Fragmented
        // output shows up here as MORE rows with SMALLER counts.
        let mut rows: Vec<(String, i64)> = Vec::new();
        for batch in &out {
            let cats = batch
                .column_by_name("cat")
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>())
                .expect("cat column");
            let counts = batch
                .column_by_name("groups")
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>())
                .expect("groups column");
            for i in 0..batch.num_rows() {
                rows.push((cats.value(i).to_owned(), counts.value(i)));
            }
        }
        rows.sort();
        rig.coordinator
            .write()
            .await
            .push_cancel_job(&krishiv_proto::JobId::try_new(job).unwrap())
            .await
            .expect("deregister");
        rows
    }

    let truth = run_job(&rig, "spipe-truth", 1, &data).await;
    assert_eq!(
        truth.len(),
        3,
        "exactly one row per cat at parallelism 1: {truth:?}"
    );
    let parallel = run_job(&rig, "spipe-par", 3, &data).await;
    assert_eq!(
        parallel, truth,
        "parallel split-pipeline output must equal the parallelism-1 ground truth"
    );
}
