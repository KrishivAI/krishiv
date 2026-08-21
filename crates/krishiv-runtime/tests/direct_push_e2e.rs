//! Task #149 fix 7 end to end: a producer resolves a run-loop job's executor
//! ingest targets through the coordinator's HTTP discovery endpoint
//! (`GET /api/v1/continuous/{job}/targets`) and pushes Arrow IPC STRAIGHT to
//! the executor's task gRPC — no coordinator push hop, no base64/JSON
//! re-encode. The rig is real: coordinator gRPC + HTTP router and an executor
//! runner behind real ports, so this pins the whole chain the harness's
//! KRISHIV_BENCH_DIRECT_PUSH fast path rides.

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

struct Rig {
    coordinator: SharedCoordinator,
    http_base: String,
}

async fn start_rig(name: &str) -> Rig {
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

    // ── coordinator's REAL HTTP router (the discovery endpoint under test) ─
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    {
        let config =
            krishiv_scheduler::parse_coordinator_daemon_config(Vec::<String>::new()).unwrap();
        let router = krishiv_scheduler::coordinator_http_router(coordinator.clone(), &config);
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, router).await;
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

    // ── the runner that executes what the coordinator dispatches ──────────
    let runner = krishiv_executor::ExecutorTaskRunner::new(inbox)
        .with_shared_loop_executors(Arc::clone(&loop_executors))
        .with_shared_continuous_inputs(Arc::clone(&continuous_inputs))
        .with_shared_continuous_outputs(Arc::clone(&continuous_outputs))
        .with_shared_continuous_notify(Arc::clone(&input_notify))
        .with_shared_continuous_connector_sources(Arc::clone(&connector_sources))
        .with_shared_class_executors(class_executors)
        .with_shared_egress_notify(Arc::clone(&egress_notify))
        .with_shared_continuous_busy(Arc::clone(&continuous_busy));
    let coord_endpoint = format!("http://{coord_addr}");
    tokio::spawn(async move {
        let service = GrpcCoordinatorService::new(coord_endpoint, LeaseGeneration::initial());
        loop {
            match runner.run_next_with(&service).await {
                Ok(Some(_)) => {}
                Ok(None) => tokio::time::sleep(Duration::from_millis(5)).await,
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
            .with_task_endpoint(format!("http://{exec_addr}")),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    Rig {
        coordinator,
        http_base: format!("http://{http_addr}"),
    }
}

/// The producer never touches the coordinator's push endpoint: it discovers
/// the executor over HTTP, pushes gRPC-direct, and the rows come out of the
/// run-loop egress. Removing the targets route (404) or breaking the direct
/// client both fail this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_push_reaches_the_executor_via_http_discovered_targets() {
    use krishiv_plan::stream_task::StreamingTaskSpec;
    let rig = start_rig("direct").await;
    let job = "direct-push-j";

    let mut spec = krishiv_plan::window::WindowExecutionSpec::tumbling("k", "ts", 10_000);
    spec.watermark_lag_ms = 0;
    spec.agg_exprs = vec![krishiv_plan::window::WindowAgg {
        kind: krishiv_plan::window::WindowAggKind::Count,
        input_column: String::from("k"),
        output_column: String::from("events"),
        filter: None,
    }];
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

    // Discovery over the coordinator's REAL HTTP surface.
    let targets = krishiv_runtime::execute_coordinator_continuous_targets(&rig.http_base, job)
        .await
        .expect("targets endpoint resolves");
    assert!(
        !targets.is_empty(),
        "a launched run-loop job must expose at least one ingest target"
    );

    // Two windows of data pushed DIRECTLY to the executor's task gRPC; the
    // second window's rows advance the watermark past the first window's end.
    let batch = {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "a"])),
                Arc::new(Int64Array::from(vec![1_000i64, 2_000, 15_000])),
            ],
        )
        .unwrap()
    };
    let (task_id, endpoint) = &targets[0];
    krishiv_runtime::push_continuous_direct(endpoint, job, task_id, &[batch])
        .await
        .expect("direct push lands on the executor");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut rows = 0usize;
    while rows == 0 && tokio::time::Instant::now() < deadline {
        let drained = krishiv_scheduler::drain_continuous_stream_coordinated(&rig.coordinator, job)
            .await
            .unwrap_or_default();
        if let Ok(batches) = krishiv_scheduler::decode_inline_record_batches(&drained) {
            rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        rows > 0,
        "directly pushed rows must close the first window and drain out"
    );
}

/// Task #149 fix 7 follow-up: ResourceExhausted from the executor is flow
/// control (the run loop's input buffer is full), not failure. The direct
/// client must back off and retry — the coordinator push path already does
/// (429 handling) and the direct path died instead (observed live: q9's two
/// sides filled one subtask's buffer in under a second). The buffer here
/// starts at cap and frees 300ms in; pre-fix the push fails immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_push_backs_off_and_retries_on_a_full_input_buffer() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let inputs: krishiv_executor::grpc::SharedContinuousInputs = Arc::new(DashMap::new());
    let batch = {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap()
    };
    let cap = krishiv_common::streaming_dials::rloop_input_buffer_cap();
    inputs
        .entry("bpjob".to_owned())
        .or_default()
        .extend(std::iter::repeat_with(|| batch.clone()).take(cap));
    {
        let inputs = Arc::clone(&inputs);
        tokio::spawn(async move {
            let server = krishiv_executor::grpc::executor_task_grpc_server_with_continuous(
                ExecutorAssignmentInbox::new_unbounded(),
                Arc::new(DashMap::new()),
                inputs,
                None,
            );
            let _ = tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The "loop" drains the buffer 300ms in — within the retry budget.
    {
        let inputs = Arc::clone(&inputs);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            inputs.remove("bpjob");
        });
    }

    krishiv_runtime::push_continuous_direct(
        &format!("http://{addr}"),
        "bpjob",
        "task-streaming-0",
        &[batch],
    )
    .await
    .expect("the push must survive transient backpressure by retrying");
}
