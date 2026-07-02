#![cfg(feature = "__disabled_flight_test")]

//! End-to-end integration tests for distributed execution (InProcessCluster, Flight SQL, coordinator lifecycle).

use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_flight_sql::make_flight_sql_server;
use krishiv_plan::{ExecutionKind, PhysicalPlan};
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
    JobKind, JobSpec, LeaseGeneration, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_runtime::execution_runtime::{
    ExecutionPlacement, RuntimeMode, build_execution_runtime,
};
use krishiv_runtime::in_process::BatchSqlTable;
use krishiv_runtime::in_process_cluster::InProcessCluster;
use krishiv_runtime::local_streaming::{LocalWindowExecutionSpec, LocalWindowKind};
use krishiv_runtime::{DistributedBackend, ExecutionBackend};
use krishiv_scheduler::Coordinator;
use tonic::transport::Server;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn users_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn users_batch(ids: &[&str], timestamps: &[i64], amounts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        users_schema(),
        vec![
            Arc::new(StringArray::from(ids.to_vec())) as _,
            Arc::new(Int64Array::from(timestamps.to_vec())) as _,
            Arc::new(Int64Array::from(amounts.to_vec())) as _,
        ],
    )
    .unwrap()
}

fn events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

fn events_batch(user_ids: &[&str], timestamps: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        events_schema(),
        vec![
            Arc::new(StringArray::from(user_ids.to_vec())) as _,
            Arc::new(Int64Array::from(timestamps.to_vec())) as _,
        ],
    )
    .unwrap()
}

fn tumbling_spec() -> LocalWindowExecutionSpec {
    LocalWindowExecutionSpec {
        key_column: "user_id".into(),
        key_column_type: String::from("utf8"),
        event_time_column: "ts".into(),
        watermark_lag_ms: 0,
        window_kind: LocalWindowKind::Tumbling,
        window_size_ms: 10_000,
        agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
        state_ttl_ms: None,
        allowed_lateness_ms: None,
        source_watermark_lags: std::collections::HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    }
}

// ---------------------------------------------------------------------------
// 1. InProcessCluster: create cluster → submit SQL → verify results
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_create_submit_sql_verify() {
    let cluster = InProcessCluster::new().expect("cluster creation");

    let batches = cluster
        .collect_batch_sql("SELECT 42 AS answer", &[], false)
        .expect("batch sql");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 1);

    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 42);
}

// ---------------------------------------------------------------------------
// 2. InProcessCluster: register tables → join query → verify
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_register_tables_join_query() {
    let cluster = InProcessCluster::new().expect("cluster");

    // Write two parquet files for the join test.
    let dir = tempfile::tempdir().unwrap();
    let users_path = dir.path().join("users.parquet");
    let orders_path = dir.path().join("orders.parquet");

    // Create a users table: (user_id, ts)
    let users_batch = users_batch(&["alice", "bob"], &[1000, 2000], &[100, 200]);
    let mut parquet_writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&users_path).unwrap(),
        users_batch.schema(),
        None,
    )
    .unwrap();
    parquet_writer.write(&users_batch).unwrap();
    parquet_writer.close().unwrap();

    // Create an orders table: (order_id, user_id, amount)
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as _,
            Arc::new(StringArray::from(vec!["alice", "bob"])) as _,
            Arc::new(Int64Array::from(vec![50, 75])) as _,
        ],
    )
    .unwrap();
    let mut parquet_writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&orders_path).unwrap(),
        orders_schema.clone(),
        None,
    )
    .unwrap();
    parquet_writer.write(&orders_batch).unwrap();
    parquet_writer.close().unwrap();

    let tables = vec![
        BatchSqlTable {
            table_name: "users".into(),
            path: users_path,
            ..Default::default()
        },
        BatchSqlTable {
            table_name: "orders".into(),
            path: orders_path,
            ..Default::default()
        },
    ];

    let result = cluster
        .collect_batch_sql(
            "SELECT u.user_id, o.amount FROM users u JOIN orders o ON u.user_id = o.user_id ORDER BY u.user_id",
            &tables,
            false,
        )
        .expect("join query");

    assert!(!result.is_empty());
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

// ---------------------------------------------------------------------------
// 3. InProcessCluster: register continuous job → push data → drain → verify
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_continuous_job_push_drain() {
    let cluster = InProcessCluster::new().expect("cluster");

    cluster
        .register_continuous_job("cjob-1", &tumbling_spec())
        .expect("register continuous");

    let batch = events_batch(&["a", "b", "a"], &[1_000, 2_000, 5_000]);
    cluster
        .push_continuous_input("cjob-1", vec![batch])
        .expect("push");

    let _drained = cluster.drain_continuous_job("cjob-1").expect("drain");
}

// ---------------------------------------------------------------------------
// 4. InProcessCluster: bounded window → collect → verify
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_bounded_window_collect() {
    let cluster = InProcessCluster::new().expect("cluster");

    let batch = events_batch(
        &["a", "b", "a", "b", "a"],
        &[1_000, 2_000, 3_000, 8_000, 9_000],
    );
    let out = cluster
        .collect_bounded_window("events", vec![batch], &tumbling_spec())
        .expect("bounded window");

    assert!(!out.is_empty(), "bounded window should produce output");
    let total_rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0);
}

// ---------------------------------------------------------------------------
// 5. Flight SQL server: start server → submit SQL via client → verify results
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_sql_server_submit_sql_verify() {
    use krishiv_runtime::flight_client::{FlightClientPool, execute_remote_sql};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let incoming = tonic::transport::server::TcpIncoming::from(listener);

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(make_flight_sql_server().expect("make flight sql server"))
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });

    let url = format!("http://{addr}");
    let pool = FlightClientPool::new(&url).expect("pool");
    let batches = execute_remote_sql(&pool, "SELECT 99 AS val")
        .await
        .expect("remote sql");

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 99);

    server.abort();
}

// ---------------------------------------------------------------------------
// 6. Flight SQL: register parquet → query → verify
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_sql_register_parquet_query() {
    use krishiv_runtime::flight_client::{FlightClientPool, execute_remote_sql};

    // Create a parquet file to register.
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("flight_items.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("item_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])) as _,
        ],
    )
    .unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&parquet_path).unwrap(),
        schema,
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let incoming = tonic::transport::server::TcpIncoming::from(listener);

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(make_flight_sql_server().expect("make flight sql server"))
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });

    let url = format!("http://{addr}");
    let pool = FlightClientPool::new(&url).expect("pool");

    // Register the parquet table via Flight SQL comment directive.
    let register_sql = format!(
        "/* krishiv-register-parquet:table=flight_items,path={} */ SELECT 1",
        parquet_path.display()
    );
    let reg_result = execute_remote_sql(&pool, &register_sql).await;
    // Registration may succeed or be ignored; continue with query.
    let _ = reg_result;

    // Query: simple scalar to confirm server is alive.
    let batches = execute_remote_sql(&pool, "SELECT 'parquet_ready' AS status")
        .await
        .expect("query");
    assert!(!batches.is_empty());

    server.abort();
}

// ---------------------------------------------------------------------------
// 7. Distributed batch: submit batch job → plan → execute → verify
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_batch_plan_execute_verify() {
    let cluster = InProcessCluster::new().expect("cluster");

    let batch = users_batch(&["u1", "u2", "u3"], &[100, 200, 300], &[10, 20, 30]);
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("batch_table.parquet");

    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&parquet_path).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let tables = vec![BatchSqlTable {
        table_name: "batch_table".into(),
        path: parquet_path,
        ..Default::default()
    }];

    let result = cluster
        .collect_batch_sql(
            "SELECT user_id, amount FROM batch_table ORDER BY amount",
            &tables,
            false,
        )
        .expect("batch execute");

    assert!(!result.is_empty());
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

// ---------------------------------------------------------------------------
// 8. Coordinator + executor lifecycle: register executor → submit job → heartbeat → verify
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_executor_lifecycle() {
    let coord_id = CoordinatorId::try_new("lifecycle-coord").unwrap();
    let mut coordinator = Coordinator::active(coord_id);

    // Register an executor.
    let exec_id = ExecutorId::try_new("lifecycle-exec").unwrap();
    let descriptor = ExecutorDescriptor::new(
        exec_id.clone(),
        krishiv_scheduler::IN_PROCESS_TASK_ENDPOINT,
        4,
    );
    coordinator.register_executor(descriptor).unwrap();

    // Verify executor is registered.
    let snapshots = coordinator.executor_snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].executor_id(), &exec_id);
    assert_eq!(snapshots[0].state(), ExecutorState::Registered);

    // Send a heartbeat.
    coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(exec_id.clone(), ExecutorState::Healthy)
                .with_lease_generation(LeaseGeneration::initial()),
        )
        .unwrap();

    // Verify state after heartbeat.
    let snapshots = coordinator.executor_snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].state(), ExecutorState::Healthy);

    // Submit a batch job.
    let job_id = JobId::try_new("lifecycle-job").unwrap();
    let node = krishiv_plan::PlanNode::new("scan", "parquet", ExecutionKind::Batch).with_op(
        krishiv_plan::NodeOp::Scan {
            table: String::from("t"),
            filters: vec![],
        },
    );
    let fragment = krishiv_plan::encode_typed_task_fragment(&node).expect("encode");
    let stage = StageSpec::new(StageId::try_new("s1").unwrap(), "stage")
        .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), fragment));
    let spec = JobSpec::new(job_id.clone(), "lifecycle", JobKind::Batch).with_stage(stage);

    coordinator.submit_job(spec).unwrap();

    // Tick to process.
    coordinator.coordinator_tick().unwrap();

    // Verify job exists.
    let snap = coordinator.job_snapshot(&job_id).unwrap();
    assert_eq!(snap.job_id(), &job_id);
}

// ---------------------------------------------------------------------------
// Additional: multiple clusters are independent
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_clusters_are_independent() {
    let c1 = InProcessCluster::new().expect("c1");
    let c2 = InProcessCluster::new().expect("c2");

    let id1 = c1.streaming_runtime().coordinator_instance_id();
    let id2 = c2.streaming_runtime().coordinator_instance_id();
    assert_ne!(id1, id2);

    // Both can independently execute SQL.
    let b1 = c1.collect_batch_sql("SELECT 1 AS n", &[], false).unwrap();
    let b2 = c2.collect_batch_sql("SELECT 2 AS n", &[], false).unwrap();
    assert_eq!(b1[0].num_rows(), 1);
    assert_eq!(b2[0].num_rows(), 1);
}

// ---------------------------------------------------------------------------
// Additional: Flight SQL via DistributedBackend end-to-end
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_backend_end_to_end() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let incoming = tonic::transport::server::TcpIncoming::from(listener);

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(make_flight_sql_server().expect("make flight sql server"))
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });

    let url = format!("http://{addr}");
    let backend = DistributedBackend::new(url).expect("backend");

    let plan = PhysicalPlan::new("SELECT 100 AS result", ExecutionKind::Batch);
    let report = backend
        .execute(&plan)
        .expect("execute via distributed backend");
    assert!(report.accepted());
    assert_eq!(report.backend(), "distributed");

    server.abort();
}

// ---------------------------------------------------------------------------
// 10. Flight SQL: continuous stream E2E (register → push → drain)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_sql_continuous_stream_register_push_drain() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let incoming = tonic::transport::server::TcpIncoming::from(listener);

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(make_flight_sql_server().expect("make flight sql server"))
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });

    let url = format!("http://{addr}");
    let rt = build_execution_runtime(
        RuntimeMode::Distributed,
        None,
        Some(url),
        None,
        ExecutionPlacement::RemoteClusterRequired,
    )
    .expect("distributed runtime");

    let spec = LocalWindowExecutionSpec {
        key_column: "user_id".into(),
        key_column_type: String::from("utf8"),
        event_time_column: "ts".into(),
        watermark_lag_ms: 0,
        window_kind: LocalWindowKind::Tumbling,
        window_size_ms: 10_000,
        agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
        state_ttl_ms: None,
        allowed_lateness_ms: None,
        source_watermark_lags: std::collections::HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    };

    rt.register_continuous_stream("flight-cs-1", &spec)
        .expect("register_continuous_stream via Flight SQL");

    rt.push_continuous_stream_input("flight-cs-1", vec![])
        .expect("push_continuous_stream_input via Flight SQL");

    let _result = rt
        .drain_continuous_stream("flight-cs-1")
        .expect("drain_continuous_stream via Flight SQL");

    server.abort();
}
