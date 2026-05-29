use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

use krishiv_governance::{MaskingRule, PolicyHook, Principal, Role, StaticApiKeyAuthProvider};
use krishiv_runtime::LocalWindowKind;

use crate::error::KrishivError;
use crate::session::{Session, SessionBuilder};
use crate::types::{ExecutionMode, StreamBatch};
use crate::window::{
    KeyedStream, MultiSourceWatermarkSpec, SessionWindowedStream, SlidingWindowedStream,
    StateTtlConfig, WatermarkSpec, WindowedStream,
};

// ── P0.3 regression: block_on must reuse the current runtime ────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn block_on_does_not_panic_inside_tokio_runtime() {
    let session = Session::builder()
        .build()
        .expect("SessionBuilder must succeed");
    let result = session.sql("SELECT 1 AS v");
    assert!(
        result.is_ok(),
        "block_on panicked inside Tokio runtime: {result:?}"
    );
}

// ── P0.1: SessionBuilder::build uses a single shared SqlEngine ───────────

#[tokio::test]
async fn session_builder_policy_engine_shares_sql_engine_context() {
    let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
        "key-ptr".into(),
        "alice".into(),
        Role::Reader,
    )]));
    let session = SessionBuilder::new()
        .with_auth(auth)
        .with_policy(Arc::new(AllowAllPolicy))
        .build()
        .unwrap();

    let temp = tempdir().unwrap();
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    session
        .register_parquet_async("people", &parquet_path)
        .await
        .unwrap();

    let df = session
        .sql_as("key-ptr", "SELECT count(*) AS n FROM people")
        .await
        .expect("policy engine should see tables registered on the shared sql_engine");
    let result = df.collect_async().await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn session_builder_defaults_to_embedded() {
    let session = match Session::builder().build() {
        Ok(session) => session,
        Err(error) => panic!("unexpected API error: {error}"),
    };

    assert_eq!(session.mode(), ExecutionMode::Embedded);
}

#[test]
fn session_builder_accepts_single_node() {
    let session = match Session::builder()
        .with_execution_mode(ExecutionMode::SingleNode)
        .build()
    {
        Ok(session) => session,
        Err(error) => panic!("unexpected API error: {error}"),
    };

    assert_eq!(session.mode(), ExecutionMode::SingleNode);
}

#[test]
fn session_builder_preserves_coordinator_grpc_url() {
    let session = Session::builder()
        .with_coordinator_grpc("http://127.0.0.1:9090")
        .build()
        .unwrap();

    assert_eq!(
        session.coordinator_grpc_url(),
        Some("http://127.0.0.1:9090")
    );
}

#[test]
fn sql_collects_literal_query() {
    let session = match Session::builder().build() {
        Ok(session) => session,
        Err(error) => panic!("unexpected API error: {error}"),
    };

    let dataframe = match session.sql("select 1 as value") {
        Ok(dataframe) => dataframe,
        Err(error) => panic!("unexpected API error: {error}"),
    };
    let result = match dataframe.collect() {
        Ok(result) => result,
        Err(error) => panic!("unexpected collect error: {error}"),
    };

    assert_eq!(result.row_count(), 1);
    assert!(result.pretty().unwrap_or_default().contains("value"));
    assert_eq!(session.jobs().len(), 1);
    assert_eq!(
        session.jobs()[0].state(),
        krishiv_runtime::JobState::Succeeded
    );
}

#[test]
fn embedded_and_single_node_sql_over_parquet_match() {
    let temp = match tempdir() {
        Ok(temp) => temp,
        Err(error) => panic!("unexpected tempdir error: {error}"),
    };
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);

    let embedded = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()
        .unwrap_or_else(|error| panic!("unexpected API error: {error}"));
    let single_node = Session::builder()
        .with_execution_mode(ExecutionMode::SingleNode)
        .build()
        .unwrap_or_else(|error| panic!("unexpected API error: {error}"));

    embedded
        .register_parquet("people", &parquet_path)
        .unwrap_or_else(|error| panic!("unexpected register error: {error}"));
    single_node
        .register_parquet("people", &parquet_path)
        .unwrap_or_else(|error| panic!("unexpected register error: {error}"));

    let query = "select city, count(*) as count from people group by city order by city";
    let embedded_pretty = embedded
        .sql(query)
        .and_then(|dataframe| dataframe.collect())
        .and_then(|result| result.pretty())
        .unwrap_or_else(|error| panic!("unexpected embedded query error: {error}"));
    let single_node_pretty = single_node
        .sql(query)
        .and_then(|dataframe| dataframe.collect())
        .and_then(|result| result.pretty())
        .unwrap_or_else(|error| panic!("unexpected single-node query error: {error}"));

    assert_eq!(embedded_pretty, single_node_pretty);
    assert!(embedded_pretty.contains("London"));
    assert!(embedded_pretty.contains("Paris"));
}

#[test]
fn read_parquet_collects_rows() {
    let temp = tempdir().unwrap_or_else(|error| panic!("unexpected tempdir error: {error}"));
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    let session = Session::builder()
        .build()
        .unwrap_or_else(|error| panic!("unexpected API error: {error}"));

    let result = session
        .read_parquet(&parquet_path)
        .and_then(|dataframe| dataframe.collect())
        .unwrap_or_else(|error| panic!("unexpected parquet read error: {error}"));

    assert_eq!(result.row_count(), 3);
}

#[test]
fn memory_stream_supports_bounded_map_filter_collect() {
    let session = match Session::builder().build() {
        Ok(session) => session,
        Err(error) => panic!("unexpected API error: {error}"),
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
        .unwrap_or_else(|error| panic!("unexpected record batch error: {error}"));
    let stream = session
        .memory_stream("numbers", vec![StreamBatch::new(0, batch)])
        .unwrap();
    let mapped = stream
        .map_batches(|batch| batch.clone())
        .unwrap_or_else(|error| panic!("unexpected stream map error: {error}"));
    let filtered = mapped
        .filter_batches(|batch| batch.sequence() == 0)
        .unwrap_or_else(|error| panic!("unexpected stream filter error: {error}"));

    assert_eq!(filtered.name(), "numbers");
    assert_eq!(filtered.collect_bounded().unwrap_or_default().len(), 1);
}

#[test]
fn unbounded_memory_stream_rejects_collect() {
    let session = Session::builder()
        .build()
        .unwrap_or_else(|error| panic!("unexpected API error: {error}"));
    let stream = session.unbounded_memory_stream("events");

    assert!(!stream.is_bounded());
    assert!(stream.collect_bounded().is_err());
}

// ── Streaming API tests ───────────────────────────────────────────────────

#[test]
fn key_by_returns_keyed_stream_with_correct_column() {
    let session = Session::builder().build().unwrap();
    let stream = session.memory_stream("events", vec![]).unwrap();
    let keyed: KeyedStream = stream.key_by("user_id");
    assert_eq!(keyed.key_column(), "user_id");
    assert!(keyed.event_time_column().is_none());
    assert!(keyed.watermark_spec().is_none());
}

#[test]
fn keyed_stream_builder_chain() {
    let session = Session::builder().build().unwrap();
    let stream = session.memory_stream("events", vec![]).unwrap();
    let keyed = stream
        .key_by("user_id")
        .with_event_time("event_ts")
        .watermark(WatermarkSpec::fixed_lag_ms(5000));

    assert_eq!(keyed.key_column(), "user_id");
    assert_eq!(keyed.event_time_column(), Some("event_ts"));
    assert_eq!(keyed.watermark_spec().unwrap().lag_ms(), 5000);
}

#[test]
fn tumbling_window_carries_correct_config() {
    let session = Session::builder().build().unwrap();
    let stream = session.memory_stream("events", vec![]).unwrap();
    let windowed: WindowedStream = stream
        .key_by("user_id")
        .with_event_time("ts")
        .watermark(WatermarkSpec::fixed_lag_ms(1000))
        .tumbling_window(60_000);

    assert_eq!(windowed.key_column(), "user_id");
    assert_eq!(windowed.event_time_column(), Some("ts"));
    assert_eq!(windowed.watermark_lag_ms(), 1000);
    assert_eq!(windowed.window_size_ms(), 60_000);
}

#[test]
fn tumbling_window_collect_executes_in_embedded_mode() {
    let session = Session::builder().build().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "b"])) as _,
            Arc::new(Int64Array::from(vec![1_000, 5_000, 2_000])) as _,
        ],
    )
    .unwrap();
    let stream = session
        .memory_stream("events", vec![StreamBatch::new(0, batch)])
        .unwrap();
    let out = stream
        .key_by("user_id")
        .with_event_time("ts")
        .watermark(WatermarkSpec::fixed_lag_ms(0))
        .tumbling_window(10_000)
        .collect()
        .expect("window collect");
    assert!(!out.is_empty());
}

#[test]
fn sliding_window_collect_via_unified_runtime() {
    let session = Session::builder().build().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "b"])) as _,
            Arc::new(Int64Array::from(vec![1_000, 5_000, 2_000])) as _,
        ],
    )
    .unwrap();
    let stream = session
        .memory_stream("events", vec![StreamBatch::new(0, batch)])
        .unwrap();
    let out = stream
        .key_by("user_id")
        .with_event_time("ts")
        .sliding_window(10_000, 5_000)
        .collect()
        .expect("sliding collect");
    assert!(!out.is_empty());
}

#[test]
fn session_window_collect_via_unified_runtime() {
    let session = Session::builder().build().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])) as _,
            Arc::new(Int64Array::from(vec![1_000, 8_000])) as _,
        ],
    )
    .unwrap();
    let stream = session
        .memory_stream("events", vec![StreamBatch::new(0, batch)])
        .unwrap();
    let out = stream
        .key_by("user_id")
        .with_event_time("ts")
        .session_window(5_000)
        .collect()
        .expect("session collect");
    assert!(!out.is_empty());
}

#[test]
fn session_subsequent_window_collects() {
    let session = Session::builder().build().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])) as _,
            Arc::new(Int64Array::from(vec![1_000])) as _,
        ],
    )
    .unwrap();
    for _ in 0..2 {
        let stream = session
            .memory_stream("events", vec![StreamBatch::new(0, batch.clone())])
            .unwrap();
        let _ = stream
            .key_by("user_id")
            .with_event_time("ts")
            .tumbling_window(10_000)
            .collect()
            .expect("collect");
    }
}

#[test]
fn watermark_spec_lag_ms_roundtrip() {
    let spec = WatermarkSpec::fixed_lag_ms(30_000);
    assert_eq!(spec.lag_ms(), 30_000);
}

#[test]
fn multi_source_watermark_spec_roundtrip() {
    let spec = MultiSourceWatermarkSpec::new()
        .source("src-a", WatermarkSpec::fixed_lag_ms(1000))
        .source("src-b", WatermarkSpec::fixed_lag_ms(2000));
    assert_eq!(spec.source_specs().len(), 2);
    assert_eq!(spec.source_specs()["src-a"].lag_ms(), 1000);
    assert_eq!(spec.source_specs()["src-b"].lag_ms(), 2000);
}

#[test]
fn state_ttl_config_roundtrip() {
    let cfg = StateTtlConfig::new(5_000);
    assert_eq!(cfg.ttl_ms(), 5_000);
}

#[test]
fn sliding_window_api_builder() {
    let session = Session::builder().build().unwrap();
    let stream = session.unbounded_memory_stream("events");
    let sliding: SlidingWindowedStream = stream
        .key_by("user_id")
        .with_event_time("ts")
        .watermark(WatermarkSpec::fixed_lag_ms(500))
        .sliding_window(2_000, 500);
    assert_eq!(sliding.key_column(), "user_id");
    assert_eq!(sliding.event_time_column(), Some("ts"));
    assert_eq!(sliding.watermark_lag_ms(), 500);
    assert_eq!(sliding.window_size_ms(), 2_000);
    assert_eq!(sliding.slide_ms(), 500);
}

#[test]
fn session_window_api_builder() {
    let session = Session::builder().build().unwrap();
    let stream = session.unbounded_memory_stream("events");
    let sess: SessionWindowedStream = stream
        .key_by("device_id")
        .with_event_time("ts")
        .session_window(30_000);
    assert_eq!(sess.key_column(), "device_id");
    assert_eq!(sess.event_time_column(), Some("ts"));
    assert_eq!(sess.session_gap_ms(), 30_000);
}

fn write_people_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["London", "Paris", "London"])),
        ],
    )
    .unwrap_or_else(|error| panic!("unexpected record batch error: {error}"));
    let file =
        File::create(path).unwrap_or_else(|error| panic!("unexpected parquet file error: {error}"));
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .unwrap_or_else(|error| panic!("unexpected parquet writer error: {error}"));
    writer
        .write(&batch)
        .unwrap_or_else(|error| panic!("unexpected parquet write error: {error}"));
    writer
        .close()
        .unwrap_or_else(|error| panic!("unexpected parquet close error: {error}"));
}

// ── sql_as tests ─────────────────────────────────────────────────────────────

struct AllowAllPolicy;
impl PolicyHook for AllowAllPolicy {
    fn check_table_access(&self, _p: &Principal, _table: &str) -> bool {
        true
    }
    fn column_masking_rule(&self, _p: &Principal, _table: &str, _col: &str) -> Option<MaskingRule> {
        None
    }
}

#[tokio::test]
async fn session_sql_as_with_valid_key_executes_query() {
    let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
        "key123".into(),
        "alice".into(),
        Role::Reader,
    )]));
    let session = SessionBuilder::new()
        .with_auth(auth)
        .with_policy(Arc::new(AllowAllPolicy))
        .build()
        .unwrap();
    let df = session.sql_as("key123", "SELECT 42 AS v").await.unwrap();
    let result = df.collect_async().await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn session_sql_as_with_invalid_key_returns_access_denied() {
    let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
        "key123".into(),
        "alice".into(),
        Role::Reader,
    )]));
    let session = SessionBuilder::new()
        .with_auth(auth)
        .with_policy(Arc::new(AllowAllPolicy))
        .build()
        .unwrap();
    let result = session.sql_as("wrong_key", "SELECT 1").await;
    assert!(matches!(result, Err(KrishivError::AccessDenied { .. })));
}

#[tokio::test]
async fn session_without_policy_sql_as_returns_access_denied() {
    let session = SessionBuilder::new().build().unwrap();
    let result = session.sql_as("any_key", "SELECT 1").await;
    assert!(matches!(result, Err(KrishivError::AccessDenied { .. })));
}

#[tokio::test]
async fn session_sql_as_can_read_registered_session_tables() {
    let temp = tempdir().unwrap();
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
        "key123".into(),
        "alice".into(),
        Role::Reader,
    )]));
    let session = SessionBuilder::new()
        .with_auth(auth)
        .with_policy(Arc::new(AllowAllPolicy))
        .build()
        .unwrap();

    session
        .register_parquet_async("people", &parquet_path)
        .await
        .unwrap();
    let df = session
        .sql_as("key123", "SELECT city FROM people ORDER BY city")
        .await
        .unwrap();
    let result = df.collect_async().await.unwrap();

    assert_eq!(result.row_count(), 3);
}

// ── GAP-RT-05: sql() / sql_async() fail-closed when policy engine is set ───

#[tokio::test(flavor = "multi_thread")]
async fn session_sql_async_fails_when_policy_configured() {
    let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
        "key-rt05".into(),
        "alice".into(),
        Role::Reader,
    )]));
    let session = SessionBuilder::new()
        .with_auth(auth)
        .with_policy(Arc::new(AllowAllPolicy))
        .build()
        .unwrap();
    let result = session.sql("SELECT 1");
    assert!(
        matches!(result, Err(KrishivError::AccessDenied { .. })),
        "expected AccessDenied but got: {result:?}"
    );
}

// ── S6.1: SessionBuilder::with_coordinator ────────────────────────────────

#[test]
fn with_coordinator_sets_distributed_mode() {
    let session = Session::builder()
        .with_coordinator("http://coord:50051")
        .build()
        .unwrap();
    assert_eq!(session.mode(), ExecutionMode::Distributed);
}

#[test]
fn session_register_scalar_udf() {
    use krishiv_udf::MultiplyScalarUdf;

    let session = SessionBuilder::new().build().unwrap();
    assert!(session.scalar_udf_names().is_empty());

    let udf = Arc::new(MultiplyScalarUdf::new("double", "x", 2));
    session.register_scalar_udf(udf);
    let names = session.scalar_udf_names();
    assert_eq!(names, vec!["double".to_string()]);

    let registry = session.udf_registry();
    let guard = registry.read().unwrap();
    let loaded = guard
        .get_scalar("double")
        .expect("udf should be registered");
    assert_eq!(loaded.name(), "double");
}

#[test]
fn with_coordinator_stores_url_accessible_via_sql() {
    let session = Session::builder()
        .with_coordinator("http://coord:50051")
        .build()
        .unwrap();
    assert_eq!(
        session.coordinator_url.as_deref(),
        Some("http://coord:50051")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn distributed_session_sql_collects_via_local_coordinator() {
    // B2: Distributed mode now defaults to real remote execution.  This test
    // explicitly opts into the local-fallback path that was the historical
    // default, since no flight server is running on 127.0.0.1:50051.
    let session = Session::builder()
        .with_coordinator("http://127.0.0.1:50051")
        .with_remote_execution(false)
        .build()
        .unwrap();
    let df = session.sql_async("SELECT 3 AS n").await.unwrap();
    let result = df.collect_async().await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running local cluster on 127.0.0.1:50051"]
async fn distributed_window_collect_via_local_cluster() {
    let session = Session::builder()
        .with_local_cluster("http://127.0.0.1:50051")
        .build()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])) as _,
            Arc::new(Int64Array::from(vec![1_000])) as _,
        ],
    )
    .unwrap();
    let stream = session
        .memory_stream("events", vec![StreamBatch::new(0, batch)])
        .unwrap();
    let out = stream
        .key_by("user_id")
        .with_event_time("ts")
        .tumbling_window(10_000)
        .collect()
        .expect("distributed window collect");
    assert!(!out.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn distributed_read_parquet_collects_via_coordinator() {
    let temp = tempdir().unwrap();
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    let session = Session::builder()
        .with_coordinator("http://127.0.0.1:50051")
        .with_remote_execution(false)
        .build()
        .unwrap();
    let df = session.read_parquet_async(&parquet_path).await.unwrap();
    let result = df.collect_async().await.unwrap();
    assert_eq!(result.row_count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_stream_job_poll_drains_via_coordinator() {
    use krishiv_runtime::LocalWindowExecutionSpec;

    let session = Session::builder().build().unwrap();
    let spec = LocalWindowExecutionSpec {
        key_column: "user_id".into(),
        event_time_column: "ts".into(),
        watermark_lag_ms: 0,
        window_kind: LocalWindowKind::Tumbling,
        window_size_ms: 10_000,
        agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
        state_ttl_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
    };
    session.submit_stream_job("events", spec).expect("submit");
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])) as _,
            Arc::new(Int64Array::from(vec![1_000])) as _,
        ],
    )
    .unwrap();
    session
        .push_stream_job_input("events", vec![batch])
        .expect("push");
    let _ = session.poll_stream_job("events").await.expect("poll");
}

#[test]
fn multi_source_watermark_window_collect_with_source_column() {
    let session = Session::builder().build().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("source_id", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])) as _,
            Arc::new(Int64Array::from(vec![1_000])) as _,
            Arc::new(StringArray::from(vec!["src-a"])) as _,
        ],
    )
    .unwrap();
    let stream = session
        .memory_stream("events", vec![StreamBatch::new(0, batch)])
        .unwrap();
    let out = stream
        .key_by("user_id")
        .with_event_time("ts")
        .with_multi_source_watermark(
            MultiSourceWatermarkSpec::new()
                .source("src-a", WatermarkSpec::fixed_lag_ms(0))
                .source("src-b", WatermarkSpec::fixed_lag_ms(0)),
        )
        .tumbling_window(10_000)
        .collect()
        .expect("multi-source collect");
    assert!(!out.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_execution_without_fallback_uses_flight_server() {
    use std::net::SocketAddr;

    use krishiv_flight_sql::make_flight_sql_server;
    use tonic::transport::Server;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(make_flight_sql_server())
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });

    let url = format!("http://{addr}");
    let session = Session::builder()
        .with_coordinator(&url)
        .with_remote_execution(true)
        .build()
        .unwrap();
    assert!(session.execution_runtime().uses_remote_execution());
    let df = session.sql_async("SELECT 99 AS n").await.unwrap();
    let result = df.collect_async().await.unwrap();
    assert_eq!(result.row_count(), 1);
    server.abort();
}
