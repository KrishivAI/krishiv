use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

use krishiv_runtime::LocalWindowKind;

use crate::error::KrishivError;
use crate::session::{Session, SessionBuilder};
use crate::types::{ExecutionMode, StreamBatch, StreamMode};
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

#[test]
fn session_builder_defaults_to_embedded() {
    let session = match Session::builder().build() {
        Ok(session) => session,
        Err(error) => panic!("unexpected API error: {error}"),
    };

    assert_eq!(session.mode(), ExecutionMode::Embedded);
}

#[test]
fn session_builder_single_node_without_coordinator_errors() {
    // SingleNode mode now requires a coordinator Flight URL.
    // Users who want in-process execution should use Embedded mode.
    let error = Session::builder()
        .with_execution_mode(ExecutionMode::SingleNode)
        .build()
        .expect_err("SingleNode without coordinator URL must fail");

    assert!(
        error.to_string().contains("coordinator Flight URL"),
        "unexpected error: {error}"
    );
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
fn two_embedded_sessions_sql_over_parquet_match() {
    // Previously compared Embedded vs SingleNode(LocalInProcess), which are now identical
    // (SingleNode requires a daemon). Use two Embedded sessions to verify consistent results.
    let temp = match tempdir() {
        Ok(temp) => temp,
        Err(error) => panic!("unexpected tempdir error: {error}"),
    };
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);

    let session_a = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()
        .unwrap_or_else(|error| panic!("unexpected API error: {error}"));
    let session_b = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()
        .unwrap_or_else(|error| panic!("unexpected API error: {error}"));

    session_a
        .register_parquet("people", &parquet_path)
        .unwrap_or_else(|error| panic!("unexpected register error: {error}"));
    session_b
        .register_parquet("people", &parquet_path)
        .unwrap_or_else(|error| panic!("unexpected register error: {error}"));

    let query = "select city, count(*) as count from people group by city order by city";
    let pretty_a = session_a
        .sql(query)
        .and_then(|dataframe| dataframe.collect())
        .and_then(|result| result.pretty())
        .unwrap_or_else(|error| panic!("unexpected query error: {error}"));
    let pretty_b = session_b
        .sql(query)
        .and_then(|dataframe| dataframe.collect())
        .and_then(|result| result.pretty())
        .unwrap_or_else(|error| panic!("unexpected query error: {error}"));

    assert_eq!(pretty_a, pretty_b);
    assert!(pretty_a.contains("London"));
    assert!(pretty_a.contains("Paris"));
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
fn direct_stream_constructor_rejects_unbounded_without_schema() {
    let error = crate::Stream::new(
        "events",
        StreamMode::Unbounded,
        Vec::new(),
        ExecutionMode::Embedded,
    )
    .expect_err("unbounded direct construction must fail");

    assert!(matches!(
        error,
        KrishivError::InvalidConfig { message }
            if message.contains("unbounded_memory_stream")
    ));
}

#[test]
fn unbounded_memory_stream_rejects_collect() {
    let session = Session::builder()
        .build()
        .unwrap_or_else(|error| panic!("unexpected API error: {error}"));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let stream = session.unbounded_memory_stream("events", schema).unwrap();

    assert!(!stream.is_bounded());
    assert!(stream.collect_bounded().is_err());
}

#[tokio::test]
async fn unbounded_memory_stream_round_trips_through_streaming_sql() {
    let session = Session::builder().build().expect("session should build");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let stream = session
        .unbounded_memory_stream("live_values", Arc::clone(&schema))
        .expect("unbounded stream should register");
    assert_eq!(stream.input_schema(), Some(&schema));
    assert!(!stream.is_input_closed().expect("input state should load"));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
        .expect("valid input batch");

    stream
        .try_push_batch(batch)
        .expect("input batch should be accepted");
    assert!(stream.close_input().expect("input should close"));
    assert!(!stream.close_input().expect("close should be idempotent"));
    assert!(stream.is_input_closed().expect("input state should load"));
    let closed_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![4]))],
    )
    .expect("valid closed-input batch");
    assert!(
        stream
            .try_push_batch(closed_batch)
            .expect_err("closed input must reject new batches")
            .to_string()
            .contains("input is closed")
    );

    let dataframe = session
        .sql_async("SELECT value FROM live_values")
        .await
        .expect("streaming query should plan");
    let mut output = dataframe
        .execute_stream_async()
        .await
        .expect("streaming query should execute");
    let mut values = Vec::new();
    while let Some(batch) = output.next().await {
        let batch = batch.expect("stream output should succeed");
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value should remain Int64");
        values.extend((0..column.len()).map(|row| column.value(row)));
    }

    assert_eq!(values, vec![1, 2, 3]);

    let mut second_execution = dataframe
        .execute_stream_async()
        .await
        .expect("second execution should return an error stream");
    let second_error = second_execution
        .next()
        .await
        .expect("error stream should emit one item")
        .expect_err("continuous table is single-consumer");
    assert!(second_error.contains("already been consumed"));
}

#[test]
fn unbounded_memory_stream_enforces_schema_and_backpressure() {
    let session = Session::builder().build().expect("session should build");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let stream = session
        .unbounded_memory_stream_with_capacity("bounded_input", Arc::clone(&schema), 1)
        .expect("unbounded stream should register");
    let first = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("valid input batch");
    stream
        .try_push_batch(first)
        .expect("first batch should fill the queue");

    let second = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2]))])
        .expect("valid input batch");
    let full_error = stream
        .try_push_batch(second)
        .expect_err("full queue must backpressure");
    assert!(full_error.to_string().contains("queue is full"));

    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let wrong_batch = RecordBatch::try_new(
        wrong_schema,
        vec![Arc::new(StringArray::from(vec!["wrong"]))],
    )
    .expect("valid wrong-schema batch");
    let schema_error = stream
        .try_push_batch(wrong_batch)
        .expect_err("schema mismatch must fail");
    assert!(schema_error.to_string().contains("schema mismatch"));
}

#[test]
fn unbounded_memory_stream_rejects_duplicate_name() {
    let session = Session::builder().build().expect("session should build");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    session
        .unbounded_memory_stream("duplicate_input", Arc::clone(&schema))
        .expect("first registration should succeed");

    let error = session
        .unbounded_memory_stream("duplicate_input", schema)
        .expect_err("duplicate table registration must fail");

    assert!(error.to_string().contains("already registered"));
}

#[test]
fn unbounded_memory_stream_rejects_empty_schema() {
    let session = Session::builder().build().expect("session should build");

    let error = session
        .unbounded_memory_stream("empty_input", Arc::new(Schema::empty()))
        .expect_err("empty stream schema must fail");

    assert!(matches!(
        error,
        KrishivError::InvalidConfig { message }
            if message.contains("at least one field")
    ));
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
    let batch = krishiv_common::test_fixtures::make_test_user_ts_batch(
        vec!["a", "a", "b"],
        vec![1_000, 5_000, 2_000],
    );
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
    let batch = krishiv_common::test_fixtures::make_test_user_ts_batch(
        vec!["a", "a", "b"],
        vec![1_000, 5_000, 2_000],
    );
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
    let batch =
        krishiv_common::test_fixtures::make_test_user_ts_batch(vec!["a", "b"], vec![1_000, 8_000]);
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
    let batch = krishiv_common::test_fixtures::make_test_user_ts_batch(vec!["a"], vec![1_000]);
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
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let stream = session.unbounded_memory_stream("events", schema).unwrap();
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
    let schema = Arc::new(Schema::new(vec![
        Field::new("device_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let stream = session.unbounded_memory_stream("events", schema).unwrap();
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
    use krishiv_plan::udf::MultiplyScalarUdf;

    let session = SessionBuilder::new().build().unwrap();
    assert!(session.scalar_udf_names().is_empty());

    let udf = Arc::new(MultiplyScalarUdf::new("double", "x", 2));
    session
        .register_scalar_udf(udf)
        .expect("scalar UDF registration should succeed");
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
async fn distributed_session_rejects_disabled_remote_execution() {
    let err = Session::builder()
        .with_coordinator("http://127.0.0.1:50051")
        .with_remote_execution(false)
        .build()
        .expect_err("distributed sessions should not silently run in-process");
    assert!(
        err.to_string()
            .contains("Distributed mode requires remote execution"),
        "unexpected error: {err}"
    );
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
async fn embedded_read_parquet_collects_locally() {
    let temp = tempdir().unwrap();
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    let session = Session::builder().build().unwrap();
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
        key_column_type: String::from("utf8"),
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

#[tokio::test(flavor = "multi_thread")]
async fn continuous_job_id_takes_precedence_over_unbounded_table_name() {
    use krishiv_runtime::LocalWindowExecutionSpec;

    let session = Session::builder().build().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    session
        .register_unbounded("shared-name", schema.clone())
        .expect("register SQL stream");
    let spec = LocalWindowExecutionSpec {
        key_column: "user_id".into(),
        key_column_type: String::from("utf8"),
        event_time_column: "ts".into(),
        watermark_lag_ms: 0,
        window_kind: LocalWindowKind::Tumbling,
        window_size_ms: 10_000,
        agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
        state_ttl_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
    };
    session
        .submit_stream_job("shared-name", spec)
        .expect("submit continuous job");
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "a"])) as _,
            Arc::new(Int64Array::from(vec![1_000, 12_000])) as _,
        ],
    )
    .unwrap();

    session
        .push_stream_job_input("shared-name", vec![batch])
        .expect("push must target the registered job");
    let output = session
        .poll_stream_job("shared-name")
        .await
        .expect("poll continuous job");
    assert!(
        !output.is_empty(),
        "job input must not be diverted to the same-name SQL table"
    );
}

#[test]
fn duplicate_continuous_job_registration_is_rejected() {
    use krishiv_runtime::LocalWindowExecutionSpec;

    let session = Session::builder().build().unwrap();
    let spec = LocalWindowExecutionSpec {
        key_column: "user_id".into(),
        key_column_type: String::from("utf8"),
        event_time_column: "ts".into(),
        watermark_lag_ms: 0,
        window_kind: LocalWindowKind::Tumbling,
        window_size_ms: 10_000,
        agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
        state_ttl_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
    };
    session
        .submit_stream_job("duplicate-job", spec.clone())
        .expect("first registration");
    let error = session
        .submit_stream_job("duplicate-job", spec)
        .expect_err("duplicate registration must not reset live state");
    assert!(error.to_string().contains("already registered"));
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
                .source("src-b", WatermarkSpec::fixed_lag_ms(0))
                .with_source_id_column("source_id"),
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
            .add_service(make_flight_sql_server().expect("make flight sql server"))
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

// ── Item 2: collect() guard for streaming sources ─────────────────────────────

#[test]
fn collect_on_batch_source_succeeds() {
    let session = crate::SessionBuilder::new().build().unwrap();
    let df = session.sql("SELECT 1 AS n").unwrap();
    let result = df.collect().unwrap();
    assert_eq!(result.row_count(), 1);
}

// ── Item 7: unified execute() interface ──────────────────────────────────────

#[tokio::test]
async fn execute_batch_query_returns_batch_result() {
    let session = crate::SessionBuilder::new().build().unwrap();
    // Use sql_async() to avoid calling block_on inside a tokio test.
    let df = session.sql_async("SELECT 42 AS answer").await.unwrap();
    let result = df.execute().await.unwrap();
    assert!(result.is_batch(), "simple SELECT must return Batch result");
    let batches = result.into_batches().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn execution_result_into_batches_works_for_batch() {
    let session = crate::SessionBuilder::new().build().unwrap();
    let df = session.sql_async("SELECT 1 AS a, 2 AS b").await.unwrap();
    let result = df.execute().await.unwrap();
    let batches = result.into_batches().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_columns(), 2);
}
