use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

use krishiv_connectors::ConnectorConfig;
use krishiv_runtime::LocalWindowKind;

use crate::error::KrishivError;
use crate::session::{Session, SessionBuilder, SubmittedSqlJobState};
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

// Regression test for a bug found auditing this exact call path: the old
// current-thread fallback in `krishiv_common::async_util::block_on` called
// `.block_on` on a *separate* fallback runtime while this thread already had
// one entered — Tokio's nesting guard is per-OS-thread, not per-runtime-
// instance, so it still panicked with "Cannot start a runtime from within a
// runtime". `#[tokio::test]` without an explicit flavor uses a current-thread
// runtime, exercising exactly the branch the multi-thread test above does not.
#[tokio::test]
async fn block_on_does_not_panic_inside_current_thread_tokio_runtime() {
    let session = Session::builder()
        .build()
        .expect("SessionBuilder must succeed");
    let result = session.sql("SELECT 1 AS v");
    assert!(
        result.is_ok(),
        "block_on panicked inside a current-thread Tokio runtime: {result:?}"
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
fn session_registers_validated_sink_configs() {
    let session = Session::builder().build().unwrap();
    let config = ConnectorConfig::new("out", "parquet").with_property("path", "/tmp/out.parquet");

    session.register_sink_config(config).unwrap();

    let sinks = session.registered_sink_configs();
    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].name, "out");
    assert_eq!(sinks[0].kind, "parquet");
    assert!(session.sink_config("out").is_some());
}

#[test]
fn session_rejects_invalid_sink_configs() {
    let session = Session::builder().build().unwrap();
    let error = session
        .register_sink_config(ConnectorConfig::new("out", "parquet"))
        .unwrap_err();

    assert!(
        matches!(error, KrishivError::InvalidConfig { .. }),
        "expected invalid config, got {error:?}"
    );
    assert!(session.registered_sink_configs().is_empty());
}

#[test]
fn session_registers_validated_source_configs() {
    let session = Session::builder().build().unwrap();
    let config =
        ConnectorConfig::new("orders_input", "parquet").with_property("path", "/tmp/in.parquet");

    session.register_source_config(config).unwrap();

    let sources = session.registered_source_configs();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].name, "orders_input");
    assert_eq!(sources[0].kind, "parquet");
    assert!(session.source_config("orders_input").is_some());
}

#[test]
fn session_rejects_invalid_source_configs() {
    let session = Session::builder().build().unwrap();
    let error = session
        .register_source_config(ConnectorConfig::new("orders_input", "parquet"))
        .unwrap_err();

    assert!(
        matches!(error, KrishivError::InvalidConfig { .. }),
        "expected invalid config, got {error:?}"
    );
    assert!(session.registered_source_configs().is_empty());
}

#[tokio::test]
async fn submit_sql_resolves_registered_connector_names() {
    use krishiv_connectors::Source;
    use krishiv_connectors::parquet::ParquetSource;

    let session = Session::builder().build().unwrap();
    let dir = tempdir().unwrap();
    let input = dir.path().join("people.parquet");
    let output = dir.path().join("cities.parquet");
    write_people_parquet(&input);
    session
        .register_source_config(
            ConnectorConfig::new("people_input", "parquet")
                .with_property("path", input.display().to_string()),
        )
        .unwrap();
    session
        .register_sink_config(
            ConnectorConfig::new("cities_output", "parquet")
                .with_property("path", output.display().to_string()),
        )
        .unwrap();

    let sql = "
        CREATE SOURCE people FROM people_input;
        CREATE SOURCE cities AS SELECT city, COUNT(*) AS n FROM people GROUP BY city;
        CREATE SINK out FROM cities INTO cities_output;
    ";

    let handle = session.submit_sql(sql).await.unwrap();

    assert_eq!(handle.status(), krishiv_engine_core::JobStatus::Completed);
    let mut reader = ParquetSource::open(&output).unwrap();
    let out = reader.read_batch().await.unwrap().expect("output batch");
    assert_eq!(out.num_rows(), 2);
}

#[tokio::test]
async fn submit_sql_background_tracks_failed_local_job() {
    let session = Session::builder().build().unwrap();
    let dir = tempdir().unwrap();
    let input = dir.path().join("people.parquet");
    let output = dir.path().join("out.parquet");
    write_people_parquet(&input);
    let sql = format!(
        "CREATE SOURCE orders FROM parquet(path='{}'); \
         CREATE SOURCE bad AS SELECT missing_column FROM orders; \
         CREATE SINK out FROM bad INTO parquet(path='{}');",
        input.display(),
        output.display()
    );

    let submitted = session.submit_sql_background(&sql).unwrap();
    assert_eq!(submitted.job_id(), "out");
    assert_eq!(submitted.state(), SubmittedSqlJobState::Running);

    let final_status = wait_for_submitted_sql_terminal(&session, "out").await;
    assert_eq!(final_status.state(), SubmittedSqlJobState::Failed);
    assert!(final_status.error().is_some());
    assert_eq!(
        session
            .jobs()
            .into_iter()
            .find(|job| job.id().as_str() == "out")
            .map(|job| job.state()),
        Some(krishiv_runtime::JobState::Failed)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_submitted_sql_job_marks_local_status_cancelled() {
    let session = Session::builder().build().unwrap();
    let dir = tempdir().unwrap();
    let input = dir.path().join("missing.parquet");
    let output = dir.path().join("out.parquet");
    let sql = format!(
        "CREATE SOURCE orders FROM parquet(path='{}'); \
         CREATE SINK out FROM orders INTO parquet(path='{}');",
        input.display(),
        output.display()
    );

    session.submit_sql_background(&sql).unwrap();
    let cancelled = session.cancel_submitted_sql_job("out").unwrap();

    assert_eq!(cancelled.state(), SubmittedSqlJobState::Cancelled);
    assert_eq!(
        session
            .jobs()
            .into_iter()
            .find(|job| job.id().as_str() == "out")
            .map(|job| job.state()),
        Some(krishiv_runtime::JobState::Cancelled)
    );
}

async fn wait_for_submitted_sql_terminal(
    session: &Session,
    job_id: &str,
) -> crate::SubmittedSqlJobStatus {
    for _ in 0..100 {
        if let Some(status) = session.submitted_sql_job_status(job_id)
            && status.state().is_terminal()
        {
            return status;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("background SQL job {job_id} did not reach a terminal state");
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
#[allow(clippy::unwrap_used)]
fn tumbling_window_collect_executes_in_embedded_mode() {
    let session = Session::builder().build().unwrap();
    let batch = krishiv_common::test_fixtures::make_test_user_ts_batch(
        vec!["a", "a", "b"],
        vec![1_000, 5_000, 2_000],
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
#[allow(clippy::unwrap_used)]
fn sliding_window_collect_via_unified_runtime() {
    let session = Session::builder().build().unwrap();
    let batch = krishiv_common::test_fixtures::make_test_user_ts_batch(
        vec!["a", "a", "b"],
        vec![1_000, 5_000, 2_000],
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
#[allow(clippy::unwrap_used)]
fn session_window_collect_via_unified_runtime() {
    let session = Session::builder().build().unwrap();
    let batch =
        krishiv_common::test_fixtures::make_test_user_ts_batch(vec!["a", "b"], vec![1_000, 8_000])
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
#[allow(clippy::unwrap_used)]
fn session_subsequent_window_collects() {
    let session = Session::builder().build().unwrap();
    let batch =
        krishiv_common::test_fixtures::make_test_user_ts_batch(vec!["a"], vec![1_000]).unwrap();
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

// ── Async twins added alongside the sync/async correctness audit ───────────

#[tokio::test]
async fn describe_async_matches_describe() {
    let session = Session::builder().build().unwrap();
    let df = session
        .sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)")
        .unwrap();
    let sync_result = df.describe().unwrap().collect().unwrap();
    let async_result = df
        .describe_async()
        .await
        .unwrap()
        .collect_async()
        .await
        .unwrap();
    assert_eq!(sync_result.row_count(), async_result.row_count());
    assert!(sync_result.row_count() > 0);
}

#[tokio::test]
async fn show_async_matches_show() {
    let session = Session::builder().build().unwrap();
    let df = session.sql("SELECT 1 AS x").unwrap();
    let sync_text = df.show(10).unwrap();
    let async_text = df.show_async(10).await.unwrap();
    assert_eq!(sync_text, async_text);
    assert!(sync_text.contains('1'));
}

#[tokio::test]
async fn read_parquet_with_options_async_collects_rows() {
    let temp = tempdir().unwrap();
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    let session = Session::builder().build().unwrap();
    let df = session
        .read_parquet_with_options_async(
            &parquet_path,
            krishiv_sql::ParquetReaderOptions::default(),
        )
        .await
        .unwrap();
    let result = df.collect_async().await.unwrap();
    assert_eq!(result.row_count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_async_embedded_push_and_drain() {
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
        allowed_lateness_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    };
    let job = session
        .stream_async("stream-async-test", spec)
        .await
        .expect("stream_async");
    assert!(matches!(job, crate::StreamJob::Embedded(_)));

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
    job.push(vec![batch]).await.expect("push");
    let _ = job.drain().await.expect("drain");
}

#[test]
fn typed_window_functions_and_frame_execute() {
    use crate::expression::{
        WindowFrame, WindowFrameBound, col, dense_rank, first_value, lag, last_value, lead, rank,
        row_number, sum,
    };

    let session = Session::builder().build().unwrap();
    let df = session
        .sql(
            "SELECT 1 AS grp, 10 AS amount UNION ALL SELECT 1 AS grp, 20 AS amount \
             UNION ALL SELECT 1 AS grp, 30 AS amount",
        )
        .unwrap();
    let order = vec![col("amount").asc()];
    let windowed = df
        .select_exprs(&[
            col("grp"),
            col("amount"),
            row_number()
                .over(vec![col("grp")], order.clone())
                .alias("rn"),
            rank().over(vec![col("grp")], order.clone()).alias("rk"),
            dense_rank()
                .over(vec![col("grp")], order.clone())
                .alias("dr"),
            lag(col("amount"), 1, None)
                .over(vec![col("grp")], order.clone())
                .alias("lag_amt"),
            lead(col("amount"), 1, None)
                .over(vec![col("grp")], order.clone())
                .alias("lead_amt"),
            first_value(col("amount"))
                .over(vec![col("grp")], order.clone())
                .alias("first_amt"),
            last_value(col("amount"))
                .over(vec![col("grp")], order.clone())
                .frame(WindowFrame::rows(
                    WindowFrameBound::UnboundedPreceding,
                    WindowFrameBound::UnboundedFollowing,
                ))
                .alias("last_amt"),
            sum(col("amount"))
                .over(vec![col("grp")], order.clone())
                .frame(WindowFrame::rows(
                    WindowFrameBound::UnboundedPreceding,
                    WindowFrameBound::CurrentRow,
                ))
                .alias("running_sum"),
        ])
        .unwrap();
    let result = windowed.collect().unwrap();
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
        allowed_lateness_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        window_timezone: None,
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
async fn local_continuous_stream_status_and_checkpoint_are_mode_aware() {
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
        allowed_lateness_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    };
    session
        .submit_stream_job("status-job", spec)
        .expect("submit");
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
        .push_stream_job_input("status-job", vec![batch])
        .expect("push");
    let _ = session.poll_stream_job("status-job").await.expect("poll");

    let status = session
        .continuous_stream_status("status-job")
        .await
        .expect("status call must succeed")
        .expect("status must exist");
    assert_eq!(status.job_id(), "status-job");
    assert_eq!(status.state(), "registered");
    assert_eq!(status.pending_input_batches(), Some(0));
    assert!(status.last_watermark_ms().is_some());
    assert!(status.snapshot_available());
    assert!(!status.uses_remote_execution());

    let statuses = session
        .list_continuous_stream_statuses()
        .await
        .expect("list call must succeed");
    assert!(statuses.iter().any(|entry| entry.job_id() == "status-job"));

    let checkpoint = session
        .checkpoint_continuous_stream("status-job")
        .await
        .expect("checkpoint export must succeed");
    assert_eq!(checkpoint.job_id(), "status-job");
    assert!(checkpoint.snapshot_available());
    assert!(checkpoint.snapshot_bytes().is_some());
    assert!(checkpoint.watermark_ms().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn local_continuous_stream_restore_rolls_back_exported_state() {
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
        allowed_lateness_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    };
    session
        .submit_stream_job("restore-job", spec)
        .expect("submit");
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a"])) as _,
            Arc::new(Int64Array::from(vec![1_000])) as _,
        ],
    )
    .unwrap();
    session
        .push_stream_job_input("restore-job", vec![first])
        .expect("push first");
    let _ = session
        .poll_stream_job("restore-job")
        .await
        .expect("drain first");
    let checkpoint1 = session
        .checkpoint_continuous_stream("restore-job")
        .await
        .expect("checkpoint 1");
    let bytes1 = checkpoint1.snapshot_bytes().unwrap().to_vec();

    let second = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])) as _,
            Arc::new(Int64Array::from(vec![2_000])) as _,
        ],
    )
    .unwrap();
    session
        .push_stream_job_input("restore-job", vec![second])
        .expect("push second");
    let _ = session
        .poll_stream_job("restore-job")
        .await
        .expect("drain second");
    let checkpoint2 = session
        .checkpoint_continuous_stream("restore-job")
        .await
        .expect("checkpoint 2");
    let bytes2 = checkpoint2.snapshot_bytes().unwrap().to_vec();
    assert_ne!(bytes1, bytes2, "second cycle should change saved state");

    let restored = session
        .restore_continuous_stream("restore-job", &bytes1)
        .await
        .expect("restore");
    assert_eq!(restored.job_id(), "restore-job");

    let checkpoint3 = session
        .checkpoint_continuous_stream("restore-job")
        .await
        .expect("checkpoint 3");
    assert_eq!(
        checkpoint3.snapshot_bytes().unwrap(),
        bytes1.as_slice(),
        "restored state should match the exported checkpoint"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn register_stream_job_sql_tracks_compiled_source_and_spec() {
    let session = Session::builder().build().unwrap();
    let registered = session
        .register_stream_job_sql(
            "windowed-events",
            "SELECT user_id, COUNT(*) AS count \
             FROM TUMBLE(TABLE events_src, DESCRIPTOR(ts), 10000) \
             GROUP BY user_id, window_start, window_end",
        )
        .await
        .expect("register continuous stream from SQL");

    assert_eq!(registered.name(), "windowed-events");
    assert_eq!(registered.source(), Some("events_src"));
    assert_eq!(registered.spec().event_time_column, "ts");
    assert_eq!(registered.spec().window_size_ms, 10_000);
    assert!(matches!(
        registered.spec().window_kind,
        LocalWindowKind::Tumbling
    ));

    let lookup = session
        .registered_stream_job("windowed-events")
        .expect("registered stream job metadata");
    assert_eq!(lookup.source(), Some("events_src"));
    assert_eq!(lookup.spec().key_column, "user_id");
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
        allowed_lateness_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        window_timezone: None,
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
        allowed_lateness_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        window_timezone: None,
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

// ── Phase 4: typed DataFrame expressions, aggregation, I/O, and config ───────

#[test]
fn typed_expressions_select_filter_and_aggregate() {
    use crate::{col, count_all, lit, sum};

    let session = Session::builder().build().unwrap();
    let df = session
        .sql("SELECT * FROM (VALUES ('a', 10), ('a', 20), ('b', 5)) AS t(category, amount)")
        .unwrap();

    let filtered = df
        .filter_expr(col("amount").gt(lit(9)))
        .unwrap()
        .select_exprs(&[
            col("category"),
            col("amount").multiply(lit(2)).alias("doubled"),
        ])
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(filtered.row_count(), 2);
    assert_eq!(filtered.batches()[0].schema().field(1).name(), "doubled");

    let grouped = df
        .group_by(&[col("category")])
        .agg(&[count_all().alias("rows"), sum(col("amount")).alias("total")])
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(grouped.row_count(), 2);
}

#[test]
fn generic_reader_writer_roundtrip_and_validate_format() {
    let session = Session::builder().build().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("phase4.parquet");
    let df = session.sql("SELECT 7 AS value").unwrap();

    df.write()
        .format("parquet")
        .unwrap()
        .save(path.to_str().unwrap())
        .unwrap();
    let result = session
        .read()
        .format("parquet")
        .unwrap()
        .load(&path)
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert!(session.read().format("orc").is_err());
}

#[test]
fn session_config_is_shared_across_clones() {
    let session = Session::builder()
        .with_config("spark.sql.shuffle.partitions", "16")
        .build()
        .unwrap();
    let clone = session.clone();
    assert_eq!(
        session
            .get_config("spark.sql.shuffle.partitions")
            .as_deref(),
        Some("16")
    );
    clone.set_config("krishiv.execution.label", "phase4");
    assert_eq!(
        session.get_config("krishiv.execution.label").as_deref(),
        Some("phase4")
    );
    assert_eq!(
        session.unset_config("krishiv.execution.label").as_deref(),
        Some("phase4")
    );
}

#[test]
fn explain_modes_include_analysis_stats() {
    let session = Session::builder().build().unwrap();
    let df = session.sql("SELECT 1 AS value").unwrap();
    assert!(
        !df.explain_with(crate::ExplainMode::Logical)
            .unwrap()
            .is_empty()
    );
    assert!(
        !df.explain_with(crate::ExplainMode::Physical)
            .unwrap()
            .is_empty()
    );
    let analyzed = df.explain_with(crate::ExplainMode::Analyze).unwrap();
    assert!(analyzed.contains("Execution statistics"));
    assert!(analyzed.contains("result_rows=1"));
}

#[test]
fn phase_c_boundedness_and_typed_catalog_are_canonical() {
    let session = Session::builder().build().unwrap();
    let dataframe = session.sql("SELECT 1 AS id").unwrap();
    assert_eq!(dataframe.boundedness(), crate::Boundedness::Bounded);
    assert!(dataframe.is_bounded());

    session
        .create_view("phase_c_view", "SELECT 7 AS value")
        .unwrap();
    let identifier = crate::TableIdentifier::new("phase_c_view").unwrap();
    let metadata = session.table_metadata(&identifier).unwrap();
    assert_eq!(metadata.identifier, identifier);
    assert_eq!(metadata.schema.field(0).name(), "value");
    assert_eq!(metadata.boundedness, crate::Boundedness::Bounded);
}

#[test]
fn create_or_replace_temp_view_replaces_existing_view() {
    let session = Session::builder().build().unwrap();

    session
        .sql("SELECT 1 AS value")
        .unwrap()
        .create_or_replace_temp_view("replace_me")
        .unwrap();
    session
        .sql("SELECT 2 AS value")
        .unwrap()
        .create_or_replace_temp_view("replace_me")
        .unwrap();

    let result = session
        .sql("SELECT value FROM replace_me")
        .unwrap()
        .collect()
        .unwrap();
    let batch = &result.batches()[0];
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 2);
}

#[test]
fn create_or_replace_temp_view_escapes_identifier_quotes() {
    let session = Session::builder().build().unwrap();

    session
        .sql("SELECT 3 AS value")
        .unwrap()
        .create_or_replace_temp_view("quoted\"view")
        .unwrap();

    let result = session
        .sql("SELECT value FROM \"quoted\"\"view\"")
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn drop_table_drops_sql_views_too() {
    let session = Session::builder().build().unwrap();

    session.create_view("drop_me", "SELECT 7 AS value").unwrap();
    assert!(session.table_exists("drop_me").unwrap());

    session.drop_table("drop_me").unwrap();

    assert!(!session.table_exists("drop_me").unwrap());
}

#[test]
fn drop_relation_uses_typed_identifier_without_double_quoting() {
    let session = Session::builder().build().unwrap();
    let identifier = crate::TableIdentifier::new("typed_drop").unwrap();

    session
        .create_temp_view(&identifier, "SELECT 11 AS value")
        .unwrap();
    assert!(session.table_exists("typed_drop").unwrap());

    session.drop_relation(&identifier).unwrap();

    assert!(!session.table_exists("typed_drop").unwrap());
}

#[test]
fn phase_c_prepared_statements_bind_typed_values() {
    let session = Session::builder().build().unwrap();
    let prepared = session
        .prepare("SELECT $1 AS name, $2 AS amount, '$3' AS literal")
        .unwrap();
    assert_eq!(prepared.parameter_count(), 2);
    let result = prepared
        .bind(&[
            crate::ScalarValue::Utf8("O'Reilly".into()),
            crate::ScalarValue::Int64(42),
        ])
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn phase_c_set_null_sample_and_shape_operations_execute() {
    use crate::{col, sum};

    let session = Session::builder().build().unwrap();
    let left = session
        .sql("SELECT 1 AS id, 'a' AS category, 10 AS amount UNION ALL SELECT 2, NULL, 20")
        .unwrap();
    let right = session
        .sql("SELECT 1 AS id, 'a' AS category, 10 AS amount")
        .unwrap();

    assert_eq!(
        left.drop_nulls(&["category"])
            .unwrap()
            .collect()
            .unwrap()
            .row_count(),
        1
    );
    assert_eq!(left.sample(0.0).unwrap().collect().unwrap().row_count(), 0);
    assert_eq!(
        left.intersect_distinct(&right)
            .unwrap()
            .collect()
            .unwrap()
            .row_count(),
        1
    );
    assert_eq!(
        left.except_distinct(&right)
            .unwrap()
            .collect()
            .unwrap()
            .row_count(),
        1
    );
    assert_eq!(
        right
            .union_distinct(&right)
            .unwrap()
            .collect()
            .unwrap()
            .row_count(),
        1
    );

    let cube = left
        .drop_nulls(&["category"])
        .unwrap()
        .group_by(&[])
        .agg_grouping(
            crate::GroupingSpec::Cube(vec![col("category")]),
            &[sum(col("amount")).alias("total")],
        )
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(cube.row_count(), 2);

    let pivoted = left
        .drop_nulls(&["category"])
        .unwrap()
        .pivot(
            &[],
            col("category"),
            sum(col("amount")),
            &[crate::PivotValue::new(
                crate::ScalarValue::Utf8("a".into()),
                "a_total",
            )],
        )
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(pivoted.row_count(), 1);

    let wide = session.sql("SELECT 1 AS id, 10 AS jan, 20 AS feb").unwrap();
    let unpivoted = wide
        .unpivot(&["jan", "feb"], "month", "amount")
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(unpivoted.row_count(), 2);
}

#[test]
fn union_by_name_aligns_columns_by_name() {
    use crate::{col, lit};
    let session = Session::builder().build().unwrap();
    // Same column names, different order: unionByName must align by name.
    let left = session.sql("SELECT 1 AS a, 2 AS b").unwrap();
    let right = session.sql("SELECT 20 AS b, 10 AS a").unwrap();

    let out = left.union_by_name(&right).unwrap().collect().unwrap();
    assert_eq!(out.row_count(), 2);

    // Right's row must land aligned by name (a=10). A positional union would
    // have put 20 under `a`, so filtering a=10 proves name alignment.
    let aligned = left
        .union_by_name(&right)
        .unwrap()
        .filter_expr(col("a").eq(lit(10i64)))
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(aligned.row_count(), 1, "right's row aligned by name (a=10)");

    // A differing column set is a clear error, never a silent misalignment.
    let mismatched = session.sql("SELECT 1 AS a, 2 AS c").unwrap();
    assert!(left.union_by_name(&mismatched).is_err());
}

#[test]
fn f_star_scalar_helpers_have_exact_semantics() {
    use crate::{abs, coalesce, col, lit, upper};
    let session = Session::builder().build().unwrap();
    let row = session
        .sql("SELECT CAST(NULL AS BIGINT) AS a, 5 AS b, 'abc' AS s, -3 AS n")
        .unwrap();

    // coalesce(a, b) picks b=5 when a is NULL.
    let filled = row
        .select_exprs(&[coalesce(vec![col("a"), col("b")]).alias("c")])
        .unwrap()
        .filter_expr(col("c").eq(lit(5i64)))
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(filled.row_count(), 1, "coalesce picks the first non-null");

    // upper('abc') = 'ABC'.
    let up = row
        .select_exprs(&[upper(col("s")).alias("u")])
        .unwrap()
        .filter_expr(col("u").eq(lit("ABC")))
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(up.row_count(), 1, "upper uppercases");

    // abs(-3) = 3.
    let a = row
        .select_exprs(&[abs(col("n")).alias("m")])
        .unwrap()
        .filter_expr(col("m").eq(lit(3i64)))
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(a.row_count(), 1, "abs of -3 is 3");
}

#[test]
fn phase_c_boundedness_metadata_exists_in_all_session_modes() {
    let sessions = [
        Session::builder().build().unwrap(),
        Session::builder()
            .with_local_cluster("http://127.0.0.1:50052")
            .build()
            .unwrap(),
        Session::builder()
            .with_coordinator("http://127.0.0.1:50051")
            .build()
            .unwrap(),
    ];
    for session in sessions {
        let dataframe = session.sql("SELECT 1 AS id").unwrap();
        assert_eq!(dataframe.boundedness(), crate::Boundedness::Bounded);
    }
}

#[test]
fn phase_d_typed_file_writer_partitions_sorts_and_overwrites() {
    use crate::{DataFormat, FileLayout, FileWriteOptions, SortField, WriteMode};

    let session = Session::builder().build().unwrap();
    let dataframe = session
        .sql("SELECT 2 AS id, 'b' AS region UNION ALL SELECT 1, 'a' UNION ALL SELECT 3, 'a'")
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("partitioned");
    let options = FileWriteOptions {
        format: DataFormat::Parquet,
        mode: WriteMode::Overwrite,
        layout: FileLayout {
            partition_by: vec!["region".into()],
            sort_by: vec![SortField {
                column: "id".into(),
                direction: crate::FileSortDirection::Ascending,
            }],
            max_rows_per_file: Some(1),
            ..FileLayout::default()
        },
        schema_evolution: crate::SchemaEvolutionMode::Strict,
    };
    dataframe
        .write()
        .file_options(options)
        .unwrap()
        .save(output.to_str().unwrap())
        .unwrap();
    assert!(output.join("region=a").is_dir());
    assert!(output.join("region=b").is_dir());
    assert_eq!(
        std::fs::read_dir(output.join("region=a")).unwrap().count(),
        2
    );
}

#[tokio::test]
async fn phase_d_async_reader_and_iceberg_writer_round_trip() {
    use std::sync::Arc;

    use krishiv_connectors::lakehouse::{
        IcebergScanOptions, IcebergTableRef, MemoryLakehouseTable, SchemaField, SchemaVersion,
    };

    let session = Session::builder().build().unwrap();
    let dataframe = session
        .sql_async("SELECT 1 AS id UNION ALL SELECT 2")
        .await
        .unwrap();
    let table = Arc::new(MemoryLakehouseTable::new(
        IcebergTableRef::new("default", "public", "phase_d"),
        SchemaVersion {
            schema_id: 1,
            fields: vec![SchemaField {
                id: 1,
                name: "id".into(),
                required: true,
                data_type: "long".into(),
            }],
        },
    ));
    dataframe
        .write()
        .mode(crate::WriteMode::Overwrite)
        .iceberg(table.clone())
        .save_target_async()
        .await
        .unwrap();
    let loaded = session
        .read()
        .iceberg(table, IcebergScanOptions::new())
        .load_source_async()
        .await
        .unwrap()
        .collect_async()
        .await
        .unwrap();
    assert_eq!(loaded.row_count(), 2);
}

#[test]
fn sql_as_rejects_missing_auth_provider() {
    let session = Session::builder().build().unwrap();
    let err = session.sql_as("key", "SELECT 1").unwrap_err();
    assert!(matches!(err, KrishivError::InvalidConfig { .. }));
}

#[test]
fn sql_as_enforces_policy_on_referenced_tables() {
    use krishiv_plan::governance::{AllowAllPolicyHook, StaticApiKeyAuthProvider};
    use std::collections::HashMap;
    use std::sync::Arc;

    struct DenySecretPolicy;
    impl krishiv_plan::governance::PolicyHook for DenySecretPolicy {
        fn check_table_access(&self, table_name: &str) -> bool {
            table_name != "secret"
        }
    }

    let mut keys = HashMap::new();
    keys.insert("dev-key".into(), "alice".into());
    let session = Session::builder()
        .with_auth(Arc::new(StaticApiKeyAuthProvider::new(keys)))
        .with_policy(Arc::new(DenySecretPolicy))
        .build()
        .unwrap();
    session
        .register_record_batches(
            "secret",
            vec![RecordBatch::new_empty(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
            ])))],
        )
        .unwrap();
    let err = session
        .sql_as("dev-key", "SELECT * FROM secret")
        .unwrap_err();
    assert!(matches!(err, KrishivError::AccessDenied { .. }));

    let mut allow_keys = HashMap::new();
    allow_keys.insert("dev-key".into(), "alice".into());
    let session_allow = Session::builder()
        .with_auth(Arc::new(StaticApiKeyAuthProvider::new(allow_keys)))
        .with_policy(Arc::new(AllowAllPolicyHook))
        .build()
        .unwrap();
    session_allow
        .register_record_batches(
            "open",
            vec![RecordBatch::new_empty(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
            ])))],
        )
        .unwrap();
    let df = session_allow
        .sql_as("dev-key", "SELECT * FROM open")
        .unwrap();
    assert_eq!(df.collect().unwrap().row_count(), 0);
}

#[test]
fn describe_and_live_table_sql_intercepts_work() {
    let session = Session::builder().build().unwrap();
    session
        .register_record_batches(
            "people",
            vec![
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int64, false),
                        Field::new("name", DataType::Utf8, true),
                    ])),
                    vec![
                        Arc::new(Int64Array::from(vec![1_i64])),
                        Arc::new(StringArray::from(vec![Some("alice")])),
                    ],
                )
                .unwrap(),
            ],
        )
        .unwrap();

    let describe = session.sql("DESCRIBE people").unwrap().collect().unwrap();
    assert_eq!(describe.row_count(), 2);

    session
        .sql("CREATE LIVE TABLE live_people AS SELECT id FROM people")
        .unwrap();
    assert!(
        session
            .live_table_registry()
            .contains("live_people")
            .unwrap()
    );
}

// ── Unified compute API: mode-aware ivm() + one feed() ──────────────────────

#[tokio::test]
async fn embedded_session_ivm_returns_embedded_job() {
    use crate::{IvmJob, Job, JobKind};

    let session = Session::builder().build().unwrap();
    let job = session.ivm("revenue").await.unwrap();
    // Regression guard for the embedded-on-remote bug: an embedded session must
    // hand out an embedded IVM job.
    assert!(matches!(job, IvmJob::Embedded(_)));
    assert_eq!(job.kind(), JobKind::Ivm);
    assert_eq!(job.job_id(), "revenue");
}

#[tokio::test]
async fn ivm_feed_from_cdc_then_step_via_unified_api() {
    use crate::{FeedableJob, IvmJob};
    use krishiv_delta::DeltaBatch;

    fn batch(ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap()
    }

    let session = Session::builder().build().unwrap();
    let job: IvmJob = session.ivm("orders_job").await.unwrap();

    // The one feed primitive + a DeltaBatch constructor (replaces feed_cdc_source).
    let insert = DeltaBatch::from_cdc(None, Some(batch(&[1, 2])))
        .unwrap()
        .unwrap();
    job.feed("orders", &insert).await.unwrap();
    let report = job.step().await.unwrap();
    assert_eq!(report.tick, 1);
}

// ── Declarative pipeline (Tier 2: source → transform → sink) ─────────────────

#[tokio::test]
async fn pipeline_ivm_cdc_source_to_memory_sink() {
    use crate::RunPolicy;
    use crate::pipeline::CdcChange;
    use std::sync::{Arc as StdArc, Mutex};

    fn order(id: i64, amount: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(Int64Array::from(vec![amount])),
            ],
        )
        .unwrap()
    }

    let sink: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let session = Session::builder().build().unwrap();

    // CDC source → incremental SUM view → in-memory sink. Mode inferred as IVM.
    session
        .pipeline("revenue")
        .source_cdc(
            "orders",
            vec![
                CdcChange::insert(order(1, 100)),
                CdcChange::insert(order(2, 50)),
            ],
        )
        .view("revenue", "SELECT SUM(amount) AS total FROM orders", true)
        .sink_memory("revenue", sink.clone())
        .run(RunPolicy::Once)
        .await
        .unwrap();

    let out = sink.lock().unwrap();
    assert_eq!(out.len(), 1, "sink should receive one snapshot batch");
    // SUM over an Int64 column is Int64 (exact integer sums, as in batch SQL).
    let total = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("SUM output column is Int64")
        .value(0);
    assert_eq!(total, 150, "SUM(amount) over the two CDC inserts");
}

#[tokio::test]
async fn pipeline_batch_memory_source_to_memory_sink() {
    use crate::RunPolicy;
    use std::sync::{Arc as StdArc, Mutex};

    fn rows(ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap()
    }

    let sink: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let session = Session::builder().build().unwrap();

    session
        .pipeline("count_job")
        .source_memory("items", vec![rows(&[1, 2, 3, 4])])
        .view("counted", "SELECT COUNT(*) AS n FROM items", false)
        .sink_memory("counted", sink.clone())
        .mode(crate::PipelineMode::Batch)
        .run(RunPolicy::Once)
        .await
        .unwrap();

    let out = sink.lock().unwrap();
    let n = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 4);
}

// ── SQL pipeline DDL: CREATE SOURCE / SINK + START PIPELINE ──────────────────

#[test]
fn sql_pipeline_create_source_view_sink_start() {
    fn order(id: i64, amount: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(Int64Array::from(vec![amount])),
            ],
        )
        .unwrap()
    }

    let session = Session::builder().build().unwrap();
    // Raw data the SOURCE query reads from.
    session
        .register_record_batches("orders_raw", vec![order(1, 100), order(2, 50)])
        .unwrap();

    // Declarative pipeline entirely in SQL.
    session
        .sql("CREATE SOURCE orders AS SELECT * FROM orders_raw")
        .unwrap();
    session
        .sql("CREATE INCREMENTAL VIEW revenue AS SELECT SUM(amount) AS total FROM orders")
        .unwrap();
    session.sql("CREATE SINK out FROM revenue").unwrap();

    // START PIPELINE runs it and returns the sink output as a result set.
    let result = session
        .sql("START PIPELINE out")
        .unwrap()
        .collect()
        .unwrap();
    let batches = result.into_batches();
    assert_eq!(batches.len(), 1);
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("SUM over Int64 is Int64")
        .value(0);
    assert_eq!(total, 150);
}

// ── Connector-backed pipeline + streaming-mode inference ────────────────────

/// A bounded in-memory connector source (drains a queue of batches).
struct VecSource {
    batches: std::collections::VecDeque<RecordBatch>,
}

impl krishiv_connectors::Source for VecSource {
    fn capabilities(&self) -> krishiv_connectors::ConnectorCapabilities {
        krishiv_connectors::ConnectorCapabilities::new().with_bounded()
    }
    async fn read_batch(&mut self) -> krishiv_connectors::ConnectorResult<Option<RecordBatch>> {
        Ok(self.batches.pop_front())
    }
    fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
        None
    }
}

/// Connector source with explicit schema metadata; used for idle stream startup.
struct SchemaVecSource {
    batches: std::collections::VecDeque<RecordBatch>,
    schema: arrow::datatypes::SchemaRef,
    bounded: bool,
}

impl krishiv_connectors::Source for SchemaVecSource {
    fn capabilities(&self) -> krishiv_connectors::ConnectorCapabilities {
        let capabilities = krishiv_connectors::ConnectorCapabilities::new();
        if self.bounded {
            capabilities.with_bounded()
        } else {
            capabilities.with_unbounded()
        }
    }

    fn source_schema(&self) -> Option<arrow::datatypes::SchemaRef> {
        Some(self.schema.clone())
    }

    async fn read_batch(&mut self) -> krishiv_connectors::ConnectorResult<Option<RecordBatch>> {
        Ok(self.batches.pop_front())
    }

    fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
        None
    }
}

/// An in-memory connector sink (collects written batches).
struct VecSink {
    out: std::sync::Arc<std::sync::Mutex<Vec<RecordBatch>>>,
}

impl krishiv_connectors::Sink for VecSink {
    fn capabilities(&self) -> krishiv_connectors::ConnectorCapabilities {
        krishiv_connectors::ConnectorCapabilities::new()
    }
    async fn write_batch(&mut self, batch: RecordBatch) -> krishiv_connectors::ConnectorResult<()> {
        self.out.lock().unwrap().push(batch);
        Ok(())
    }
    async fn flush(&mut self) -> krishiv_connectors::ConnectorResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn pipeline_stream_idle_unbounded_connector_uses_schema_metadata() {
    use crate::pipeline::{Egress, Ingest};
    use crate::{PipelineMode, RunPolicy};
    use std::collections::VecDeque;
    use std::time::Duration;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let src = SchemaVecSource {
        batches: VecDeque::new(),
        schema,
        bounded: false,
    };
    let collected: Arc<std::sync::Mutex<Vec<RecordBatch>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = VecSink {
        out: collected.clone(),
    };

    let session = Session::builder().build().unwrap();
    let run = session
        .pipeline("idle_schema_stream")
        .mode(PipelineMode::Stream)
        .source("orders", Ingest::Connector(Box::new(src)))
        .view("revenue", "SELECT SUM(amount) AS total FROM orders", true)
        .sink("revenue", Egress::Connector(Box::new(sink)))
        .run(RunPolicy::Once);

    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("RunPolicy::Once should return after an idle unbounded poll")
        .expect("schema metadata should allow planning before the first batch");

    assert!(collected.lock().unwrap().is_empty());
}

#[tokio::test]
async fn pipeline_stream_connector_source_to_connector_sink() {
    use crate::pipeline::{Egress, Ingest};
    use crate::{PipelineMode, RunPolicy};
    use std::collections::VecDeque;
    use std::sync::{Arc as StdArc, Mutex};

    fn order(id: i64, amount: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(Int64Array::from(vec![amount])),
            ],
        )
        .unwrap()
    }

    let src = VecSource {
        batches: VecDeque::from(vec![order(1, 100), order(2, 50)]),
    };
    let collected: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let sink = VecSink {
        out: collected.clone(),
    };

    let session = Session::builder().build().unwrap();
    let pipeline = session
        .pipeline("conn")
        .source("orders", Ingest::Connector(Box::new(src)))
        .view("revenue", "SELECT SUM(amount) AS total FROM orders", true)
        .sink("revenue", Egress::Connector(Box::new(sink)))
        .build();

    // A connector record source infers Stream mode.
    assert_eq!(pipeline.mode(), PipelineMode::Stream);
    pipeline.run(RunPolicy::Once).await.unwrap();

    let out = collected.lock().unwrap();
    assert_eq!(out.len(), 1, "connector sink should receive one batch");
    let total = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("SUM over Int64 is Int64")
        .value(0);
    assert_eq!(total, 150);
}

#[tokio::test]
async fn pipeline_stream_connector_source_steps_without_pre_draining() {
    use crate::RunPolicy;
    use crate::pipeline::{Egress, Ingest};
    use std::collections::VecDeque;
    use std::sync::{Arc as StdArc, Mutex};

    fn order(id: i64, amount: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(Int64Array::from(vec![amount])),
            ],
        )
        .unwrap()
    }

    fn total(batch: &RecordBatch) -> i64 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("SUM over Int64 is Int64")
            .value(0)
    }

    let src = VecSource {
        batches: VecDeque::from(vec![order(1, 100), order(2, 50)]),
    };
    let collected: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let sink = VecSink {
        out: collected.clone(),
    };

    let session = Session::builder().build().unwrap();
    session
        .pipeline("conn_incremental")
        .source("orders", Ingest::Connector(Box::new(src)))
        .view("revenue", "SELECT SUM(amount) AS total FROM orders", true)
        .sink("revenue", Egress::Connector(Box::new(sink)))
        .run(RunPolicy::EveryRows(1))
        .await
        .unwrap();

    let out = collected.lock().unwrap();
    assert_eq!(
        out.len(),
        2,
        "stream connector should publish after each coalesced step, not only after source exhaustion"
    );
    assert_eq!(total(&out[0]), 100);
    assert_eq!(total(&out[1]), 150);
}

#[test]
fn sql_pipeline_parquet_source_and_sink() {
    let dir = tempdir().unwrap();
    let in_path = dir.path().join("orders.parquet");
    let out_path = dir.path().join("revenue.parquet");

    // Write input parquet: orders[id, amount] = (1,100),(2,50).
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![100, 50])),
            ],
        )
        .unwrap();
        let file = File::create(&in_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let session = Session::builder().build().unwrap();
    // Fully connector-backed SQL pipeline: parquet source → view → parquet sink.
    session
        .sql(format!(
            "CREATE SOURCE orders FROM PARQUET(path='{}')",
            in_path.display()
        ))
        .unwrap();
    session
        .sql("CREATE INCREMENTAL VIEW revenue AS SELECT SUM(amount) AS total FROM orders")
        .unwrap();
    session
        .sql(format!(
            "CREATE SINK out FROM revenue INTO PARQUET(path='{}')",
            out_path.display()
        ))
        .unwrap();
    session.sql("START PIPELINE out").unwrap();

    // Read the output parquet the sink wrote.
    let result = session.read_parquet(&out_path).unwrap().collect().unwrap();
    let batches = result.into_batches();
    assert_eq!(batches.len(), 1);
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("SUM over Int64 is Int64")
        .value(0);
    assert_eq!(total, 150);
}

// ── SDP parity: expectations + validation ───────────────────────────────────

fn amounts(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vals.to_vec()))],
    )
    .unwrap()
}

#[tokio::test]
async fn pipeline_expectation_drop_filters_violating_rows() {
    use crate::{OnViolation, PipelineMode, RunPolicy};
    use std::sync::{Arc as StdArc, Mutex};

    let sink: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let session = Session::builder().build().unwrap();

    // passthrough view of amounts; expect amount > 0, drop violations (-5).
    session
        .pipeline("dq")
        .source_memory("raw", vec![amounts(&[10, -5, 20])])
        .view("clean", "SELECT amount FROM raw", true)
        .expect("clean", "positive_amount", "amount > 0", OnViolation::Drop)
        .sink_memory("clean", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once)
        .await
        .unwrap();

    let out = sink.lock().unwrap();
    let combined = arrow::compute::concat_batches(&out[0].schema(), out.iter()).unwrap();
    assert_eq!(combined.num_rows(), 2, "the -5 row should be dropped");
}

#[tokio::test]
async fn pipeline_expectation_fail_errors_on_violation() {
    use crate::{OnViolation, PipelineMode, RunPolicy};
    use std::sync::{Arc as StdArc, Mutex};

    let sink: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let session = Session::builder().build().unwrap();

    let err = session
        .pipeline("dq")
        .source_memory("raw", vec![amounts(&[10, -5])])
        .view("clean", "SELECT amount FROM raw", true)
        .expect("clean", "positive_amount", "amount > 0", OnViolation::Fail)
        .sink_memory("clean", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once)
        .await
        .expect_err("FAIL expectation must error on a violation");
    assert!(
        err.to_string().contains("positive_amount"),
        "error should name the expectation: {err}"
    );
}

#[tokio::test]
async fn pipeline_validate_detects_undefined_sink_view() {
    use crate::PipelineMode;
    use std::sync::{Arc as StdArc, Mutex};

    let sink: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let session = Session::builder().build().unwrap();

    // Sink references "missing" but only "clean" is declared.
    let pipeline = session
        .pipeline("dq")
        .source_memory("raw", vec![amounts(&[1])])
        .view("clean", "SELECT amount FROM raw", true)
        .sink_memory("missing", sink.clone())
        .mode(PipelineMode::Ivm)
        .build();

    let err = pipeline.validate().await.expect_err("validation must fail");
    assert!(
        err.to_string().contains("undefined view 'missing'"),
        "{err}"
    );
}

#[tokio::test]
async fn pipeline_validate_passes_for_well_formed_pipeline() {
    use crate::PipelineMode;
    use std::sync::{Arc as StdArc, Mutex};

    let sink: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let session = Session::builder().build().unwrap();

    let pipeline = session
        .pipeline("dq")
        .source_memory("raw", vec![amounts(&[1, 2, 3])])
        .view("total", "SELECT SUM(amount) AS s FROM raw", true)
        .sink_memory("total", sink.clone())
        .mode(PipelineMode::Ivm)
        .build();

    pipeline
        .validate()
        .await
        .expect("well-formed pipeline validates");
}

// ── DP-A: temporary views + flows (fan-in) ──────────────────────────────────

fn id_amount(id: i64, amount: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![amount])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn pipeline_flows_fan_in_union() {
    use crate::{PipelineMode, RunPolicy};
    use std::sync::{Arc as StdArc, Mutex};

    let sink: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let session = Session::builder().build().unwrap();

    // Two sources fan into one target view via append flows.
    session
        .pipeline("fanin")
        .source_memory("west", vec![id_amount(1, 10)])
        .source_memory("east", vec![id_amount(2, 20)])
        .flow("all_orders", "SELECT id, amount FROM west")
        .flow("all_orders", "SELECT id, amount FROM east")
        .sink_memory("all_orders", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once)
        .await
        .unwrap();

    let out = sink.lock().unwrap();
    let combined = arrow::compute::concat_batches(&out[0].schema(), out.iter()).unwrap();
    assert_eq!(
        combined.num_rows(),
        2,
        "both flows should be unioned into the target"
    );
}

#[tokio::test]
async fn pipeline_temp_view_intermediate() {
    use crate::{PipelineMode, RunPolicy};
    use std::sync::{Arc as StdArc, Mutex};

    let sink: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    let session = Session::builder().build().unwrap();

    // temp view "big" feeds the final counted view.
    session
        .pipeline("tv")
        .source_memory("raw", vec![id_amount(1, 100), id_amount(2, 50)])
        .temp_view("big", "SELECT id, amount FROM raw WHERE amount > 60")
        .view("count_big", "SELECT COUNT(*) AS n FROM big", true)
        .sink_memory("count_big", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once)
        .await
        .unwrap();

    let out = sink.lock().unwrap();
    let n = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 1, "only the amount=100 row passes the temp view filter");
}

// ── DP-B: persistent incremental runs + refresh (full-refresh) ──────────────

#[tokio::test]
async fn pipeline_persistent_incremental_and_refresh() {
    use crate::{PipelineMode, RunPolicy};
    use std::sync::{Arc as StdArc, Mutex};

    fn sum_of(sink: &StdArc<Mutex<Vec<RecordBatch>>>) -> i64 {
        let out = sink.lock().unwrap();
        out[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("SUM over Int64 is Int64")
            .value(0)
    }

    let session = Session::builder().build().unwrap();

    // Run 1: SUM over [10] = 10.
    let s1: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    session
        .pipeline("acc")
        .source_memory("raw", vec![amounts(&[10])])
        .view("total", "SELECT SUM(amount) AS s FROM raw", true)
        .sink_memory("total", s1.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once)
        .await
        .unwrap();
    assert_eq!(sum_of(&s1), 10);

    // Run 2 (same pipeline name): feed only the NEW row [5] → accumulates to 15.
    let s2: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    session
        .pipeline("acc")
        .source_memory("raw", vec![amounts(&[5])])
        .view("total", "SELECT SUM(amount) AS s FROM raw", true)
        .sink_memory("total", s2.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once)
        .await
        .unwrap();
    assert_eq!(
        sum_of(&s2),
        15,
        "persistent run accumulates across invocations"
    );

    // Refresh: reset state, then feed [100] → 100 (not 115).
    let s3: StdArc<Mutex<Vec<RecordBatch>>> = StdArc::new(Mutex::new(Vec::new()));
    session
        .pipeline("acc")
        .source_memory("raw", vec![amounts(&[100])])
        .view("total", "SELECT SUM(amount) AS s FROM raw", true)
        .sink_memory("total", s3.clone())
        .mode(PipelineMode::Ivm)
        .refresh(RunPolicy::Once)
        .await
        .unwrap();
    assert_eq!(sum_of(&s3), 100, "refresh resets to a fresh state");
}
