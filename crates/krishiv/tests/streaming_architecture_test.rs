//! Streaming architecture smoke test — exercises the public streaming API.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use krishiv::{MultiSourceWatermarkSpec, Session, StreamBatch, WatermarkSpec};
use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind};

// ── helpers ──────────────────────────────────────────────────────────────────

fn events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_type", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
    ]))
}

fn events_batch(event_types: &[&str], timestamps: &[i64], keys: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        events_schema(),
        vec![
            Arc::new(StringArray::from(event_types.to_vec())) as _,
            Arc::new(Int64Array::from(timestamps.to_vec())) as _,
            Arc::new(StringArray::from(keys.to_vec())) as _,
        ],
    )
    .expect("valid events batch")
}

fn events_with_source_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_type", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("source_id", DataType::Utf8, false),
    ]))
}

fn events_batch_with_source(
    event_types: &[&str],
    timestamps: &[i64],
    keys: &[&str],
    sources: &[&str],
) -> RecordBatch {
    RecordBatch::try_new(
        events_with_source_schema(),
        vec![
            Arc::new(StringArray::from(event_types.to_vec())) as _,
            Arc::new(Int64Array::from(timestamps.to_vec())) as _,
            Arc::new(StringArray::from(keys.to_vec())) as _,
            Arc::new(StringArray::from(sources.to_vec())) as _,
        ],
    )
    .expect("valid events batch with source")
}

fn sum_last_int64_column(batches: &[StreamBatch]) -> i64 {
    let mut total: i64 = 0;
    for sb in batches {
        let batch = sb.batch();
        if batch.num_columns() == 0 {
            continue;
        }
        let last_col_idx = batch.num_columns() - 1;
        if let Some(arr) = batch
            .column(last_col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
        {
            for i in 0..arr.len() {
                total += arr.value(i);
            }
        }
    }
    total
}

fn concat_stream_batches(batches: &[StreamBatch]) -> RecordBatch {
    let rbs: Vec<RecordBatch> = batches.iter().map(|b| b.batch().clone()).collect();
    arrow::compute::concat_batches(&rbs[0].schema(), &rbs).expect("concat")
}

fn collect_int64_col(result: &[StreamBatch], col_idx: usize) -> Vec<i64> {
    let tbl = concat_stream_batches(result);
    tbl.column(col_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn sql_result_int64_col(result: &krishiv::QueryResult, col_idx: usize) -> Vec<i64> {
    let batches = result.batches();
    let tbl = arrow::compute::concat_batches(&batches[0].schema(), batches).expect("concat");
    tbl.column(col_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

// ── 1. Batch SQL ──────────────────────────────────────────────────────────────

#[test]
fn test_batch_sql() {
    let session = Session::builder().build().expect("session build");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .expect("batch");
    session
        .register_record_batches("t", vec![batch])
        .expect("register");
    let result = session
        .sql("SELECT x, y * 2 AS double_y FROM t ORDER BY x")
        .expect("sql")
        .collect()
        .expect("collect");
    assert_eq!(result.row_count(), 3);
    let double_y = sql_result_int64_col(&result, 1);
    assert_eq!(double_y, vec![20, 40, 60]);
    println!("[PASS] batch_sql");
}

// ── 2. Tumbling window ───────────────────────────────────────────────────────

#[test]
fn test_tumbling_window() {
    let session = Session::builder().build().expect("session build");
    let batch = events_batch(
        &["click", "click", "view", "click"],
        &[1_000, 4_000, 7_000, 12_000],
        &["a", "a", "b", "a"],
    );
    let stream = session
        .memory_stream("events", vec![StreamBatch::new(0, batch)])
        .expect("memory_stream");
    let windowed = stream
        .key_by("key")
        .with_event_time("timestamp")
        .watermark(WatermarkSpec::fixed_lag_ms(0))
        .tumbling_window(10_000);
    let result = windowed.collect().expect("tumbling collect");
    assert!(!result.is_empty(), "tumbling window should produce output");
    let total_count = sum_last_int64_column(&result);
    assert_eq!(total_count, 4, "all 4 events should be counted");
    println!("[PASS] tumbling_window: {total_count} events");
}

// ── 3. Sliding window ─────────────────────────────────────────────────────────

#[test]
fn test_sliding_window() {
    let session = Session::builder().build().expect("session build");
    let batch = events_batch(
        &["a", "b", "a", "b", "a"],
        &[1_000, 2_000, 3_000, 8_000, 15_000],
        &["x", "x", "x", "x", "x"],
    );
    let stream = session
        .memory_stream("events", vec![StreamBatch::new(0, batch)])
        .expect("memory_stream");
    let sliding = stream
        .key_by("key")
        .with_event_time("timestamp")
        .watermark(WatermarkSpec::fixed_lag_ms(0))
        .sliding_window(10_000, 5_000);
    let result = sliding.collect().expect("sliding collect");
    assert!(!result.is_empty(), "sliding window should produce output");
    println!("[PASS] sliding_window: {} batches", result.len());
}

// ── 4. Session window ─────────────────────────────────────────────────────────

#[test]
fn test_session_window() {
    let session = Session::builder().build().expect("session build");
    let batch = events_batch(
        &["evt", "evt", "evt", "evt"],
        &[1_000, 2_000, 20_000, 21_000],
        &["dev-1", "dev-1", "dev-1", "dev-1"],
    );
    let stream = session
        .memory_stream("events", vec![StreamBatch::new(0, batch)])
        .expect("memory_stream");
    let windowed = stream
        .key_by("key")
        .with_event_time("timestamp")
        .session_window(5_000);
    let result = windowed.collect().expect("session collect");
    assert!(!result.is_empty(), "session window should produce output");
    let total_count = sum_last_int64_column(&result);
    assert_eq!(total_count, 4, "all 4 events in sessions counted");
    println!("[PASS] session_window: {total_count} events");
}

// ── 5. Multi-source watermark ─────────────────────────────────────────────────

#[test]
fn test_multi_source_watermark() {
    let session = Session::builder().build().expect("session build");
    let batch_a = events_batch_with_source(
        &["click", "click"],
        &[1_000, 2_000],
        &["u1", "u1"],
        &["src-a", "src-a"],
    );
    let batch_b = events_batch_with_source(
        &["click", "click"],
        &[1_500, 2_500],
        &["u1", "u1"],
        &["src-b", "src-b"],
    );
    let stream = session
        .memory_stream(
            "events",
            vec![StreamBatch::new(0, batch_a), StreamBatch::new(1, batch_b)],
        )
        .expect("memory_stream");
    let ms = MultiSourceWatermarkSpec::new()
        .with_source_id_column("source_id")
        .source("src-a", WatermarkSpec::fixed_lag_ms(0))
        .source("src-b", WatermarkSpec::fixed_lag_ms(0));
    let windowed = stream
        .key_by("key")
        .with_event_time("timestamp")
        .with_multi_source_watermark(ms)
        .tumbling_window(5_000);
    let result = windowed.collect().expect("multi-source collect");
    assert!(!result.is_empty());
    println!("[PASS] multi_source_watermark: {} batches", result.len());
}

// ── 6. Continuous job lifecycle ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_continuous_job_lifecycle() {
    let session = Session::builder().build().expect("session build");
    let spec = LocalWindowExecutionSpec {
        key_column_type: String::from("utf8"),
        key_column: "key".into(),
        event_time_column: "timestamp".into(),
        watermark_lag_ms: 0,
        window_kind: LocalWindowKind::Tumbling,
        window_size_ms: 10_000,
        agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
        state_ttl_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        allowed_lateness_ms: None,
        window_timezone: None,
    };
    let job_name = session
        .submit_stream_job("continuous-test", spec)
        .expect("submit job");
    assert_eq!(job_name, "continuous-test");

    let batch = events_batch(&["evt", "evt"], &[1_000, 2_000], &["k1", "k1"]);
    session
        .push_stream_job_input("continuous-test", vec![batch])
        .expect("push 1");
    let _ = session
        .poll_stream_job("continuous-test")
        .await
        .expect("poll 1");

    let batch2 = events_batch(&["evt"], &[3_000], &["k1"]);
    session
        .push_stream_job_input("continuous-test", vec![batch2])
        .expect("push 2");
    let _ = session
        .poll_stream_job("continuous-test")
        .await
        .expect("poll 2");

    println!("[PASS] continuous_job_lifecycle");
}

// ── 7. State across drain cycles ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_state_across_drain_cycles() {
    let session = Session::builder().build().expect("session build");
    let spec = LocalWindowExecutionSpec {
        key_column_type: String::from("utf8"),
        key_column: "key".into(),
        event_time_column: "timestamp".into(),
        watermark_lag_ms: 0,
        window_kind: LocalWindowKind::Tumbling,
        window_size_ms: 10_000,
        agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
        state_ttl_ms: Some(60_000),
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        allowed_lateness_ms: None,
        window_timezone: None,
    };
    session
        .submit_stream_job("state-test", spec)
        .expect("submit job");

    let batch1 = events_batch(&["click", "click"], &[1_000, 2_000], &["user-1", "user-1"]);
    session
        .push_stream_job_input("state-test", vec![batch1])
        .expect("push 1");
    let _ = session
        .poll_stream_job("state-test")
        .await
        .expect("drain 1");

    let batch2 = events_batch(&["click"], &[3_000], &["user-1"]);
    session
        .push_stream_job_input("state-test", vec![batch2])
        .expect("push 2");
    let _ = session
        .poll_stream_job("state-test")
        .await
        .expect("drain 2");

    println!("[PASS] state_across_drain_cycles");
}

// ── 8. SQL aggregation ───────────────────────────────────────────────────────

#[test]
fn test_sql_aggregation() {
    let session = Session::builder().build().expect("session build");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b"])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )
    .expect("batch");
    session
        .register_record_batches("src", vec![batch])
        .expect("register");

    let result = session
        .sql("SELECT category, SUM(amount) AS total FROM src GROUP BY category ORDER BY category")
        .expect("sql")
        .collect()
        .expect("collect");
    assert_eq!(result.row_count(), 2);
    let totals = sql_result_int64_col(&result, 1);
    assert!(totals.contains(&60), "expected total=60 for 'a'");
    assert!(totals.contains(&90), "expected total=90 for 'b'");
    println!("[PASS] sql_aggregation: totals={totals:?}");
}

// ── 9. SQL CTE ───────────────────────────────────────────────────────────────

#[test]
fn test_sql_cte() {
    let session = Session::builder().build().expect("session build");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "alice", "bob", "carol", "dave", "eve",
            ])),
            Arc::new(Int64Array::from(vec![90, 80, 70, 60, 50])),
        ],
    )
    .expect("batch");
    session
        .register_record_batches("src", vec![batch])
        .expect("register");

    let result = session
        .sql(
            "WITH high_scores AS (SELECT * FROM src WHERE score >= 70) \
             SELECT name, score FROM high_scores ORDER BY score DESC",
        )
        .expect("sql")
        .collect()
        .expect("collect");
    assert_eq!(result.row_count(), 3);
    println!("[PASS] sql_cte: {} rows", result.row_count());
}

// ── 10. SQL expressions ──────────────────────────────────────────────────────

#[test]
fn test_sql_expressions() {
    let session = Session::builder().build().expect("session build");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .expect("batch");
    session
        .register_record_batches("src", vec![batch])
        .expect("register");

    let result = session
        .sql("SELECT x, x * x AS x_sq FROM src ORDER BY x")
        .expect("sql")
        .collect()
        .expect("collect");
    let sq = sql_result_int64_col(&result, 1);
    assert_eq!(sq, vec![1, 4, 9, 16, 25]);
    println!("[PASS] sql_expressions: x_squared={sq:?}");
}

// ── 11. SQL multiple aggregations ────────────────────────────────────────────

#[test]
fn test_sql_multiple_aggs() {
    let session = Session::builder().build().expect("session build");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("revenue", DataType::Int64, false),
            Field::new("cost", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["east", "east", "west", "west"])),
            Arc::new(Int64Array::from(vec![100, 200, 150, 250])),
            Arc::new(Int64Array::from(vec![50, 80, 70, 120])),
        ],
    )
    .expect("batch");
    session
        .register_record_batches("src", vec![batch])
        .expect("register");

    let result = session
        .sql(
            "SELECT region, SUM(revenue) - SUM(cost) AS profit \
             FROM src GROUP BY region ORDER BY region",
        )
        .expect("sql")
        .collect()
        .expect("collect");
    let profits = sql_result_int64_col(&result, 1);
    assert!(profits.contains(&170), "expected profit=170 for east");
    assert!(profits.contains(&210), "expected profit=210 for west");
    println!("[PASS] sql_multiple_aggs: profits={profits:?}");
}

// ── 12. SQL filter + ORDER BY + LIMIT ────────────────────────────────────────

#[test]
fn test_sql_filter_order_limit() {
    let session = Session::builder().build().expect("session build");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("rank", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![5, 3, 1, 4, 2])),
            Arc::new(StringArray::from(vec!["e", "c", "a", "d", "b"])),
        ],
    )
    .expect("batch");
    session
        .register_record_batches("src", vec![batch])
        .expect("register");

    let result = session
        .sql("SELECT name, rank FROM src WHERE rank <= 3 ORDER BY rank ASC LIMIT 3")
        .expect("sql")
        .collect()
        .expect("collect");
    let ranks = sql_result_int64_col(&result, 1);
    assert_eq!(ranks, vec![1, 2, 3]);
    println!("[PASS] sql_filter_order_limit: ranks={ranks:?}");
}
