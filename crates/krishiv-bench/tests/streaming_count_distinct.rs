//! `COUNT(DISTINCT …)` from SQL text to computed value.
//!
//! The defect this replaces returned a plain row count. Every test here
//! therefore asserts the NUMBER, with a fixture where distinct-count and
//! row-count differ — a test that only checked "a count came out" passed
//! against the bug for as long as it existed.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

const SQL: &str = "SELECT auction, COUNT(DISTINCT bidder) AS distinct_bidders \
                   FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
                   GROUP BY auction, window_start, window_end";

fn bids(bidders: Vec<i64>) -> RecordBatch {
    bids_from(bidders, 1_000)
}

/// Timestamps start at `t0`, all inside the single 10s window.
///
/// A later batch MUST carry later timestamps. With `watermark_lag_ms = 0` the
/// watermark advances to the highest event time seen, so a second batch reusing
/// the first batch's timestamps is late and gets dropped — which silently
/// turned a 4-row fixture into a 3-row one and made the cross-batch test pass
/// against the very bug it was written to catch.
fn bids_from(bidders: Vec<i64>, t0: i64) -> RecordBatch {
    let n = bidders.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![7_i64; n])) as ArrayRef,
            Arc::new(Int64Array::from(bidders)) as ArrayRef,
            Arc::new(Int64Array::from(
                (0..n as i64).map(|i| t0 + i).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("batch")
}

fn distinct_count(batches: Vec<RecordBatch>) -> i64 {
    let plan = compile_streaming_window_sql(SQL).expect("compiles");
    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    exec.drain(batches).expect("drain");
    let out = exec.flush_all().expect("flush");
    let batch = out.first().expect("one output batch");
    let idx = batch
        .schema()
        .index_of("distinct_bidders")
        .expect("output column");
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 count")
        .value(0)
}

/// Three bids from one bidder are ONE distinct bidder.
///
/// This is the exact case that returned 3 before the fix.
#[test]
fn repeated_values_count_once() {
    assert_eq!(
        distinct_count(vec![bids(vec![7, 7, 7])]),
        1,
        "three bids from bidder 7 is one distinct bidder, not three rows"
    );
}

/// Distinctness holds across batch boundaries, not just within one batch.
///
/// A per-batch implementation would return 4 here (2 + 2) instead of 3, and a
/// plain row count returns 4 as well — so this fails against both defects. The
/// second batch's timestamps are strictly later than the first's so that no row
/// is late-dropped; see `bids_from`.
#[test]
fn distinctness_spans_batches() {
    assert_eq!(
        distinct_count(vec![
            bids_from(vec![1, 2], 1_000),
            bids_from(vec![2, 3], 2_000)
        ]),
        3,
        "bidders {{1,2}} then {{2,3}} is three distinct bidders across the window"
    );
}

/// NULLs are not counted, matching SQL.
#[test]
fn nulls_are_not_distinct_values() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, true),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![7_i64; 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(1)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_000_i64, 1_001, 1_002])) as ArrayRef,
        ],
    )
    .expect("batch");

    assert_eq!(
        distinct_count(vec![batch]),
        1,
        "SQL COUNT(DISTINCT x) ignores NULL; only bidder 1 is a distinct value"
    );
}

/// A distinct count and a plain count of the same column disagree, and both
/// are computed correctly in ONE query.
///
/// Guards the arm that dispatches per aggregate: a version that handled the
/// distinct aggregate by changing behaviour for every COUNT would fail here.
#[test]
fn a_distinct_count_and_a_plain_count_coexist_in_one_query() {
    let sql = "SELECT auction, COUNT(DISTINCT bidder) AS d, COUNT(*) AS c \
               FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
               GROUP BY auction, window_start, window_end";
    let plan = compile_streaming_window_sql(sql).expect("compiles");
    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    exec.drain(vec![bids(vec![7, 7, 7])]).expect("drain");
    let out = exec.flush_all().expect("flush");
    let batch = out.first().expect("one output batch");

    let read = |name: &str| -> i64 {
        let idx = batch.schema().index_of(name).expect("column");
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0)
    };

    assert_eq!(read("d"), 1, "one distinct bidder");
    assert_eq!(read("c"), 3, "three rows");
}
