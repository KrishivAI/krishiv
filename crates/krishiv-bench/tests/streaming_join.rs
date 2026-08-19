//! A stream-to-stream interval join, from SQL text to joined rows.
//!
//! NEXMark Q3's shape: bids joined to their auctions within an event-time
//! band. Spans krishiv-sql (compiles the ON clause), krishiv-plan (carries the
//! spec) and krishiv-dataflow (runs the join) — the seam where the previous
//! key-type defect hid.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::{WatermarkWindowJoinOperator, WatermarkWindowJoinSpec};
use krishiv_sql::streaming_join_plan::compile_streaming_join_sql;

const Q3: &str = "SELECT b.auction, a.category \
                  FROM bid b JOIN auction a \
                    ON b.auction = a.id \
                   AND b.dateTime BETWEEN a.dateTime - 5000 AND a.dateTime + 5000";

fn bids(rows: Vec<(i64, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("bid batch")
}

fn auctions(rows: Vec<(i64, &str, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("auction batch")
}

fn operator() -> WatermarkWindowJoinOperator {
    let plan = compile_streaming_join_sql(Q3).expect("Q3 compiles");
    WatermarkWindowJoinOperator::new(WatermarkWindowJoinSpec::from(&plan.spec))
}

fn joined_rows(out: &[RecordBatch]) -> usize {
    out.iter().map(RecordBatch::num_rows).sum()
}

/// A bid matches its auction inside the band, and only its own auction.
#[test]
fn a_bid_joins_the_auction_it_names_within_the_band() {
    let mut op = operator();

    // Two auctions buffered first.
    let pre = op.process_right(&auctions(vec![
        (7, "electronics", 1_000),
        (9, "books", 1_000),
    ]));
    assert_eq!(joined_rows(&pre), 0, "no bids yet, so nothing can match");

    // One bid on auction 7, inside the band.
    let out = op.process_left(&bids(vec![(7, 2_000)]));
    assert_eq!(
        joined_rows(&out),
        1,
        "the bid must match auction 7 only, not both auctions"
    );
}

/// A bid outside the band does not match, even with the right key.
///
/// This is the assertion the whole feature turns on: without the time bound
/// the join is unbounded in both state and results, so a test that only
/// checked key matching would pass against a join that ignored time entirely.
#[test]
fn a_bid_outside_the_band_does_not_match_its_auction() {
    let mut op = operator();
    op.process_right(&auctions(vec![(7, "electronics", 1_000)]));

    // 5000 ms band: 1_000 + 5_000 = 6_000 is the edge; 20_000 is far outside.
    let outside = op.process_left(&bids(vec![(7, 20_000)]));
    assert_eq!(
        joined_rows(&outside),
        0,
        "same key but outside the event-time band must not join"
    );

    // Same key, inside the band, does join — so the miss above is the band and
    // not a broken key comparison.
    let inside = op.process_left(&bids(vec![(7, 3_000)]));
    assert_eq!(
        joined_rows(&inside),
        1,
        "the same key inside the band must join, proving the band caused the miss"
    );
}

/// A bid whose auction never arrives produces nothing.
#[test]
fn a_bid_with_no_matching_auction_produces_nothing() {
    let mut op = operator();
    op.process_right(&auctions(vec![(7, "electronics", 1_000)]));
    let out = op.process_left(&bids(vec![(999, 1_500)]));
    assert_eq!(joined_rows(&out), 0, "no auction 999 exists");
}

/// The join emits left columns followed by right columns.
#[test]
fn the_joined_row_carries_columns_from_both_sides() {
    let mut op = operator();
    op.process_right(&auctions(vec![(7, "electronics", 1_000)]));
    let out = op.process_left(&bids(vec![(7, 1_500)]));

    let batch = out.first().expect("one joined batch");
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert!(
        names.iter().any(|n| n == "category"),
        "the right stream's columns must survive the join; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "auction"),
        "the left stream's columns must survive the join; got {names:?}"
    );
}

/// Advancing the watermark past the window evicts buffered state.
#[test]
fn advancing_the_watermark_evicts_state_beyond_the_window() {
    let mut op = operator();
    op.process_right(&auctions(vec![(7, "electronics", 1_000)]));

    // Push the watermark far past the auction's window.
    op.advance_watermark(1_000_000);

    let out = op.process_left(&bids(vec![(7, 1_500)]));
    assert_eq!(
        joined_rows(&out),
        0,
        "state older than the watermark's window must be evicted, or the join \
         grows without bound"
    );
}
