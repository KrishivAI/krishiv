//! Per-key top-N and keep-last dedup, SQL text to emitted rows (task #142).
//!
//! The SQL shape is `ORDER BY <col> [DESC] LIMIT <n>` on a windowed query:
//! within each window, each grouping key keeps its n best rows. NEXMark Q19
//! is `LIMIT 10` by price per auction; Q18 is `LIMIT 1` by event time per
//! (bidder, auction) — dedup as the degenerate top-N.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

fn bid_batch(rows: &[(i64, i64, i64, i64)]) -> RecordBatch {
    // (auction, bidder, price, dateTime)
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
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
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("batch")
}

fn run(sql: &str, rows: &[(i64, i64, i64, i64)]) -> Vec<RecordBatch> {
    let plan = compile_streaming_window_sql(sql).expect("top-N query must compile");
    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    exec.drain(vec![bid_batch(rows)]).expect("drain");
    exec.flush_all().expect("flush")
}

fn column_i64(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let idx = batch.schema().index_of(name).expect(name);
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    (0..batch.num_rows()).map(|i| arr.value(i)).collect()
}

/// Q19 shape: top-2 by price per auction. Auction 1 has three bids; only its
/// two best survive, best first. Auction 2's single bid survives untouched.
#[test]
fn top_n_keeps_the_n_best_rows_per_key() {
    let out = run(
        "SELECT auction, bidder, price FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
         GROUP BY auction, window_start, window_end ORDER BY price DESC LIMIT 2",
        &[
            (1, 10, 100, 1_000),
            (1, 11, 900, 1_001),
            (1, 12, 500, 1_002),
            (2, 13, 50, 1_003),
        ],
    );
    let mut per_auction: Vec<(i64, Vec<i64>)> = out
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| (column_i64(b, "auction")[0], column_i64(b, "price")))
        .collect();
    per_auction.sort_by_key(|(a, _)| *a);
    assert_eq!(
        per_auction,
        vec![(1, vec![900, 500]), (2, vec![50])],
        "auction 1 keeps its two best prices, best first; the 100 bid is cut"
    );
}

/// Q18 shape: LIMIT 1 by event time per (bidder, auction) — keep-last dedup.
/// The same bidder bids twice on the same auction; only the LATER bid's
/// price survives.
#[test]
fn limit_one_by_time_is_keep_last_dedup() {
    let out = run(
        "SELECT bidder, auction, price FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
         GROUP BY bidder, auction, window_start, window_end ORDER BY \"dateTime\" DESC LIMIT 1",
        &[
            (42, 7, 111, 1_000),
            (42, 7, 222, 1_500),
            (42, 8, 333, 1_200),
        ],
    );
    let mut rows: Vec<(i64, i64)> = out
        .iter()
        .filter(|b| b.num_rows() > 0)
        .flat_map(|b| {
            let bidders = column_i64(b, "bidder");
            let prices = column_i64(b, "price");
            bidders.into_iter().zip(prices).collect::<Vec<_>>()
        })
        .collect();
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![(7, 222), (8, 333)],
        "bidder 7's earlier bid (111) must be deduplicated away"
    );
}

/// Refusals stay refusals, and each names its reason.
#[test]
fn partial_top_n_shapes_are_refused_by_name() {
    let base = "SELECT auction, price FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
                GROUP BY auction, window_start, window_end";
    for (sql, needle) in [
        (format!("{base} ORDER BY price DESC"), "without LIMIT"),
        (format!("{base} LIMIT 10"), "without ORDER BY"),
        (
            "SELECT auction, MAX(price) AS mx FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
             GROUP BY auction, window_start, window_end ORDER BY mx DESC LIMIT 2"
                .to_string(),
            "cannot be combined",
        ),
    ] {
        let err = compile_streaming_window_sql(&sql).expect_err("must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains(needle),
            "{sql}\n  error {msg:?} lacks {needle:?}"
        );
    }
}

/// A bare selected column with no top-N and no place in the GROUP BY was
/// SILENTLY DROPPED from output for the lifetime of this compiler. It is now
/// refused, naming the column.
#[test]
fn ungrouped_bare_column_is_refused_not_dropped() {
    let err = compile_streaming_window_sql(
        "SELECT auction, price, COUNT(*) AS c FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
         GROUP BY auction, window_start, window_end",
    )
    .expect_err("must refuse");
    let msg = err.to_string();
    assert!(
        msg.contains("price") && msg.contains("silently dropped"),
        "the refusal must name the dropped column: {msg}"
    );
}
