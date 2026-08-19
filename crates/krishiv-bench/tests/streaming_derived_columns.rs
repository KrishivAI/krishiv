//! An aggregate over an expression, from SQL text to computed value.
//!
//! Spans krishiv-sql (lowers the expression into a derived column) and
//! krishiv-dataflow (materialises it before grouping). Neither crate can test
//! this alone, and the previous key-type bug in this same seam is why these
//! tests live here rather than in either crate's unit tests.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

/// NEXMark Q1: bid prices converted from dollars to euros.
const Q1: &str = "SELECT auction, SUM(price * 908 / 1000) AS total_euro \
                  FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
                  GROUP BY auction, window_start, window_end";

fn bids(prices: Vec<i64>) -> RecordBatch {
    let n = prices.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![7_i64; n])) as ArrayRef,
            Arc::new(Int64Array::from(prices)) as ArrayRef,
            Arc::new(Int64Array::from(
                (0..n as i64).map(|i| 1_000 + i).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("batch")
}

/// The aggregate must sum the CONVERTED prices, not the raw ones.
///
/// This is the assertion that matters. A version that only checked the query
/// compiles, or that some column came out, would pass just as happily if the
/// expression were ignored and `SUM(price)` computed instead — which is
/// precisely what the old `_ => {}` arm did before it left input_column empty.
#[test]
fn an_expression_aggregate_sums_converted_values_not_raw_ones() {
    // 1000*908/1000 = 908 ; 2000*908/1000 = 1816 ; 3000*908/1000 = 2724
    let prices = vec![1_000_i64, 2_000, 3_000];
    let converted_total: i64 = 908 + 1_816 + 2_724; // 5448
    let raw_total: i64 = prices.iter().sum(); // 6000 — what the bug produced
    assert_ne!(
        converted_total, raw_total,
        "premise: the conversion must change the answer, or this test proves nothing"
    );

    let plan = compile_streaming_window_sql(Q1).expect("Q1 compiles");
    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    exec.drain(vec![bids(prices)]).expect("drain");
    let out = exec.flush_all().expect("flush");

    let batch = out.first().expect("one output batch");
    let total_idx = batch
        .schema()
        .index_of("total_euro")
        .expect("aggregate output column");
    let totals = batch.column(total_idx);
    let got = totals
        .as_any()
        .downcast_ref::<Int64Array>()
        .map(|a| a.value(0))
        .or_else(|| {
            totals
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .map(|a| a.value(0) as i64)
        })
        .expect("numeric aggregate output");

    assert_eq!(
        got, converted_total,
        "SUM(price * 908 / 1000) must sum converted prices ({converted_total}), \
         not raw prices ({raw_total})"
    );
}

/// The derived column must not leak into the window's output schema.
#[test]
fn the_derived_column_does_not_appear_in_the_output() {
    let plan = compile_streaming_window_sql(Q1).expect("Q1 compiles");
    let derived_name = plan.spec.derived_columns[0].name.clone();

    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    exec.drain(vec![bids(vec![1_000, 2_000])]).expect("drain");
    let out = exec.flush_all().expect("flush");
    let batch = out.first().expect("one output batch");

    assert!(
        batch.schema().index_of(&derived_name).is_err(),
        "the internal column '{derived_name}' must not be visible to consumers; \
         output schema was {:?}",
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    );
}
