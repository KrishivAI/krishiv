//! Global (no grouping key) windowed aggregation, SQL text to emitted columns
//! (task #140).
//!
//! Two failure modes are pinned, because each survived a different revert:
//! the compiler refusing the shape at all (the pre-#140 state), and the spec
//! compiling but the emit path publishing the compiler's synthetic
//! `__krishiv_global` column — a column the user never named, leaking an
//! implementation detail into every downstream schema.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

const SQL: &str = "SELECT MAX(price) AS mx FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
                   GROUP BY window_start, window_end";

/// Bids from THREE distinct auctions in one window; the global max must span
/// all of them. A keyed grouping (any key) would emit three rows.
fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100_i64, 900, 500])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_000_i64, 1_001, 1_002])) as ArrayRef,
        ],
    )
    .expect("batch")
}

fn run() -> Vec<RecordBatch> {
    let plan = compile_streaming_window_sql(SQL).expect("global aggregate must compile");
    assert!(
        plan.spec.key_is_synthetic,
        "premise: the compiler must mark the injected key synthetic"
    );
    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    exec.drain(vec![batch()]).expect("drain");
    exec.flush_all().expect("flush")
}

/// One window, one row, and the max spans every auction.
#[test]
fn global_max_spans_all_keys_in_one_output_row() {
    let out = run();
    let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "a global aggregate emits ONE row per window");
    let batch = out.iter().find(|b| b.num_rows() > 0).expect("output row");
    let mx_idx = batch.schema().index_of("mx").expect("mx column");
    let mx = batch
        .column(mx_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 max");
    assert_eq!(mx.value(0), 900, "MAX must span all three auctions");
}

/// The output schema is exactly window bounds + aggregate — no key column.
///
/// This is the half a compile-acceptance test cannot see: with the emit
/// suppression reverted, the query still compiles and still computes 900,
/// but every output batch leads with a `__krishiv_global` column of zeros
/// that the user never named.
#[test]
fn global_output_carries_no_key_column() {
    let out = run();
    let batch = out.iter().find(|b| b.num_rows() > 0).expect("output row");
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        vec!["window_start_ms", "window_end_ms", "mx"],
        "the synthetic key must not leak into output"
    );
}

/// A GROUP BY column missing from the SELECT list is still refused, and the
/// error names the column. Accepting it silently would be the exact clause-
/// drop shape the compiler fails closed on everywhere else.
#[test]
fn group_by_column_absent_from_select_is_refused_by_name() {
    let err = compile_streaming_window_sql(
        "SELECT MAX(price) AS mx FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
         GROUP BY auction, window_start, window_end",
    )
    .expect_err("must refuse");
    let msg = err.to_string();
    assert!(
        msg.contains("auction"),
        "the refusal must NAME the column: {msg}"
    );
}
