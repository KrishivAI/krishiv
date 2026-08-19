//! Multi-column `GROUP BY` from SQL text to emitted columns.
//!
//! The defect this replaces collapsed `GROUP BY a, b` to `a` and aggregated
//! across `b` — a larger number, no error. So every test here asserts the
//! GROUPING, on a fixture where grouping by one column and by two give
//! different answers.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

const SQL: &str = "SELECT auction, channel, COUNT(*) AS c \
                   FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 60000) \
                   GROUP BY auction, channel, window_start, window_end";

/// Two auctions x two channels, with a deliberately uneven distribution.
fn batch() -> RecordBatch {
    let auctions = vec![1_i64, 1, 1, 2];
    let channels = vec!["apple", "apple", "google", "apple"];
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auctions)) as ArrayRef,
            Arc::new(StringArray::from(channels)) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_000_i64, 1_001, 1_002, 1_003])) as ArrayRef,
        ],
    )
    .expect("batch")
}

fn run(sql: &str) -> Vec<RecordBatch> {
    let plan = compile_streaming_window_sql(sql).expect("compiles");
    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    exec.drain(vec![batch()]).expect("drain");
    exec.flush_all().expect("flush")
}

/// Grouping by two columns produces three groups, not two.
///
/// By `auction` alone the answer is 2 groups (counts 3 and 1). By
/// `(auction, channel)` it is 3 groups: (1,apple)=2, (1,google)=1, (2,apple)=1.
/// Collapsing to the first column — the original defect — yields 2 rows and a
/// count of 3 where the query asked for 2.
#[test]
fn grouping_by_two_columns_splits_groups_the_single_key_would_merge() {
    let out = run(SQL);
    let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        rows, 3,
        "(auction, channel) has three distinct combinations; collapsing to \
         auction alone would give two"
    );

    let mut seen: Vec<(i64, String, i64)> = Vec::new();
    for b in &out {
        let a = b
            .column(b.schema().index_of("auction").expect("auction column"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("auction stays Int64, not the encoded key")
            .clone();
        let ch = b
            .column(b.schema().index_of("channel").expect("channel column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("channel stays Utf8")
            .clone();
        let c = b
            .column(b.schema().index_of("c").expect("count column"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .clone();
        for i in 0..b.num_rows() {
            seen.push((a.value(i), ch.value(i).to_owned(), c.value(i)));
        }
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![
            (1, "apple".to_owned(), 2),
            (1, "google".to_owned(), 1),
            (2, "apple".to_owned(), 1),
        ],
        "each (auction, channel) pair must carry its own count"
    );
}

/// The synthetic composite key must not appear in the output.
#[test]
fn the_encoded_key_is_not_published() {
    let out = run(SQL);
    let batch = out.first().expect("output");
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert!(
        !names.iter().any(|n| n.starts_with("__krishiv")),
        "internal columns must not reach consumers; got {names:?}"
    );
    assert!(names.contains(&"auction".to_owned()));
    assert!(names.contains(&"channel".to_owned()));
}

/// A separator inside a value cannot merge two groups.
///
/// The whole reason the encoding is length-prefixed rather than delimiter
/// joined: with a `:` delimiter, `("a:b", "c")` and `("a", "b:c")` encode
/// identically and two distinct groups silently become one.
#[test]
fn values_containing_the_separator_stay_distinct() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    let b = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a:b", "a"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["c", "b:c"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_000_i64, 1_001])) as ArrayRef,
        ],
    )
    .expect("batch");

    let plan = compile_streaming_window_sql(SQL).expect("compiles");
    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    exec.drain(vec![b]).expect("drain");
    let out = exec.flush_all().expect("flush");

    let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        rows, 2,
        "(\"a:b\",\"c\") and (\"a\",\"b:c\") are different groups; a delimiter-joined \
         key would merge them into one"
    );
}
