//! The key type a SQL user actually gets, end to end.
//!
//! This file exists because of a specific mistake. `key_column_type` was built
//! in TWO places — a constructor in `krishiv-plan` and the SQL compiler in
//! `krishiv-sql` — and the first fix landed with a test that went through the
//! constructor. It passed. The SQL path, which is the only one a user reaches
//! by writing a query, was still hardcoded to `"utf8"` and still emitted every
//! numeric key as a string.
//!
//! A unit test on either crate alone cannot see this: `krishiv-sql` compiles
//! the spec but never runs an operator, and `krishiv-dataflow` runs operators
//! but builds its specs by hand. The defect lives exactly in the seam, so the
//! test has to span it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

const SQL: &str = "SELECT auction, COUNT(*) AS c \
                   FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
                   GROUP BY auction, window_start, window_end";

fn batch_with_key(key: ArrayRef) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", key.data_type().clone(), false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            key,
            Arc::new(Int64Array::from(vec![1_000_i64, 1_100])) as ArrayRef,
        ],
    )
    .expect("batch")
}

/// The SQL compiler must not declare a key type it cannot know.
#[test]
fn the_sql_compiler_leaves_the_key_type_to_be_inferred() {
    let plan = compile_streaming_window_sql(SQL).expect("compiles");
    assert_eq!(
        plan.spec.key_column_type, "auto",
        "the SQL text says nothing about the key's type, so the compiler must \
         not assert one — hardcoding 'utf8' here is what emitted every numeric \
         key as a string"
    );
}

/// A key column keeps its type from SQL text all the way to emitted output.
#[test]
fn a_key_keeps_its_type_from_sql_text_to_emitted_output() {
    let cases: Vec<(ArrayRef, DataType)> = vec![
        (
            Arc::new(Int64Array::from(vec![100_i64, 20])) as ArrayRef,
            DataType::Int64,
        ),
        (
            Arc::new(UInt64Array::from(vec![100_u64, 20])) as ArrayRef,
            DataType::UInt64,
        ),
        (
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            DataType::Utf8,
        ),
    ];

    for (key, expected) in cases {
        let source_type = key.data_type().clone();
        let plan = compile_streaming_window_sql(SQL).expect("compiles");
        let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
        exec.drain(vec![batch_with_key(key)]).expect("drain");
        let out = exec.flush_all().expect("flush");
        let first = out.first().expect("one output batch");

        assert_eq!(
            first.schema().field(0).data_type(),
            &expected,
            "a {source_type} key written in SQL must be emitted as {expected}, \
             not stringified"
        );
    }
}
