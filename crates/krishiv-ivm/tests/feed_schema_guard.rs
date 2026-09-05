//! A delta whose columns differ from the source's relation is refused at
//! `feed`, names the source and both column lists, and leaves the flow usable.
//!
//! Before this guard the delta was accepted into `pending`; the tick then
//! failed inside DataFusion with "Mismatch between schema and batches" — no
//! source name, no columns — and, because a failed tick returns its pending
//! deltas to the queue, every later tick failed the same way. The Python
//! suite's `test_a_view_that_cannot_be_evaluated_raises_instead_of_going_quiet`
//! is where CI first showed it.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn orders(rows: &[(&str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn total(flow: &IncrementalFlow) -> i64 {
    let snap = flow.snapshot("totals").unwrap().unwrap();
    let col = snap
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..col.len()).map(|i| col.value(i)).sum()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delta_with_the_wrong_columns_is_refused_at_feed_and_the_flow_stays_usable() {
    let flow = IncrementalFlow::new();
    let out = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("total", DataType::Int64, true),
    ]));
    flow.register_view(IncrementalViewSpec {
        name: "totals".into(),
        body_sql: "SELECT k, SUM(v) AS total FROM orders GROUP BY k".into(),
        output_schema: out,
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    })
    .unwrap();

    flow.feed(
        "orders",
        DeltaBatch::from_inserts(orders(&[("a", 10)])).unwrap(),
    )
    .unwrap();
    flow.step_datafusion().await.unwrap();
    assert_eq!(total(&flow), 10);

    // Same arity, different column names and types.
    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("kk", DataType::Utf8, false),
            Field::new("vv", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let err = flow
        .feed("orders", DeltaBatch::from_inserts(wrong).unwrap())
        .expect_err("a delta with the wrong columns must be refused at feed");
    let msg = err.to_string();
    assert!(
        msg.contains("source 'orders' schema mismatch")
            && msg.contains("kk: Utf8")
            && msg.contains("k: Utf8"),
        "the error must name the source and both column lists: {msg}"
    );

    // Nothing entered the queue: a tick is a no-op and the snapshot is intact.
    let summary = flow.step_datafusion().await.unwrap();
    assert!(
        summary.errored_views.is_empty(),
        "{:?}",
        summary.errored_views
    );
    assert_eq!(total(&flow), 10);

    // And the source is not poisoned: a correct delta still maintains the view.
    flow.feed(
        "orders",
        DeltaBatch::from_inserts(orders(&[("a", 5)])).unwrap(),
    )
    .unwrap();
    flow.step_datafusion().await.unwrap();
    assert_eq!(total(&flow), 15);
}
