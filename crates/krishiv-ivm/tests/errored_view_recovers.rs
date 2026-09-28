//! H12: a view whose incremental operator fails on one tick must not lose
//! that tick's delta for good. The tick still commits its inputs (other views
//! consumed them), so the failed view has to catch up from its sources.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Array as _, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]))
}

fn rows(values: &[(i64, i64, i64)]) -> RecordBatch {
    let col = |f: fn(&(i64, i64, i64)) -> i64| {
        Arc::new(Int64Array::from(values.iter().map(f).collect::<Vec<_>>()))
    };
    RecordBatch::try_new(schema(), vec![col(|r| r.0), col(|r| r.1), col(|r| r.2)]).unwrap()
}

fn spec() -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: "v".into(),
        body_sql: "SELECT k, SUM(a / b) AS s FROM t GROUP BY k".into(),
        output_schema: Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Int64, true),
        ])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

fn view_rows(flow: &IncrementalFlow) -> Vec<(i64, Option<i64>)> {
    let Some(snap) = flow.snapshot("v").unwrap() else {
        return Vec::new();
    };
    let k = snap
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let s = snap
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut out: Vec<_> = (0..snap.num_rows())
        .map(|i| (k.value(i), (!s.is_null(i)).then(|| s.value(i))))
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a_view_that_fails_one_tick_recovers_the_rows_of_that_tick() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec()).unwrap();
    flow.feed("t", DeltaBatch::from_inserts(rows(&[(2, 8, 2)])).unwrap())
        .unwrap();
    flow.step_datafusion().await.unwrap();
    let (inc, why) = flow
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "the test needs an incremental plan: {why}");
    assert_eq!(view_rows(&flow), vec![(2, Some(4))]);

    // One good row and one that divides by zero, in the same delta.
    flow.feed(
        "t",
        DeltaBatch::from_inserts(rows(&[(1, 10, 2), (1, 5, 0)])).unwrap(),
    )
    .unwrap();
    let summary = flow.step_datafusion().await.unwrap();
    assert!(
        !summary.errored_views.is_empty(),
        "the divide-by-zero row must surface as an errored view"
    );

    // The bad row is retracted. The view must now equal a full recompute
    // over what the source holds: (1, 10, 2) and (2, 8, 2).
    flow.feed("t", DeltaBatch::from_deletes(rows(&[(1, 5, 0)])).unwrap())
        .unwrap();
    let summary = flow.step_datafusion().await.unwrap();
    assert!(
        summary.errored_views.is_empty(),
        "{:?}",
        summary.errored_views
    );
    assert_eq!(view_rows(&flow), vec![(1, Some(5)), (2, Some(4))]);

    // And it stays incremental and correct afterwards.
    flow.feed("t", DeltaBatch::from_inserts(rows(&[(1, 6, 3)])).unwrap())
        .unwrap();
    flow.step_datafusion().await.unwrap();
    assert_eq!(view_rows(&flow), vec![(1, Some(7)), (2, Some(4))]);
}
