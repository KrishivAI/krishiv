//! IVM-AUD-EMPTY-1: an empty upstream view is a value, not an absence.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

fn sales_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}
fn sales(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        sales_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}
fn spec(name: &str, sql: &str, out: SchemaRef) -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: name.into(),
        body_sql: sql.into(),
        output_schema: out,
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

/// A **global** aggregate is the shape that makes this a wrong answer rather
/// than harmless noise: `SELECT COUNT(*) FROM v` over an empty `v` is one row
/// containing zero, not zero rows. A consumer that sees no rows reads "no
/// data" where the answer is "the count is zero".
///
/// The downstream view is deliberately un-incrementalizable (an aggregate over
/// a computed derived table, refused by IVM-AUD-RESOLVE-1) so it executes
/// through the tick's SQL path, which is where the upstream table is resolved.
#[tokio::test(flavor = "multi_thread")]
async fn a_global_aggregate_over_an_emptied_upstream_view_still_publishes_its_row() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "hot",
        "SELECT region, amount FROM sales WHERE amount > 1000",
        sales_schema(),
    ))
    .unwrap();
    let out = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    flow.register_view(spec(
        "agg",
        "SELECT COUNT(*) AS n FROM (SELECT region, amount * 2 AS amount FROM hot) t",
        out,
    ))
    .unwrap();

    // Fill, so the view is genuinely maintained before it is emptied.
    flow.feed(
        "sales",
        DeltaBatch::from_inserts(sales(&[(1, 5000), (2, 9000)])).unwrap(),
    )
    .unwrap();
    flow.step_datafusion().await.unwrap();
    assert_eq!(flow.snapshot("agg").unwrap().unwrap().num_rows(), 1);

    // Empty the upstream view by retracting everything that passed its filter.
    flow.feed(
        "sales",
        DeltaBatch::from_deletes(sales(&[(1, 5000), (2, 9000)])).unwrap(),
    )
    .unwrap();
    let summary = flow.step_datafusion().await.unwrap();

    assert_eq!(
        flow.snapshot("hot").unwrap().unwrap().num_rows(),
        0,
        "fixture premise: the upstream view is now empty"
    );
    assert!(
        summary.errored_views.is_empty(),
        "an empty upstream view is a normal state, not a planning failure: {:?}",
        summary
            .errored_views
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
    );
    let snap = flow
        .snapshot("agg")
        .unwrap()
        .expect("agg must still publish");
    assert_eq!(
        snap.num_rows(),
        1,
        "COUNT(*) over an empty relation is one row, not zero rows"
    );
    let n = snap
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(n.value(0), 0, "and the value in that row is zero");

    // And it recovers when the upstream refills.
    flow.feed(
        "sales",
        DeltaBatch::from_inserts(sales(&[(1, 7000)])).unwrap(),
    )
    .unwrap();
    flow.step_datafusion().await.unwrap();
    let snap = flow.snapshot("agg").unwrap().unwrap();
    let n = snap
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(n.value(0), 1, "refill is counted again");
}
