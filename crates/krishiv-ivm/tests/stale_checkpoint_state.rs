//! IVM-AUD-STALE-1: checkpointed view state belongs to the SQL that made it.
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
fn spec(sql: &str) -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: "v".into(),
        body_sql: sql.into(),
        output_schema: Arc::new(Schema::new(vec![
            Field::new("region", DataType::Int64, true),
            Field::new("total", DataType::Int64, true),
        ])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}
const SQL_A: &str = "SELECT region, SUM(amount) AS total FROM sales GROUP BY region";
const SQL_B: &str =
    "SELECT region, SUM(amount) AS total FROM sales WHERE amount > 1000 GROUP BY region";

fn totals(f: &IncrementalFlow) -> Vec<(i64, i64)> {
    match f.snapshot("v").unwrap() {
        None => vec![],
        Some(snap) => {
            let r = snap
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let t = snap
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mut v: Vec<(i64, i64)> = (0..snap.num_rows())
                .map(|i| (r.value(i), t.value(i)))
                .collect();
            v.sort();
            v
        }
    }
}

/// Build a checkpoint under `SQL_A` holding real accumulator state.
async fn checkpoint_under_sql_a() -> Vec<u8> {
    let f = IncrementalFlow::new();
    f.register_view(spec(SQL_A)).unwrap();
    f.feed(
        "sales",
        DeltaBatch::from_inserts(sales(&[(1, 100), (2, 200)])).unwrap(),
    )
    .unwrap();
    f.step_datafusion().await.unwrap();
    assert_eq!(totals(&f), vec![(1, 100), (2, 200)], "fixture premise");
    f.checkpoint_full().unwrap()
}

/// The operational shape: a view's SQL is edited, the job restarts, and the
/// checkpoint written under the OLD definition is restored onto the new one.
///
/// Before this fix the view published `[(1, 5100), (2, 200)]` — 5100 mixes the
/// two queries, and region 2 is a row the new filter excludes outright — while
/// `view_plan_classification` reported `Incremental`.
#[tokio::test(flavor = "multi_thread")]
async fn state_from_a_different_query_is_refused_not_adopted() {
    let blob = checkpoint_under_sql_a().await;

    let f = IncrementalFlow::new();
    f.register_view(spec(SQL_B)).unwrap();
    f.restore_full(&blob).unwrap();
    assert_eq!(
        f.retained_state().unwrap().views_with_pending_plan_state,
        0,
        "the mismatched accumulator must be dropped at restore, not left to be adopted"
    );

    f.feed(
        "sales",
        DeltaBatch::from_inserts(sales(&[(1, 5000)])).unwrap(),
    )
    .unwrap();
    f.step_datafusion().await.unwrap();

    assert_eq!(
        totals(&f),
        vec![(1, 5000)],
        "only rows the new filter admits may count"
    );
}

/// The other half, and the one that makes the fix falsifiable: an UNCHANGED
/// view must still adopt its checkpointed accumulator. Values alone cannot
/// tell adoption from a reseed — both produce the right answer — so this
/// asserts the state is actually pending after restore. Without it, a fix that
/// refused every restore would pass the test above and silently turn every
/// recovery into a full reseed.
#[tokio::test(flavor = "multi_thread")]
async fn state_from_the_same_query_is_still_adopted() {
    let blob = checkpoint_under_sql_a().await;

    let f = IncrementalFlow::new();
    f.register_view(spec(SQL_A)).unwrap();
    f.restore_full(&blob).unwrap();
    assert_eq!(
        f.retained_state().unwrap().views_with_pending_plan_state,
        1,
        "an unchanged view keeps its accumulator for the next tick to adopt"
    );

    f.feed(
        "sales",
        DeltaBatch::from_inserts(sales(&[(1, 5000)])).unwrap(),
    )
    .unwrap();
    f.step_datafusion().await.unwrap();

    assert_eq!(
        totals(&f),
        vec![(1, 5100), (2, 200)],
        "same SQL: the restored totals carry forward"
    );
    assert_eq!(
        f.retained_state().unwrap().views_with_pending_plan_state,
        0,
        "and the pending state was consumed by the tick"
    );
}
