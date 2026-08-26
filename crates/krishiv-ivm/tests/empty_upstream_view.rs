//! IVM-AUD-EMPTY-1: an empty upstream view is a value, not an absence.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Array as _, Int64Array, RecordBatch};
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

/// IVM-AUD-EMPTY-2: the fully **incremental** chain form. EMPTY-1 fixed the
/// SQL path's view of an empty upstream; the O(Δ) path had the same defect one
/// layer down — a dirty view whose input delta was empty was skipped outright,
/// so its snapshot stayed `None` (an absence where the value is "zero rows"),
/// no delta reached the views above it, and a global aggregate two hops down
/// was never applied — the one row it owes over empty input (GLOBAL-1) never
/// established. `SELECT SUM(amount) FROM (filter admitting nothing)` answered
/// nothing; SQL says it answers one row holding NULL.
///
/// Every hop here must classify `Incremental` — under a revert both views
/// degrade to DiffBased and the values come out right for the wrong reason
/// (the fallback mask), so the classification assertions are what keep this
/// test honest.
#[tokio::test(flavor = "multi_thread")]
async fn an_incremental_chain_over_an_empty_filter_still_answers() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "hot2",
        "SELECT region, amount FROM sales WHERE amount > 1000",
        sales_schema(),
    ))
    .unwrap();
    let out = Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        true,
    )]));
    flow.register_view(spec("tot", "SELECT SUM(amount) AS total FROM hot2", out))
        .unwrap();

    // Nothing clears the filter.
    flow.feed(
        "sales",
        DeltaBatch::from_inserts(sales(&[(1, 5), (2, 40)])).unwrap(),
    )
    .unwrap();
    let s = flow.step_datafusion().await.unwrap();
    assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);

    // Both hops maintain incrementally — the whole point of the chain.
    for v in ["hot2", "tot"] {
        let (inc, why) = flow
            .view_plan_classification(v)
            .unwrap()
            .expect("registered");
        assert!(inc, "{v} fell back to DiffBased: {why}");
    }

    // The filter view's value is the empty relation — zero rows, not None.
    let hot = flow
        .snapshot("hot2")
        .unwrap()
        .expect("an empty incremental view is a value, not an absence");
    assert_eq!(hot.num_rows(), 0);

    // The global SUM over it is one row holding NULL (SQL: SUM over zero
    // non-null inputs is NULL — not 0, which is what a saturating default
    // state would publish).
    let tot = flow
        .snapshot("tot")
        .unwrap()
        .expect("a global aggregate over an empty chain still owes its row");
    assert_eq!(tot.num_rows(), 1);
    let col = tot
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 total");
    assert!(
        col.is_null(0),
        "SUM over an empty input is NULL, not {}",
        col.value(0)
    );
}
