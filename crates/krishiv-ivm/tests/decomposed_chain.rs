//! DECOMP-2 wiring: a multi-operator view the single-operator matchers refuse
//! is cut into a `ViewPlan::Chain` at plan time, maintained O(Δ), and its
//! stateful hops checkpoint and restore losslessly.
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
/// Filter + computed aggregate argument + renaming projection: four hops, none
/// of which the single-operator matchers accept as a whole.
const CHAIN_SQL: &str = "SELECT SUM(amount * 2) AS total FROM sales WHERE region = 10";

fn chain_spec() -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: "v".into(),
        body_sql: CHAIN_SQL.into(),
        output_schema: Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Int64,
            true,
        )])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

fn total(flow: &IncrementalFlow) -> Option<i64> {
    let snap = flow.snapshot("v").unwrap().expect("published");
    assert_eq!(snap.num_rows(), 1);
    let col = snap
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 total");
    (!col.is_null(0)).then(|| col.value(0))
}

/// The chain's accumulator must survive checkpoint/restore **losslessly**.
/// Batch one carries a genuinely duplicate row, which the materialized source
/// snapshot (a set) cannot represent — so a restore that falls back to
/// snapshot seeding under-counts, and only the checkpointed per-hop state
/// bytes (CHN1 framing) give the continuous answer. That is what makes this a
/// test of the restore path rather than of seeding.
#[tokio::test(flavor = "multi_thread")]
async fn a_chain_view_checkpoints_and_restores_losslessly() {
    let flow = IncrementalFlow::new();
    flow.register_view(chain_spec()).unwrap();
    // (10, 5) twice — the duplicate is the point.
    flow.feed(
        "sales",
        DeltaBatch::from_inserts(sales(&[(10, 5), (10, 5), (20, 1)])).unwrap(),
    )
    .unwrap();
    flow.step_datafusion().await.unwrap();
    let (inc, why) = flow
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc && why.contains("chain"), "not a chain: {why}");
    assert_eq!(total(&flow), Some(20), "2 * (5 + 5)");

    let blob = flow.checkpoint_full().unwrap();

    let restored = IncrementalFlow::new();
    restored.register_view(chain_spec()).unwrap();
    restored.restore_full(&blob).unwrap();
    restored
        .feed(
            "sales",
            DeltaBatch::from_inserts(sales(&[(10, 3)])).unwrap(),
        )
        .unwrap();
    restored.step_datafusion().await.unwrap();
    assert_eq!(
        total(&restored),
        Some(26),
        "restored chain must carry both duplicate contributions: 2*(5+5+3)"
    );

    // The uninterrupted flow agrees — restore changed nothing but the process.
    flow.feed(
        "sales",
        DeltaBatch::from_inserts(sales(&[(10, 3)])).unwrap(),
    )
    .unwrap();
    flow.step_datafusion().await.unwrap();
    assert_eq!(total(&flow), Some(26));
}

/// Retract every contributing row through the chain: the global SUM returns to
/// NULL (SQL over empty input), not 0 and not an absent row.
#[tokio::test(flavor = "multi_thread")]
async fn a_chain_global_sum_returns_to_null_when_fully_retracted() {
    let flow = IncrementalFlow::new();
    flow.register_view(chain_spec()).unwrap();
    let batch = sales(&[(10, 5), (10, 7)]);
    flow.feed("sales", DeltaBatch::from_inserts(batch.clone()).unwrap())
        .unwrap();
    flow.step_datafusion().await.unwrap();
    // Plan-kind first: DiffBased computes these values too (the fallback
    // mask), so without this the test cannot tell the chain from a revert.
    let (inc, why) = flow
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc && why.contains("chain"), "not a chain: {why}");
    assert_eq!(total(&flow), Some(24));

    flow.feed("sales", DeltaBatch::from_deletes(batch).unwrap())
        .unwrap();
    flow.step_datafusion().await.unwrap();
    assert_eq!(
        total(&flow),
        None,
        "SUM over a fully-retracted input is NULL"
    );
}
