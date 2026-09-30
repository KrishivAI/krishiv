//! IVM-AUD-INT-F5 / API-B1: a reader can follow a view's output without
//! losing the deltas it was too slow to see.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::{IncrementalFlow, PartitionedIncrementalFlow};

fn sales_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn sales(rows: &[(i64, i64)]) -> DeltaBatch {
    let batch = RecordBatch::try_new(
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
    .unwrap();
    DeltaBatch::from_inserts(batch).unwrap()
}

/// A stateless view: every fed row comes straight out, so each tick's output
/// is exactly what was fed that tick.
fn passthrough() -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: "v".into(),
        body_sql: "SELECT region, amount FROM sales".into(),
        output_schema: sales_schema(),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

fn totals() -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: "v".into(),
        body_sql: "SELECT region, SUM(amount) AS total FROM sales GROUP BY region".into(),
        output_schema: Arc::new(Schema::new(vec![
            Field::new("region", DataType::Int64, true),
            Field::new("total", DataType::Int64, true),
        ])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

async fn tick(flow: &IncrementalFlow, rows: &[(i64, i64)]) {
    flow.feed("sales", sales(rows)).unwrap();
    flow.step_datafusion().await.unwrap();
}

/// The watch holds one delta. Three ticks between two reads used to cost the
/// reader two of them; the log hands back all three, in order.
#[tokio::test]
async fn a_slow_reader_gets_every_delta_in_order() {
    let flow = IncrementalFlow::new();
    flow.register_view(passthrough()).unwrap();
    tick(&flow, &[(1, 10)]).await;
    tick(&flow, &[(2, 20), (3, 30)]).await;
    tick(&flow, &[(4, 40)]).await;

    // What the coalescing watch still has: only the newest.
    assert_eq!(flow.view_output_peek("v").unwrap().unwrap().num_rows(), 1);

    let since = flow.view_output_since("v", 0).unwrap();
    assert!(!since.missed);
    let rows: Vec<usize> = since.deltas.iter().map(|(_, d)| d.num_rows()).collect();
    assert_eq!(rows, [1, 2, 1]);
    let ticks: Vec<u64> = since.deltas.iter().map(|(tick, _)| *tick).collect();
    assert!(ticks.windows(2).all(|w| w[0] < w[1]), "{ticks:?}");

    // Carrying the cursor forward serves only what is new, and nothing twice.
    let cursor = ticks[1];
    let since = flow.view_output_since("v", cursor).unwrap();
    assert_eq!(since.deltas.len(), 1);
    assert_eq!(since.deltas[0].0, ticks[2]);
    let since = flow.view_output_since("v", ticks[2]).unwrap();
    assert!(since.deltas.is_empty() && !since.missed);
}

/// A tick that changes nothing for the view leaves no entry, and does not
/// leave the previous delta looking fresh.
#[tokio::test]
async fn a_quiet_tick_adds_nothing() {
    let flow = IncrementalFlow::new();
    flow.register_view(passthrough()).unwrap();
    tick(&flow, &[(1, 10)]).await;
    let first = flow.view_output_since("v", 0).unwrap();
    let cursor = first.deltas[0].0;
    flow.step_datafusion().await.unwrap();
    flow.step_datafusion().await.unwrap();
    let since = flow.view_output_since("v", cursor).unwrap();
    assert!(since.deltas.is_empty() && !since.missed);
}

/// Retention is bounded, and a reader that fell behind it is told so instead
/// of being handed a changelog with a hole in it.
#[tokio::test]
async fn a_reader_behind_the_retention_bound_is_told_it_missed_some() {
    let flow = IncrementalFlow::new();
    flow.set_output_retention(2, usize::MAX).unwrap();
    flow.register_view(passthrough()).unwrap();
    for region in 1..=5 {
        tick(&flow, &[(region, 1)]).await;
    }
    let all = flow.view_output_since("v", 0).unwrap();
    assert!(all.missed, "three deltas were evicted");
    assert_eq!(all.deltas.len(), 2, "only the newest two are retained");

    // A reader that had already consumed everything that was evicted has no gap.
    let oldest_retained = all.deltas[0].0;
    let caught_up = flow.view_output_since("v", oldest_retained - 1).unwrap();
    assert!(!caught_up.missed);
    assert_eq!(caught_up.deltas.len(), 2);

    // The byte bound evicts too.
    let flow = IncrementalFlow::new();
    flow.set_output_retention(usize::MAX, 1).unwrap();
    flow.register_view(passthrough()).unwrap();
    tick(&flow, &[(1, 1)]).await;
    let since = flow.view_output_since("v", 0).unwrap();
    assert!(since.missed && since.deltas.is_empty());
}

/// A restore replaces the state; nothing published before it can be served.
#[tokio::test]
async fn a_restore_invalidates_older_cursors() {
    let flow = IncrementalFlow::new();
    flow.register_view(totals()).unwrap();
    tick(&flow, &[(1, 10)]).await;
    tick(&flow, &[(1, 5)]).await;
    let checkpoint = flow.checkpoint_full().unwrap();
    let cursor_before = flow.view_output_since("v", 0).unwrap().deltas[0].0;

    let restored = IncrementalFlow::new();
    restored.register_view(totals()).unwrap();
    restored.restore_full(&checkpoint).unwrap();
    let since = restored.view_output_since("v", cursor_before).unwrap();
    assert!(
        since.missed,
        "the delta after the cursor did not survive the restore"
    );
    assert!(since.deltas.is_empty());

    // From the restored tick onwards the log is whole again.
    let at_restore = restored.tick().unwrap();
    tick(&restored, &[(1, 1)]).await;
    let since = restored.view_output_since("v", at_restore).unwrap();
    assert!(!since.missed);
    assert_eq!(since.deltas.len(), 1);
}

#[tokio::test]
async fn an_unknown_view_is_an_error_and_a_dropped_view_forgets_its_log() {
    let flow = IncrementalFlow::new();
    assert!(flow.view_output_since("nope", 0).is_err());
    flow.register_view(passthrough()).unwrap();
    tick(&flow, &[(1, 10)]).await;
    assert!(flow.drop_view("v").unwrap());
    flow.register_view(passthrough()).unwrap();
    assert!(flow.view_output_since("v", 0).unwrap().deltas.is_empty());
}

/// A partitioned flow merges its shards' deltas tick by tick.
#[tokio::test]
async fn a_partitioned_flow_merges_shards_per_tick() {
    let flow = PartitionedIncrementalFlow::new(4, "region");
    flow.register_view(totals()).unwrap();
    flow.feed("sales", sales(&[(1, 10), (2, 20), (3, 30)]))
        .unwrap();
    flow.step_datafusion().await.unwrap();
    flow.feed("sales", sales(&[(1, 1)])).unwrap();
    flow.step_datafusion().await.unwrap();

    let since = flow.view_output_since("v", 0).unwrap();
    assert!(!since.missed);
    assert_eq!(since.deltas.len(), 2, "one merged delta per tick");
    // Tick 1: three new groups. Tick 2: one group's total replaced.
    assert_eq!(since.deltas[0].1.num_rows(), 3);
    assert_eq!(since.deltas[1].1.num_rows(), 2);
    let cursor = since.deltas[0].0;
    assert_eq!(flow.view_output_since("v", cursor).unwrap().deltas.len(), 1);
}
