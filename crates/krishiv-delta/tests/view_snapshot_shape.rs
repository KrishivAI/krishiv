//! IVM-AUD-PERF-3: a materialized view's snapshot maintenance must be O(Δ).
//!
//! The tick cost of a view whose output carries retractions was linear in the
//! ACCUMULATED SNAPSHOT, not in the delta: `apply_delta` concatenated the whole
//! prior snapshot with the delta and re-consolidated the lot, stringifying
//! every row. Measured before the fix, with the delta pinned at ~1000 rows:
//! 10k snapshot → 20.3 ms, 50k → 122.4 ms, 100k → 253.8 ms.
//!
//! That is the defect this file exists to keep out. It asserts the SHAPE — the
//! per-tick cost must not grow with the snapshot — rather than a wall-clock
//! constant, so it stays meaningful on slower CI hardware.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use krishiv_delta::{DeltaBatch, IncrementalView, IncrementalViewSpec};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn rows(start: i64, n: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from((start..start + n).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (start..start + n)
                    .map(|i| format!("n{i}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn view() -> IncrementalView {
    let (v, _rx) = IncrementalView::new(IncrementalViewSpec {
        name: "v".into(),
        body_sql: "SELECT id, name FROM t".into(),
        output_schema: schema(),
        is_materialized: true,
        is_recursive: false,
        lateness: Vec::new(),
    });
    v
}

/// Grow the snapshot to `snapshot_rows`, then time ONE more tick whose delta
/// carries a retraction — the keep-last/top-N/session shape that takes the
/// expensive path.
fn tick_cost_at(snapshot_rows: i64) -> std::time::Duration {
    let v = view();
    const DELTA: i64 = 1_000;
    let mut next = 0i64;
    while next < snapshot_rows {
        let take = DELTA.min(snapshot_rows - next);
        v.apply_output_delta(&DeltaBatch::from_inserts(rows(next, take)).unwrap())
            .unwrap();
        next += take;
    }
    // The timed tick: insert 1000 new rows and retract one existing row.
    let delta = DeltaBatch::concat(&[
        DeltaBatch::from_inserts(rows(next, DELTA)).unwrap(),
        DeltaBatch::from_deletes(rows(0, 1)).unwrap(),
    ])
    .unwrap();
    let t0 = Instant::now();
    v.apply_output_delta(&delta).unwrap();
    t0.elapsed()
}

#[test]
fn a_view_tick_does_not_grow_with_the_accumulated_snapshot() {
    // Warm up so the first measurement does not carry one-time costs.
    let _ = tick_cost_at(2_000);

    let small = tick_cost_at(10_000);
    let large = tick_cost_at(100_000);

    // 10x the snapshot, same delta. O(state) gives ~10x (measured 20.3 → 253.8
    // ms before the fix, 12.5x). O(delta) gives ~1x. The 4x bar passes an
    // O(delta) tick with generous slack on noisy hardware and still fails the
    // old behaviour by a wide margin.
    let ratio = large.as_secs_f64() / small.as_secs_f64().max(1e-9);
    assert!(
        ratio < 4.0,
        "view tick grew {ratio:.1}x for 10x the snapshot ({small:?} → {large:?}); \
         snapshot maintenance is O(state), not O(delta)"
    );
}

/// The shape fix must not change what the snapshot CONTAINS. Multiset
/// semantics, retraction netting, and the clamp on a retraction with nothing
/// to cancel are all pinned here against the pre-fix behaviour.
#[test]
fn the_snapshot_contents_are_unchanged_by_the_representation() {
    let v = view();
    // Two copies of row 0 (multiset), plus rows 1 and 2.
    v.apply_output_delta(&DeltaBatch::from_inserts(rows(0, 3)).unwrap())
        .unwrap();
    v.apply_output_delta(&DeltaBatch::from_inserts(rows(0, 1)).unwrap())
        .unwrap();
    assert_eq!(
        v.snapshot().unwrap().unwrap().num_rows(),
        4,
        "multiset: 2x row 0"
    );

    // Retract one copy of row 0 — the other copy must survive.
    v.apply_output_delta(&DeltaBatch::from_deletes(rows(0, 1)).unwrap())
        .unwrap();
    assert_eq!(v.snapshot().unwrap().unwrap().num_rows(), 3);

    // Retract row 0 twice more: one cancels the last copy, the second has
    // nothing to cancel and is CLAMPED (the pre-fix contract, IVM-AUD-CORE-2b).
    v.apply_output_delta(&DeltaBatch::from_deletes(rows(0, 1)).unwrap())
        .unwrap();
    v.apply_output_delta(&DeltaBatch::from_deletes(rows(0, 1)).unwrap())
        .unwrap();
    assert_eq!(v.snapshot().unwrap().unwrap().num_rows(), 2);

    // A later insert of row 0 must APPEAR, not be swallowed by the clamped
    // retraction — the clamp forgets the debt, it does not bank it.
    v.apply_output_delta(&DeltaBatch::from_inserts(rows(0, 1)).unwrap())
        .unwrap();
    assert_eq!(v.snapshot().unwrap().unwrap().num_rows(), 3);
}
