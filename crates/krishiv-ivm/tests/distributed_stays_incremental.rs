//! Delta-batch views must stay incremental on an EXECUTOR, not only centrally.
//!
//! The resident protocol ships a job's specs and full state to an executor once
//! at `delta:attach:`, and every tick afterwards carries only that tick's input
//! deltas plus a fence; the executor answers with per-view **output deltas**,
//! never snapshots. The whole point of residency is that compiled plans and
//! operator accumulators stay warm in that process — so `force_diff_based` is
//! deliberately not set there.
//!
//! That is the design. Nothing asserted it. This does, three ways: every view
//! classifies `Incremental` **on the resident flow**, `is_force_diff_based` is
//! false there, and the coordinator's mirrored result matches a flow that
//! computed the same ticks locally — because "incremental on the executor" is
//! worth nothing if what comes back over the wire disagrees with the truth.
//!
//! It was written to check a specific claim, from a recon of the IVM seams,
//! that an executor flow runs every view DiffBased because `force_diff_based`
//! is checkpoint-serialized and restored. The claim is false, and this is how
//! that stays known.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)]
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;
use std::collections::HashMap;
use std::sync::Arc;

fn s() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}
fn b(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        s(),
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

#[tokio::test(flavor = "multi_thread")]
async fn resident_executor_path_keeps_views_incremental() {
    let shapes: Vec<(&str, &str, SchemaRef)> = vec![
        (
            "agg",
            "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Int64, true),
                Field::new("total", DataType::Int64, true),
            ])),
        ),
        (
            "map",
            "SELECT region, amount * 2 AS doubled FROM sales WHERE amount > 10",
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Int64, false),
                Field::new("doubled", DataType::Int64, false),
            ])),
        ),
        (
            "glob",
            "SELECT COUNT(*) AS n FROM sales",
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
        ),
        // DECOMP-2: a decomposed chain must survive the attach round trip too —
        // its per-hop accumulators ship inside the same full-state blob (CHN1
        // framing) and the resident tick folds the delta through warm hops.
        (
            "chain",
            "SELECT SUM(amount * 2) AS total FROM sales WHERE region = 1",
            Arc::new(Schema::new(vec![Field::new(
                "total",
                DataType::Int64,
                true,
            )])),
        ),
    ];

    // Coordinator-authoritative flow.
    let auth = IncrementalFlow::new();
    for (n, sql, out) in &shapes {
        auth.register_view(IncrementalViewSpec {
            name: (*n).into(),
            body_sql: (*sql).into(),
            output_schema: out.clone(),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();
    }
    auth.feed(
        "sales",
        DeltaBatch::from_inserts(b(&[(1, 100), (2, 200)])).unwrap(),
    )
    .unwrap();
    auth.step_datafusion().await.unwrap();

    // delta:attach: — specs + full state ship to the executor ONCE.
    let resident = IncrementalFlow::new();
    for spec in auth.view_specs().unwrap() {
        resident.register_view(spec).unwrap();
    }
    resident
        .restore_full(&auth.checkpoint_full().unwrap())
        .unwrap();

    // delta:tick: — only this tick's deltas cross the wire.
    let delta = DeltaBatch::from_inserts(b(&[(1, 25), (3, 700)])).unwrap();
    auth.feed("sales", delta.clone()).unwrap();
    resident.feed("sales", delta).unwrap();
    resident.step_datafusion().await.unwrap();

    println!("{:<8}{:>14}{:>14}", "view", "central", "resident");
    let mut all = true;
    for (n, _, _) in &shapes {
        let c = auth.view_plan_classification(n).unwrap().unwrap().0;
        let r = resident.view_plan_classification(n).unwrap().unwrap().0;
        if !r {
            all = false;
        }
        println!("{n:<8}{c:>14}{r:>14}");
    }
    println!(
        "resident force_diff_based = {:?}",
        resident.is_force_diff_based().unwrap()
    );
    println!("ALL INCREMENTAL ON EXECUTOR: {all}");

    // The executor answers with per-view output deltas; the coordinator mirrors.
    let mut view_deltas: HashMap<String, DeltaBatch> = HashMap::new();
    for name in resident.view_names().unwrap() {
        if let Some(d) = resident.take_step_output(&name).unwrap() {
            view_deltas.insert(name, d);
        }
    }
    let local_pending = auth.take_pending().unwrap();
    let summary = auth.apply_remote_tick(local_pending, view_deltas).unwrap();
    println!(
        "mirrored rows={} errored={:?}",
        summary.total_output_rows,
        summary
            .errored_views
            .iter()
            .map(|e| format!("{:?}", e.kind))
            .collect::<Vec<_>>()
    );

    // central (recomputed locally) vs mirrored-from-executor must agree
    let central = IncrementalFlow::new();
    for (n, sql, out) in &shapes {
        central
            .register_view(IncrementalViewSpec {
                name: (*n).into(),
                body_sql: (*sql).into(),
                output_schema: out.clone(),
                is_materialized: true,
                is_recursive: false,
                lateness: vec![],
            })
            .unwrap();
    }
    central
        .feed(
            "sales",
            DeltaBatch::from_inserts(b(&[(1, 100), (2, 200)])).unwrap(),
        )
        .unwrap();
    central.step_datafusion().await.unwrap();
    central
        .feed(
            "sales",
            DeltaBatch::from_inserts(b(&[(1, 25), (3, 700)])).unwrap(),
        )
        .unwrap();
    central.step_datafusion().await.unwrap();
    for (n, _, _) in &shapes {
        let a = auth.snapshot(n).unwrap().map(|x| x.num_rows());
        let c = central.snapshot(n).unwrap().map(|x| x.num_rows());
        println!(
            "{n:<8} mirrored_rows={a:?} central_rows={c:?} {}",
            if a == c { "agree" } else { "DISAGREE" }
        );
    }
    assert!(
        all,
        "the resident executor path must keep views incremental"
    );
    assert!(
        !resident.is_force_diff_based().unwrap(),
        "residency exists to keep plans warm; forcing recompute there defeats it"
    );
    assert!(
        summary.errored_views.is_empty(),
        "mirroring the executor's output deltas must not error"
    );
    for (n, _, _) in &shapes {
        assert_eq!(
            auth.snapshot(n).unwrap().map(|x| x.num_rows()),
            central.snapshot(n).unwrap().map(|x| x.num_rows()),
            "view {n}: mirrored-from-executor disagreed with locally-computed"
        );
    }
}
