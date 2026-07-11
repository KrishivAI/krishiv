//! Property tests for IVM correctness (audit §14 TEST-3): for randomized
//! multi-tick insert/retract sequences, the incrementally maintained view
//! must equal (a) the same flow forced onto the diff-based fallback path and
//! (b) a one-shot DataFusion recompute over the final consolidated input.
//!
//! This is the randomized generalisation of `property_tests.rs` (fixed
//! inputs) — DBSP proves incremental = batch; Krishiv checks it empirically
//! over generated histories, including group-emptying retractions.

// Test harness: panicking on invariant violation is the assertion.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;
use proptest::prelude::*;

// ── Input model ───────────────────────────────────────────────────────────────

/// One tick: rows to insert, then delete picks against the live multiset.
type Tick = (Vec<(i64, i64)>, Vec<prop::sample::Index>);

fn ticks_strategy() -> impl Strategy<Value = Vec<Tick>> {
    proptest::collection::vec(
        (
            proptest::collection::vec((0_i64..4, -5_i64..5), 0..5),
            proptest::collection::vec(any::<prop::sample::Index>(), 0..3),
        ),
        1..=3,
    )
}

fn src_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn rows_batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        src_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build src batch")
}

fn agg_spec() -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: "agg".into(),
        body_sql: "SELECT k, COUNT(*) AS n, SUM(v) AS s FROM src GROUP BY k".into(),
        output_schema: Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Int64, true),
        ])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

/// Replay the generated ticks into `flow`, returning the final live rows.
/// Deletes are resolved against the live multiset so every retraction targets
/// a row that exists.
async fn replay(flow: &IncrementalFlow, ticks: &[Tick]) -> Vec<(i64, i64)> {
    let mut live: Vec<(i64, i64)> = Vec::new();
    for (inserts, deletes) in ticks {
        let mut removed: Vec<(i64, i64)> = Vec::new();
        for pick in deletes {
            if live.is_empty() {
                break;
            }
            removed.push(live.swap_remove(pick.index(live.len())));
        }
        live.extend_from_slice(inserts);

        let mut deltas = Vec::new();
        if !inserts.is_empty() {
            deltas.push(DeltaBatch::from_inserts(rows_batch(inserts)).expect("inserts"));
        }
        if !removed.is_empty() {
            deltas.push(DeltaBatch::from_deletes(rows_batch(&removed)).expect("deletes"));
        }
        if deltas.is_empty() {
            continue;
        }
        let delta = DeltaBatch::concat(&deltas).expect("concat tick delta");
        flow.feed("src", delta).expect("feed");
        flow.step_datafusion().await.expect("step");
    }
    live
}

/// Normalize a snapshot batch into sorted `(k, n, s)` rows.
fn norm_agg(batch: Option<RecordBatch>) -> Vec<(i64, i64, i64)> {
    let Some(batch) = batch else {
        return Vec::new();
    };
    let col = |name: &str| -> &Int64Array {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name}"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column {name} is not Int64"))
    };
    let (k, n, s) = (col("k"), col("n"), col("s"));
    let mut rows: Vec<(i64, i64, i64)> = (0..batch.num_rows())
        .map(|i| (k.value(i), n.value(i), s.value(i)))
        .collect();
    rows.sort_unstable();
    rows
}

/// The reference: aggregate the final live rows in plain Rust.
fn model_agg(live: &[(i64, i64)]) -> Vec<(i64, i64, i64)> {
    let mut groups: std::collections::BTreeMap<i64, (i64, i64)> = std::collections::BTreeMap::new();
    for (k, v) in live {
        let entry = groups.entry(*k).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += *v;
    }
    groups.into_iter().map(|(k, (n, s))| (k, n, s)).collect()
}

/// One-shot DataFusion recompute of the same SQL over the final live rows.
async fn recompute_agg(live: &[(i64, i64)]) -> Vec<(i64, i64, i64)> {
    if live.is_empty() {
        return Vec::new();
    }
    let ctx = datafusion::prelude::SessionContext::new();
    let table =
        datafusion::datasource::MemTable::try_new(src_schema(), vec![vec![rows_batch(live)]])
            .expect("mem table");
    ctx.register_table("src", Arc::new(table))
        .expect("register");
    let batches = ctx
        .sql("SELECT k, COUNT(*) AS n, SUM(v) AS s FROM src GROUP BY k")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let merged = arrow::compute::concat_batches(
        &batches.first().expect("non-empty result").schema(),
        &batches,
    )
    .expect("concat");
    norm_agg(Some(merged))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    /// incremental == diff-based == one-shot recompute == plain-Rust model,
    /// for the same randomized insert/retract history.
    #[test]
    fn incremental_agg_equals_recompute(ticks in ticks_strategy()) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            let incremental = IncrementalFlow::new();
            incremental.register_view(agg_spec()).expect("register inc");

            let diff_based = IncrementalFlow::new();
            diff_based.force_diff_based().expect("force diff");
            diff_based.register_view(agg_spec()).expect("register diff");

            let live = replay(&incremental, &ticks).await;
            let live_diff = replay(&diff_based, &ticks).await;
            assert_eq!(live, live_diff, "replay must be deterministic");

            let inc_rows = norm_agg(incremental.snapshot("agg").expect("snapshot inc"));
            let diff_rows = norm_agg(diff_based.snapshot("agg").expect("snapshot diff"));
            let model_rows = model_agg(&live);
            let recomputed = recompute_agg(&live).await;

            assert_eq!(
                inc_rows, model_rows,
                "incremental snapshot diverged from the model (live rows: {live:?})"
            );
            assert_eq!(
                diff_rows, model_rows,
                "diff-based snapshot diverged from the model (live rows: {live:?})"
            );
            assert_eq!(recomputed, model_rows, "DataFusion recompute sanity check");
        });
    }
}
