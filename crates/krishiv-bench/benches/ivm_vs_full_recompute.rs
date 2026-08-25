//! IVM vs full-recompute: the platform's headline cost claim, measured.
//!
//! Krishiv's pitch for live tables is that an incrementally-maintained view
//! processes only the *delta* since the last update, while a naive
//! "recompute the whole query" approach must rescan everything that has
//! accumulated so far. This benchmark measures both costs directly, at a
//! range of accumulated-table sizes, for the same `GROUP BY SUM` query:
//!
//! - `full_recompute/<n>`: given a table that already has `n` rows, the cost
//!   of running `SELECT region, SUM(amount) AS total FROM orders GROUP BY
//!   region` from scratch (a fresh `SqlEngine`, full table scan).
//! - `ivm_incremental_feed/<n>`: given an `IncrementalFlow` whose
//!   materialized view already reflects `n - BATCH_SIZE` rows, the cost of
//!   feeding *one more* `BATCH_SIZE`-row batch and stepping the view forward
//!   to `n` rows.
//!
//! Both benchmarks use `iter_batched` so the (expensive, untimed) setup —
//! building the pre-existing `n`-row or `n - BATCH_SIZE`-row state — happens
//! outside the timed region; only the operation actually being compared is
//! measured.
//!
//! **Measured 2026-08-24 (IVM-AUD-PERF-1), superseding the 2026-07-05 note
//! that used to sit here.** That note claimed `full_recompute` was ~100x
//! faster than `ivm_incremental_feed` and blamed a fresh `SessionContext` per
//! tick, predicting a crossover near 23M rows. All three parts were wrong by
//! the time it was read: `step_datafusion` caches its context, the measured
//! gap was 1-4x rather than 100x, and there was no crossover because the tick
//! was itself growing with accumulated rows — 7.6x the time for 7.5x the rows,
//! for an identical `BATCH_SIZE` delta.
//!
//! That growth was the real finding, and it was a defect, not a property of
//! IVM: `SourceState` held its relation as one contiguous `RecordBatch` and
//! rebuilt it with `concat_batches` on every append, copying the whole
//! accumulated relation per tick. See `SourceState`'s module doc. With the
//! relation chunked, the tick no longer scales with accumulated rows.
//!
//! Keep this note honest against what the benchmark actually prints. Per
//! `docs/BENCHMARKING.md` these are self-comparison numbers for regression
//! detection, not figures to publish against unlike hardware.
//! To run: `cargo bench -p krishiv-bench --bench ivm_vs_full_recompute`.
//! Per `docs/BENCHMARKING.md`: record the commit, dirty-worktree state, and
//! hardware alongside any published result — this file does not do that for
//! you.

// Benchmark harness: a panic is the failure signal, and clippy.toml's
// `allow-*-in-tests` does not cover bench targets.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

/// Distinct group keys in the `region` column — representative of a
/// "revenue by region/segment/tenant" aggregation, not a single global sum.
const NUM_REGIONS: i64 = 100;
/// Rows added per incremental step. Kept well under any of the `TOTAL_ROWS`
/// scale points so the O(delta) vs O(n) gap is visible even at the smallest.
const BATCH_SIZE: i64 = 5_000;
/// Accumulated-table sizes to benchmark at. The 10M point exists because the
/// Phase 51 yardstick records IVM tick latency at 1M *and* 10M rows; it
/// needs roughly 2 GB of headroom for its untimed per-iteration setup, so
/// on smaller machines set `KRISHIV_BENCH_IVM_MAX_ROWS=1000000` to stop the
/// ladder at 1M.
const TOTAL_ROWS: &[i64] = &[50_000, 200_000, 500_000, 1_000_000, 10_000_000];

/// The `TOTAL_ROWS` ladder truncated to `KRISHIV_BENCH_IVM_MAX_ROWS` (if set).
/// `KRISHIV_BENCH_IVM_ROWS` (comma-separated row counts) replaces the ladder
/// outright — the #179 crossover residual needs samples between 1M and 10M,
/// and pinning a crossover means choosing points a fixed ladder doesn't have.
fn total_rows_ladder() -> Vec<i64> {
    if let Ok(list) = std::env::var("KRISHIV_BENCH_IVM_ROWS") {
        let rows: Vec<i64> = list
            .split(',')
            .filter_map(|v| v.trim().parse::<i64>().ok())
            .filter(|&n| n > 0)
            .collect();
        if !rows.is_empty() {
            return rows;
        }
    }
    let cap = std::env::var("KRISHIV_BENCH_IVM_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(i64::MAX);
    TOTAL_ROWS.iter().copied().filter(|&n| n <= cap).collect()
}

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

/// Build `n` rows of deterministic (not random — reproducibility over
/// realism) order data, starting the region/amount sequence at `offset`.
fn orders_batch(offset: i64, n: i64) -> RecordBatch {
    let regions: Vec<String> = (offset..offset + n)
        .map(|i| format!("region-{}", i % NUM_REGIONS))
        .collect();
    let amounts: Vec<i64> = (offset..offset + n).map(|i| 1 + (i % 997)).collect();
    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(StringArray::from(regions)),
            Arc::new(Int64Array::from(amounts)),
        ],
    )
    .expect("orders batch construction")
}

fn revenue_view_spec() -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: "revenue".into(),
        body_sql: "SELECT region, SUM(amount) AS total FROM orders GROUP BY region".into(),
        output_schema: Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
        ])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

/// A fresh `IncrementalFlow` with `revenue` already reflecting `baseline_rows`
/// of `orders` data — the state a benchmarked "feed one more batch" call
/// starts from.
fn seeded_flow(rt: &tokio::runtime::Runtime, baseline_rows: i64) -> IncrementalFlow {
    let flow = IncrementalFlow::new();
    flow.register_view(revenue_view_spec())
        .expect("register revenue view");
    if baseline_rows > 0 {
        flow.feed(
            "orders",
            DeltaBatch::from_inserts(orders_batch(0, baseline_rows)).expect("baseline delta batch"),
        )
        .expect("feed baseline");
        rt.block_on(flow.step_datafusion())
            .expect("step baseline into the materialized view");
    }
    flow
}

/// Report — and require — that the benchmarked view actually executes O(Δ).
///
/// `ivm_incremental_feed` only measures incremental maintenance if `revenue`
/// got an incremental plan. A view that degrades to `DiffBased` re-runs the
/// whole SQL every tick, which is O(n) *by design*, and it would still produce
/// a plausible number here — indistinguishable from real incremental
/// maintenance except by being slow. That is exactly the confusion the
/// 2026-07-05 note above fell into. Failing loudly means a future run cannot
/// quietly benchmark the fallback and call it IVM.
fn require_incremental_plan(rt: &tokio::runtime::Runtime) {
    let flow = seeded_flow(rt, 10_000);
    let (incremental, reason) = flow
        .view_plan_classification("revenue")
        .expect("classify revenue view")
        .expect("revenue view is registered");
    println!("ivm_vs_full_recompute: revenue incremental={incremental} ({reason})");
    assert!(
        incremental,
        "the benchmarked view degraded to full recompute, so this run would not          be measuring incremental maintenance: {reason}"
    );
}

fn bench_ivm_incremental_feed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    require_incremental_plan(&rt);
    let mut group = c.benchmark_group("ivm_incremental_feed");
    for total in total_rows_ladder() {
        let baseline = total - BATCH_SIZE;
        group.bench_with_input(
            BenchmarkId::from_parameter(total),
            &baseline,
            |b, &baseline| {
                b.iter_batched(
                    || seeded_flow(&rt, baseline),
                    |flow| {
                        flow.feed(
                            "orders",
                            DeltaBatch::from_inserts(orders_batch(baseline, BATCH_SIZE))
                                .expect("incremental delta batch"),
                        )
                        .expect("feed incremental batch");
                        rt.block_on(flow.step_datafusion())
                            .expect("step incremental batch into the materialized view");
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn bench_full_recompute(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("full_recompute");
    for total in total_rows_ladder() {
        group.bench_with_input(BenchmarkId::from_parameter(total), &total, |b, &total| {
            b.iter_batched(
                || orders_batch(0, total),
                |batch| {
                    rt.block_on(async {
                        let engine = krishiv_sql::SqlEngine::new();
                        engine
                            .register_record_batches("orders", vec![batch])
                            .await
                            .expect("register orders table");
                        let df = engine
                            .sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region")
                            .await
                            .expect("plan full recompute query");
                        df.collect().await.expect("run full recompute query")
                    });
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_ivm_incremental_feed, bench_full_recompute
}
criterion_main!(benches);
