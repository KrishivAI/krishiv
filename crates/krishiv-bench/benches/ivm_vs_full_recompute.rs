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
//! **Measured result (2026-07-05, see `docs/implementation/status.md` for
//! full numbers/methodology) contradicts the naive expectation**:
//! `full_recompute` is ~100x *faster* than `ivm_incremental_feed` at every
//! size tested (up to 1M rows), because every production call site uses the
//! `step_datafusion()` convenience method, which constructs a fresh
//! `SessionContext` on every tick — that fixed setup cost dominates the true
//! O(Δ) aggregate work at these scales. Extrapolating the measured
//! `full_recompute` growth, the crossover (where a full recompute costs as
//! much as one current IVM tick) is around 23M rows. Below that, this
//! benchmark's workload is genuinely faster to recompute from scratch than
//! to maintain incrementally, as currently implemented.
//! To run: `cargo bench -p krishiv-bench --bench ivm_vs_full_recompute`.
//! Per `docs/BENCHMARKING.md`: record the commit, dirty-worktree state, and
//! hardware alongside any published result — this file does not do that for
//! you.

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
/// Accumulated-table sizes to benchmark at. Kept modest (max 1M rows) to fit
/// comfortably in a resource-constrained sandbox — the O(delta) vs O(n)
/// shape is scale-invariant, so this doesn't need enterprise-scale data to
/// demonstrate.
const TOTAL_ROWS: &[i64] = &[50_000, 200_000, 500_000, 1_000_000];

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

fn bench_ivm_incremental_feed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("ivm_incremental_feed");
    for &total in TOTAL_ROWS {
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
    for &total in TOTAL_ROWS {
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
