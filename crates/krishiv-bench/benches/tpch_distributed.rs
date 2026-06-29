//! TPC-H benchmarks through the in-process coordinator + executor cluster.
//!
//! These run the same queries as the `tpch_sf10` bench target, but instead of
//! calling the embedded `SqlEngine` directly they submit each query through
//! `krishiv_runtime::InProcessCluster` — the session-scoped coordinator +
//! executor control plane that distributed-mode sessions and the standalone
//! Flight SQL server use (`InProcessCluster::collect_batch_sql`, the same
//! entry point as `FlightExecutionHost`'s in-process backend). Comparing the
//! two targets isolates the cost of the cluster submission path (table
//! registration via `BatchSqlTable`, coordinator-owned SQL engine, parquet
//! footer cache) over raw embedded execution.
//!
//! Limitations (documented per the Phase 2.10 roadmap):
//! - `InProcessCluster` provisions exactly one coordinator and one in-process
//!   executor per cluster; the public API exposes no executor-count knob, so a
//!   true 2-executor topology cannot be wired from this crate. Multi-executor
//!   clusters require spawning separate executor processes (the `krishiv
//!   cluster` binary) and a Flight coordinator URL, which is outside the scope
//!   of an in-process Criterion harness.
//! - Pure SELECT batch queries take the runtime's inline fast path, which
//!   bypasses the coordinator job state machine. These numbers therefore
//!   measure the distributed session entry point with single-executor
//!   placement, not cross-executor shuffle.
//!
//! To run: cargo bench -p krishiv-bench --bench tpch_distributed
//! Data paths use the same scale ladder as `tpch_sf10`:
//!   - KRISHIV_TPCH_DATA_DIR_SF1   (~1 GB)
//!   - KRISHIV_TPCH_DATA_DIR_SF10  (~10 GB; KRISHIV_TPCH_DATA_DIR is a legacy alias)
//!   - KRISHIV_TPCH_DATA_DIR_SF100 (~100 GB)
//!
//! Unset scale factors are skipped with a notice on stderr.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use krishiv_bench::tpch;
use krishiv_runtime::{BatchSqlTable, InProcessCluster};
use std::path::PathBuf;
use std::time::Duration;

/// Run `query` through a fresh `InProcessCluster` at every configured scale
/// factor, forwarding each table's Parquet file as a `BatchSqlTable`
/// registration. A new cluster per iteration mirrors the embedded bench,
/// which constructs a fresh `SqlEngine` per iteration.
fn bench_distributed_query(
    c: &mut Criterion,
    group_name: &str,
    bench_name: &str,
    query: &str,
    tables: &[&str],
) {
    let scale_dirs = tpch::scale_dirs();

    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    for (sf, dir) in &scale_dirs {
        group.bench_with_input(BenchmarkId::new(bench_name, sf), dir, |b, dir| {
            b.iter(|| {
                if !tpch::tables_exist(dir, tables) {
                    // Missing Parquet files: skip execution.
                    return;
                }
                let cluster = InProcessCluster::new().expect("in-process cluster creation");
                let registrations: Vec<BatchSqlTable> = tables
                    .iter()
                    .map(|table| BatchSqlTable {
                        table_name: (*table).to_string(),
                        path: PathBuf::from(format!("{dir}/{table}.parquet")),
                        ..Default::default()
                    })
                    .collect();
                let _ = cluster.collect_batch_sql(query, &registrations, false);
            })
        });
    }
    group.finish();
}

fn bench_tpch_q1_distributed(c: &mut Criterion) {
    // TPC-H Q1: pricing summary report
    bench_distributed_query(
        c,
        "tpch_distributed_q1",
        "q1_pricing_summary",
        tpch::Q1,
        tpch::Q1_TABLES,
    );
}

fn bench_tpch_q3_distributed(c: &mut Criterion) {
    // TPC-H Q3: shipping priority — join orders, lineitem, customer; filter by date and segment.
    bench_distributed_query(
        c,
        "tpch_distributed_q3",
        "q3_shipping_priority",
        tpch::Q3,
        tpch::Q3_TABLES,
    );
}

fn bench_tpch_q5_distributed(c: &mut Criterion) {
    // TPC-H Q5: local supplier volume — multi-table join with region filter.
    bench_distributed_query(
        c,
        "tpch_distributed_q5",
        "q5_local_supplier_volume",
        tpch::Q5,
        tpch::Q5_TABLES,
    );
}

fn bench_tpch_q6_distributed(c: &mut Criterion) {
    // TPC-H Q6: forecasting revenue change
    bench_distributed_query(
        c,
        "tpch_distributed_q6",
        "q6_forecasting_revenue",
        tpch::Q6,
        tpch::Q6_TABLES,
    );
}

fn bench_tpch_q10_distributed(c: &mut Criterion) {
    // TPC-H Q10: returned-item reporting — customer/orders/lineitem/nation join,
    // group by customer, top 20 by revenue.
    bench_distributed_query(
        c,
        "tpch_distributed_q10",
        "q10_returned_item_reporting",
        tpch::Q10,
        tpch::Q10_TABLES,
    );
}

fn bench_tpch_q18_distributed(c: &mut Criterion) {
    // TPC-H Q18: large-volume customer — customer/orders/lineitem with
    // HAVING SUM(l_quantity) > 300.
    bench_distributed_query(
        c,
        "tpch_distributed_q18",
        "q18_large_volume_customer",
        tpch::Q18,
        tpch::Q18_TABLES,
    );
}

criterion_group!(
    tpch_distributed_benches,
    bench_tpch_q1_distributed,
    bench_tpch_q3_distributed,
    bench_tpch_q5_distributed,
    bench_tpch_q6_distributed,
    bench_tpch_q10_distributed,
    bench_tpch_q18_distributed
);
criterion_main!(tpch_distributed_benches);
