//! TPC-H benchmark gates for Krishiv R10 (embedded engine).
//!
//! These benchmarks measure query execution time against the TPC-H dataset
//! across a scale-factor ladder. They are gated against the targets defined in
//! Keep benchmark thresholds in code or in the minimal docs when they are
//! reintroduced as enforced gates.
//!
//! To run: cargo bench -p krishiv-bench --bench tpch_sf10
//! Data paths: set one env var per scale factor, each pointing to a directory
//! containing Parquet files generated from TPC-H at that scale (e.g.
//! lineitem.parquet, orders.parquet, etc.):
//!   - KRISHIV_TPCH_DATA_DIR_SF1   (~1 GB)
//!   - KRISHIV_TPCH_DATA_DIR_SF10  (~10 GB; KRISHIV_TPCH_DATA_DIR is a legacy alias)
//!   - KRISHIV_TPCH_DATA_DIR_SF100 (~100 GB)
//!
//! Each benchmark runs once per configured scale factor; unset scale factors
//! are skipped with a notice on stderr.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use krishiv_bench::tpch;
use std::time::Duration;

/// Run `query` through the embedded `SqlEngine` at every configured scale
/// factor, registering each table's Parquet file from the scale's data dir.
fn bench_embedded_query(
    c: &mut Criterion,
    group_name: &str,
    bench_name: &str,
    query: &str,
    tables: &[&str],
) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let scale_dirs = tpch::scale_dirs();

    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    for (sf, dir) in &scale_dirs {
        group.bench_with_input(BenchmarkId::new(bench_name, sf), dir, |b, dir| {
            b.iter(|| {
                rt.block_on(async {
                    if !tpch::tables_exist(dir, tables) {
                        // Missing Parquet files: skip execution.
                        return;
                    }
                    let engine = krishiv_sql::SqlEngine::new();
                    for table in tables {
                        engine
                            .register_parquet(*table, format!("{dir}/{table}.parquet"))
                            .await
                            .ok();
                    }
                    let df = engine.sql(query).await;
                    if let Ok(df) = df {
                        let _ = df.collect().await;
                    }
                })
            })
        });
    }
    group.finish();
}

fn bench_tpch_q1(c: &mut Criterion) {
    // TPC-H Q1: pricing summary report
    bench_embedded_query(
        c,
        "tpch_q1",
        "q1_pricing_summary",
        tpch::Q1,
        tpch::Q1_TABLES,
    );
}

fn bench_tpch_q3(c: &mut Criterion) {
    // TPC-H Q3: shipping priority — join orders, lineitem, customer; filter by date and segment.
    bench_embedded_query(
        c,
        "tpch_q3",
        "q3_shipping_priority",
        tpch::Q3,
        tpch::Q3_TABLES,
    );
}

fn bench_tpch_q5(c: &mut Criterion) {
    // TPC-H Q5: local supplier volume — multi-table join with region filter.
    bench_embedded_query(
        c,
        "tpch_q5",
        "q5_local_supplier_volume",
        tpch::Q5,
        tpch::Q5_TABLES,
    );
}

fn bench_tpch_q6(c: &mut Criterion) {
    // TPC-H Q6: forecasting revenue change
    bench_embedded_query(
        c,
        "tpch_q6",
        "q6_forecasting_revenue",
        tpch::Q6,
        tpch::Q6_TABLES,
    );
}

fn bench_tpch_q10(c: &mut Criterion) {
    // TPC-H Q10: returned-item reporting — customer/orders/lineitem/nation join,
    // group by customer, top 20 by revenue.
    bench_embedded_query(
        c,
        "tpch_q10",
        "q10_returned_item_reporting",
        tpch::Q10,
        tpch::Q10_TABLES,
    );
}

fn bench_tpch_q18(c: &mut Criterion) {
    // TPC-H Q18: large-volume customer — customer/orders/lineitem with
    // HAVING SUM(l_quantity) > 300.
    bench_embedded_query(
        c,
        "tpch_q18",
        "q18_large_volume_customer",
        tpch::Q18,
        tpch::Q18_TABLES,
    );
}

criterion_group!(
    tpch_benches,
    bench_tpch_q1,
    bench_tpch_q3,
    bench_tpch_q5,
    bench_tpch_q6,
    bench_tpch_q10,
    bench_tpch_q18
);
criterion_main!(tpch_benches);
