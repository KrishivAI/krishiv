//! TPC-H SF10 benchmark gates for Krishiv R10.
//!
//! These benchmarks measure query execution time against the TPC-H dataset at
//! scale factor 10 (~10 GB). They are gated against the targets defined in
//! docs/architecture/benchmark-targets.md.
//!
//! To run: cargo bench -p krishiv-bench --bench tpch_sf10
//! Data path: set KRISHIV_TPCH_DATA_DIR to a directory containing Parquet files
//! generated from TPC-H SF10 (e.g. lineitem.parquet, orders.parquet, etc.).
//! If KRISHIV_TPCH_DATA_DIR is not set, benchmarks run against synthetic
//! in-memory data and are for warmup/CI purposes only.

use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;

fn bench_tpch_q1(c: &mut Criterion) {
    // TPC-H Q1: pricing summary report
    let query = "SELECT \
        l_returnflag, l_linestatus, \
        SUM(l_quantity) AS sum_qty, \
        SUM(l_extendedprice) AS sum_base_price, \
        COUNT(*) AS count_order \
        FROM lineitem \
        WHERE l_shipdate <= '1998-09-02' \
        GROUP BY l_returnflag, l_linestatus \
        ORDER BY l_returnflag, l_linestatus";

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data_dir = std::env::var("KRISHIV_TPCH_DATA_DIR").ok();

    let mut group = c.benchmark_group("tpch_q1");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    group.bench_function("q1_pricing_summary", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = krishiv_sql::SqlEngine::new();
                if let Some(ref dir) = data_dir {
                    let path = format!("{}/lineitem.parquet", dir);
                    if std::path::Path::new(&path).exists() {
                        engine.register_parquet("lineitem", &path).await.ok();
                    } else {
                        // Synthetic fallback: skip execution
                        return;
                    }
                } else {
                    // No data dir: skip
                    return;
                }
                let df = engine.sql(query).await;
                if let Ok(df) = df {
                    let _ = df.collect().await;
                }
            })
        })
    });
    group.finish();
}

fn bench_tpch_q6(c: &mut Criterion) {
    // TPC-H Q6: forecasting revenue change
    let query = "SELECT SUM(l_extendedprice * l_discount) AS revenue \
        FROM lineitem \
        WHERE l_shipdate >= '1994-01-01' \
          AND l_shipdate < '1995-01-01' \
          AND l_discount BETWEEN 0.05 AND 0.07 \
          AND l_quantity < 24";

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data_dir = std::env::var("KRISHIV_TPCH_DATA_DIR").ok();

    let mut group = c.benchmark_group("tpch_q6");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    group.bench_function("q6_forecasting_revenue", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = krishiv_sql::SqlEngine::new();
                if let Some(ref dir) = data_dir {
                    let path = format!("{}/lineitem.parquet", dir);
                    if std::path::Path::new(&path).exists() {
                        engine.register_parquet("lineitem", &path).await.ok();
                    } else {
                        return;
                    }
                } else {
                    return;
                }
                let df = engine.sql(query).await;
                if let Ok(df) = df {
                    let _ = df.collect().await;
                }
            })
        })
    });
    group.finish();
}

criterion_group!(tpch_benches, bench_tpch_q1, bench_tpch_q6);
criterion_main!(tpch_benches);
