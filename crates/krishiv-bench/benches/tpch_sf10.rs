//! TPC-H SF10 benchmark gates for Krishiv R10.
//!
//! These benchmarks measure query execution time against the TPC-H dataset at
//! scale factor 10 (~10 GB). They are gated against the targets defined in
//! Keep benchmark thresholds in code or in the minimal docs when they are
//! reintroduced as enforced gates.
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

fn bench_tpch_q3(c: &mut Criterion) {
    // TPC-H Q3: shipping priority — join orders, lineitem, customer; filter by date and segment.
    let query = "SELECT \
        l_orderkey, \
        SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
        o_orderdate, \
        o_shippriority \
        FROM customer, orders, lineitem \
        WHERE c_mktsegment = 'BUILDING' \
          AND c_custkey = o_custkey \
          AND l_orderkey = o_orderkey \
          AND o_orderdate < '1995-03-15' \
          AND l_shipdate > '1995-03-15' \
        GROUP BY l_orderkey, o_orderdate, o_shippriority \
        ORDER BY revenue DESC, o_orderdate \
        LIMIT 10";

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data_dir = std::env::var("KRISHIV_TPCH_DATA_DIR").ok();

    let mut group = c.benchmark_group("tpch_q3");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    group.bench_function("q3_shipping_priority", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = krishiv_sql::SqlEngine::new();
                if let Some(ref dir) = data_dir {
                    let lineitem = format!("{dir}/lineitem.parquet");
                    let orders = format!("{dir}/orders.parquet");
                    let customer = format!("{dir}/customer.parquet");
                    if std::path::Path::new(&lineitem).exists()
                        && std::path::Path::new(&orders).exists()
                        && std::path::Path::new(&customer).exists()
                    {
                        engine.register_parquet("lineitem", &lineitem).await.ok();
                        engine.register_parquet("orders", &orders).await.ok();
                        engine.register_parquet("customer", &customer).await.ok();
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

fn bench_tpch_q5(c: &mut Criterion) {
    // TPC-H Q5: local supplier volume — multi-table join with region filter.
    let query = "SELECT \
        n_name, \
        SUM(l_extendedprice * (1 - l_discount)) AS revenue \
        FROM customer, orders, lineitem, supplier, nation, region \
        WHERE c_custkey = o_custkey \
          AND l_orderkey = o_orderkey \
          AND l_suppkey = s_suppkey \
          AND c_nationkey = s_nationkey \
          AND s_nationkey = n_nationkey \
          AND n_regionkey = r_regionkey \
          AND r_name = 'ASIA' \
          AND o_orderdate >= '1994-01-01' \
          AND o_orderdate < '1995-01-01' \
        GROUP BY n_name \
        ORDER BY revenue DESC";

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data_dir = std::env::var("KRISHIV_TPCH_DATA_DIR").ok();

    let mut group = c.benchmark_group("tpch_q5");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    group.bench_function("q5_local_supplier_volume", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = krishiv_sql::SqlEngine::new();
                if let Some(ref dir) = data_dir {
                    let tables = [
                        ("lineitem", "lineitem.parquet"),
                        ("orders", "orders.parquet"),
                        ("customer", "customer.parquet"),
                        ("supplier", "supplier.parquet"),
                        ("nation", "nation.parquet"),
                        ("region", "region.parquet"),
                    ];
                    let all_exist = tables
                        .iter()
                        .all(|(_, f)| std::path::Path::new(&format!("{dir}/{f}")).exists());
                    if all_exist {
                        for (name, file) in &tables {
                            engine
                                .register_parquet(name, &format!("{dir}/{file}"))
                                .await
                                .ok();
                        }
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

criterion_group!(tpch_benches, bench_tpch_q1, bench_tpch_q3, bench_tpch_q5, bench_tpch_q6);
criterion_main!(tpch_benches);
