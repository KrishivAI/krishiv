//! TPC-DS smoke bench.
//!
//! Runs each bundled TPC-DS query end-to-end against the embedded
//! `SqlEngine` so the planner, AQE, CBO, and connector layers see
//! real star-schema / snowflake-schema workloads. Used as a
//! regression gate by `scripts/bench-tpcds-gate.sh` (see
//! `docs/benchmarks/tpcds-gate.md`).
//!
//! To run: `cargo bench -p krishiv-bench --bench tpcds_smoke`
//! Data path: `KRISHIV_TPCDS_DATA_DIR` env var pointing to the
//! directory of Parquet files.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use krishiv_bench::tpcds;
use std::time::Duration;

/// Run every bundled TPC-DS query through the embedded `SqlEngine`.
fn bench_tpcds_queries(c: &mut Criterion) {
    let Some(dir) = tpcds::data_dir() else {
        eprintln!("skipping TPC-DS smoke: KRISHIV_TPCDS_DATA_DIR not set");
        return;
    };
    if !std::path::Path::new(&dir).is_dir() {
        eprintln!("skipping TPC-DS smoke: data dir '{dir}' is not a directory");
        return;
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("tpcds_smoke");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    for (name, query, tables) in tpcds::ALL_QUERIES {
        if !tpcds::tables_exist(&dir, tables) {
            eprintln!(
                "skipping TPC-DS {name}: missing Parquet for at least one table in {tables:?}"
            );
            continue;
        }
        group.bench_with_input(BenchmarkId::new("query", name), &dir, |b, dir| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = krishiv_sql::SqlEngine::new();
                    for table in *tables {
                        if engine
                            .register_parquet(*table, format!("{dir}/{table}.parquet"))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    if let Ok(df) = engine.sql(query).await {
                        let _ = df.collect().await;
                    }
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_tpcds_queries);
criterion_main!(benches);
