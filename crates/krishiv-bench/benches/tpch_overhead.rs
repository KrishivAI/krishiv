//! Engine-overhead microbenchmark (production-readiness audit §2b).
//!
//! Runs the same TPC-H queries through the three execution entry points and
//! makes the engine's scheduling/setup/serialization tax a tracked number:
//!
//! - `raw_df`: a bare `datafusion::SessionContext` — the floor. Anything the
//!   engine adds on top of this is Krishiv overhead, not query cost.
//! - `embedded`: `krishiv_sql::SqlEngine` — the embedded placement, i.e. the
//!   session-configuration layer only.
//! - `coordinated`: `krishiv_runtime::InProcessCluster::collect_batch_sql` —
//!   the distributed session entry point with single-executor placement
//!   (same caveats as the `tpch_distributed` bench target: inline fast path,
//!   no cross-executor shuffle).
//!
//! Every leg constructs its session/cluster inside the timed region, so the
//! per-query setup tax is part of what each leg measures; the *difference*
//! between legs at a given scale factor is the overhead budget. The tax is
//! additive, so it is most visible at SF1 where the query itself is cheap;
//! Sail's published TPC-H-derived numbers vs Spark are the external
//! reference for what a thin-tax DataFusion engine achieves.
//!
//! To run: cargo bench -p krishiv-bench --bench tpch_overhead
//! Data paths use the same scale ladder as `tpch_sf10`:
//!   - KRISHIV_TPCH_DATA_DIR_SF1   (~1 GB)
//!   - KRISHIV_TPCH_DATA_DIR_SF10  (~10 GB; KRISHIV_TPCH_DATA_DIR is a legacy alias)
//!   - KRISHIV_TPCH_DATA_DIR_SF100 (~100 GB)
//!
//! Unset scale factors are skipped with a notice on stderr.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use krishiv_bench::tpch;
use krishiv_runtime::{BatchSqlTable, InProcessCluster};
use std::path::PathBuf;
use std::time::Duration;

/// Queries tracked for the overhead budget: a scan-heavy aggregate (Q1), a
/// selective filter (Q6), and a three-table join (Q3) — enough shapes to
/// notice a leg regressing without re-running the whole ladder.
const QUERIES: &[(&str, &str, &[&str])] = &[
    ("q1", tpch::Q1, tpch::Q1_TABLES),
    ("q6", tpch::Q6, tpch::Q6_TABLES),
    ("q3", tpch::Q3, tpch::Q3_TABLES),
];

fn bench_overhead_query(c: &mut Criterion, name: &str, query: &str, tables: &[&str]) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let scale_dirs = tpch::scale_dirs();

    let mut group = c.benchmark_group(format!("engine_overhead_{name}"));
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    for (sf, dir) in &scale_dirs {
        if !tpch::tables_exist(dir, tables) {
            #[allow(clippy::print_stderr)]
            {
                eprintln!("skipping engine_overhead_{name} at {sf}: missing Parquet under {dir}");
            }
            continue;
        }

        group.bench_with_input(BenchmarkId::new("raw_df", sf), dir, |b, dir| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    for table in tables {
                        ctx.register_parquet(
                            *table,
                            format!("{dir}/{table}.parquet"),
                            ParquetReadOptions::default(),
                        )
                        .await
                        .expect("register parquet with raw DataFusion");
                    }
                    let df = ctx.sql(query).await.expect("plan with raw DataFusion");
                    df.collect().await.expect("collect with raw DataFusion")
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("embedded", sf), dir, |b, dir| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = krishiv_sql::SqlEngine::new();
                    for table in tables {
                        engine
                            .register_parquet(*table, format!("{dir}/{table}.parquet"))
                            .await
                            .expect("register parquet with embedded engine");
                    }
                    let df = engine.sql(query).await.expect("plan with embedded engine");
                    df.collect().await.expect("collect with embedded engine")
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("coordinated", sf), dir, |b, dir| {
            b.iter(|| {
                let cluster = InProcessCluster::new().expect("in-process cluster creation");
                let registrations: Vec<BatchSqlTable> = tables
                    .iter()
                    .map(|table| BatchSqlTable {
                        table_name: (*table).to_string(),
                        path: PathBuf::from(format!("{dir}/{table}.parquet")),
                        ..Default::default()
                    })
                    .collect();
                cluster
                    .collect_batch_sql(query, &registrations, false)
                    .expect("collect through in-process cluster")
            })
        });
    }
    group.finish();
}

fn bench_engine_overhead(c: &mut Criterion) {
    for (name, query, tables) in QUERIES {
        bench_overhead_query(c, name, query, tables);
    }
}

criterion_group!(overhead_benches, bench_engine_overhead);
criterion_main!(overhead_benches);
