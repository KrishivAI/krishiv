// A verification CLI: its report on stdout/stderr is the whole deliverable.
#![allow(clippy::print_stdout, clippy::print_stderr)]

//! Execute every TPC-H query once and report which succeed.
//!
//! Timing an unrunnable query is worse than not timing it: the benchmark
//! silently drops it and the published number quietly covers less than it
//! claims. This binary is the gate that runs before any measurement — it
//! executes all 22 against a real dataset and prints a per-query verdict with
//! row counts, so "22 queries" is a checked statement.
//!
//! Usage:
//!   KRISHIV_TPCH_DATA_DIR_SF1=/path/to/sf1 \
//!     cargo run -p krishiv-bench --bin tpch_verify --release
//!
//! `KRISHIV_TPCH_ONLY=q10,q21` restricts the run to named queries. Diagnosing
//! one slow query should not cost a full 22-query sweep, and at SF100 that
//! difference is hours.
//!
//! Exit code is the number of queries that failed (0 = all ran).

use std::collections::BTreeSet;
use std::time::Instant;

use krishiv_bench::tpch_queries::TPCH_QUERIES;

/// TPC-H generators emit either `<table>.parquet` or a `<table>/` directory of
/// shards. Only the first was accepted here, so this binary — the gate that is
/// supposed to run before any measurement — could not read the sharded layout
/// every scale factor above SF1 is generated in.
fn table_path(dir: &str, table: &str) -> String {
    let nested = format!("{dir}/{table}");
    if std::path::Path::new(&nested).is_dir() {
        format!("{nested}/")
    } else {
        format!("{dir}/{table}.parquet")
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    let dir = std::env::var("KRISHIV_TPCH_DATA_DIR_SF1")
        .or_else(|_| std::env::var("KRISHIV_TPCH_DATA_DIR_SF10"))
        .or_else(|_| std::env::var("KRISHIV_TPCH_DATA_DIR"))
        .unwrap_or_else(|_| {
            eprintln!("set KRISHIV_TPCH_DATA_DIR_SF1 (or _SF10) to a TPC-H parquet directory");
            std::process::exit(2);
        });

    // Q11's threshold is scale-dependent; the env var names the scale so the
    // verifier checks the same SQL the runner will execute.
    let scale_factor: f64 = std::env::var("KRISHIV_TPCH_SCALE_FACTOR")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &f64| v.is_finite() && *v > 0.0)
        .unwrap_or(1.0);

    // An explicit allow-list, so "verify everything" stays the default and
    // narrowing is always a visible choice in the command line.
    let only: BTreeSet<String> = std::env::var("KRISHIV_TPCH_ONLY")
        .unwrap_or_default()
        .split(',')
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty())
        .collect();

    let mut failures = 0usize;
    let mut failed_ids: BTreeSet<&str> = BTreeSet::new();
    let mut ran = 0usize;

    for query in TPCH_QUERIES {
        if !only.is_empty() && !only.contains(query.id) {
            continue;
        }
        ran += 1;
        // A fresh engine per query so one query's registrations cannot mask a
        // missing declaration in the next.
        let engine = krishiv_sql::SqlEngine::new();
        let mut setup_failed = None;
        for table in query.tables {
            if let Err(error) = engine
                .register_parquet(*table, table_path(&dir, table))
                .await
            {
                setup_failed = Some(format!("register {table}: {error}"));
                break;
            }
        }
        if let Some(reason) = setup_failed {
            println!("FAIL {:>3} {:<32} {reason}", query.id, query.name);
            failures += 1;
            failed_ids.insert(query.id);
            continue;
        }

        let started = Instant::now();
        match engine.sql(&query.sql_at_scale(scale_factor)[..]).await {
            Err(error) => {
                println!("FAIL {:>3} {:<32} plan: {error}", query.id, query.name);
                failures += 1;
                failed_ids.insert(query.id);
            }
            Ok(df) => match df.collect().await {
                Err(error) => {
                    println!("FAIL {:>3} {:<32} execute: {error}", query.id, query.name);
                    failures += 1;
                    failed_ids.insert(query.id);
                }
                Ok(batches) => {
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    println!(
                        "ok   {:>3} {:<32} {rows:>7} rows  {:>8.1} ms",
                        query.id,
                        query.name,
                        started.elapsed().as_secs_f64() * 1000.0
                    );
                }
            },
        }
    }

    println!("\n{} / {ran} queries executed", ran - failures);
    if failures > 0 {
        println!("failed: {failed_ids:?}");
    }
    std::process::ExitCode::from(u8::try_from(failures).unwrap_or(u8::MAX))
}
