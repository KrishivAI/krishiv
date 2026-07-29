// This binary's entire output is a plan dump on stdout; it is a diagnostic
// pipe source, not a service.
#![allow(clippy::print_stdout, clippy::print_stderr)]

//! Print the distributed stage breakdown a TPC-H query gets, exactly as the
//! coordinator plans it.
//!
//! This exists because "which operator is in which fragment" is the first
//! question any distributed-execution memory or performance investigation
//! asks, and reading it off a live cluster means waiting for a multi-minute
//! SF100 run and then correlating logs. Planning is metadata-only — it reads
//! parquet footers, never row groups — so the same answer is available in
//! seconds against the same dataset.
//!
//! It uses [`krishiv_sql::distributed_plan::build_distributed_stages`], the
//! production stage builder, on a [`planning_session_context`] with the
//! production `target_partitions`. A dump from here is what the cluster runs,
//! not a model of it.
//!
//! Usage:
//! ```text
//! TP=18 cargo run -p krishiv-bench --bin stage_dump --release -- /data/tpch-sf100 q8 q9
//! ```
//! With no query ids, every query in the corpus is dumped.

use datafusion::physical_plan::displayable;
use krishiv_bench::tpch_queries::TPCH_QUERIES;
use krishiv_sql::distributed_plan::{
    build_distributed_stages, planning_session_context, register_python_udf_signatures_and_strip,
};
use std::sync::Arc;

/// TPC-H generators emit either `<table>.parquet` or a `<table>/` directory of
/// shards; accept both so one flag points at any scale factor.
fn table_path(dir: &str, table: &str) -> String {
    let nested = format!("{dir}/{table}");
    if std::path::Path::new(&nested).is_dir() {
        format!("{nested}/")
    } else {
        format!("{dir}/{table}.parquet")
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let Some(dir) = args.next() else {
        eprintln!("usage: stage_dump <data_dir> [query_id ...]   (env TP=<target_partitions>)");
        return Ok(());
    };
    // Match the cluster: production resolves this from live slots, and the
    // 3-node cert cluster resolves it to 18.
    let target_partitions: usize = std::env::var("TP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(18);
    // Plans are dumped to explain cluster runs, which are SF100; Q11's
    // threshold is scale-dependent, so a mismatched scale would dump a plan
    // for SQL the cluster never ran.
    let scale_factor: f64 = std::env::var("SF")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &f64| v.is_finite() && *v > 0.0)
        .unwrap_or(100.0);
    let ids: Vec<String> = args.collect();

    for query in TPCH_QUERIES
        .iter()
        .filter(|q| ids.is_empty() || ids.iter().any(|i| i == q.id))
    {
        println!(
            "\n================ {} ({}) ================",
            query.id, query.name
        );
        let ctx = planning_session_context(target_partitions);
        for table in query.tables {
            let path = table_path(&dir, table);
            if let Err(error) = ctx
                .register_parquet(
                    *table,
                    &path,
                    datafusion::prelude::ParquetReadOptions::default(),
                )
                .await
            {
                println!("  register {table} @ {path} failed: {error}");
                continue;
            }
        }
        let bound = query.sql_at_scale(scale_factor);
        let sql = register_python_udf_signatures_and_strip(&ctx, &bound)?;
        let df = match ctx.sql(&sql).await {
            Ok(df) => df,
            Err(error) => {
                println!("  sql failed: {error}");
                continue;
            }
        };
        let plan = match df.create_physical_plan().await {
            Ok(plan) => plan,
            Err(error) => {
                println!("  physical planning failed: {error}");
                continue;
            }
        };
        println!("---- whole physical plan ----");
        println!("{}", displayable(plan.as_ref()).indent(false));

        match build_distributed_stages(Arc::clone(&plan)) {
            Ok(Some(staged)) => {
                println!(
                    "---- staged: {} stages, {} tasks total ----",
                    staged.stages.len(),
                    staged.stages.iter().map(|s| s.task_count()).sum::<usize>()
                );
                for (index, stage) in staged.stages.iter().enumerate() {
                    println!(
                        "  stage {index}: tasks={} shuffle={:?} upstreams={:?}",
                        stage.task_count(),
                        stage
                            .shuffle
                            .as_ref()
                            .map(|s| (s.key_columns.clone(), s.num_output_partitions)),
                        stage.upstream_stage_indexes
                    );
                }
            }
            // `build_distributed_stages` refuses any plan whose fragments are
            // not partition-independent, so reaching this arm without an error
            // is itself the proof that no fragment contains a RepartitionExec.
            Ok(None) => println!("---- staged: DECLINED (fallback to single task) ----"),
            Err(error) => println!("---- staged: ERROR {error} ----"),
        }
    }
    Ok(())
}
