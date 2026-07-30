//! Stage shape of every query Spark still beats us on.
//!
//! Measured 2026-07-30 (`fast-2602c89e` vs the Spark SF100 baseline):
//!
//! ```text
//!   q10  2348.9s vs  188.7s  12.45x
//!   q7    278.0s vs  173.5s   1.60x
//!   q21   678.6s vs  432.1s   1.57x
//!   q16    61.6s vs   43.4s   1.42x
//!   q22    73.5s vs   55.1s   1.33x
//!   q18   400.6s vs  340.5s   1.18x
//! ```
//!
//! q10 was diagnosed by measurement, not inspection: 63.6 GB of shuffle served
//! against a plan implying ~3.5 GB, no skew (8.3 of 9 slots busy), no wire cap
//! (77.5 MB/s sustained). The mechanism was a wide build side admitted by the
//! width-blind broadcast row ceiling.
//!
//! This prints the same evidence for the other five so the next fix starts from
//! shape rather than from a guess. **The bytes-per-row figures are the load-
//! bearing output** — they are schema-driven and therefore valid regardless of
//! the fixture's statistics.
//!
//! # What this cannot tell you
//!
//! The fixtures are empty, so every cardinality is degenerate and the *plan
//! shape* here is not necessarily the shape SF100 produces — the cluster's q10
//! is 6 stages where this prints 8. Read the widths and the shuffle keys; do
//! not read the join order as gospel.

use datafusion::prelude::SessionContext;
use krishiv_bench::tpch_fixture::fixture_ddl;
use krishiv_bench::tpch_queries::TPCH_QUERIES;
use krishiv_sql::distributed_plan::{
    build_distributed_stages, execute_dfplan_body, planning_session_context_with_options,
};

/// Queries Spark still wins, worst ratio first.
const LOSING: &[(&str, f64)] = &[
    ("q10", 12.45),
    ("q7", 1.60),
    ("q21", 1.57),
    ("q16", 1.42),
    ("q22", 1.33),
    ("q18", 1.18),
];

/// Rough on-wire width of a TPC-H column, in bytes per row.
fn width_of(col: &str) -> usize {
    let c = col.to_ascii_lowercase();
    match () {
        _ if c.contains("comment") => 60,
        _ if c.contains("address") => 25,
        _ if c.contains("name") => 20,
        _ if c.contains("phone") => 15,
        _ if c.contains("mktsegment") || c.contains("brand") || c.contains("container") => 12,
        _ if c.contains("type") || c.contains("mfgr") => 20,
        _ if c.contains("flag") || c.contains("status") || c.contains("priority") => 8,
        _ if c.ends_with("key") => 8,
        _ if c.contains("price") || c.contains("bal") || c.contains("discount")
            || c.contains("tax") || c.contains("cost") || c.contains("revenue") => 16,
        _ if c.contains("date") => 4,
        _ => 8,
    }
}

/// A stage carrying more than this per row is worth looking at on a cluster
/// whose pod network is the binding constraint.
const WIDE_ROW_BYTES: usize = 100;

#[tokio::test(flavor = "multi_thread")]
async fn print_stage_shape_for_every_query_spark_wins() {
    let ctx: SessionContext = planning_session_context_with_options(4, Some(0), Some(0));
    for ddl in fixture_ddl() {
        ctx.sql(ddl)
            .await
            .expect("fixture DDL")
            .collect()
            .await
            .expect("fixture DDL exec");
    }

    println!("\n================ stage shape: queries Spark wins ================");
    for (name, ratio) in LOSING {
        let Some(query) = TPCH_QUERIES.iter().find(|q| q.id == *name) else {
            println!("\n### {name}: not in corpus");
            continue;
        };
        let sql = query.sql_at_scale(1.0);

        let plan = match ctx.sql(&sql).await {
            Ok(df) => match df.create_physical_plan().await {
                Ok(p) => p,
                Err(e) => {
                    println!("\n### {name} ({ratio}x): physical plan failed: {e}");
                    continue;
                }
            },
            Err(e) => {
                println!("\n### {name} ({ratio}x): plan failed: {e}");
                continue;
            }
        };

        let staged = match build_distributed_stages(plan) {
            Ok(Some(s)) => s,
            _ => {
                println!("\n### {name} ({ratio}x): declined to stage (no exchange to cut)");
                continue;
            }
        };

        println!("\n### {name} — Spark {ratio}x faster — {} stages", staged.stages.len());
        let mut widest = 0usize;
        for (i, stage) in staged.stages.iter().enumerate() {
            let declared = stage.task_bodies.first().and_then(|b| {
                execute_dfplan_body(b, &ctx, None).ok().map(|(s, _)| s)
            });
            let (ncols, width, cols) = match &declared {
                Some(s) => {
                    let cols: Vec<String> =
                        s.fields().iter().map(|f| f.name().to_string()).collect();
                    let w: usize = cols.iter().map(|c| width_of(c)).sum();
                    (cols.len(), w, cols)
                }
                None => (0, 0, Vec::new()),
            };
            widest = widest.max(width);
            let flag = if width > WIDE_ROW_BYTES { "  <-- WIDE" } else { "" };
            match &stage.shuffle {
                Some(sh) => println!(
                    "  s{i}: {:>3} tasks  key={:?} -> {} parts   {ncols} cols ~{width} B/row{flag}\n       {cols:?}",
                    stage.task_count(),
                    sh.key_columns,
                    sh.num_output_partitions
                ),
                None => println!(
                    "  s{i}: {:>3} tasks  TERMINAL              {ncols} cols ~{width} B/row",
                    stage.task_count()
                ),
            }
        }
        println!("  widest shuffle payload: ~{widest} B/row");
    }
    println!("\n================ end ================\n");
}
