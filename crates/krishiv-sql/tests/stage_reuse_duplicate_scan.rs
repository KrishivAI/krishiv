//! Does stage reuse actually collapse TPC-H q18's duplicated `lineitem` scan?
//!
//! # Why this exists
//!
//! `dedupe_identical_stages` was written for exactly three measured cases
//! (2026-07-30): q18 scans `lineitem` twice *unfiltered* (600M rows), q21 scans
//! it twice, q2 scans `partsupp` twice. On the SF100 sweep of `fast-6f586954`
//! the rule fired **twice across all 22 queries**, removing one stage each
//! time — so at most one of those three is actually collapsing.
//!
//! The rule keys on the **encoded plan bytes**, which is the right identity
//! (plan text once collapsed two stages producing different rows). But it means
//! anything that encodes per-instance state — a dynamic filter placeholder, a
//! file-group ordering — makes two semantically identical scans encode
//! differently and silently blocks the collapse.
//!
//! This test reproduces q18's shape over **real parquet files**, because that
//! is the path SF100 takes: a `ListingTable` scan under a partitioned hash
//! join, where DataFusion attaches `DynamicFilter [ empty ]` to the probe side.
//! A MemTable fixture cannot show this — the same blind spot that let
//! `declared_key_plan_drift.rs` miss the statistics regression.
//!
//! It reports rather than asserts a fixed count: the point is to see *whether*
//! the duplicate collapses and, when it does not, what differs.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use datafusion::prelude::SessionContext;
use krishiv_sql::distributed_plan::{
    build_distributed_stages, planning_session_context_with_options,
};

/// Write the two tables q18's shape needs, as real parquet.
async fn write_tables(dir: &std::path::Path) -> (String, String) {
    let ctx = SessionContext::new();
    let orders = dir.join("orders.parquet");
    let lineitem = dir.join("lineitem.parquet");
    let (orders, lineitem) = (
        orders.to_str().unwrap().to_owned(),
        lineitem.to_str().unwrap().to_owned(),
    );
    ctx.sql(&format!(
        "COPY (SELECT v AS o_orderkey, v % 7 AS o_custkey \
         FROM UNNEST(range(0, 400)) AS u(v)) TO '{orders}' STORED AS PARQUET"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    ctx.sql(&format!(
        "COPY (SELECT v % 400 AS l_orderkey, v AS l_linenumber, \
         CAST(v % 13 AS DOUBLE) AS l_quantity \
         FROM UNNEST(range(0, 2000)) AS u(v)) TO '{lineitem}' STORED AS PARQUET"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    (orders, lineitem)
}

/// The SF100 configuration: nothing may be broadcast, so every join
/// hash-partitions and the probe side carries a dynamic filter.
async fn staged_context(orders: &str, lineitem: &str) -> SessionContext {
    let ctx = planning_session_context_with_options(4, Some(0), Some(0));
    for (name, path) in [("orders", orders), ("lineitem", lineitem)] {
        ctx.register_parquet(
            name,
            path,
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .unwrap_or_else(|e| panic!("register {name}: {e}"));
    }
    ctx
}

/// q18's shape: `lineitem` is scanned once for the join and once for the
/// `IN (SELECT ... GROUP BY ... HAVING ...)` subquery.
const Q18_SHAPE: &str = "\
    SELECT o.o_orderkey, sum(l.l_quantity) AS total \
    FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey \
    WHERE o.o_orderkey IN ( \
        SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 20 \
    ) \
    GROUP BY o.o_orderkey";

#[tokio::test(flavor = "multi_thread")]
async fn report_whether_the_duplicated_lineitem_scan_collapses() {
    // The rule reads its flag from the environment, and this workspace forbids
    // `unsafe`, so `set_var` is not available: the flag comes from the caller
    // (`KRISHIV_STAGE_REUSE=1 cargo test ...`). Saying so out loud beats a
    // silent pass that proves nothing.
    assert!(
        krishiv_sql::distributed_plan::stage_reuse_enabled(),
        "run this with KRISHIV_STAGE_REUSE=1 — without it the rule is off and \
         this test would 'pass' while measuring nothing"
    );

    let dir = tempfile::tempdir().unwrap();
    let (orders, lineitem) = write_tables(dir.path()).await;
    let ctx = staged_context(&orders, &lineitem).await;

    let plan = ctx
        .sql(Q18_SHAPE)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let staged = build_distributed_stages(plan)
        .expect("staging must not error")
        .expect("this shape must stage");

    // Decode each stage rather than substring-matching its body: the body is
    // base64 protobuf, so `contains("lineitem")` is a coin flip that reports a
    // confident number either way. (My first version did exactly that and
    // "found" one lineitem stage in a plan that has two.)
    let task_ctx = krishiv_sql::distributed_plan::fragment_decode_session_context().task_ctx();
    let codec = krishiv_sql::distributed_plan::KrishivPhysicalCodec::coordinator();

    println!(
        "\n=== q18-shape staged plan: {} stages ===",
        staged.stages.len()
    );
    let mut scans_of: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (index, stage) in staged.stages.iter().enumerate() {
        let Some(body) = stage.task_bodies.first() else {
            continue;
        };
        let text = match krishiv_sql::distributed_plan::decode_dfplan_task(body, &task_ctx, &codec)
        {
            Ok((_, plan)) => datafusion::physical_plan::displayable(plan.as_ref())
                .indent(false)
                .to_string(),
            Err(error) => format!("<undecodable: {error}>"),
        };
        let tables: Vec<&str> = ["orders", "lineitem"]
            .into_iter()
            .filter(|t| text.contains(&format!("{t}.parquet")))
            .collect();
        for table in &tables {
            *scans_of.entry((*table).to_owned()).or_default() += 1;
        }
        println!(
            "stage {index:>2}  tasks={:<3} shuffle={:<5} scans={tables:?}  dynfilter={}",
            stage.task_bodies.len(),
            stage.shuffle.is_some(),
            text.matches("DynamicFilter").count(),
        );
    }
    // When the duplicate survives, the interesting question is what differs.
    // Print the two lineitem stages side by side rather than asserting a guess.
    let lineitem_stages: Vec<(usize, String)> = staged
        .stages
        .iter()
        .enumerate()
        .filter_map(|(index, stage)| {
            let body = stage.task_bodies.first()?;
            let (_, plan) =
                krishiv_sql::distributed_plan::decode_dfplan_task(body, &task_ctx, &codec).ok()?;
            let text = datafusion::physical_plan::displayable(plan.as_ref())
                .indent(true)
                .to_string();
            text.contains("lineitem.parquet").then_some((index, text))
        })
        .collect();
    if lineitem_stages.len() > 1 {
        println!("\n--- the two lineitem stages, verbatim ---");
        for (index, text) in &lineitem_stages {
            println!("### stage {index}\n{text}");
        }
        let (a, b) = (&lineitem_stages[0].1, &lineitem_stages[1].1);
        let differing: Vec<(usize, &str, &str)> = a
            .lines()
            .zip(b.lines())
            .enumerate()
            .filter(|(_, (x, y))| x != y)
            .map(|(n, (x, y))| (n, x, y))
            .collect();
        println!("--- lines that differ ({}) ---", differing.len());
        for (n, x, y) in differing {
            println!("  line {n}\n    A: {}\n    B: {}", x.trim(), y.trim());
        }
    }

    println!("\nscans per table across stages: {scans_of:?}");
    println!(
        "q18's duplicate is collapsed when lineitem == 1; == 2 means the two \
         identical scans encoded differently and reuse declined."
    );
    println!("=== end ===\n");
}
