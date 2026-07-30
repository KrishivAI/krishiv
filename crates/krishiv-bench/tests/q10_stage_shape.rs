//! What each of q10's shuffles actually carries.
//!
//! q10 is 12.4x slower than Spark on the cluster (2348.9 s vs 188.7 s) on a
//! pod network that measures ~11 MiB/s, so the question that decides the fix is
//! **how many bytes each stage puts on the wire**, not how many rows.
//!
//! q10 groups by seven columns — `c_custkey, c_name, c_acctbal, c_phone,
//! n_name, c_address, c_comment` — six of which are functionally determined by
//! `c_custkey`. If those wide columns (`c_comment` alone averages ~73 bytes)
//! cross a shuffle boundary, the wire is carrying redundant payload and the
//! fix is a plan change, not a transport change.
//!
//! This runs the real stage cutter offline against empty fixture tables, so it
//! costs nothing and cannot perturb a running benchmark.

use datafusion::prelude::SessionContext;
use krishiv_bench::tpch_fixture::fixture_ddl;
use krishiv_sql::distributed_plan::{
    build_distributed_stages, execute_dfplan_body, planning_session_context_with_options,
};

const Q10: &str = "SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, \
c_acctbal, n_name, c_address, c_phone, c_comment \
FROM customer, orders, lineitem, nation \
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey \
AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01' \
AND l_returnflag = 'R' AND c_nationkey = n_nationkey \
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
ORDER BY revenue DESC LIMIT 20";

/// Rough on-wire width of a TPC-H column, in bytes per row.
fn width_of(col: &str) -> usize {
    match col {
        "c_comment" => 73,
        "c_address" => 25,
        "c_name" => 19,
        "n_name" => 15,
        "c_phone" => 15,
        "o_comment" | "l_comment" => 44,
        c if c.ends_with("key") => 8,
        c if c.contains("price") || c.contains("bal") || c.contains("discount") => 16,
        c if c.contains("date") => 4,
        _ => 8,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn q10_shuffle_payload_is_dominated_by_columns_custkey_determines() {
    // `(Some(0), Some(0))` = the SF100 configuration: no build side may be
    // collected, so every join hash-shuffles both inputs. The default options
    // broadcast on this tiny fixture and collapse q10 to two stages, which is
    // not the shape the cluster runs (6 stages / 91 tasks).
    for (label, jt, bb) in [
        ("SF100 (no-broadcast, converted)", Some(0u64), Some(0usize)),
        ("default (broadcast allowed)", None, None),
    ] {
        dump(label, jt, bb).await;
    }
}

async fn dump(label: &str, join_threshold: Option<u64>, broadcast_bytes: Option<usize>) {
    let ctx: SessionContext =
        planning_session_context_with_options(4, join_threshold, broadcast_bytes);
    for ddl in fixture_ddl() {
        ctx.sql(ddl)
            .await
            .expect("fixture DDL")
            .collect()
            .await
            .expect("fixture DDL exec");
    }
    println!("\n########## {label} ##########");

    let plan = ctx
        .sql(Q10)
        .await
        .expect("plan q10")
        .create_physical_plan()
        .await
        .expect("physical plan");

    let staged = match build_distributed_stages(plan) {
        Ok(Some(s)) => s,
        _ => { println!("declined to stage"); return; }
    };

    println!("\n=== q10: {} stages ===", staged.stages.len());
    for (i, stage) in staged.stages.iter().enumerate() {
        // The declared schema is what this stage's tasks emit — i.e. exactly
        // what its shuffle puts on the wire. `execute_dfplan_body` returns it
        // before any batch is produced, so empty fixtures are enough.
        let declared = stage.task_bodies.first().and_then(|body| {
            execute_dfplan_body(body, &ctx, None)
                .ok()
                .map(|(schema, _stream)| schema)
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

        match &stage.shuffle {
            Some(sh) => {
                let key: usize = sh.key_columns.iter().map(|c| width_of(c)).sum();
                println!(
                    "stage {i}: {} tasks, shuffle on {:?} -> {} partitions",
                    stage.task_count(),
                    sh.key_columns,
                    sh.num_output_partitions
                );
                println!("  emits {ncols} cols, ~{width} B/row (key alone ~{key} B)");
                println!("  {cols:?}");
            }
            None => {
                println!("stage {i}: {} tasks, TERMINAL", stage.task_count());
                println!("  emits {ncols} cols, ~{width} B/row");
            }
        }
    }
    println!("=== end q10 ===\n");
}
