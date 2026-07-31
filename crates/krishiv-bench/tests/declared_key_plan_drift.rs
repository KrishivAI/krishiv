//! Does declaring a primary key change a query's PHYSICAL plan?
//!
//! # Why this exists
//!
//! Declaring TPC-H's primary keys is what enables the late-materialisation
//! join-back (`krishiv_sql::late_materialize`, 14.4x on q10 at SF100). But at
//! SF100 two queries then failed with `Resources exhausted: HashJoinInput` —
//! q10 with the rewrite disabled, and q21 with it enabled. q21's `GROUP BY
//! s_name` has a single grouping column, so the rewrite *declines* on it: if
//! the declaration is nevertheless changing q21's plan, it is doing so through
//! some other mechanism, and that mechanism is the thing to find.
//!
//! `Resources exhausted: HashJoinInput` is the exact signature of
//! `SpillableJoinSelection` not converting a join it should have — the failure
//! that rule was written for (see its module docs). That rule decides from the
//! *estimated build size*, so anything that moves an estimate can silently
//! flip it.
//!
//! This test does not assert a verdict. It **reports** which queries change
//! plan when a key is declared, so the blast radius is a list rather than a
//! guess. Run it with `--nocapture`.

// Integration-test crate: helpers run outside `#[test]` fns, so clippy.toml's
// `allow-unwrap-in-tests` does not reach them.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]

use datafusion::prelude::SessionContext;
use krishiv_bench::tpch_fixture::{declare_fixture_primary_keys, fixture_ddl};
use krishiv_bench::tpch_queries::TPCH_QUERIES;
use krishiv_sql::distributed_plan::planning_session_context_with_options;

/// The SF100 configuration: no build side may be collected, so every join
/// hash-shuffles both inputs. The default options broadcast on this tiny
/// fixture, which is not the shape the cluster runs.
async fn context(declare_keys: bool) -> SessionContext {
    let ctx = planning_session_context_with_options(4, Some(0), Some(0));
    for ddl in fixture_ddl() {
        ctx.sql(ddl)
            .await
            .unwrap_or_else(|e| panic!("fixture DDL: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("fixture DDL exec: {e}"));
    }
    if declare_keys {
        declare_fixture_primary_keys(&ctx)
            .await
            .unwrap_or_else(|e| panic!("declaring keys: {e}"));
    }
    ctx
}

async fn physical_plan(ctx: &SessionContext, sql: &str) -> Option<String> {
    let plan = ctx.sql(sql).await.ok()?.create_physical_plan().await.ok()?;
    Some(format!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn report_which_queries_change_plan_when_a_key_is_declared() {
    let plain = context(false).await;
    let keyed = context(true).await;

    let mut changed = Vec::new();
    let mut unchanged = Vec::new();
    for query in TPCH_QUERIES {
        let sql = query.sql_at_scale(1.0);
        let (a, b) = (
            physical_plan(&plain, &sql).await,
            physical_plan(&keyed, &sql).await,
        );
        match (a, b) {
            (Some(a), Some(b)) if a == b => unchanged.push(query.id),
            (Some(a), Some(b)) => {
                // Count the join operators on each side: a changed join count
                // or mode is what would move a SpillableJoinSelection decision.
                let joins = |p: &str| {
                    (
                        p.matches("HashJoinExec").count(),
                        p.matches("SortMergeJoinExec").count(),
                        p.matches("mode=CollectLeft").count(),
                        p.matches("mode=Partitioned").count(),
                    )
                };
                changed.push((query.id, joins(&a), joins(&b)));
            }
            _ => println!("{}: did not plan on one side", query.id),
        }
    }

    println!("\n=== declaring a primary key: physical-plan drift ===");
    println!("unchanged ({}): {unchanged:?}", unchanged.len());
    println!("\nchanged ({}):", changed.len());
    println!(
        "{:<6} {:>28}   {:>28}",
        "query", "without keys (hash,smj,cl,part)", "with keys"
    );
    for (id, a, b) in &changed {
        let flag = if a != b { "  <-- JOIN SHAPE MOVED" } else { "" };
        println!("{id:<6} {a:>28?}   {b:>28?}{flag}");
    }
    println!("=== end ===\n");
}
