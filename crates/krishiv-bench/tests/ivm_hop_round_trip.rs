//! Can a sub-plan of a real query be published as an incremental view?
//!
//! An automatic view-DAG decomposer works by cutting a multi-operator query
//! into single-operator **hops**, each registered as its own incremental view.
//! A view is defined by SQL text, so every hop has to survive
//! `plan -> SQL -> plan` with the columns it started with. If it does not, the
//! consuming hop cannot reference them and the whole approach collapses.
//!
//! This measures that on the committed TPC-H corpus, both ways, because the
//! difference between them *is* the design decision:
//!
//! - **Bare hop** — unparse the sub-plan as it appears in the plan tree.
//! - **Wrapped hop** — unparse an explicit projection of every column the
//!   sub-plan exposes, aliased positionally so duplicates cannot collide.
//!
//! Bare fails badly and for two specific reasons, both visible in the
//! generated SQL. A `Join` or `Filter` node carries no projection of its own,
//! so the unparser emits `SELECT FROM "part" CROSS JOIN supplier ...` — an
//! empty select list that re-plans to **zero columns**. And a hop spanning a
//! self-join repeats a qualified name, so DataFusion rejects it with
//! "Projections require unique expression names ... `supplier.s_suppkey` at
//! position 0 and at position 48".
//!
//! Wrapping fixes both, and it is not a workaround: the positional alias is
//! what turns a plan fragment into a *relation with a stable schema*. That is
//! the same property IVM-AUD-SCHEMA-1's guard demands (it compares a view's
//! emitted relation against its declared one, exactly), so the naming that
//! satisfies the unparser is the naming the decomposer needed anyway.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)]

use datafusion::common::Column;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use krishiv_bench::tpch_fixture::fixture_ddl;
use krishiv_bench::tpch_queries::TPCH_QUERIES;

/// Every sub-plan of `plan`, including itself.
fn walk<'a>(plan: &'a LogicalPlan, out: &mut Vec<&'a LogicalPlan>) {
    out.push(plan);
    for child in plan.inputs() {
        walk(child, out);
    }
}

/// The decomposer's hop rule: project every column the sub-plan exposes, under
/// a positional alias. Positional rather than derived from the source name
/// because a self-join legitimately exposes the same qualified name twice.
fn as_hop(node: &LogicalPlan) -> Option<LogicalPlan> {
    let exprs: Vec<Expr> = node
        .schema()
        .iter()
        .enumerate()
        .map(|(i, (qualifier, field))| {
            Expr::Column(Column::new(qualifier.cloned(), field.name())).alias(format!("h{i}"))
        })
        .collect();
    LogicalPlanBuilder::from(node.clone())
        .project(exprs)
        .ok()?
        .build()
        .ok()
}

/// Round-trip `candidate` and report whether it still exposes `want` columns.
async fn round_trips(ctx: &SessionContext, candidate: &LogicalPlan, want: usize) -> bool {
    let Ok(text) = datafusion::sql::unparser::plan_to_sql(candidate) else {
        return false;
    };
    match ctx.sql(&text.to_string()).await {
        Ok(re) => re.schema().fields().len() == want,
        Err(_) => false,
    }
}

/// Re-planning generated, deeply nested SQL overflows the default test stack in
/// a debug build (TPC-H q21 nests seventeen deep) — so the probe runs on its
/// own large stack rather than being quietly limited to the shallow queries.
#[test]
fn every_tpch_hop_survives_the_round_trip_only_when_projected() {
    std::thread::Builder::new()
        .stack_size(512 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(measure());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn measure() {
    let ctx = SessionContext::new();
    for ddl in fixture_ddl() {
        ctx.sql(ddl).await.unwrap().collect().await.unwrap();
    }

    let (mut total, mut bare_ok, mut wrapped_ok) = (0usize, 0usize, 0usize);
    for q in TPCH_QUERIES {
        let sql = q.sql_at_scale(1.0);
        let Ok(df) = ctx.sql(&sql).await else {
            continue;
        };
        let plan = df.logical_plan().clone();
        let mut nodes = Vec::new();
        walk(&plan, &mut nodes);

        for node in nodes {
            total += 1;
            let want = node.schema().fields().len();
            if round_trips(&ctx, node, want).await {
                bare_ok += 1;
            }
            if let Some(hop) = as_hop(node)
                && round_trips(&ctx, &hop, want).await
            {
                wrapped_ok += 1;
            }
        }
    }

    println!("TPC-H sub-plans: {total}  bare: {bare_ok}  as-hop: {wrapped_ok}");

    // Exact, not a floor: this is the evidence the decomposer's hop rule rests
    // on, and a DataFusion upgrade that moves either number must be looked at
    // rather than absorbed.
    assert_eq!(total, 220, "TPC-H corpus shape changed");
    assert_eq!(
        bare_ok, 165,
        "bare sub-plans that round-trip; the shortfall is why hops are projected"
    );
    assert_eq!(
        wrapped_ok, 220,
        "every hop must round-trip — the decomposer cannot publish one that does not"
    );
}
