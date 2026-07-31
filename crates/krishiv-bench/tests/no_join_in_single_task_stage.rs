//! A one-task stage must never contain a hash join.
//!
//! # Why this file exists
//!
//! TPC-H q3 at SF100 ran for **55+ minutes** with eight of nine slots idle and
//! the one busy executor pegged at **4.7% CPU** — not computing, pulling. Its
//! plan was
//!
//! ```text
//! SortPreservingMerge(fetch=10)          <- 1 partition
//!   SortExec TopK(fetch=10)              <- 18 partitions
//!     Aggregate(SinglePartitioned)       <- 18 partitions
//!       HashJoin(Partitioned)            <- 18 partitions
//!         ShuffleRead / ShuffleRead
//! ```
//!
//! and the cutter recognised only `RepartitionExec` and
//! `CoalescePartitionsExec` as exchanges. The two repartitions sit *below* the
//! join, so everything above them became one stage — and because the merge
//! emits a single partition, that stage got exactly one task. One executor ran
//! the whole 18-partition join and aggregate and dragged 13.2 GB of shuffle
//! across an ~11 MiB/s pod network to do it.
//!
//! The shape is the bug, and the shape is checkable offline against empty
//! fixture tables in milliseconds. A partitioned hash join in a one-task stage
//! means the query is not distributed no matter how many stages it reports.

// Integration-test crate: helpers run outside `#[test]` fns, so clippy.toml's
// `allow-unwrap-in-tests` does not reach them. A panic is the failure signal.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use krishiv_bench::tpch_fixture::fixture_ddl;
use krishiv_bench::tpch_queries::TPCH_QUERIES;
use krishiv_sql::distributed_plan::{
    build_distributed_stages, decode_dfplan_task, fragment_decode_session_context,
    planning_session_context_with_options,
};
use std::sync::Arc;

/// A join that is **hash-partitioned** across the cluster, i.e. one whose whole
/// point is that N tasks each handle one partition.
///
/// A `CollectLeft` (broadcast) join in a one-task stage is a different shape
/// and not necessarily wrong — its build side was chosen to be small, and the
/// probe may legitimately be one partition. A `Partitioned` join in a one-task
/// stage is always wrong: the plan paid for the shuffle and then threw the
/// parallelism away.
fn partitioned_join(plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>()
        && *join.partition_mode() == PartitionMode::Partitioned
    {
        return Some(format!("HashJoinExec(Partitioned) on {:?}", join.on()));
    }
    for child in plan.children() {
        if let Some(found) = partitioned_join(child) {
            return Some(found);
        }
    }
    None
}

#[tokio::test]
async fn no_tpch_query_runs_a_join_inside_a_one_task_stage() {
    // `broadcast_join_bytes: Some(0)` forbids collecting a build side, so every
    // join hash-shuffles — the SF100 configuration, and the only one in which
    // this invariant is meaningful. With the tiny fixture and broadcast
    // allowed, every join is legitimately `CollectLeft` over one partition.
    let ctx = planning_session_context_with_options(18, None, Some(0));
    for ddl in fixture_ddl() {
        ctx.sql(ddl).await.unwrap().collect().await.unwrap();
    }

    let decode_ctx = fragment_decode_session_context().task_ctx();
    let codec = krishiv_sql::distributed_plan::KrishivPhysicalCodec::coordinator();
    let mut offenders: Vec<String> = Vec::new();

    for query in TPCH_QUERIES {
        let name = query.id;
        let Ok(df) = ctx.sql(&query.sql_at_scale(1.0)).await else {
            continue;
        };
        let Ok(plan) = df.create_physical_plan().await else {
            continue;
        };
        // A query that declines to stage is a different concern; this test is
        // about the shape of the plans that DO stage.
        let Ok(Some(staged)) = build_distributed_stages(plan) else {
            continue;
        };
        for (index, stage) in staged.stages.iter().enumerate() {
            if stage.task_count() != 1 {
                continue;
            }
            let Some(body) = stage.task_bodies.first() else {
                continue;
            };
            let Ok((_, plan)) = decode_dfplan_task(body, &decode_ctx, &codec) else {
                continue;
            };
            if let Some(op) = partitioned_join(&plan) {
                offenders.push(format!(
                    "{name}: stage {index} of {} has ONE task but contains {op}",
                    staged.stages.len()
                ));
            }
        }
    }

    offenders.sort();
    offenders.dedup();

    // q15 and q20 were here until the cutter learned to cut a fetch-less
    // `SortPreservingMergeExec` when a hash-partitioned join sits below it.
    //
    // q11's two are a different route to the same shape: they are the
    // **scalar-subquery** stages, which the cutter treats as their own boundary
    // (`ScalarSubqueryExec`), not as a gather — so no merge rule reaches them.
    // Fixing those means giving the subquery cut the same partition-awareness,
    // which is a separate change.
    //
    // Asserted as an exact set, not a floor: a NEW entry is a regression, and a
    // DISAPPEARING entry means someone fixed one and must delete it here. Both
    // directions should make a human look.
    let known_gaps = ["q11: stage 1", "q11: stage 3"];
    let unexpected: Vec<&String> = offenders
        .iter()
        .filter(|o| !known_gaps.iter().any(|k| o.starts_with(k)))
        .collect();
    assert!(
        unexpected.is_empty(),
        "a hash-partitioned join in a one-task stage means the query is not \
         really distributed — one executor does the whole join and pulls every \
         shuffle byte to itself (q3 at SF100: 55+ min at 4.7% CPU):\n  {}",
        unexpected
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
    let fixed: Vec<&str> = known_gaps
        .iter()
        .filter(|k| !offenders.iter().any(|o| o.starts_with(*k)))
        .copied()
        .collect();
    assert!(
        fixed.is_empty(),
        "these known gaps no longer reproduce — delete them from `known_gaps`: {fixed:?}"
    );
}
