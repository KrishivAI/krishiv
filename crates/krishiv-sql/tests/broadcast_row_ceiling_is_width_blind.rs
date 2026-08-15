//! Does a *wide* build side with no byte estimate get broadcast?
//!
//! q10 moves ~63 GB in 13.5 minutes against a plan implying ~3.5 GB of shuffle,
//! with no skew (8.3 of 9 slots busy throughout) and no wire cap (77.5 MB/s
//! sustained). The suspected mechanism is the broadcast **row** ceiling:
//!
//! `supports_collect_by_thresholds` prefers `total_byte_size` and falls back to
//! `num_rows`. `ShuffleReadExec` propagates whatever the cut subtree reported,
//! and DataFusion routinely loses `total_byte_size` through joins and
//! aggregates — so above a shuffle boundary the decision is frequently made on
//! rows alone, against a ceiling we raised from DataFusion's 128k to 1,000,000.
//!
//! A row count cannot see row width. 900k rows of q10's customer-shaped
//! intermediate (~180 B/row) is ~155 MB — five times the 32 MiB byte ceiling
//! that the row ceiling exists to approximate — and `CollectLeft` copies it to
//! every task of the stage.
//!
//! This test does not assert the fix. It asserts *what DataFusion currently
//! does*, so the mechanism is established from behaviour rather than from
//! reading `supports_collect_by_thresholds` and hoping. Changing the rule
//! without this is how the last attempt regressed four queries
//! (see `krishiv_sql::join_estimates`).

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use krishiv_sql::distributed_plan::{ShuffleReadExec, planning_session_context_with_options};
use std::sync::Arc;

/// customer's shape in q10: one key plus four wide strings, ~180 B/row.
fn wide_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ]))
}

fn narrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderkey", DataType::Int64, false),
    ]))
}

/// A shuffle read with `rows` known and bytes ABSENT — the shape produced when
/// the upstream stage's byte estimate was lost through a join or aggregate.
fn build_side(rows: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        ShuffleReadExec::new(0, 4, 1, wide_schema(), None).with_upstream_estimate(Some(rows), None),
    )
}

/// The probe side, multi-partition so there is parallelism to lose.
///
/// These helpers return `Result` rather than unwrapping internally: clippy's
/// `allow-expect-in-tests` exempts `#[test]` bodies but not plain helper
/// functions in the same file, and `unwrap`/`expect`/`panic!` are all denied
/// here — so the fallible step belongs at the call sites, which are exempt.
fn probe_side() -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let read: Arc<dyn ExecutionPlan> = Arc::new(
        ShuffleReadExec::new(1, 4, 4, narrow_schema(), None)
            .with_upstream_estimate(Some(5_000_000), Some(80_000_000)),
    );
    Ok(Arc::new(RepartitionExec::try_new(
        read,
        Partitioning::RoundRobinBatch(4),
    )?))
}

/// Ask DataFusion's own `JoinSelection` what mode it picks, under the exact
/// config the distributed planner uses (32 MiB / 1,000,000 rows).
fn chosen_mode(build_rows: usize) -> datafusion::error::Result<PartitionMode> {
    let ctx = planning_session_context_with_options(4, None, None);
    let state = ctx.state();
    let opts = state.config().options();

    let join = HashJoinExec::try_new(
        build_side(build_rows),
        probe_side()?,
        vec![(
            Arc::new(Column::new("c_custkey", 0)),
            Arc::new(Column::new("o_custkey", 0)),
        )],
        None,
        &datafusion::common::JoinType::Inner,
        None,
        // Start from Partitioned so any CollectLeft in the result is a decision
        // JoinSelection made, not something this test handed it.
        PartitionMode::Auto,
        datafusion::common::NullEquality::NullEqualsNothing,
        false,
    )?;

    let optimized = datafusion::physical_optimizer::join_selection::JoinSelection::new()
        .optimize(Arc::new(join), opts)?;

    let hj = optimized.downcast_ref::<HashJoinExec>().ok_or_else(|| {
        datafusion::error::DataFusionError::Internal(String::from(
            "JoinSelection replaced the hash join with another operator",
        ))
    })?;
    Ok(*hj.partition_mode())
}

#[test]
fn a_wide_sub_ceiling_build_side_with_no_byte_estimate_is_broadcast() {
    // 900k rows: under the 1,000,000 row ceiling, so the row fallback admits it.
    // At ~180 B/row that is ~155 MB — far over the 32 MiB byte ceiling the row
    // number is standing in for.
    let mode = chosen_mode(900_000).expect("join selection");
    assert_eq!(
        mode,
        PartitionMode::CollectLeft,
        "MECHANISM CHECK: if this is no longer CollectLeft, the width-blind row \
         ceiling is not how q10 broadcasts a wide build side, and the fix must \
         be re-derived before touching join_estimates"
    );
}

/// The fix: `redistribute_unsplittable_broadcast_joins` converts the wide
/// broadcast above into a partitioned join, so the build side is hash-exchanged
/// once instead of copied to every task.
#[test]
fn the_wide_broadcast_is_converted_to_a_partitioned_join() {
    let join = wide_join(900_000).expect("wide join");
    let converted =
        krishiv_sql::distributed_plan::redistribute_unsplittable_broadcast_joins(Arc::clone(&join))
            .expect("conversion");

    let mode = converted
        .downcast_ref::<HashJoinExec>()
        .map(|hj| *hj.partition_mode());
    assert_eq!(
        mode,
        Some(PartitionMode::Partitioned),
        "a build side the ROW ceiling admitted but the BYTE ceiling would not \
         must be hash-partitioned, not copied to every task"
    );
}

/// A *narrow* build side under the row ceiling is genuinely small and must stay
/// broadcast — that is the case the raised ceiling exists to serve, and
/// converting it is what cost q8 4.1x.
#[test]
fn a_narrow_sub_ceiling_build_side_stays_broadcast() {
    // 900k rows of two i64s = ~14 MB, under the 32 MiB ceiling.
    let build: Arc<dyn ExecutionPlan> = Arc::new(
        ShuffleReadExec::new(0, 4, 1, narrow_schema(), None)
            .with_upstream_estimate(Some(900_000), None),
    );
    let join: Arc<dyn ExecutionPlan> = Arc::new(
        HashJoinExec::try_new(
            build,
            probe_side().expect("probe side"),
            vec![(
                Arc::new(Column::new("o_custkey", 0)),
                Arc::new(Column::new("o_custkey", 0)),
            )],
            None,
            &datafusion::common::JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            datafusion::common::NullEquality::NullEqualsNothing,
            false,
        )
        .expect("hash join"),
    );

    let converted =
        krishiv_sql::distributed_plan::redistribute_unsplittable_broadcast_joins(Arc::clone(&join))
            .expect("conversion");
    assert_eq!(
        converted
            .downcast_ref::<HashJoinExec>()
            .map(|hj| *hj.partition_mode()),
        Some(PartitionMode::CollectLeft),
        "a genuinely small build side must still broadcast; converting these is \
         the regression join_estimates was written about"
    );
}

/// Build the wide `CollectLeft` join the fix targets.
fn wide_join(rows: usize) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(HashJoinExec::try_new(
        build_side(rows),
        probe_side()?,
        vec![(
            Arc::new(Column::new("c_custkey", 0)),
            Arc::new(Column::new("o_custkey", 0)),
        )],
        None,
        &datafusion::common::JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        datafusion::common::NullEquality::NullEqualsNothing,
        false,
    )?))
}

#[test]
fn the_q8_shape_stays_partitioned_and_must_keep_doing_so() {
    // The regression guard from `join_estimates`: rows well over the ceiling,
    // bytes absent. Converting these cost q8 4.1x, q9 2.5x and q17 4.8x. Any
    // width-aware rule must leave this exactly as it is.
    let mode = chosen_mode(4_000_000).expect("join selection");
    assert_ne!(
        mode,
        PartitionMode::CollectLeft,
        "a 4M-row build side must not broadcast; this is the shape whose \
         conversion regressed four queries"
    );
}
