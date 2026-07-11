//! Phase 52: staged (partition-parallel) batch SQL job construction.
//!
//! Plans a batch SELECT over local parquet tables on a raw DataFusion
//! context, cuts the physical plan at hash-exchange boundaries
//! (`krishiv_sql::distributed_plan::build_distributed_stages`, ADR-0003),
//! and converts the cut into scheduler [`StageSpec`]s: ShuffleMap stages
//! whose tasks carry `dfplan:v1:` bodies plus a [`ShuffleWriteConfig`], and
//! a terminal Result stage gated on its upstream map stages.
//!
//! Everything here is best-effort: any query, table, or plan shape the
//! stage builder cannot prove correct yields `None` and the caller runs the
//! single-task `sql:` path exactly as before (capability honesty).

use krishiv_proto::{ShuffleWriteConfig, StageId, StageKind, StageSpec, TaskId, TaskSpec};
use krishiv_sql::distributed_plan::{
    DistributedStagePlan, build_stages_for_parquet_query, shuffle_stage_key, stage_split_enabled,
};

/// Try to plan `query` over local parquet `tables` as shuffle-connected
/// partition-parallel stages.
///
/// Returns `None` whenever the single-task `sql:` path must be used
/// instead: stage splitting disabled (`KRISHIV_STAGE_SPLIT=off`), a
/// non-SELECT statement, a planning failure (krishiv SQL extensions do not
/// plan on a raw DataFusion context), or a plan shape the stage builder
/// declines. The stages come back in dependency order with the Result
/// stage last, ready for `JobSpec::with_stage`.
pub async fn plan_staged_batch_stages(
    query: &str,
    tables: &[(String, std::path::PathBuf)],
) -> Option<Vec<StageSpec>> {
    if !stage_split_enabled() {
        return None;
    }
    // Only plain SELECT/WITH queries are stage-split. DDL/DML and engine
    // extensions keep the coordinator lifecycle semantics of the
    // single-task path (and would not plan on a raw context anyway).
    let trimmed = query.trim_start();
    let is_select = ["SELECT", "WITH"].iter().any(|prefix| {
        trimmed.len() >= prefix.len() && trimmed[..prefix.len()].eq_ignore_ascii_case(prefix)
    });
    if !is_select {
        return None;
    }

    let table_paths: Vec<(String, String)> = tables
        .iter()
        .map(|(name, path)| Some((name.clone(), path.to_str()?.to_owned())))
        .collect::<Option<_>>()?;
    let staged = match build_stages_for_parquet_query(query, &table_paths).await {
        Ok(Some(staged)) => staged,
        Ok(None) => return None,
        Err(error) => {
            tracing::debug!(%error, "staged batch planning failed; using single-task path");
            return None;
        }
    };
    stage_specs_from_plan(&staged)
}

/// Convert a builder [`DistributedStagePlan`] into scheduler stage specs.
///
/// Wire contract: map task `t` of builder stage `i` writes its shuffle
/// output under the sub-stage key [`shuffle_stage_key`]`(i, t)` — the
/// executor's dfplan shuffle reader derives the same key from the
/// `ShuffleReadExec` leaves in downstream plans.
fn stage_specs_from_plan(staged: &DistributedStagePlan) -> Option<Vec<StageSpec>> {
    let stage_id = |index: usize| StageId::try_new(format!("dist-s{index}")).ok();
    let mut specs = Vec::with_capacity(staged.stages.len());
    for (stage_index, stage) in staged.stages.iter().enumerate() {
        let (kind, name) = match &stage.shuffle {
            Some(_) => (
                StageKind::ShuffleMap,
                format!("batch-sql-dist-map-{stage_index}"),
            ),
            None => (StageKind::Result, String::from("batch-sql-dist-result")),
        };
        let mut spec = StageSpec::new(stage_id(stage_index)?, name).with_kind(kind);
        if let Some(shuffle) = &stage.shuffle {
            spec = spec
                .with_output_partition_count(u32::try_from(shuffle.num_output_partitions).ok()?);
        }
        for upstream in &stage.upstream_stage_indexes {
            spec = spec.with_upstream_stage(stage_id(*upstream)?);
        }
        for (task_index, body) in stage.task_bodies.iter().enumerate() {
            let task_id = TaskId::try_new(format!("dist-s{stage_index}-t{task_index}")).ok()?;
            let fragment =
                krishiv_plan::TypedTaskFragment::new(krishiv_plan::ExecutionKind::Batch, body)
                    .encode()
                    .ok()?;
            let mut task = TaskSpec::new(task_id, fragment);
            if let Some(shuffle) = &stage.shuffle {
                task = task.with_shuffle_write(ShuffleWriteConfig {
                    stage_id: StageId::try_new(shuffle_stage_key(stage_index, task_index)).ok()?,
                    num_partitions: shuffle.num_output_partitions,
                    key_columns: shuffle.key_columns.clone(),
                    // Retried map tasks intentionally reuse token 0: the
                    // store replaces on duplicate keys, so a retry
                    // overwrites its own sub-stage output idempotently.
                    lease_token: 0,
                });
            }
            spec = spec.with_task(task);
        }
        specs.push(spec);
    }
    Some(specs)
}
