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
    ClusterCapacity, DistributedStagePlan, build_stages_for_parquet_query, shuffle_stage_key,
    stage_split_enabled,
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
    cluster: Option<ClusterCapacity>,
) -> Option<Vec<StageSpec>> {
    match plan_staged_batch_stages_verbose(query, tables, cluster).await {
        Ok(stages) => Some(stages),
        Err(reason) => {
            // WARN, not DEBUG. Declining to stage means the query runs as a
            // single task: correct results, but the whole dataset scanned by
            // one executor while the rest of the cluster idles. That is a
            // performance cliff with no other symptom, and it hid a real bug
            // for an entire benchmarking session because the only evidence was
            // a debug line nobody had enabled. Anyone reading logs at default
            // level should see when their cluster stopped being a cluster.
            tracing::warn!(
                reason = %reason,
                "batch SQL will run as a SINGLE task (no stage split); \
                 the query executes on one executor"
            );
            None
        }
    }
}

/// [`plan_staged_batch_stages`], but returning why staging was declined.
///
/// Split out so the reason is a value rather than a log side effect: tests
/// assert on it, and the caller decides how loudly to report it.
pub async fn plan_staged_batch_stages_verbose(
    query: &str,
    tables: &[(String, std::path::PathBuf)],
    cluster: Option<ClusterCapacity>,
) -> Result<Vec<StageSpec>, String> {
    if !stage_split_enabled() {
        return Err(format!(
            "stage splitting disabled via {}",
            krishiv_sql::distributed_plan::STAGE_SPLIT_ENV
        ));
    }
    // A Python scalar UDF query carries leading `/* krishiv-register-python-udf */`
    // directive(s). Split them off: they ride along on every stage fragment (so
    // each executor reconstructs the worker-backed UDF before decoding its
    // dfplan), and the SELECT/WITH gate must look at the SQL past them. The
    // coordinator's planner (build_stages) re-registers the UDF by signature and
    // strips the directive itself.
    let (udf_directives, gate_query) = split_leading_python_udf_directives(query);

    // Only plain SELECT/WITH queries are stage-split. DDL/DML and engine
    // extensions keep the coordinator lifecycle semantics of the
    // single-task path (and would not plan on a raw context anyway).
    let trimmed = gate_query.trim_start();
    let is_select = ["SELECT", "WITH"].iter().any(|prefix| {
        trimmed.len() >= prefix.len() && trimmed[..prefix.len()].eq_ignore_ascii_case(prefix)
    });
    if !is_select {
        return Err(String::from(
            "not a plain SELECT/WITH query; DDL, DML and engine extensions \
             keep single-task lifecycle semantics",
        ));
    }

    let mut table_paths: Vec<(String, String)> = Vec::with_capacity(tables.len());
    for (name, path) in tables {
        let Some(text) = path.to_str() else {
            return Err(format!("table '{name}' has a non-UTF-8 path"));
        };
        table_paths.push((name.clone(), text.to_owned()));
    }
    let staged = match build_stages_for_parquet_query(query, &table_paths, cluster).await {
        Ok(Some(staged)) => staged,
        Ok(None) => return Err(String::from("planner produced no distributable stages")),
        Err(error) => return Err(error.to_string()),
    };
    stage_specs_from_plan(&staged, &udf_directives)
        .ok_or_else(|| String::from("stage specs could not be built from the staged plan"))
}

/// Split leading `/* krishiv-register-python-udf:… */` scalar-UDF directive
/// comment(s) off the front of `query`, returning them joined by newlines and
/// the remaining SQL. Aggregate directives are left in place — staged planning
/// declines aggregate queries, so they never reach a staged fragment.
fn split_leading_python_udf_directives(query: &str) -> (String, &str) {
    const OPEN: &str = "/* krishiv-register-python-udf:";
    const CLOSE: &str = " */";
    let mut directives: Vec<&str> = Vec::new();
    let mut rest = query.trim_start();
    while rest.starts_with(OPEN) {
        let Some(end) = rest.find(CLOSE) else { break };
        directives.push(&rest[..end + CLOSE.len()]);
        rest = rest[end + CLOSE.len()..].trim_start();
    }
    (directives.join("\n"), rest)
}

/// Convert a builder [`DistributedStagePlan`] into scheduler stage specs.
///
/// Wire contract: map task `t` of builder stage `i` writes its shuffle
/// output under the sub-stage key [`shuffle_stage_key`]`(i, t)` — the
/// executor's dfplan shuffle reader derives the same key from the
/// `ShuffleReadExec` leaves in downstream plans.
fn stage_specs_from_plan(
    staged: &DistributedStagePlan,
    udf_directives: &str,
) -> Option<Vec<StageSpec>> {
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
            // Prepend the Python-UDF directive(s) so the executor registers the
            // worker-backed UDF before decoding the dfplan; the body is otherwise
            // unchanged. `parse_dfplan_task_body` on the executor strips them.
            let framed_body = if udf_directives.is_empty() {
                body.clone()
            } else {
                format!("{udf_directives}\n{body}")
            };
            let fragment = krishiv_plan::TypedTaskFragment::new(
                krishiv_plan::ExecutionKind::Batch,
                &framed_body,
            )
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
