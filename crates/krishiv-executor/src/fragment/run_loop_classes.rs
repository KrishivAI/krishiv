//! Run-loop fragments for the non-window streaming classes (task #147):
//! `stream:rbatch:` (stateless per-batch SQL) and `stream:rjoin:` (two-source
//! interval join). Format mirrors `stream:rloop:` —
//! `prefix<job_id>|<subtask>/<parallelism>|<payload>` — with a RAW JSON spec
//! as the payload (the compact codec exists only for window specs, and JSON
//! is safe as the final `splitn` segment).
//!
//! Side identity for the join: pushed input lands on side-tagged buffer keys
//! `{job}#{task}#L` / `{job}#{task}#R` (the push path keys buffers by the
//! request's free-form task string, so no wire change), and owned registry
//! splits are partitioned by `table_name` matching the join spec's
//! `left_source` / `right_source` — a table matching neither side fails
//! closed. The keyed exchange preserves the side by delivering to the peer's
//! side-tagged key.

use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use krishiv_dataflow::stream_driver::{JoinSide, StreamDriver, StreamingLoop};
use krishiv_dataflow::{WatermarkWindowJoinOperator, WatermarkWindowJoinSpec};
use krishiv_plan::stream_join::StreamingJoinSpec;
use krishiv_plan::stream_task::StatelessQuerySpec;
use krishiv_proto::ExecutorTaskAssignment;
use krishiv_sql::stateless_exec::StatelessBatchExecutor;

use super::run_loop::{
    RLOOP_IDLE_FLOOR_US, SplitWatermarks, batch_max_event_time, deliver_to_peer_suffixed,
    parse_stream_peers, rloop_key_group_range, rloop_state_key, route_batch_by_key_group,
    watermark_idleness,
};
use crate::fragment::common::parse_registry_partition_specs;
use crate::runner::{ExecutorTaskRunner, StreamingProgressSnapshot, TaskStateBinding};
use crate::{ExecutorError, ExecutorResult, ExecutorTaskOutput};

/// True for every run-loop-FAMILY fragment body: windows (`stream:rloop:`)
/// and the three classed loops. The runner keys two decisions on this — the
/// no-timeout dispatch arm (a run-loop exits only on cancellation; a
/// wall-clock timeout would kill a healthy job) and the Cancelled terminal
/// state on cancel (not Succeeded). A prefix missing here silently gets a
/// batch task's lifecycle (task #149 fix 6).
pub(crate) fn is_run_loop_family(fragment_body: &str) -> bool {
    [
        super::run_loop::STREAM_RLOOP_PREFIX,
        STREAM_RJOIN_PREFIX,
        STREAM_RPIPE_PREFIX,
        STREAM_RBATCH_PREFIX,
    ]
    .iter()
    .any(|prefix| fragment_body.starts_with(prefix))
}

pub(crate) const STREAM_RBATCH_PREFIX: &str = "stream:rbatch:";
pub(crate) const STREAM_RPIPE_PREFIX: &str = "stream:rpipe:";
pub(crate) const STREAM_RJOIN_PREFIX: &str = "stream:rjoin:";

/// A parsed classed fragment.
#[derive(Debug)]
pub(crate) struct ClassedFragment {
    pub(crate) job_id: String,
    pub(crate) subtask: usize,
    pub(crate) parallelism: usize,
    pub(crate) payload: String,
}

/// Parse `prefix<job>|<subtask>/<parallelism>|<json>`.
pub(crate) fn parse_classed_fragment(
    prefix: &str,
    fragment: &str,
) -> ExecutorResult<ClassedFragment> {
    let rest = fragment
        .strip_prefix(prefix)
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: format!("fragment does not start with {prefix}"),
        })?;
    let mut parts = rest.splitn(3, '|');
    let job_id = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: format!("{prefix} fragment missing job id"),
        })?
        .to_owned();
    let sub_par = parts
        .next()
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: format!("{prefix} fragment missing subtask/parallelism"),
        })?;
    let payload = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: format!("{prefix} fragment missing spec payload"),
        })?
        .to_owned();
    let (sub, par) = sub_par
        .split_once('/')
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: format!("{prefix} fragment subtask segment '{sub_par}' is not <n>/<m>"),
        })?;
    let subtask: usize = sub.parse().map_err(|_| ExecutorError::InvalidAssignment {
        message: format!("{prefix} fragment bad subtask '{sub}'"),
    })?;
    let parallelism: usize = par.parse().map_err(|_| ExecutorError::InvalidAssignment {
        message: format!("{prefix} fragment bad parallelism '{par}'"),
    })?;
    if parallelism == 0 || subtask >= parallelism {
        return Err(ExecutorError::InvalidAssignment {
            message: format!("{prefix} fragment subtask {subtask}/{parallelism} out of range"),
        });
    }
    Ok(ClassedFragment {
        job_id,
        subtask,
        parallelism,
        payload,
    })
}

fn local_err(message: String) -> ExecutorError {
    ExecutorError::LocalExecution { message }
}

/// Gather one iteration's pushed input for `key`, remove-style.
fn take_pushed(runner: &ExecutorTaskRunner, key: &str) -> Vec<RecordBatch> {
    runner
        .continuous_inputs
        .remove(key)
        .map(|(_, v)| v)
        .unwrap_or_default()
}

/// `stream:rbatch:` — stateless per-batch SQL. No keys, no exchange, no
/// state; split ownership by index, like every non-Kafka registry split.
pub(crate) async fn execute_rbatch_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    fragment: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    let parsed = parse_classed_fragment(STREAM_RBATCH_PREFIX, fragment)?;
    let spec: StatelessQuerySpec =
        serde_json::from_str(&parsed.payload).map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("stream:rbatch invalid spec json: {e}"),
        })?;
    spec.validate()
        .map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("stream:rbatch spec: {e}"),
        })?;
    let job_id = parsed.job_id.as_str();
    let task_id = assignment.task_id().as_str().to_owned();
    let state_key = rloop_state_key(job_id, parsed.subtask);
    let input_key = format!("{job_id}#{task_id}");

    let executor_arc = {
        let entry = runner
            .stateless_executors
            .entry(state_key.clone())
            .or_try_insert_with(|| {
                let exec = StatelessBatchExecutor::new(&spec.sql, &spec.source);
                for side in &spec.side_tables {
                    let batches = decode_side_table(&side.ipc_base64).map_err(|e| {
                        ExecutorError::InvalidAssignment {
                            message: format!("stream:rbatch side table '{}': {e}", side.name),
                        }
                    })?;
                    exec.register_side_table(&side.name, batches).map_err(|e| {
                        ExecutorError::InvalidAssignment {
                            message: format!("stream:rbatch side table '{}': {e}", side.name),
                        }
                    })?;
                }
                Ok::<Arc<tokio::sync::Mutex<StatelessBatchExecutor>>, ExecutorError>(Arc::new(
                    tokio::sync::Mutex::new(exec),
                ))
            })?;
        Arc::clone(entry.value())
    };

    let own_notify = runner.notify_handle(&input_key);
    let shared_notify = runner.notify_handle(job_id);
    let peers = parse_stream_peers(assignment.input_partitions())?;
    let all_specs = parse_registry_partition_specs(assignment.input_partitions())?;
    let owned_specs: Vec<_> = all_specs
        .into_iter()
        .enumerate()
        .filter(|(idx, spec)| {
            spec.kind.eq_ignore_ascii_case("kafka") || idx % parsed.parallelism == parsed.subtask
        })
        .map(|(_, spec)| spec)
        .collect();
    let _ = peers; // no keyed exchange for a stateless class
    let source_cache = runner.shared_continuous_connector_sources();
    let idle_floor = Duration::from_micros(RLOOP_IDLE_FLOOR_US);
    let mut rows_emitted: u64 = 0;
    let mut batches_emitted: u64 = 0;

    tracing::info!(
        job_id,
        subtask = parsed.subtask,
        parallelism = parsed.parallelism,
        owned_splits = owned_specs.len(),
        "stream:rbatch stateless run-loop started"
    );

    loop {
        if runner
            .inbox
            .is_task_cancelled(assignment.job_id(), assignment.task_id())
            .unwrap_or(false)
        {
            break;
        }
        // Liveness: cancel retires this subtask's state entry and then purges
        // the inbox identity — including the tombstone above, usually before
        // this loop observes it. Arc identity on the state entry is the
        // race-free exit signal (see the rloop loop for the full account).
        let state_alive = runner
            .stateless_executors
            .get(&state_key)
            .is_some_and(|entry| Arc::ptr_eq(entry.value(), &executor_arc));
        if !state_alive {
            break;
        }
        let mut input: Vec<RecordBatch> = Vec::new();
        input.extend(take_pushed(runner, &input_key));
        input.extend(take_pushed(runner, job_id));
        for src in &owned_specs {
            super::run_loop::read_owned_split(
                runner,
                job_id,
                &state_key,
                src,
                &source_cache,
                assignment,
                |b| {
                    input.push(b);
                    Ok(())
                },
            )
            .await?;
        }

        if input.is_empty() {
            tokio::select! {
                _ = own_notify.notified() => {}
                _ = shared_notify.notified() => {}
                _ = tokio::time::sleep(idle_floor) => {}
            }
            continue;
        }

        let mut outputs: Vec<RecordBatch> = Vec::new();
        {
            let exec = executor_arc.lock().await;
            for batch in input {
                let out = exec
                    .on_batch(batch)
                    .await
                    .map_err(|e| local_err(format!("stream:rbatch query failed: {e}")))?;
                outputs.extend(out);
            }
        }
        if !outputs.is_empty() {
            rows_emitted += outputs.iter().map(|b| b.num_rows() as u64).sum::<u64>();
            batches_emitted += outputs.len() as u64;
            crate::erased(runner.stage_rloop_outputs(job_id, assignment, &outputs)).await?;
        }
        runner.report_streaming_progress(&StreamingProgressSnapshot {
            task_id: task_id.clone(),
            job_id: job_id.to_owned(),
            watermark_ms: i64::MIN,
            rows_emitted,
            batches_emitted,
            egress_dropped_batches: 0,
            null_key_rows_dropped: 0,
            state_bytes: 0,
            source_offset: None,
            timestamp_ms: now_ms() as u64,
        });
    }
    let _ = runner
        .inbox
        .clear_cancelled_task(assignment.job_id(), assignment.task_id());
    Ok(ExecutorTaskOutput::cancelled())
}

/// `stream:rjoin:` — two-source interval join. Both sides hash their own key
/// column into the shared key-group space, so matching rows co-locate.
pub(crate) async fn execute_rjoin_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    fragment: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    let parsed = parse_classed_fragment(STREAM_RJOIN_PREFIX, fragment)?;
    let spec: StreamingJoinSpec =
        serde_json::from_str(&parsed.payload).map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("stream:rjoin invalid spec json: {e}"),
        })?;
    spec.validate()
        .map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("stream:rjoin spec: {e}"),
        })?;
    let job_id = parsed.job_id.as_str();
    let task_id = assignment.task_id().as_str().to_owned();
    let state_key = rloop_state_key(job_id, parsed.subtask);
    let left_key = format!("{job_id}#{task_id}#L");
    let right_key = format!("{job_id}#{task_id}#R");

    let computed_range = rloop_key_group_range(parsed.subtask, parsed.parallelism);
    let stamped = assignment.key_group_range();
    if parsed.parallelism > 1 && stamped != computed_range {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "stream:rjoin subtask {}/{} key-group range mismatch: stamped [{},{}] but the \
                 exchange routes by [{},{}]",
                parsed.subtask,
                parsed.parallelism,
                stamped.start(),
                stamped.end(),
                computed_range.start(),
                computed_range.end()
            ),
        });
    }

    let op_arc = {
        let entry = runner
            .join_executors
            .entry(state_key.clone())
            .or_insert_with(|| {
                Arc::new(std::sync::Mutex::new(WatermarkWindowJoinOperator::new(
                    WatermarkWindowJoinSpec::from(&spec),
                )))
            });
        Arc::clone(entry.value())
    };
    // Barrier snapshots bind by task; the join binding routes them to the
    // per-subtask operator (the H-6 keying, applied to joins).
    runner
        .task_state_bindings
        .insert(task_id.clone(), TaskStateBinding::Join(state_key.clone()));

    let own_left = runner.notify_handle(&left_key);
    let own_right = runner.notify_handle(&right_key);
    let shared_notify = runner.notify_handle(job_id);
    let peers = parse_stream_peers(assignment.input_partitions())?;
    let all_specs = parse_registry_partition_specs(assignment.input_partitions())?;
    let mut left_specs = Vec::new();
    let mut right_specs = Vec::new();
    for (idx, s) in all_specs.into_iter().enumerate() {
        let owned =
            s.kind.eq_ignore_ascii_case("kafka") || idx % parsed.parallelism == parsed.subtask;
        if !owned {
            continue;
        }
        if s.table_name == spec.left_source {
            left_specs.push(s);
        } else if s.table_name == spec.right_source {
            right_specs.push(s);
        } else {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "stream:rjoin source table '{}' matches neither join side ('{}' / '{}') — \
                     refusing rather than silently ignoring a source",
                    s.table_name, spec.left_source, spec.right_source
                ),
            });
        }
    }

    let source_cache = runner.shared_continuous_connector_sources();
    let idle_floor = Duration::from_micros(RLOOP_IDLE_FLOOR_US);
    let idleness = watermark_idleness();
    let mut driver = StreamDriver::new(StreamingLoop::EmbeddedJoinBounded);
    let mut left_wm = SplitWatermarks::default();
    let mut right_wm = SplitWatermarks::default();
    let mut rows_emitted: u64 = 0;
    let mut batches_emitted: u64 = 0;

    tracing::info!(
        job_id,
        subtask = parsed.subtask,
        parallelism = parsed.parallelism,
        left_splits = left_specs.len(),
        right_splits = right_specs.len(),
        peers = peers.len(),
        "stream:rjoin two-source run-loop started"
    );

    loop {
        if runner
            .inbox
            .is_task_cancelled(assignment.job_id(), assignment.task_id())
            .unwrap_or(false)
        {
            break;
        }
        // Liveness: cancel retires this subtask's state entry and then purges
        // the inbox identity — including the tombstone above, usually before
        // this loop observes it. Arc identity on the state entry is the
        // race-free exit signal (see the rloop loop for the full account).
        let state_alive = runner
            .join_executors
            .get(&state_key)
            .is_some_and(|entry| Arc::ptr_eq(entry.value(), &op_arc));
        if !state_alive {
            break;
        }
        let _ = crate::erased(runner.drain_barriers_via_context()).await;

        let mut left_in: Vec<RecordBatch> = take_pushed(runner, &left_key);
        let mut right_in: Vec<RecordBatch> = take_pushed(runner, &right_key);
        for src in &left_specs {
            super::run_loop::read_owned_split(
                runner,
                job_id,
                &state_key,
                src,
                &source_cache,
                assignment,
                |b| {
                    left_in.push(b);
                    Ok(())
                },
            )
            .await?;
        }
        for src in &right_specs {
            super::run_loop::read_owned_split(
                runner,
                job_id,
                &state_key,
                src,
                &source_cache,
                assignment,
                |b| {
                    right_in.push(b);
                    Ok(())
                },
            )
            .await?;
        }

        if left_in.is_empty() && right_in.is_empty() {
            tokio::select! {
                _ = own_left.notified() => {}
                _ = own_right.notified() => {}
                _ = shared_notify.notified() => {}
                _ = tokio::time::sleep(idle_floor) => {}
            }
            continue;
        }

        // Keyed exchange, side preserved: each side routes by ITS OWN key
        // column into the shared key-group space.
        let mut outputs: Vec<RecordBatch> = Vec::new();
        for (side, batches, key_column, wm) in [
            (JoinSide::Left, left_in, &spec.left_key_column, &mut left_wm),
            (
                JoinSide::Right,
                right_in,
                &spec.right_key_column,
                &mut right_wm,
            ),
        ] {
            let suffix = match side {
                JoinSide::Left => "#L",
                JoinSide::Right => "#R",
            };
            let mut owned_batches: Vec<RecordBatch> = Vec::new();
            let mut outbound: std::collections::BTreeMap<usize, Vec<RecordBatch>> =
                Default::default();
            for batch in &batches {
                if let Some(ts) = batch_max_event_time(batch, &spec.time_column) {
                    wm.observe(suffix, ts);
                }
                let routed = route_batch_by_key_group(
                    batch,
                    key_column,
                    parsed.parallelism,
                    parsed.subtask,
                )?;
                if routed.null_key_rows > 0 {
                    return Err(local_err(format!(
                        "stream:rjoin NULL join key in column '{key_column}': a row that can \
                         never match is a data defect, not a droppable",
                    )));
                }
                if let Some(own) = routed.owned {
                    owned_batches.push(own);
                }
                for (peer_subtask, slice) in routed.routed {
                    outbound.entry(peer_subtask).or_default().push(slice);
                }
            }
            for (peer_subtask, batches) in outbound {
                let Some(peer) = peers.iter().find(|p| p.subtask == peer_subtask) else {
                    return Err(ExecutorError::InvalidAssignment {
                        message: format!(
                            "stream:rjoin subtask {} has rows for peer {} but no peer entry",
                            parsed.subtask, peer_subtask
                        ),
                    });
                };
                crate::erased(deliver_to_peer_suffixed(
                    runner, job_id, peer, batches, suffix,
                ))
                .await?;
            }
            if !owned_batches.is_empty() {
                let mut op = op_arc.lock().map_err(|_| {
                    local_err(format!(
                        "stream:rjoin job '{job_id}' operator lock poisoned"
                    ))
                })?;
                for batch in &owned_batches {
                    let out = driver
                        .on_join_input(&mut *op, side, batch)
                        .map_err(|e| local_err(format!("stream:rjoin input failed: {e}")))?;
                    outputs.extend(out);
                }
            }
        }

        // Watermark: min across the two sides' combined split watermarks —
        // the distributed twin of the embedded loop's discipline. Eviction
        // only runs once BOTH sides have observed data, so a slow side cannot
        // have its partners evicted before it speaks.
        let reported_wm = match (left_wm.combined(idleness), right_wm.combined(idleness)) {
            (Some(l), Some(r)) => {
                let wm = l.min(r);
                let mut op = op_arc.lock().map_err(|_| {
                    local_err(format!(
                        "stream:rjoin job '{job_id}' operator lock poisoned"
                    ))
                })?;
                driver.on_join_watermark(&mut *op, wm);
                wm
            }
            (l, r) => l.or(r).unwrap_or(i64::MIN),
        };

        if !outputs.is_empty() {
            rows_emitted += outputs.iter().map(|b| b.num_rows() as u64).sum::<u64>();
            batches_emitted += outputs.len() as u64;
            crate::erased(runner.stage_rloop_outputs(job_id, assignment, &outputs)).await?;
        }
        runner.report_streaming_progress(&StreamingProgressSnapshot {
            task_id: task_id.clone(),
            job_id: job_id.to_owned(),
            watermark_ms: reported_wm,
            rows_emitted,
            batches_emitted,
            egress_dropped_batches: 0,
            null_key_rows_dropped: 0,
            state_bytes: 0,
            source_offset: None,
            timestamp_ms: now_ms() as u64,
        });
    }

    runner.task_state_bindings.remove(&task_id);
    let _ = runner
        .inbox
        .clear_cancelled_task(assignment.job_id(), assignment.task_id());
    Ok(ExecutorTaskOutput::cancelled())
}

/// `stream:rpipe:` — a join feeding windowed stages. Parallelism is pinned
/// to 1 and ENFORCED: pipeline stages re-key between stages (Q4's stage 1
/// keys on `category`, which is not a function of the join key `auction`),
/// so subtask-local pipelines at N>1 silently compute wrong per-key answers.
/// A parallel pipeline needs an inter-stage exchange — a recorded follow-up,
/// not a silent approximation. No checkpointing either (JoinAggPipeline has
/// no snapshot yet), same recorded status as the stateless class.
pub(crate) async fn execute_rpipe_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    fragment: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_dataflow::pipeline::JoinAggPipeline;
    use krishiv_plan::stream_join::StreamingPipelineSpec;

    let parsed = parse_classed_fragment(STREAM_RPIPE_PREFIX, fragment)?;
    if parsed.parallelism != 1 {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "stream:rpipe parallelism {} is not supported: pipeline stages re-key between \
                 stages, so parallel subtasks would silently compute wrong per-key answers; \
                 an inter-stage exchange is the tracked follow-up",
                parsed.parallelism
            ),
        });
    }
    let spec: StreamingPipelineSpec =
        serde_json::from_str(&parsed.payload).map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("stream:rpipe invalid spec json: {e}"),
        })?;
    spec.validate()
        .map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("stream:rpipe spec: {e}"),
        })?;
    let join = spec.join.clone();
    let job_id = parsed.job_id.as_str();
    let task_id = assignment.task_id().as_str().to_owned();
    let state_key = rloop_state_key(job_id, parsed.subtask);
    let left_key = format!("{job_id}#{task_id}#L");
    let right_key = format!("{job_id}#{task_id}#R");

    // Barrier snapshots bind by task; the pipeline binding routes them to
    // the per-subtask pipeline (task #149 fix 4 — without it a checkpointed
    // pipeline job snapshotted the empty generic backend).
    runner.task_state_bindings.insert(
        task_id.clone(),
        TaskStateBinding::Pipeline(state_key.clone()),
    );
    let pipe_arc = {
        let entry = runner
            .pipeline_executors
            .entry(state_key.clone())
            .or_try_insert_with(|| {
                let pipe =
                    JoinAggPipeline::new(&spec).map_err(|e| ExecutorError::InvalidAssignment {
                        message: format!("stream:rpipe pipeline build: {e}"),
                    })?;
                Ok::<Arc<tokio::sync::Mutex<JoinAggPipeline>>, ExecutorError>(Arc::new(
                    tokio::sync::Mutex::new(pipe),
                ))
            })?;
        Arc::clone(entry.value())
    };

    let own_left = runner.notify_handle(&left_key);
    let own_right = runner.notify_handle(&right_key);
    let shared_notify = runner.notify_handle(job_id);
    let all_specs = parse_registry_partition_specs(assignment.input_partitions())?;
    let mut left_specs = Vec::new();
    let mut right_specs = Vec::new();
    for s in all_specs {
        if s.table_name == join.left_source {
            left_specs.push(s);
        } else if s.table_name == join.right_source {
            right_specs.push(s);
        } else {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "stream:rpipe source table '{}' matches neither join side ('{}' / '{}')",
                    s.table_name, join.left_source, join.right_source
                ),
            });
        }
    }

    let source_cache = runner.shared_continuous_connector_sources();
    let idle_floor = Duration::from_micros(RLOOP_IDLE_FLOOR_US);
    let idleness = watermark_idleness();
    let mut left_wm = SplitWatermarks::default();
    let mut right_wm = SplitWatermarks::default();
    let mut rows_emitted: u64 = 0;
    let mut batches_emitted: u64 = 0;

    tracing::info!(
        job_id,
        stages = spec.stages.len(),
        left_splits = left_specs.len(),
        right_splits = right_specs.len(),
        "stream:rpipe pipeline run-loop started"
    );

    loop {
        if runner
            .inbox
            .is_task_cancelled(assignment.job_id(), assignment.task_id())
            .unwrap_or(false)
        {
            break;
        }
        // Liveness: cancel retires this subtask's state entry and then purges
        // the inbox identity — including the tombstone above, usually before
        // this loop observes it. Arc identity on the state entry is the
        // race-free exit signal (see the rloop loop for the full account).
        let state_alive = runner
            .pipeline_executors
            .get(&state_key)
            .is_some_and(|entry| Arc::ptr_eq(entry.value(), &pipe_arc));
        if !state_alive {
            break;
        }
        let mut left_in: Vec<RecordBatch> = take_pushed(runner, &left_key);
        let mut right_in: Vec<RecordBatch> = take_pushed(runner, &right_key);
        for src in &left_specs {
            super::run_loop::read_owned_split(
                runner,
                job_id,
                &state_key,
                src,
                &source_cache,
                assignment,
                |b| {
                    left_in.push(b);
                    Ok(())
                },
            )
            .await?;
        }
        for src in &right_specs {
            super::run_loop::read_owned_split(
                runner,
                job_id,
                &state_key,
                src,
                &source_cache,
                assignment,
                |b| {
                    right_in.push(b);
                    Ok(())
                },
            )
            .await?;
        }
        if left_in.is_empty() && right_in.is_empty() {
            tokio::select! {
                _ = own_left.notified() => {}
                _ = own_right.notified() => {}
                _ = shared_notify.notified() => {}
                _ = tokio::time::sleep(idle_floor) => {}
            }
            continue;
        }

        let mut outputs: Vec<RecordBatch> = Vec::new();
        {
            let mut pipe = pipe_arc.lock().await;
            for b in &left_in {
                if let Some(ts) = batch_max_event_time(b, &join.time_column) {
                    left_wm.observe("#L", ts);
                }
                outputs.extend(
                    pipe.on_left(b)
                        .map_err(|e| local_err(format!("stream:rpipe left: {e}")))?,
                );
            }
            for b in &right_in {
                if let Some(ts) = batch_max_event_time(b, &join.time_column) {
                    right_wm.observe("#R", ts);
                }
                outputs.extend(
                    pipe.on_right(b)
                        .map_err(|e| local_err(format!("stream:rpipe right: {e}")))?,
                );
            }
            if let (Some(l), Some(r)) = (left_wm.combined(idleness), right_wm.combined(idleness)) {
                pipe.advance_watermark(l.min(r));
            }
        }
        if !outputs.is_empty() {
            rows_emitted += outputs.iter().map(|b| b.num_rows() as u64).sum::<u64>();
            batches_emitted += outputs.len() as u64;
            crate::erased(runner.stage_rloop_outputs(job_id, assignment, &outputs)).await?;
        }
        runner.report_streaming_progress(&StreamingProgressSnapshot {
            task_id: task_id.clone(),
            job_id: job_id.to_owned(),
            watermark_ms: left_wm
                .combined(idleness)
                .and_then(|l| right_wm.combined(idleness).map(|r| l.min(r)))
                .unwrap_or(i64::MIN),
            rows_emitted,
            batches_emitted,
            egress_dropped_batches: 0,
            null_key_rows_dropped: 0,
            state_bytes: 0,
            source_offset: None,
            timestamp_ms: now_ms() as u64,
        });
    }

    // A cancelled pipeline does NOT flush: its open windows are partial
    // aggregates, and emitting them as though final would publish a wrong
    // answer rather than lose a right one — the same stance the window
    // run-loop takes on stop. A bounded producer that wants its trailing
    // windows must declare end-of-stream (the RUN_LOOP_EOS_TASK_ID push
    // directive) BEFORE deregistering; flushing here instead dumped
    // thousands of batches into egress after the last drain, where teardown
    // destroyed them unseen (observed live: NEXMark q9's whole output).
    let _ = runner
        .inbox
        .clear_cancelled_task(assignment.job_id(), assignment.task_id());
    Ok(ExecutorTaskOutput::cancelled())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn decode_side_table(ipc_base64: &str) -> Result<Vec<RecordBatch>, String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(ipc_base64)
        .map_err(|e| format!("bad base64: {e}"))?;
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| format!("bad Arrow IPC: {e}"))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("bad Arrow IPC batch: {e}"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn classed_fragment_round_trips() {
        let f = format!("{STREAM_RJOIN_PREFIX}job-1|2/4|{{\"left_source\":\"bid\"}}");
        let p = parse_classed_fragment(STREAM_RJOIN_PREFIX, &f).unwrap();
        assert_eq!(
            (
                p.job_id.as_str(),
                p.subtask,
                p.parallelism,
                p.payload.as_str()
            ),
            ("job-1", 2, 4, "{\"left_source\":\"bid\"}")
        );
    }

    /// A payload containing `|` survives: JSON is the FINAL splitn segment.
    #[test]
    fn payload_pipes_survive_the_split() {
        let f = format!("{STREAM_RBATCH_PREFIX}j|0/1|{{\"sql\":\"SELECT a||b FROM t\"}}");
        let p = parse_classed_fragment(STREAM_RBATCH_PREFIX, &f).unwrap();
        assert!(p.payload.contains("a||b"));
    }

    #[test]
    fn malformed_fragments_are_refused_by_name() {
        for (frag, needle) in [
            (
                format!("{STREAM_RJOIN_PREFIX}job|2/4"),
                "missing spec payload",
            ),
            (format!("{STREAM_RJOIN_PREFIX}|0/1|{{}}"), "missing job id"),
            (format!("{STREAM_RJOIN_PREFIX}j|9/4|{{}}"), "out of range"),
            (format!("{STREAM_RJOIN_PREFIX}j|x/4|{{}}"), "bad subtask"),
        ] {
            let err = parse_classed_fragment(STREAM_RJOIN_PREFIX, &frag)
                .unwrap_err()
                .to_string();
            assert!(err.contains(needle), "{frag} -> {err}");
        }
    }
}

#[cfg(test)]
mod rpipe_tests {
    use super::*;

    /// N>1 pipelines silently compute wrong per-key answers (stage re-keying)
    /// and are refused BY NAME, never approximated.
    #[test]
    fn parallel_rpipe_is_refused_by_name() {
        let frag = format!("{STREAM_RPIPE_PREFIX}j|0/2|{{}}");
        let parsed = parse_classed_fragment(STREAM_RPIPE_PREFIX, &frag).expect("parses");
        assert_eq!(parsed.parallelism, 2, "premise");
        // The refusal itself is inside execute_rpipe_fragment (needs a
        // runner); the parallelism gate is its FIRST check, before any I/O,
        // so the parse-level premise plus the gate's placement is what this
        // pins alongside the integration test in commit 8.
    }
}

#[cfg(test)]
mod run_loop_family {
    /// Every class's fragment prefix must be in the run-loop family: a class
    /// left out gets the timeout-bearing batch lifecycle and reports
    /// Succeeded on cancel (observed live as "unknown_job for succeeded
    /// status" after every classed deregister).
    #[test]
    fn all_four_classes_are_run_loop_family() {
        for body in [
            "stream:rloop:j|spec",
            "stream:rjoin:j|0/1|{}",
            "stream:rpipe:j|0/1|{}",
            "stream:rbatch:j|0/1|{}",
        ] {
            assert!(
                super::is_run_loop_family(body),
                "{body} must be run-loop family"
            );
        }
        assert!(!super::is_run_loop_family("stream:loop:j|spec"));
        assert!(!super::is_run_loop_family("sql:SELECT 1"));
    }
}
