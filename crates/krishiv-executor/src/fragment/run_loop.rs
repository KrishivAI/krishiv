//! Phase 55: the promoted long-lived streaming run-loop (`stream:rloop:`).
//!
//! The cycle model (`stream:loop:`, see `fragment/streaming.rs`) drives one
//! coordinator-fenced input cycle per task assignment: every cycle pays full
//! task-assignment machinery, output parks in coordinator memory until an HTTP
//! drain poll, and the hot path serializes O(state) twice per cycle. The
//! embedded engine already had the right loop (`run_streaming_continuous` in
//! krishiv-api) — long-running, source-owning, notify-driven, idle-ticking.
//! This module promotes that loop into the distributed runtime:
//!
//! - The task launches **once** and runs until cancelled (the coordinator is
//!   control-plane-only: stop = `CancelTask`, checkpoint = barrier commands,
//!   restore = `RestoreFromCheckpointCommand`).
//! - The subtask **owns its source splits** (registry connector sources,
//!   filtered by subtask index) and wakes on pushed-input notifies with a
//!   microsecond floor and a millisecond fallback tick.
//! - **Key-group parallelism**: a job registers N subtasks; each owns a
//!   contiguous key-group range. Rows outside the owned range are forwarded to
//!   the owning peer over the executor→executor `push_continuous_input` RPC
//!   under a per-channel [`krishiv_common::CreditGate`] (Flink FLINK-7282
//!   model); same-process peers short-circuit through the shared input map.
//! - **Barriers run live**: every iteration boundary drains pending barriers
//!   (`drain_pending_barriers`) so checkpoints are coordinator-driven barrier
//!   alignments; state snapshots happen ONLY at barrier epochs (no per-cycle
//!   snapshot ship, no per-cycle queryable-state RocksDB rebuild).
//! - **Sinks commit at epochs**: output staged into the job's transactional
//!   sink participant is prepared at the barrier and committed by the
//!   checkpoint-complete notification (Iceberg G7 path; Kafka transactional
//!   sink under the same contract).
//! - **Egress**: emitted windows land in a bounded per-job egress buffer the
//!   drain API serves — never the coordinator's inline result store, retiring
//!   the undrained-409 wedge class for run-mode jobs.
//! - **Latency instrument**: each iteration records source-read→operator-emit
//!   latency into the µs-bucket `krishiv_stream_record_latency_seconds`
//!   histogram (the Phase 55 exit-gate instrument).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_proto::{ExecutorTaskAssignment, KeyGroupRange};

use crate::fragment::common::{
    checkpoint_offset_from_dyn_source, parse_registry_partition_specs,
    read_continuous_restore_hint,
};
use crate::runner::{ExecutorTaskOutput, ExecutorTaskRunner, TaskStateBinding};
use crate::{ExecutorError, ExecutorResult};
use krishiv_plan::window::decode_window_execution_spec;

/// Fragment prefix for the promoted run-loop model.
///
/// Format: `stream:rloop:<job_id>|<subtask>/<parallelism>|<window_fragment>`.
/// The prefix is distinct from `stream:loop:` so every cycle-model path
/// (coordinator fencing, G8 certification, HTTP push/drain drivers) is
/// untouched — the cycle model remains the escape hatch.
pub const STREAM_RLOOP_PREFIX: &str = "stream:rloop:";

/// Idle safety floor for the notify wake path (µs), mirroring the embedded loop.
const RLOOP_IDLE_FLOOR_US: u64 = 50;
/// Fallback wake tick when no notify fires (ms), mirroring the embedded loop.
const RLOOP_IDLE_TICK_MS: u64 = 5;
/// Egress buffer cap (batches). Overflow drops the oldest batch — the drain
/// API is best-effort by contract (DUR-5); durable consumption goes through
/// the transactional sink or queryable state.
const RLOOP_EGRESS_CAP: usize = 512;

/// How long a source split may stay silent before it is treated as idle for
/// watermark min-combining (`KRISHIV_WATERMARK_IDLE_MS`, default 30 000).
fn watermark_idleness() -> Duration {
    let ms = std::env::var("KRISHIV_WATERMARK_IDLE_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(30_000);
    Duration::from_millis(ms)
}

/// ST-4 idle-tick interval (`KRISHIV_IDLE_TICK_MS`, default 500) — the same
/// dial the embedded loop reads, now honored distributed.
fn idle_tick_interval() -> Duration {
    let ms = std::env::var("KRISHIV_IDLE_TICK_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(500);
    Duration::from_millis(ms)
}

/// Batch/linger dial: how long the loop accumulates input before draining.
/// `KRISHIV_STREAM_LINGER_MS` wins when set; otherwise `KRISHIV_STREAM_PROFILE`
/// (`throughput` ⇒ 5 ms micro-batching, anything else ⇒ 0 = emit immediately).
/// This carries the embedded `StreamProfile` dial into the distributed loop.
fn stream_linger() -> Duration {
    if let Some(ms) = std::env::var("KRISHIV_STREAM_LINGER_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
    {
        return Duration::from_millis(ms);
    }
    let profile = std::env::var("KRISHIV_STREAM_PROFILE").unwrap_or_default();
    if profile.trim().eq_ignore_ascii_case("throughput") {
        Duration::from_millis(5)
    } else {
        Duration::ZERO
    }
}

/// Parsed identity of one `stream:rloop:` fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RloopFragment {
    pub job_id: String,
    pub subtask: usize,
    pub parallelism: usize,
    pub window_spec: String,
}

/// Parse `stream:rloop:<job_id>|<subtask>/<parallelism>|<window_spec>`.
pub(crate) fn parse_rloop_fragment(fragment: &str) -> ExecutorResult<RloopFragment> {
    let payload = fragment.strip_prefix(STREAM_RLOOP_PREFIX).ok_or_else(|| {
        ExecutorError::InvalidAssignment {
            message: format!(
                "run-loop fragment must start with '{STREAM_RLOOP_PREFIX}'; got: {fragment}"
            ),
        }
    })?;
    let mut parts = payload.splitn(3, '|');
    let job_id = parts.next().unwrap_or("").trim();
    let subtask_spec = parts.next().unwrap_or("").trim();
    let window_spec = parts.next().unwrap_or("").trim();
    if job_id.is_empty() || window_spec.is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "stream:rloop fragment must be \
                 stream:rloop:<job_id>|<subtask>/<parallelism>|<window_spec>; got: {fragment}"
            ),
        });
    }
    let (subtask, parallelism) = subtask_spec
        .split_once('/')
        .and_then(|(s, p)| {
            Some((
                s.trim().parse::<usize>().ok()?,
                p.trim().parse::<usize>().ok()?,
            ))
        })
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: format!(
                "stream:rloop subtask segment '{subtask_spec}' must be <subtask>/<parallelism>"
            ),
        })?;
    if parallelism == 0 || subtask >= parallelism {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "stream:rloop subtask {subtask}/{parallelism} is out of range (need \
                 parallelism ≥ 1 and subtask < parallelism)"
            ),
        });
    }
    Ok(RloopFragment {
        job_id: job_id.to_owned(),
        subtask,
        parallelism,
        window_spec: window_spec.to_owned(),
    })
}

/// State-map key for one run-loop subtask (fixes the H-6 audit gap: two
/// subtasks of the same job never collide on the executor map).
pub(crate) fn rloop_state_key(job_id: &str, subtask: usize) -> String {
    format!("{job_id}#{subtask}")
}

/// Key-group range owned by `subtask` of `parallelism` — MUST match the
/// coordinator's `key_group_range_for_task` (crates/krishiv-scheduler
/// job/record.rs). The run-loop asserts its computed range against the range
/// stamped on the assignment and fails closed on drift.
pub(crate) fn rloop_key_group_range(subtask: usize, parallelism: usize) -> KeyGroupRange {
    const MAX_KEY_GROUPS: u32 = 32_768;
    let p = parallelism.max(1) as u32;
    let idx = subtask as u32;
    let base = MAX_KEY_GROUPS / p;
    let rem = MAX_KEY_GROUPS % p;
    let extra_before = idx.min(rem);
    let start = idx.saturating_mul(base) + extra_before;
    let count = base + u32::from(idx < rem);
    let end = start + count - 1;
    KeyGroupRange::new(start, end)
}

/// One exchange peer: a sibling subtask of the same job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RloopPeer {
    pub subtask: usize,
    pub task_id: String,
    pub endpoint: String,
}

/// Description prefix for the coordinator-injected peer-table partition:
/// `stream-peers:<subtask>=<task_id>@<endpoint>;<subtask>=<task_id>@<endpoint>…`
pub(crate) const STREAM_PEERS_PARTITION_PREFIX: &str = "stream-peers:";

pub(crate) fn parse_stream_peers(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<RloopPeer>> {
    let mut peers = Vec::new();
    for partition in partitions {
        let desc = partition.description().trim();
        let Some(payload) = desc.strip_prefix(STREAM_PEERS_PARTITION_PREFIX) else {
            continue;
        };
        for entry in payload.split(';') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let parsed = entry.split_once('=').and_then(|(subtask, rest)| {
                let (task_id, endpoint) = rest.split_once('@')?;
                Some(RloopPeer {
                    subtask: subtask.trim().parse::<usize>().ok()?,
                    task_id: task_id.trim().to_owned(),
                    endpoint: endpoint.trim().to_owned(),
                })
            });
            match parsed {
                Some(peer) if !peer.task_id.is_empty() && !peer.endpoint.is_empty() => {
                    peers.push(peer)
                }
                _ => {
                    return Err(ExecutorError::InvalidAssignment {
                        message: format!(
                            "stream-peers entry '{entry}' must be \
                             <subtask>=<task_id>@<endpoint>"
                        ),
                    });
                }
            }
        }
    }
    Ok(peers)
}

/// Per-split watermark tracker: max event time per split, min-combined across
/// non-idle splits (Flink watermarks v2 model — a lagging split holds the
/// subtask watermark back; an idle split is excluded after the idleness
/// timeout so it does not stall every downstream window).
#[derive(Debug, Default)]
pub(crate) struct SplitWatermarks {
    splits: std::collections::HashMap<String, (i64, Instant)>,
}

impl SplitWatermarks {
    pub(crate) fn observe(&mut self, split: &str, max_event_time_ms: i64) {
        let now = Instant::now();
        let entry = self
            .splits
            .entry(split.to_owned())
            .or_insert((i64::MIN, now));
        if max_event_time_ms > entry.0 {
            entry.0 = max_event_time_ms;
        }
        entry.1 = now;
    }

    /// Min-combine across splits seen within `idleness`; `None` until any
    /// split has reported. When *every* split is idle the last combined value
    /// over all splits is used (nothing new can be late).
    pub(crate) fn combined(&self, idleness: Duration) -> Option<i64> {
        if self.splits.is_empty() {
            return None;
        }
        let now = Instant::now();
        let active_min = self
            .splits
            .values()
            .filter(|(_, seen)| now.duration_since(*seen) < idleness)
            .map(|(wm, _)| *wm)
            .min();
        active_min.or_else(|| self.splits.values().map(|(wm, _)| *wm).min())
    }
}

/// Compute per-batch max event time for a column of Int64 / Timestamp type.
pub(crate) fn batch_max_event_time(batch: &RecordBatch, column: &str) -> Option<i64> {
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, TimeUnit};
    let idx = batch.schema().index_of(column).ok()?;
    let col = batch.column(idx);
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>()?;
            (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .max()
        }
        DataType::Timestamp(unit, _) => {
            use arrow::array::{
                TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
                TimestampSecondArray,
            };
            let scale = |v: i64| match unit {
                TimeUnit::Second => v.saturating_mul(1_000),
                TimeUnit::Millisecond => v,
                TimeUnit::Microsecond => v / 1_000,
                TimeUnit::Nanosecond => v / 1_000_000,
            };
            let values: Vec<i64> = match unit {
                TimeUnit::Second => {
                    let arr = col.as_any().downcast_ref::<TimestampSecondArray>()?;
                    (0..arr.len())
                        .filter(|&i| !arr.is_null(i))
                        .map(|i| arr.value(i))
                        .collect()
                }
                TimeUnit::Millisecond => {
                    let arr = col.as_any().downcast_ref::<TimestampMillisecondArray>()?;
                    (0..arr.len())
                        .filter(|&i| !arr.is_null(i))
                        .map(|i| arr.value(i))
                        .collect()
                }
                TimeUnit::Microsecond => {
                    let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
                    (0..arr.len())
                        .filter(|&i| !arr.is_null(i))
                        .map(|i| arr.value(i))
                        .collect()
                }
                TimeUnit::Nanosecond => {
                    let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
                    (0..arr.len())
                        .filter(|&i| !arr.is_null(i))
                        .map(|i| arr.value(i))
                        .collect()
                }
            };
            values.into_iter().map(scale).max()
        }
        _ => None,
    }
}

/// Result of routing a batch by key group: `(owned_rows, per_peer_rows)` where
/// `owned_rows` are the local subtask's rows and each `(subtask, batch)` pair in
/// `per_peer_rows` is destined for a co-located peer subtask.
type RoutedBatch = (Option<RecordBatch>, Vec<(usize, RecordBatch)>);

/// Split one batch's rows by owning subtask (via the shared keyed hash →
/// key-group mapping). Returns `(owned_rows, per_peer_rows)`; batches whose
/// key column is absent are treated as fully owned (nothing to route on).
pub(crate) fn route_batch_by_key_group(
    batch: &RecordBatch,
    key_column: &str,
    parallelism: usize,
    own_subtask: usize,
) -> ExecutorResult<RoutedBatch> {
    use arrow::array::{Array, BooleanArray, Int64Array, StringArray};

    if parallelism <= 1 {
        return Ok((Some(batch.clone()), Vec::new()));
    }
    let Ok(key_idx) = batch.schema().index_of(key_column) else {
        return Ok((Some(batch.clone()), Vec::new()));
    };
    let col = batch.column(key_idx);
    // Key bytes per row, matching krishiv-state's keyed hash input.
    let mut row_groups: Vec<u32> = Vec::with_capacity(batch.num_rows());
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        for i in 0..arr.len() {
            let bytes: &[u8] = if arr.is_null(i) {
                &[]
            } else {
                arr.value(i).as_bytes()
            };
            row_groups.push(u32::from(krishiv_state::key_group::key_group_for_key(bytes)));
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        for i in 0..arr.len() {
            let v = if arr.is_null(i) { 0 } else { arr.value(i) };
            row_groups.push(u32::from(krishiv_state::key_group::key_group_for_key(
                &v.to_be_bytes(),
            )));
        }
    } else {
        // Unroutable key type: process locally rather than dropping rows.
        return Ok((Some(batch.clone()), Vec::new()));
    }

    let ranges: Vec<KeyGroupRange> = (0..parallelism)
        .map(|i| rloop_key_group_range(i, parallelism))
        .collect();
    let owner_of = |group: u32| -> usize {
        ranges
            .iter()
            .position(|r| group >= r.start() && group <= r.end())
            .unwrap_or(own_subtask)
    };

    let mut owned_mask = Vec::with_capacity(batch.num_rows());
    let mut peer_rows: std::collections::BTreeMap<usize, Vec<bool>> = Default::default();
    for &group in &row_groups {
        let owner = owner_of(group);
        owned_mask.push(owner == own_subtask);
        if owner != own_subtask {
            peer_rows.entry(owner).or_default();
        }
    }
    if peer_rows.is_empty() {
        return Ok((Some(batch.clone()), Vec::new()));
    }
    for (peer, mask) in peer_rows.iter_mut() {
        *mask = row_groups
            .iter()
            .map(|&g| owner_of(g) == *peer)
            .collect::<Vec<bool>>();
    }

    let filter = |mask: &[bool]| -> ExecutorResult<Option<RecordBatch>> {
        if !mask.iter().any(|&m| m) {
            return Ok(None);
        }
        let mask_array = BooleanArray::from(mask.to_vec());
        arrow::compute::filter_record_batch(batch, &mask_array)
            .map(Some)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("key-group routing filter failed: {e}"),
            })
    };

    let owned = filter(&owned_mask)?;
    let mut routed = Vec::new();
    for (peer, mask) in &peer_rows {
        if let Some(slice) = filter(mask)? {
            routed.push((*peer, slice));
        }
    }
    Ok((owned, routed))
}

/// Deliver exchanged rows to a peer subtask: same-process peers append to the
/// shared input map directly; remote peers go over `push_continuous_input`
/// gated by a per-endpoint credit window.
async fn deliver_to_peer(
    runner: &ExecutorTaskRunner,
    job_id: &str,
    peer: &RloopPeer,
    batches: Vec<RecordBatch>,
) -> ExecutorResult<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let peer_key = format!("{job_id}#{}", peer.task_id);
    let is_local = runner
        .own_task_endpoint
        .as_deref()
        .is_some_and(|own| own == peer.endpoint);
    if is_local {
        runner
            .continuous_inputs
            .entry(peer_key.clone())
            .or_default()
            .extend(batches);
        runner.notify_continuous_input(&peer_key);
        return Ok(());
    }
    runner
        .stream_exchange
        .send(job_id, &peer.task_id, &peer.endpoint, batches)
        .await
}

/// Execute one `stream:rloop:` fragment: the long-lived promoted run-loop.
///
/// Returns only when the task is cancelled (coordinator stop / job teardown)
/// with [`ExecutorTaskOutput::cancelled`], or on a fatal execution error.
pub(crate) async fn execute_run_loop_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    fragment: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    let parsed = parse_rloop_fragment(fragment)?;
    let job_id = parsed.job_id.as_str();
    let task_id = assignment.task_id().as_str().to_owned();
    let state_key = rloop_state_key(job_id, parsed.subtask);
    let input_key = format!("{job_id}#{task_id}");

    // Fail closed if the coordinator's stamped range drifted from the
    // formula the exchange routes by — silent drift would misroute keys.
    let computed_range = rloop_key_group_range(parsed.subtask, parsed.parallelism);
    let stamped = assignment.key_group_range();
    if parsed.parallelism > 1 && stamped != computed_range {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "stream:rloop subtask {}/{} key-group range mismatch: assignment stamped \
                 [{},{}] but the exchange routes by [{},{}]",
                parsed.subtask,
                parsed.parallelism,
                stamped.start(),
                stamped.end(),
                computed_range.start(),
                computed_range.end()
            ),
        });
    }

    let window_spec = decode_window_execution_spec(&parsed.window_spec).map_err(|e| {
        ExecutorError::InvalidAssignment {
            message: format!(
                "stream:rloop invalid window spec '{}': {e}",
                parsed.window_spec
            ),
        }
    })?;
    let key_column = window_spec.key_column.clone();
    let event_time_column = window_spec.event_time_column.clone();

    // Build (or reattach to) this subtask's stateful window executor. Keyed by
    // (job, subtask) — the H-6 fix: sibling subtasks never collide.
    let executor_arc = {
        let entry = runner
            .loop_executors
            .entry(state_key.clone())
            .or_try_insert_with(|| {
                let job_state_dir = runner.state_dir.as_ref().map(|d| d.join(&state_key));
                let mut exec = ContinuousWindowExecutor::new_with_state_dir(
                    window_spec.clone(),
                    job_state_dir.as_deref(),
                )
                .map_err(|e| ExecutorError::InvalidAssignment {
                    message: format!("stream:rloop failed to create window executor: {e}"),
                })?;
                if let Some((_, restored)) = runner.pending_restores.remove(job_id) {
                    let mut non_empty = restored.snapshots.iter().filter(|b| !b.is_empty());
                    if let Some(first) = non_empty.next() {
                        exec.restore_from_snapshot(first).map_err(|e| {
                            ExecutorError::LocalExecution {
                                message: format!(
                                    "stream:rloop restore from epoch {} failed: {e}",
                                    restored.epoch
                                ),
                            }
                        })?;
                        for rest in non_empty {
                            exec.merge_snapshot(rest).map_err(|e| {
                                ExecutorError::LocalExecution {
                                    message: format!(
                                        "stream:rloop merge restore from epoch {} failed: {e}",
                                        restored.epoch
                                    ),
                                }
                            })?;
                        }
                    }
                }
                Ok::<_, ExecutorError>(Arc::new(Mutex::new(exec)))
            })?;
        Arc::clone(entry.value())
    };
    if let Some((snapshot_bytes, _)) = read_continuous_restore_hint(assignment.input_partitions())
    {
        executor_arc
            .lock()
            .map_err(|_| ExecutorError::LocalExecution {
                message: format!("stream:rloop job '{job_id}' executor lock poisoned"),
            })?
            .restore_from_snapshot(&snapshot_bytes)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("stream:rloop restore hint failed: {e}"),
            })?;
    }

    // Bind this task's checkpoint state to the subtask executor so barrier
    // snapshots capture per-subtask state (not a sibling's).
    runner
        .task_state_bindings
        .insert(task_id.clone(), TaskStateBinding::Window(state_key.clone()));
    // Phase 56: restore-time key-group redistribution routes by the job's
    // declared parallelism, not by however many subtasks this process hosts.
    runner
        .rloop_parallelism
        .insert(job_id.to_owned(), parsed.parallelism);
    // Egress buffer + input notifies must exist before the first push races us.
    runner.continuous_outputs.entry(job_id.to_owned()).or_default();
    let own_notify = runner.notify_handle(&input_key);
    let shared_notify = runner.notify_handle(job_id);

    // Peers for the keyed exchange (absent for parallelism 1).
    let peers = parse_stream_peers(assignment.input_partitions())?;

    // Split ownership: non-Kafka registry splits are owned by index
    // (idx % parallelism == subtask). Kafka consumer-group sources are NOT
    // index-filtered — each subtask joins the group and the broker assigns
    // partitions, which also gives dynamic partition discovery for free
    // (topic growth triggers a rebalance, no job restart needed).
    let all_specs = parse_registry_partition_specs(assignment.input_partitions())?;
    let owned_specs: Vec<_> = all_specs
        .into_iter()
        .enumerate()
        .filter(|(idx, spec)| {
            spec.kind.eq_ignore_ascii_case("kafka") || idx % parsed.parallelism == parsed.subtask
        })
        .map(|(_, spec)| spec)
        .collect();

    let restored_source_offsets = runner
        .source_restore_offsets
        .get(job_id)
        .map(|entry| entry.clone())
        .unwrap_or_default();
    let source_cache = runner.shared_continuous_connector_sources();

    let idle_floor = Duration::from_micros(RLOOP_IDLE_FLOOR_US);
    let fallback_tick = Duration::from_millis(RLOOP_IDLE_TICK_MS);
    let idle_tick_period = idle_tick_interval();
    let linger = stream_linger();
    let idleness = watermark_idleness();
    let mut last_idle_tick = Instant::now();
    let mut split_watermarks = SplitWatermarks::default();
    let mut rows_emitted: u64 = 0;
    let mut batches_emitted: u64 = 0;
    let metrics = krishiv_metrics::global_metrics();

    tracing::info!(
        job_id,
        subtask = parsed.subtask,
        parallelism = parsed.parallelism,
        owned_splits = owned_specs.len(),
        peers = peers.len(),
        "stream:rloop promoted run-loop started"
    );

    loop {
        // Cancellation is the run-loop's only exit; check every iteration.
        if runner
            .inbox
            .is_task_cancelled(assignment.task_id())
            .unwrap_or(false)
        {
            break;
        }

        // Leg C: barriers align at iteration boundaries. Snapshots happen
        // ONLY here (barrier epochs) — never per iteration.
        let barriers = runner.drain_barriers_via_context().await;
        if barriers > 0 {
            // Refresh queryable state from the barrier-consistent snapshot
            // (Leg D: no per-cycle ephemeral RocksDB rebuild).
            if let Some(qs) = runner.queryable_state.as_ref()
                && let Ok(mut exec) = executor_arc.lock()
                && let Ok(bytes) = exec.snapshot()
                && !bytes.is_empty()
            {
                use krishiv_state::StateBackend as _;
                let registered = (|| {
                    let mut backend = krishiv_state::RocksDbStateBackend::ephemeral().ok()?;
                    backend.load_snapshot(&bytes).ok()?;
                    qs.register(job_id, "window-exec", Arc::new(backend));
                    Some(())
                })();
                if registered.is_none() {
                    tracing::debug!(job_id, "queryable-state refresh skipped at barrier");
                }
            }
        }

        // Gather this iteration's input.
        let read_started = Instant::now();
        let mut input: Vec<RecordBatch> = Vec::new();
        if let Some((_, pushed)) = runner.continuous_inputs.remove(&input_key) {
            input.extend(pushed);
        }
        if let Some((_, pushed)) = runner.continuous_inputs.remove(job_id) {
            input.extend(pushed);
        }
        if !input.is_empty() {
            for batch in &input {
                if let Some(ts) = batch_max_event_time(batch, &event_time_column) {
                    split_watermarks.observe("push", ts);
                }
            }
        }

        // Owned registry connector splits (the source-owning seam).
        for spec in &owned_specs {
            let source_key = spec.continuous_source_key(&state_key);
            let source_arc = if let Some(entry) = source_cache.get(&source_key) {
                Arc::clone(entry.value())
            } else {
                let mut source = runner
                    .connector_registry
                    .open_source(&spec.connector_config)
                    .await
                    .map_err(|e| ExecutorError::LocalExecution {
                        message: format!(
                            "stream:rloop source open failed for kind '{}' table '{}' \
                             partition '{}': {e}",
                            spec.kind, spec.table_name, spec.partition_id
                        ),
                    })?;
                let restored = (!restored_source_offsets.is_empty())
                    .then_some(restored_source_offsets.as_slice());
                if let Some(offset) = spec.restored_offset(restored) {
                    source
                        .restore_encoded_checkpoint_offset_dyn(&offset.encoded_offset)
                        .map_err(|e| ExecutorError::LocalExecution {
                            message: format!(
                                "stream:rloop source restore failed for partition '{}': {e}",
                                spec.partition_id
                            ),
                        })?;
                }
                let opened = Arc::new(tokio::sync::Mutex::new(source));
                let entry = source_cache
                    .entry(source_key)
                    .or_insert_with(|| Arc::clone(&opened));
                Arc::clone(entry.value())
            };
            let mut source = source_arc.lock().await;
            while let Some(batch) =
                source
                    .read_batch_dyn()
                    .await
                    .map_err(|e| ExecutorError::LocalExecution {
                        message: format!(
                            "stream:rloop source read failed for partition '{}': {e}",
                            spec.partition_id
                        ),
                    })?
            {
                if let Some(ts) = batch_max_event_time(&batch, &event_time_column) {
                    split_watermarks.observe(&spec.partition_id, ts);
                }
                input.push(batch);
            }
            if let Some(offset) = checkpoint_offset_from_dyn_source(spec, source.as_ref())? {
                let task_id_typed = assignment.task_id().clone();
                runner
                    .checkpoint_runners
                    .entry(task_id_typed.clone())
                    .or_insert_with(|| {
                        Arc::new(Mutex::new(crate::runner::TaskRunner::new(task_id_typed)))
                    })
                    .lock()
                    .map_err(|_| ExecutorError::LocalExecution {
                        message: "stream:rloop checkpoint runner lock poisoned".into(),
                    })?
                    .upsert_source_offset(offset);
            }
        }

        if input.is_empty() {
            // ST-4 idle tick: close session windows whose gap elapsed even
            // when every source is quiet (now proven distributed).
            if last_idle_tick.elapsed() >= idle_tick_period {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let idle_outputs = {
                    let mut exec =
                        executor_arc
                            .lock()
                            .map_err(|_| ExecutorError::LocalExecution {
                                message: format!(
                                    "stream:rloop job '{job_id}' executor lock poisoned"
                                ),
                            })?;
                    exec.tick(now_ms)
                        .map_err(|e| ExecutorError::LocalExecution {
                            message: format!("stream:rloop idle tick failed: {e}"),
                        })?
                };
                if !idle_outputs.is_empty() {
                    rows_emitted += idle_outputs.iter().map(|b| b.num_rows() as u64).sum::<u64>();
                    batches_emitted += idle_outputs.len() as u64;
                    runner.stage_rloop_outputs(job_id, assignment, &idle_outputs).await?;
                }
                last_idle_tick = Instant::now();
            }
            // Wake on push notify (own key or the job's shared key), the µs
            // safety floor, or the ms fallback tick — the embedded loop's
            // wake discipline, promoted.
            tokio::select! {
                _ = own_notify.notified() => {}
                _ = shared_notify.notified() => {}
                _ = tokio::time::sleep(idle_floor.max(fallback_tick)) => {}
            }
            continue;
        }

        if !linger.is_zero() {
            // Throughput profile: micro-batch by lingering so per-drain fixed
            // costs amortize over more rows (Arroyo's batch/linger dial).
            tokio::time::sleep(linger).await;
            if let Some((_, pushed)) = runner.continuous_inputs.remove(&input_key) {
                input.extend(pushed);
            }
            if let Some((_, pushed)) = runner.continuous_inputs.remove(job_id) {
                input.extend(pushed);
            }
        }

        // Keyed exchange: keep owned rows, forward the rest to their owners.
        let mut owned_batches: Vec<RecordBatch> = Vec::new();
        let mut outbound: std::collections::BTreeMap<usize, Vec<RecordBatch>> = Default::default();
        for batch in &input {
            let (owned, routed) =
                route_batch_by_key_group(batch, &key_column, parsed.parallelism, parsed.subtask)?;
            if let Some(own) = owned {
                owned_batches.push(own);
            }
            for (peer_subtask, slice) in routed {
                outbound.entry(peer_subtask).or_default().push(slice);
            }
        }
        for (peer_subtask, batches) in outbound {
            let Some(peer) = peers.iter().find(|p| p.subtask == peer_subtask) else {
                return Err(ExecutorError::InvalidAssignment {
                    message: format!(
                        "stream:rloop subtask {} has rows for peer {} but no peer table entry",
                        parsed.subtask, peer_subtask
                    ),
                });
            };
            deliver_to_peer(runner, job_id, peer, batches).await?;
        }

        if owned_batches.is_empty() {
            continue;
        }

        // Drain through the retained window operator.
        let (outputs, loop_watermark) = {
            let mut exec = executor_arc
                .lock()
                .map_err(|_| ExecutorError::LocalExecution {
                    message: format!(
                        "stream:rloop job '{job_id}' executor lock poisoned; \
                         window state is inconsistent — restart the job"
                    ),
                })?;
            let outputs = exec
                .drain(owned_batches)
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!("stream:rloop drain error: {e}"),
                })?;
            (outputs, exec.last_watermark_ms())
        };

        if !outputs.is_empty() {
            // Leg G: the exit-gate instrument — in-engine source-read →
            // operator-emit latency at µs resolution.
            metrics.observe_stream_record_latency(job_id, read_started.elapsed().as_secs_f64());
            rows_emitted += outputs.iter().map(|b| b.num_rows() as u64).sum::<u64>();
            batches_emitted += outputs.len() as u64;
            runner.stage_rloop_outputs(job_id, assignment, &outputs).await?;
        }

        // Report per-subtask progress; the coordinator min-combines subtask
        // watermarks (watermarks v2 across subtasks).
        let reported_wm = split_watermarks
            .combined(idleness)
            .map(|wm| wm.saturating_sub(window_spec.watermark_lag_ms as i64))
            .unwrap_or(loop_watermark);
        runner.report_streaming_progress(&crate::runner::StreamingProgressSnapshot {
            task_id: task_id.clone(),
            job_id: job_id.to_owned(),
            watermark_ms: reported_wm,
            rows_emitted,
            batches_emitted,
            state_bytes: 0,
            source_offset: None,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        });
    }

    runner.task_state_bindings.remove(&task_id);
    let _ = runner.inbox.clear_cancelled_task(assignment.task_id());
    tracing::info!(
        job_id,
        subtask = parsed.subtask,
        rows_emitted,
        "stream:rloop run-loop stopped on cancellation"
    );
    Ok(ExecutorTaskOutput::cancelled())
}

impl ExecutorTaskRunner {
    /// Route one iteration's emitted windows: append to the job's bounded
    /// egress buffer (served by `drain_continuous_output`) and stage into the
    /// job's transactional sink participant when the assignment carries one.
    ///
    /// Leg D economics: staging only — the barrier lifecycle prepares
    /// (`pre_commit` at the barrier ack) and commits (`commit_through` on the
    /// checkpoint-complete notification). Sink visibility latency is the
    /// checkpoint interval, never the push cadence, and nothing here calls
    /// `commit_cycle`.
    pub(crate) async fn stage_rloop_outputs(
        &self,
        job_id: &str,
        assignment: &ExecutorTaskAssignment,
        outputs: &[RecordBatch],
    ) -> ExecutorResult<()> {
        if outputs.is_empty() {
            return Ok(());
        }
        {
            let mut egress = self.continuous_outputs.entry(job_id.to_owned()).or_default();
            egress.extend(outputs.iter().cloned());
            if egress.len() > RLOOP_EGRESS_CAP {
                let overflow = egress.len() - RLOOP_EGRESS_CAP;
                egress.drain(..overflow);
                krishiv_metrics::global_metrics().inc_output_buffer_flush("rloop-egress-drop");
                tracing::warn!(
                    job_id,
                    dropped_batches = overflow,
                    "run-loop egress buffer overflowed; oldest batches dropped                      (drain is best-effort — consume durably via the sink or queryable state)"
                );
            }
        }

        let contract = assignment.output_contract();
        if let Some(descriptor) = crate::fragment::common::iceberg_sink_descriptor(contract)? {
            self.stage_rloop_iceberg(job_id, descriptor, outputs).await?;
        } else if let Some(parsed) =
            krishiv_proto::OutputContractDescriptor::parse_kafka_sink(contract.description())
                .or_else(|| {
                    matches!(
                        contract.descriptor(),
                        Some(krishiv_proto::OutputContractDescriptor::KafkaSink { .. })
                    )
                    .then(|| Ok(contract.descriptor().cloned().unwrap_or(
                        krishiv_proto::OutputContractDescriptor::InlineRecordBatches,
                    )))
                })
        {
            let descriptor = parsed.map_err(|message| ExecutorError::InvalidAssignment {
                message,
            })?;
            self.stage_rloop_kafka(job_id, assignment, descriptor, outputs)
                .await?;
        }
        Ok(())
    }

    /// Stage into the Iceberg streaming sink participant (barrier-lifecycle
    /// 2PC — the G7 path; contrast the cycle model's `commit_cycle`).
    #[cfg(feature = "iceberg")]
    async fn stage_rloop_iceberg(
        &self,
        job_id: &str,
        descriptor: krishiv_proto::OutputContractDescriptor,
        outputs: &[RecordBatch],
    ) -> ExecutorResult<()> {
        use krishiv_connectors::lakehouse::streaming_sink::{
            IcebergSinkTarget, IcebergStreamingSink, schema_version_from_arrow,
        };
        let krishiv_proto::OutputContractDescriptor::IcebergSink {
            root,
            table,
            mode,
            key_columns,
            op_column,
        } = descriptor
        else {
            return Err(ExecutorError::InvalidAssignment {
                message: "stage_rloop_iceberg requires an IcebergSink descriptor".into(),
            });
        };
        let Some(schema) = outputs.first().map(|b| b.schema()) else {
            return Ok(());
        };
        let registry = self.transaction_log.clone();
        let job = job_id.to_owned();
        let batches = outputs.to_vec();
        tokio::task::spawn_blocking(move || {
            let participant = registry.get_or_register(&job, || {
                let schema_version = schema_version_from_arrow(&schema, op_column.as_deref())?;
                IcebergStreamingSink::open(
                    IcebergSinkTarget {
                        root: std::path::PathBuf::from(root),
                        table,
                        mode,
                        key_columns,
                        op_column,
                    },
                    schema_version,
                )
            })?;
            let mut guard =
                participant
                    .lock()
                    .map_err(|_| krishiv_connectors::ConnectorError::Protocol {
                        message: format!(
                            "iceberg sink participant lock poisoned for job {job};                              sink state is unreliable — restart the job"
                        ),
                    })?;
            for batch in &batches {
                guard.stage(batch)?;
            }
            Ok::<_, krishiv_connectors::ConnectorError>(())
        })
        .await
        .map_err(|join_error| ExecutorError::LocalExecution {
            message: format!("iceberg sink staging task panicked: {join_error}"),
        })?
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("iceberg sink staging failed for job {job_id}: {error}"),
        })
    }

    #[cfg(not(feature = "iceberg"))]
    async fn stage_rloop_iceberg(
        &self,
        job_id: &str,
        _descriptor: krishiv_proto::OutputContractDescriptor,
        _outputs: &[RecordBatch],
    ) -> ExecutorResult<()> {
        Err(ExecutorError::InvalidAssignment {
            message: format!(
                "job {job_id} requests an Iceberg sink but this executor was built                  without the `iceberg` feature"
            ),
        })
    }

    /// Stage into the transactional Kafka sink under the same epoch/2PC
    /// contract (Phase 55: sink descriptors beyond Iceberg — the parked
    /// `kafka_transactional_sink` finally wired; `read_committed` consumers
    /// observe exactly-once output).
    #[cfg(feature = "kafka")]
    async fn stage_rloop_kafka(
        &self,
        job_id: &str,
        assignment: &ExecutorTaskAssignment,
        descriptor: krishiv_proto::OutputContractDescriptor,
        outputs: &[RecordBatch],
    ) -> ExecutorResult<()> {
        use krishiv_connectors::two_phase::EpochTransactionLog;
        let krishiv_proto::OutputContractDescriptor::KafkaSink {
            bootstrap_servers,
            topic,
            transactional_id_prefix,
        } = descriptor
        else {
            return Err(ExecutorError::InvalidAssignment {
                message: "stage_rloop_kafka requires a KafkaSink descriptor".into(),
            });
        };
        let registry = self.transaction_log.clone();
        let job = job_id.to_owned();
        // Stable per-subtask transactional.id — required for Kafka zombie
        // fencing (per-epoch IDs would let a stale producer commit).
        let transactional_id = format!(
            "{transactional_id_prefix}/{job_id}/{}",
            assignment.task_id().as_str()
        );
        let batches = outputs.to_vec();
        tokio::task::spawn_blocking(move || {
            let participant = registry.get_or_register(&job, || {
                let sink = krishiv_connectors::RdkafkaTransactionalSink::new(
                    &bootstrap_servers,
                    topic,
                    &transactional_id,
                )?;
                Ok(EpochTransactionLog::new(sink))
            })?;
            let mut guard =
                participant
                    .lock()
                    .map_err(|_| krishiv_connectors::ConnectorError::Protocol {
                        message: format!(
                            "kafka sink participant lock poisoned for job {job};                              sink state is unreliable — restart the job"
                        ),
                    })?;
            for batch in &batches {
                guard.stage(batch)?;
            }
            Ok::<_, krishiv_connectors::ConnectorError>(())
        })
        .await
        .map_err(|join_error| ExecutorError::LocalExecution {
            message: format!("kafka sink staging task panicked: {join_error}"),
        })?
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("kafka sink staging failed for job {job_id}: {error}"),
        })
    }

    #[cfg(not(feature = "kafka"))]
    async fn stage_rloop_kafka(
        &self,
        job_id: &str,
        _assignment: &ExecutorTaskAssignment,
        _descriptor: krishiv_proto::OutputContractDescriptor,
        _outputs: &[RecordBatch],
    ) -> ExecutorResult<()> {
        Err(ExecutorError::InvalidAssignment {
            message: format!(
                "job {job_id} requests a Kafka sink but this executor was built                  without the `kafka` feature"
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn rloop_fragment_round_trips_identity() {
        let parsed =
            parse_rloop_fragment("stream:rloop:job-a|1/3|stream:tw:key=k:time=ts:win=1000:lag=0:agg=count")
                .unwrap();
        assert_eq!(parsed.job_id, "job-a");
        assert_eq!(parsed.subtask, 1);
        assert_eq!(parsed.parallelism, 3);
        assert!(parsed.window_spec.starts_with("stream:tw:"));
    }

    #[test]
    fn rloop_fragment_rejects_out_of_range_subtask() {
        let err = parse_rloop_fragment("stream:rloop:job-a|3/3|spec").unwrap_err();
        assert!(err.to_string().contains("out of range"), "{err}");
    }

    #[test]
    fn key_group_ranges_partition_the_full_space() {
        for parallelism in [1usize, 2, 3, 5, 8] {
            let mut covered = 0u32;
            let mut prev_end: Option<u32> = None;
            for i in 0..parallelism {
                let r = rloop_key_group_range(i, parallelism);
                if let Some(end) = prev_end {
                    assert_eq!(r.start(), end + 1);
                }
                covered += r.end() - r.start() + 1;
                prev_end = Some(r.end());
            }
            assert_eq!(covered, 32_768, "parallelism {parallelism}");
            assert_eq!(prev_end, Some(32_767));
        }
    }

    #[test]
    fn stream_peers_parse_and_reject_malformed() {
        let ok = krishiv_proto::InputPartition::new(
            "p0",
            "stream-peers:0=task-streaming-0@http://a:1;2=task-streaming-2@http://b:2",
        );
        let peers = parse_stream_peers(std::slice::from_ref(&ok)).unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[1].subtask, 2);
        assert_eq!(peers[1].endpoint, "http://b:2");

        let bad = krishiv_proto::InputPartition::new("p0", "stream-peers:0=broken");
        assert!(parse_stream_peers(std::slice::from_ref(&bad)).is_err());
    }

    #[test]
    fn split_watermarks_min_combine_with_idleness() {
        let mut wm = SplitWatermarks::default();
        wm.observe("p0", 1_000);
        wm.observe("p1", 5_000);
        // Both active → min wins (the lagging split holds the watermark back).
        assert_eq!(wm.combined(Duration::from_secs(60)), Some(1_000));
        // Idleness zero → both idle → fall back to the min over all splits.
        assert_eq!(wm.combined(Duration::ZERO), Some(1_000));
    }

    #[test]
    fn route_batch_partitions_rows_by_key_group_ownership() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let keys: Vec<String> = (0..200).map(|i| format!("user-{i}")).collect();
        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(StringArray::from(key_refs)) as _,
                StdArc::new(Int64Array::from((0..200).collect::<Vec<i64>>())) as _,
            ],
        )
        .unwrap();

        let parallelism = 3;
        let mut total = 0usize;
        for subtask in 0..parallelism {
            let (owned, routed) =
                route_batch_by_key_group(&batch, "k", parallelism, subtask).unwrap();
            let own_rows = owned.map(|b| b.num_rows()).unwrap_or(0);
            total += own_rows;
            // Rows routed away from this subtask must exactly complement it.
            let routed_rows: usize = routed.iter().map(|(_, b)| b.num_rows()).sum();
            assert_eq!(own_rows + routed_rows, 200);
        }
        // Each row is owned by exactly one subtask.
        assert_eq!(total, 200);
    }
}
