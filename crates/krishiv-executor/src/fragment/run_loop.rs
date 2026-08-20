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
//!   microsecond safety floor.
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
    checkpoint_offset_from_dyn_source, coerce_batch_for_window, parse_registry_partition_specs,
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
pub(crate) const RLOOP_IDLE_FLOOR_US: u64 = 50;

/// How long a source split may stay silent before it is treated as idle for
/// watermark min-combining (`KRISHIV_WATERMARK_IDLE_MS`, default 30 000).
pub(crate) fn watermark_idleness() -> Duration {
    let ms = std::env::var("KRISHIV_WATERMARK_IDLE_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(30_000);
    Duration::from_millis(ms)
}

// ST-4 idle-tick interval and the batch/linger dial, shared verbatim with the
// embedded loop.
//
// These used to be a second implementation of each — same defaults, same
// `"throughput"` comparison, written out again. They agreed, but nothing made
// them agree, and a dial that means one thing embedded and another distributed
// is invisible: both engines run, both look configured, and only a side-by-side
// benchmark shows it. See `krishiv_common::streaming_dials`.
use krishiv_common::streaming_dials::{rloop_egress_cap, stream_linger};

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

/// Result of routing a batch by key group.
pub(crate) struct RoutedBatch {
    /// Rows this subtask owns and will process locally.
    pub owned: Option<RecordBatch>,
    /// Rows destined for a peer subtask, keyed by that peer's subtask index.
    pub routed: Vec<(usize, RecordBatch)>,
    /// Rows dropped because their group key is NULL — see
    /// [`split_null_key_rows`].
    pub null_key_rows: usize,
}

/// Split off rows whose group key is NULL.
///
/// The engine's group-key policy is fail-closed: `extract_agg_key` rejects a
/// NULL key rather than inventing a null group, and every window operator
/// propagates that rejection. That policy is fine — but its **blast radius**
/// was not.
///
/// Routing called `extract_agg_key(..)?` per row inside the run-loop's own
/// `for batch in &input` loop, so one NULL key in one row of one batch returned
/// `Err` out of `execute_run_loop_fragment` and **terminated the long-lived
/// streaming job**. Worse, `input` is a loop-local `Vec` filled by destructive
/// `remove` from the shared input map, so the error took every other buffered
/// row in that cycle down with it — rows that were perfectly well-formed and
/// had already been removed from the only place they existed.
///
/// For a batch query "reject the query" is a reasonable answer to bad input.
/// For an unbounded stream reading a source you do not control, one malformed
/// row is a permanent outage plus silent loss of its neighbours. So the rows
/// are quarantined and counted instead, and the job stays up.
///
/// Type errors are deliberately still fatal: `coerce_batch_for_window` casts
/// into the legal key types, so an unsupported key type is a plan or coercion
/// fault that is uniform across the whole job, not bad data in one row.
fn split_null_key_rows(
    batch: &RecordBatch,
    key_idx: usize,
) -> ExecutorResult<(Option<RecordBatch>, usize)> {
    let null_count = batch.column(key_idx).null_count();
    if null_count == 0 {
        // The overwhelmingly common case, and O(1) on Arrow — no per-row work
        // is added to the hot path to buy this guarantee.
        return Ok((Some(batch.clone()), 0));
    }
    if null_count == batch.num_rows() {
        return Ok((None, null_count));
    }
    let keep = arrow::compute::is_not_null(batch.column(key_idx).as_ref()).map_err(|e| {
        ExecutorError::LocalExecution {
            message: format!("null-key mask failed: {e}"),
        }
    })?;
    let kept = arrow::compute::filter_record_batch(batch, &keep).map_err(|e| {
        ExecutorError::LocalExecution {
            message: format!("null-key filter failed: {e}"),
        }
    })?;
    Ok((Some(kept), null_count))
}

/// Split one batch's rows by owning subtask (via the shared keyed hash →
/// key-group mapping). Returns `(owned_rows, per_peer_rows)`; batches whose
/// key column is absent are treated as fully owned (nothing to route on).
pub(crate) fn route_batch_by_key_group(
    batch: &RecordBatch,
    key_column: &str,
    parallelism: usize,
    own_subtask: usize,
) -> ExecutorResult<RoutedBatch> {
    use arrow::array::BooleanArray;

    let Ok(key_idx) = batch.schema().index_of(key_column) else {
        return Ok(RoutedBatch {
            owned: Some(batch.clone()),
            routed: Vec::new(),
            null_key_rows: 0,
        });
    };
    // Quarantine NULL-key rows BEFORE the parallelism check, not after: at
    // parallelism 1 routing short-circuits entirely, so a NULL key would sail
    // past here and kill the job one layer down inside the window operator
    // instead. The guard has to sit above the short-circuit to hold for every
    // shape of run-loop job.
    let (batch, null_key_rows) = split_null_key_rows(batch, key_idx)?;
    let Some(batch) = batch else {
        return Ok(RoutedBatch {
            owned: None,
            routed: Vec::new(),
            null_key_rows,
        });
    };
    let batch = &batch;

    if parallelism <= 1 {
        return Ok(RoutedBatch {
            owned: Some(batch.clone()),
            routed: Vec::new(),
            null_key_rows,
        });
    }
    // ROUTING/PERSISTENCE CONTRACT.
    //
    // These bytes MUST be byte-identical to the group key the window operators
    // persist — `extract_agg_key(..).to_string()` (krishiv-dataflow
    // window/tumbling.rs, sliding.rs, session.rs) — because restore-time
    // key-group redistribution re-hashes the *persisted* form. Deriving them
    // from the same call is what keeps the two in agreement; hashing anything
    // else here reintroduces the divergence below.
    //
    // This used to hash `arr.value(i).as_bytes()` for Utf8 and
    // `v.to_be_bytes()` for Int64. Utf8 agreed with persistence by coincidence
    // (same characters); Int64 did not — persistence writes ASCII decimal, so
    // routing and redistribution hashed different byte strings and landed in
    // unrelated key groups. Invisible while a job runs, because every subtask
    // routes identically; it bit on a redistributing restore, where each Int64
    // key had a (P-1)/P chance of being handed to a subtask that would never
    // receive its rows. Utf8-only routing tests is why nothing noticed.
    //
    // Using `extract_agg_key` also makes Int32 / Float64 / Bool / LargeUtf8 /
    // Utf8View routable. They are declared-legal key types and
    // `coerce_batch_for_window` casts into them, but they previously fell into
    // an "unroutable, process locally" fallback — which meant no exchange at
    // all: index-partitioned splits gave each subtask a disjoint row subset but
    // an OVERLAPPING key set, so every shared key accumulated an independent
    // partial aggregate per subtask and emitted one output row each.
    let mut row_groups: Vec<u32> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let key = krishiv_dataflow::join::extract_agg_key(batch, key_idx, row)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("stream:rloop cannot derive a routing key: {e}"),
            })?
            .to_string();
        row_groups.push(u32::from(krishiv_state::key_group::key_group_for_key(
            key.as_bytes(),
        )));
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
        return Ok(RoutedBatch {
            owned: Some(batch.clone()),
            routed: Vec::new(),
            null_key_rows,
        });
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
    Ok(RoutedBatch {
        owned,
        routed,
        null_key_rows,
    })
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
    deliver_to_peer_suffixed(runner, job_id, peer, batches, "").await
}

/// Peer delivery with a side suffix on the input key (task #147): a join
/// exchange must land rows on the peer's `#L` / `#R` buffer so side
/// identity survives the hop. The window exchange passes `""`.
pub(crate) async fn deliver_to_peer_suffixed(
    runner: &ExecutorTaskRunner,
    job_id: &str,
    peer: &RloopPeer,
    batches: Vec<RecordBatch>,
    suffix: &str,
) -> ExecutorResult<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let peer_key = format!("{job_id}#{}{suffix}", peer.task_id);
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
    // The remote leg must carry the suffix IN the task id: the receiving
    // executor rebuilds the buffer key as `{job}#{request.task_id}`, so a
    // bare task id here would land side-tagged rows on the untagged buffer
    // and silently erase the side.
    let target_task = format!("{}{suffix}", peer.task_id);
    runner
        .stream_exchange
        .send(job_id, &target_task, &peer.endpoint, batches)
        .await
}

/// Open-or-reattach one owned registry split, read every ready batch through
/// `on_batch`, and upsert its checkpoint offset. Factored from the rloop body
/// so the classed loops (`stream:rjoin:`/`stream:rbatch:`) share the exact
/// open/restore/read/offset discipline instead of a drifting copy.
pub(crate) async fn read_owned_split(
    runner: &ExecutorTaskRunner,
    job_id: &str,
    state_key: &str,
    spec: &crate::fragment::common::RegistryPartitionSpec,
    source_cache: &crate::runner::SharedContinuousConnectorSources,
    assignment: &ExecutorTaskAssignment,
    mut on_batch: impl FnMut(RecordBatch) -> ExecutorResult<()>,
) -> ExecutorResult<()> {
    let source_key = spec.continuous_source_key(state_key);
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
        // Read the restore table HERE, not once before the loop.
        //
        // A `RestoreFromCheckpointCommand` arriving mid-run rewrites
        // `runner.source_restore_offsets` and then calls
        // `clear_continuous_connector_sources_for_job`, which evicts this
        // subtask's cached sources (they key on `{job}#{subtask}`). The
        // next iteration therefore lands right here and reopens them — so
        // a snapshot taken at loop start would re-apply the *pre-restore*
        // offsets and resume the subtask from the wrong position, quietly
        // replaying or skipping records. The cycle model never had this:
        // `read_continuous_registry_sources` is called per cycle and so
        // reads the table fresh every time.
        let restored_source_offsets = runner
            .source_restore_offsets
            .get(job_id)
            .map(|entry| entry.clone())
            .unwrap_or_default();
        let restored =
            (!restored_source_offsets.is_empty()).then_some(restored_source_offsets.as_slice());
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
        on_batch(batch)?;
    }
    if let Some(offset) = checkpoint_offset_from_dyn_source(spec, source.as_ref())? {
        let task_id_typed = assignment.task_id().clone();
        runner
            .checkpoint_runners
            .entry(task_id_typed.clone())
            .or_insert_with(|| Arc::new(Mutex::new(crate::runner::TaskRunner::new(task_id_typed))))
            .lock()
            .map_err(|_| ExecutorError::LocalExecution {
                message: "stream:rloop checkpoint runner lock poisoned".into(),
            })?
            .upsert_source_offset(offset);
    }
    Ok(())
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

    // Phase 56: restore-time key-group redistribution routes by the job's
    // declared parallelism, not by however many subtasks this process hosts.
    //
    // Published BEFORE the executor becomes visible in `loop_executors`. It
    // used to be set afterwards, so a RestoreFromCheckpointCommand landing in
    // that window found the executor but not the parallelism, and fell back to
    // the local subtask count — redistributing into the wrong number of
    // buckets and scattering state to subtasks that do not own it.
    runner
        .rloop_parallelism
        .insert(job_id.to_owned(), parsed.parallelism);

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
                // The coordinator stashes the WHOLE job's per-task snapshots
                // under the bare job id, but this closure runs once per
                // (job, subtask). A job-keyed `remove` therefore handed the
                // entire checkpoint to whichever sibling constructed first —
                // which loaded snapshot[0] into itself and merged every other
                // subtask's state into itself too — and left every sibling
                // with nothing.
                //
                // The result was a silent wrong answer, not a crash: the
                // greedy subtask holds accumulators for key groups it does not
                // own, so no post-restore row for those keys ever reaches it,
                // yet it still flushes them when its own watermark passes.
                // Meanwhile the true owner restarts those keys from zero. Each
                // affected (key, window) emits twice, both values wrong, and
                // the greedy subtask re-persists the foreign key groups at its
                // next barrier, making the corruption durable.
                //
                // Route by key group at the job's DECLARED parallelism and load
                // only this subtask's share — the same contract the live
                // restore path uses. `get` rather than `remove`: every sibling
                // in this process needs the same entry.
                if let Some(restored) = runner
                    .pending_restores
                    .get(job_id)
                    .map(|entry| entry.value().clone())
                {
                    let shares = krishiv_state::redistribute_snapshots(
                        &restored.snapshots,
                        parsed.parallelism as u32,
                        krishiv_state::EntryRouting::WindowGroupKey,
                    )
                    .map_err(|e| ExecutorError::LocalExecution {
                        message: format!(
                            "stream:rloop key-group redistribution of epoch {} failed: {e}",
                            restored.epoch
                        ),
                    })?;
                    let share = shares.get(parsed.subtask).ok_or_else(|| {
                        ExecutorError::LocalExecution {
                            message: format!(
                                "stream:rloop subtask {} is outside the redistribution range \
                                 for parallelism {}",
                                parsed.subtask, parsed.parallelism
                            ),
                        }
                    })?;
                    // An empty share means this subtask owns no checkpointed
                    // key group; the decoder rejects empty bytes, so skip it.
                    if !share.is_empty() {
                        exec.restore_from_snapshot(share).map_err(|e| {
                            ExecutorError::LocalExecution {
                                message: format!(
                                    "stream:rloop restore from epoch {} failed: {e}",
                                    restored.epoch
                                ),
                            }
                        })?;
                    }
                }
                Ok::<_, ExecutorError>(Arc::new(Mutex::new(exec)))
            })?;
        Arc::clone(entry.value())
    };
    if let Some((snapshot_bytes, _)) = read_continuous_restore_hint(assignment.input_partitions()) {
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
    // Egress buffer + input notifies must exist before the first push races us.
    runner
        .continuous_outputs
        .entry(job_id.to_owned())
        .or_default();
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

    let source_cache = runner.shared_continuous_connector_sources();

    let idle_floor = Duration::from_micros(RLOOP_IDLE_FLOOR_US);
    // The idle-tick interval and its elapsed gate both live in the driver now.
    // `StreamingLoop::RunLoop` declares `IdleTick::WallClock`, which is the
    // record of what used to be an unwritten agreement between this loop and
    // the embedded one — and an unwritten agreement the cycle and the seam did
    // not share.
    let mut driver = krishiv_dataflow::stream_driver::StreamDriver::new(
        krishiv_dataflow::stream_driver::StreamingLoop::RunLoop,
    );
    let linger = stream_linger();
    let idleness = watermark_idleness();
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
            .is_task_cancelled(assignment.job_id(), assignment.task_id())
            .unwrap_or(false)
        {
            break;
        }
        // Liveness: the cancel handler retires this subtask's state entry and
        // then purges the job's inbox identity — INCLUDING the cancel
        // tombstone above, usually before this loop wakes to observe it. The
        // entry's Arc identity is the race-free signal: gone (cancelled) or
        // replaced (a recreated incarnation took the key) both mean this loop
        // must exit. Without this every deregistered job left its loop running
        // and leaked the runner slot it occupied (the 3-node k3s wedge).
        let state_alive = runner
            .loop_executors
            .get(&state_key)
            .is_some_and(|entry| Arc::ptr_eq(entry.value(), &executor_arc));
        if !state_alive {
            break;
        }

        // Leg C: barriers align at iteration boundaries. Snapshots happen
        // ONLY here (barrier epochs) — never per iteration.
        let barriers = crate::erased(runner.drain_barriers_via_context()).await;
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
        // Pushed input (external producers AND peer-exchange deliveries) gets
        // the same coercion as the owned-split path below. It used to be
        // extended raw, so an unsigned source column (NEXMark's u64 price)
        // reached the aggregate uncoerced and killed the fragment with
        // "unsupported column type for pre-downcast: UInt64" — the embedded
        // loop coerces every batch, and the two paths must not disagree.
        for (_, pushed) in [
            runner.continuous_inputs.remove(&input_key),
            runner.continuous_inputs.remove(job_id),
        ]
        .into_iter()
        .flatten()
        {
            for batch in pushed {
                input.push(coerce_batch_for_window(&batch, &window_spec)?);
            }
        }
        if !input.is_empty() {
            for batch in &input {
                if let Some(ts) = batch_max_event_time(batch, &event_time_column) {
                    split_watermarks.observe("push", ts);
                }
            }
        }

        // Owned registry connector splits (the source-owning seam), through the
        // ONE shared reader (task #147 factored it so the classed loops cannot
        // drift from this one — the sibling-defect rule).
        for spec in &owned_specs {
            read_owned_split(
                runner,
                job_id,
                &state_key,
                spec,
                &source_cache,
                assignment,
                |batch| {
                    let batch = coerce_batch_for_window(&batch, &window_spec)?;
                    if let Some(ts) = batch_max_event_time(&batch, &event_time_column) {
                        split_watermarks.observe(&spec.partition_id, ts);
                    }
                    input.push(batch);
                    Ok(())
                },
            )
            .await?;
        }

        // ST-4 idle tick: close session windows whose inactivity gap elapsed.
        //
        // Gated on wall-clock elapsed only — deliberately NOT on `input`. It
        // used to sit inside the `input.is_empty()` branch below, which ties a
        // wall-clock obligation to whether this iteration happened to find
        // input. Under sustained arrival — the backpressure case, i.e. exactly
        // when it matters — the combined input never goes empty and the tick
        // never fires, so a quiet split's session windows stay open while a
        // chatty split keeps the loop busy. With per-source watermarks the
        // effective watermark is the min across sources, so the quiet source
        // holds every window open until `apply_idle_source_policy` runs, which
        // is what `tick` does.
        //
        // NOT covered by a regression test, deliberately: reproducing it needs
        // arrival to outpace drain for a whole tick period, and every fixture
        // that pushes at a test-reasonable rate lets the input empty between
        // batches, so the tick fires and the test passes against the broken
        // code too. A test that cannot tell the two apart is worse than none.
        // The change is justified by construction instead: it matches the
        // embedded loop, which ticks on elapsed wall clock regardless of input.
        {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let idle_outputs = {
                let mut exec = executor_arc
                    .lock()
                    .map_err(|_| ExecutorError::LocalExecution {
                        message: format!("stream:rloop job '{job_id}' executor lock poisoned"),
                    })?;
                driver
                    .on_idle(&mut *exec, now_ms)
                    .map_err(|e| ExecutorError::LocalExecution {
                        message: format!("stream:rloop idle tick failed: {e}"),
                    })?
            };
            if !idle_outputs.is_empty() {
                rows_emitted += idle_outputs
                    .iter()
                    .map(|b| b.num_rows() as u64)
                    .sum::<u64>();
                batches_emitted += idle_outputs.len() as u64;
                crate::erased(runner.stage_rloop_outputs(job_id, assignment, &idle_outputs))
                    .await?;
            }
        }

        if input.is_empty() {
            // Wake on push notify (own key or the job's shared key) or the µs
            // safety floor — the embedded loop's wake discipline, promoted.
            //
            // This used to sleep `idle_floor.max(fallback_tick)`. The floor is
            // 50 µs and the fallback 5 ms, so the max was *always* 5 ms and the
            // floor never applied — a 100x slower re-poll than the constant and
            // the module doc both advertise. The embedded loop only reaches for
            // its 5 ms fallback when a source has no notify at all; a run-loop
            // subtask always has both notifies, so there is no such case here.
            tokio::select! {
                _ = own_notify.notified() => {}
                _ = shared_notify.notified() => {}
                _ = tokio::time::sleep(idle_floor) => {}
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
        let mut null_key_rows = 0usize;
        for batch in &input {
            let routed =
                route_batch_by_key_group(batch, &key_column, parsed.parallelism, parsed.subtask)?;
            null_key_rows += routed.null_key_rows;
            if let Some(own) = routed.owned {
                owned_batches.push(own);
            }
            for (peer_subtask, slice) in routed.routed {
                outbound.entry(peer_subtask).or_default().push(slice);
            }
        }
        if null_key_rows > 0 {
            // Quarantined, not fatal — see `split_null_key_rows`. Counted so
            // the loss is discoverable: a job that drops rows and says nothing
            // is the same silent-wrong-answer shape as one that returns the
            // wrong number.
            let total = runner
                .continuous_null_key_rows
                .entry(job_id.to_string())
                .and_modify(|n| *n += null_key_rows as u64)
                .or_insert(null_key_rows as u64);
            tracing::warn!(
                job_id = %job_id,
                subtask = parsed.subtask,
                dropped = null_key_rows,
                total = *total,
                key_column = %key_column,
                "stream:rloop dropped rows whose group key is NULL; they cannot be \
                 aggregated and are excluded rather than failing the job"
            );
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
            crate::erased(deliver_to_peer(runner, job_id, peer, batches)).await?;
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
            // Routing runs BEFORE this, so the driver only ever sees rows this
            // subtask owns. `StreamingLoop::RunLoop` declares
            // `InputTyping::PreCoerced` because the cast already happened at
            // the read site — routing reads the key column and must read it at
            // its final type, so coercing again here would be a second pass
            // over every batch on the hot path for no change in result.
            let outputs = driver.on_input(&mut *exec, owned_batches).map_err(|e| {
                ExecutorError::LocalExecution {
                    message: format!("stream:rloop drain error: {e}"),
                }
            })?;
            (outputs, exec.last_watermark_ms())
        };

        if !outputs.is_empty() {
            // Leg G: the exit-gate instrument — in-engine source-read →
            // operator-emit latency at µs resolution.
            metrics.observe_stream_record_latency(job_id, read_started.elapsed().as_secs_f64());
            rows_emitted += outputs.iter().map(|b| b.num_rows() as u64).sum::<u64>();
            batches_emitted += outputs.len() as u64;
            crate::erased(runner.stage_rloop_outputs(job_id, assignment, &outputs)).await?;
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
            egress_dropped_batches: runner
                .continuous_egress_dropped
                .get(job_id)
                .map(|e| *e.value())
                .unwrap_or(0),
            null_key_rows_dropped: runner
                .continuous_null_key_rows
                .get(job_id)
                .map(|e| *e.value())
                .unwrap_or(0),
            state_bytes: 0,
            source_offset: None,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        });
    }

    runner.task_state_bindings.remove(&task_id);
    // The restore entry is read, not consumed, by the executor constructor
    // (every sibling subtask needs the same entry), so it is dropped at
    // teardown — otherwise a job re-created with the same deterministic id
    // would be silently seeded from the dead incarnation's checkpoint.
    //
    // But `pending_restores` is keyed by JOB while this teardown runs per
    // SUBTASK, so removing it unconditionally would let the first subtask to
    // stop delete an entry a sibling that has not yet constructed its executor
    // still needs — reintroducing, in a narrower window, exactly the starvation
    // this file was just fixed for. Only the last subtask of this job in this
    // process clears it.
    //
    // `task_state_bindings` is the right thing to count: it is keyed by task
    // and this task's own binding was removed on the line above, so what
    // remains is the live siblings. (`loop_executors` cannot be used — the
    // run-loop deliberately keeps executors cached across reattach, so its
    // entries never go away.)
    let job_binding_prefix = format!("{job_id}#");
    let siblings_still_running = runner.task_state_bindings.iter().any(|entry| {
        matches!(entry.value(), TaskStateBinding::Window(key) if key.starts_with(&job_binding_prefix))
    });
    if !siblings_still_running {
        runner.pending_restores.remove(job_id);
        // The same last-subtask rule governs every other JOB-keyed entry: the
        // source read positions, the shared registry sink, and the per-job loss
        // counters are all shared by the siblings, so retiring them on the
        // first subtask to stop would pull them out from under the others.
        runner.retire_continuous_job_state(job_id);
    }
    let _ = runner
        .inbox
        .clear_cancelled_task(assignment.job_id(), assignment.task_id());
    // What a stop costs, stated at the moment it costs it.
    //
    // The teardown above is bookkeeping: it does NOT flush open windows and
    // does NOT snapshot. Not force-flushing is right — `flush_all` is scoped to
    // an exhausted bounded source, and forcing it on an unbounded stop would
    // emit partial aggregates as if they were complete. The recoverable
    // alternative is a snapshot, and that only exists for a job with barrier
    // checkpointing: its state survives to the last committed epoch, and only
    // what came after is lost. A job registered without checkpointing has no
    // resume point at all, so the whole window is gone.
    //
    // Either way the loss was silent. `flush_all` and the bounded connector
    // path both warn when they give something up; this path is the larger loss
    // of the three and said nothing.
    //
    // The decision not to flush is `StreamingLoop::RunLoop`'s
    // `EndOfStream::NoFlush`, and `on_stop` is what reads it. Routing that
    // through the driver rather than checking `has_open_windows` inline means
    // this loop's refusal and the embedded bounded loop's flush are the same
    // decision answered differently, in one place, rather than two unrelated
    // pieces of code that happen to disagree.
    if let Ok(mut exec) = executor_arc.lock() {
        match driver.on_stop(
            &mut *exec,
            krishiv_dataflow::stream_driver::StopReason::Cancelled,
        ) {
            Ok(krishiv_dataflow::stream_driver::StopOutcome::NotFlushed {
                open_windows: true,
                because,
            }) => {
                tracing::warn!(
                    job_id,
                    subtask = parsed.subtask,
                    because,
                    "stream:rloop stopped with window state still open: it is neither \
                     flushed (a forced flush would emit partial aggregates as complete) nor \
                     snapshotted here. A checkpointed job resumes from its last committed \
                     epoch and loses only what came after; a job registered without \
                     checkpointing has no resume point and loses all of it."
                );
            }
            Ok(_) => {}
            Err(error) => {
                // Never fail a cancellation over the stop report. The task is
                // already going away and the caller asked for that; turning a
                // clean cancel into an error would be a worse outcome than a
                // missing warn line.
                tracing::warn!(
                    job_id,
                    subtask = parsed.subtask,
                    %error,
                    "stream:rloop could not determine whether window state was left open",
                );
            }
        }
    }
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
    /// Does this assignment deliver through a sink that already durably holds
    /// its rows?
    ///
    /// Mirrors the dispatch below exactly — Iceberg or Kafka — because the two
    /// answering differently is what would resurrect the false alarm.
    fn has_durable_sink_contract(contract: &krishiv_proto::OutputContract) -> ExecutorResult<bool> {
        if crate::fragment::common::iceberg_sink_descriptor(contract)?.is_some() {
            return Ok(true);
        }
        Ok(
            krishiv_proto::OutputContractDescriptor::parse_kafka_sink(contract.description())
                .is_some()
                || matches!(
                    contract.descriptor(),
                    Some(krishiv_proto::OutputContractDescriptor::KafkaSink { .. })
                ),
        )
    }

    pub(crate) async fn stage_rloop_outputs(
        &self,
        job_id: &str,
        assignment: &ExecutorTaskAssignment,
        outputs: &[RecordBatch],
    ) -> ExecutorResult<()> {
        if outputs.is_empty() {
            return Ok(());
        }
        // Whether this buffer IS the job's delivery path, or merely a staging
        // area beside a durable sink. Computed before the buffer block because
        // the buffer is filled unconditionally — including for jobs whose real
        // output goes to Iceberg or Kafka — and that is precisely what made its
        // drop counter a false alarm.
        let has_durable_sink = Self::has_durable_sink_contract(assignment.output_contract())?;

        {
            let mut egress = self
                .continuous_outputs
                .entry(job_id.to_owned())
                .or_default();
            egress.extend(outputs.iter().cloned());
            // The cap is a durability dial: it is how much computed output a
            // slow consumer may lose before catching up. It is also a per-JOB
            // budget shared by every co-located subtask, so effective headroom
            // is cap/parallelism — which is why it needs an override.
            let cap = rloop_egress_cap();
            if egress.len() > cap {
                let overflow = egress.len() - cap;
                egress.drain(..overflow);
                // Count BATCHES, not overflow events. This used to add 1 per
                // overflow regardless of how many batches went, so the metric
                // systematically under-reported the loss it exists to measure
                // — and under-reported it worst exactly when the loss was
                // largest.
                krishiv_metrics::global_metrics()
                    .add_output_buffer_flush("rloop-egress-drop", overflow as u64);

                if krishiv_common::streaming_dials::EgressLoss::drop_lost_output(has_durable_sink) {
                    // The buffer was the only way out, so this is real loss and
                    // the coordinator is right to escalate it.
                    let mut loss = krishiv_common::streaming_dials::EgressLoss::none();
                    loss.record(overflow as u64);
                    *self
                        .continuous_egress_dropped
                        .entry(job_id.to_owned())
                        .or_insert(0) += loss.dropped_batches();
                    tracing::warn!(
                        job_id,
                        dropped_batches = overflow,
                        cap,
                        "{}",
                        krishiv_common::streaming_dials::EgressLoss::lost_output_advice()
                    );
                } else {
                    // A durable sink already has these rows. Counting them as
                    // dropped made the coordinator report "this job's output is
                    // incomplete" for a job whose output was complete — a false
                    // alarm on top of a real one, which is the worst place for
                    // it: it teaches the reader to discount the signal that
                    // matters. Debug, not warn, and NOT counted.
                    tracing::debug!(
                        job_id,
                        trimmed_batches = overflow,
                        cap,
                        "run-loop egress staging buffer trimmed; this job delivers through a \
                         durable sink, so nothing was lost"
                    );
                }
            }
        }

        let contract = assignment.output_contract();
        if let Some(descriptor) = crate::fragment::common::iceberg_sink_descriptor(contract)? {
            crate::erased(self.stage_rloop_iceberg(job_id, descriptor, outputs)).await?;
        } else if let Some(parsed) =
            krishiv_proto::OutputContractDescriptor::parse_kafka_sink(contract.description())
                .or_else(|| {
                    matches!(
                        contract.descriptor(),
                        Some(krishiv_proto::OutputContractDescriptor::KafkaSink { .. })
                    )
                    .then(|| {
                        Ok(contract.descriptor().cloned().unwrap_or(
                            krishiv_proto::OutputContractDescriptor::InlineRecordBatches,
                        ))
                    })
                })
        {
            let descriptor =
                parsed.map_err(|message| ExecutorError::InvalidAssignment { message })?;
            crate::erased(self.stage_rloop_kafka(job_id, assignment, descriptor, outputs)).await?;
        } else if contract
            .description()
            .trim()
            .starts_with(crate::runner::REGISTRY_SINK_PREFIX)
        {
            crate::erased(self.stage_rloop_connector(job_id, contract, outputs)).await?;
        }
        Ok(())
    }

    /// Stage a cycle's output into a registry-dispatched connector sink (#197).
    ///
    /// The streaming counterpart of the batch `registry-sink:` export: it makes
    /// every registered sink driver reachable from a run-loop job, not just the
    /// three that have a typed `OutputContractDescriptor` variant.
    ///
    /// **At-least-once, by construction.** The sink is opened once per job and
    /// flushed at the end of each cycle, so committed cycles are durable — but
    /// it is not a barrier-aligned two-phase participant, so a cycle replayed
    /// after a failure re-delivers its rows. Exactly-once streaming output
    /// still means the Iceberg (G7) or Kafka (Phase 55) sinks.
    async fn stage_rloop_connector(
        &self,
        job_id: &str,
        contract: &krishiv_proto::OutputContract,
        outputs: &[RecordBatch],
    ) -> ExecutorResult<()> {
        let config = crate::fragment::batch::parse_registry_sink_contract(contract.description())
            .map_err(|message| ExecutorError::InvalidAssignment { message })?;

        // Open once per job: reopening every cycle would truncate file sinks and
        // re-handshake network ones. `entry` alone cannot hold an async open, so
        // check first, then insert — a lost race just drops one redundant sink.
        let sink = match self.rloop_connector_sinks.get(job_id) {
            Some(existing) => existing.clone(),
            None => {
                // Reject terminal-flush sinks up front. A Parquet file sink
                // finalizes its footer on flush and can never take another row,
                // so flushing it once per cycle would fail on cycle 2 with an
                // opaque driver error partway into a running stream. The
                // capability makes that a clean rejection at open instead.
                let kind =
                    krishiv_connectors::ConnectorKind::parse(&config.kind).map_err(|error| {
                        ExecutorError::InvalidAssignment {
                            message: format!("rloop registry-sink kind '{}': {error}", config.kind),
                        }
                    })?;
                let resumable = self
                    .connector_registry
                    .sink_descriptor(kind)
                    .map(|descriptor| descriptor.default_capabilities.resumable_flush())
                    .unwrap_or(false);
                if !resumable {
                    return Err(ExecutorError::InvalidAssignment {
                        message: format!(
                            "connector '{}' cannot be a streaming sink: its flush finalizes the \
                             sink, so it cannot be flushed once per cycle. Use it as a batch \
                             export sink, or pick a connector whose driver declares \
                             resumable_flush (csv, kafka, jdbc-sink, elasticsearch, \
                             cassandra, hbase)",
                            config.kind
                        ),
                    });
                }
                let opened = self
                    .connector_registry
                    .open_sink(&config)
                    .await
                    .map_err(|error| ExecutorError::LocalExecution {
                        message: format!("rloop registry-sink open ({}): {error}", config.kind),
                    })?;
                let handle: crate::runner::RloopConnectorSink =
                    std::sync::Arc::new(tokio::sync::Mutex::new(opened));
                self.rloop_connector_sinks
                    .entry(job_id.to_owned())
                    .or_insert_with(|| handle.clone())
                    .clone()
            }
        };

        let mut guard = sink.lock().await;
        for batch in outputs {
            guard
                .write_batch_dyn(batch.clone())
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!("rloop registry-sink write ({}): {error}", config.kind),
                })?;
        }
        // Flush per cycle: what makes the cycle's output durable. A flush
        // failure fails the cycle rather than reporting output that never
        // landed.
        guard
            .flush_dyn()
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("rloop registry-sink flush ({}): {error}", config.kind),
            })?;
        tracing::debug!(
            job_id,
            kind = %config.kind,
            batches = outputs.len(),
            "run-loop cycle staged into a registry-dispatched sink (at-least-once)"
        );
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
        use krishiv_connectors::lakehouse::streaming_sink::schema_version_from_arrow;
        let krishiv_proto::OutputContractDescriptor::IcebergSink {
            root,
            table,
            mode,
            key_columns,
            op_column,
            catalog,
            namespace,
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
                crate::fragment::streaming::open_streaming_iceberg_sink(
                    root,
                    table,
                    mode,
                    key_columns,
                    op_column,
                    catalog,
                    namespace,
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
                "job {job_id} requests an Iceberg sink but this executor was built without the `iceberg` feature"
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
                "job {job_id} requests a Kafka sink but this executor was built without the `kafka` feature"
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// #197 streaming leg: a run-loop cycle whose output contract is
    /// `registry-sink:` writes through the connector registry, reuses the sink
    /// across cycles, and flushes each cycle. CSV is used because it has no
    /// typed `OutputContractDescriptor` variant — success can only come from
    /// registry dispatch.
    #[tokio::test]
    async fn rloop_registry_sink_writes_and_reuses_the_sink_across_cycles() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use base64::Engine as _;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cycles.csv");
        let json = serde_json::json!({
            "name": "rloop-test",
            "properties": { "path": path.to_string_lossy() },
        });
        let contract_str = format!(
            "{}csv|{}",
            crate::runner::REGISTRY_SINK_PREFIX,
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&json).unwrap()),
        );
        let contract = krishiv_proto::OutputContract::new(
            krishiv_proto::OutputContractKind::Sink,
            contract_str,
        );

        let runner = crate::runner::ExecutorTaskRunner::new(crate::ExecutorAssignmentInbox::new());
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = |values: Vec<i64>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![std::sync::Arc::new(Int64Array::from(values))],
            )
            .unwrap()
        };

        runner
            .stage_rloop_connector("job-rs", &contract, &[batch(vec![1, 2])])
            .await
            .expect("first cycle");
        assert_eq!(
            runner.rloop_connector_sinks.len(),
            1,
            "sink is cached per job"
        );
        runner
            .stage_rloop_connector("job-rs", &contract, &[batch(vec![3])])
            .await
            .expect("second cycle");
        assert_eq!(
            runner.rloop_connector_sinks.len(),
            1,
            "second cycle must reuse the open sink, not open another"
        );

        let written = std::fs::read_to_string(&path).unwrap();
        // Both cycles' rows are present and the header was written once — proof
        // the second cycle appended to the same open writer instead of
        // truncating the file by reopening it.
        assert_eq!(written.matches("n\n").count(), 1, "{written}");
        for value in ["1", "2", "3"] {
            assert!(written.contains(value), "missing {value} in {written}");
        }
    }

    /// A sink whose flush finalizes it (Parquet writes a footer) is rejected at
    /// open, not on the second cycle. Without the `resumable_flush` gate this
    /// failed mid-stream with a driver-level error — which is how the gate came
    /// to exist.
    #[tokio::test]
    async fn rloop_registry_sink_rejects_a_terminal_flush_sink() {
        use base64::Engine as _;

        let dir = tempfile::tempdir().unwrap();
        let json = serde_json::json!({
            "name": "rloop-test",
            "properties": { "path": dir.path().join("out.parquet").to_string_lossy() },
        });
        let contract = krishiv_proto::OutputContract::new(
            krishiv_proto::OutputContractKind::Sink,
            format!(
                "{}parquet|{}",
                crate::runner::REGISTRY_SINK_PREFIX,
                base64::engine::general_purpose::STANDARD
                    .encode(serde_json::to_vec(&json).unwrap()),
            ),
        );
        let runner = crate::runner::ExecutorTaskRunner::new(crate::ExecutorAssignmentInbox::new());
        let error = runner
            .stage_rloop_connector("job-parquet", &contract, &[])
            .await
            .expect_err("a terminal-flush sink must be rejected at open");
        let message = error.to_string();
        assert!(message.contains("cannot be a streaming sink"), "{message}");
        assert!(
            runner.rloop_connector_sinks.is_empty(),
            "a rejected sink must not be cached"
        );

        // s3 is rejected for a different reason worth pinning: its flush is
        // repeatable but *destructive* — it rewrites one fixed object, so a
        // second cycle would replace the first cycle's rows rather than append.
        // It must not be usable as a streaming sink either.
        let json = serde_json::json!({
            "name": "rloop-test",
            "properties": { "object_path": "s3://bucket/out.parquet" },
        });
        let s3_contract = krishiv_proto::OutputContract::new(
            krishiv_proto::OutputContractKind::Sink,
            format!(
                "{}s3|{}",
                crate::runner::REGISTRY_SINK_PREFIX,
                base64::engine::general_purpose::STANDARD
                    .encode(serde_json::to_vec(&json).unwrap()),
            ),
        );
        let error = runner
            .stage_rloop_connector("job-s3", &s3_contract, &[])
            .await
            .expect_err("s3 must not be a streaming sink");
        assert!(
            error.to_string().contains("cannot be a streaming sink"),
            "{error}"
        );
    }

    /// A malformed contract fails the cycle instead of silently discarding the
    /// output — the same rule the batch export follows.
    #[tokio::test]
    async fn rloop_registry_sink_rejects_a_malformed_contract() {
        let contract = krishiv_proto::OutputContract::new(
            krishiv_proto::OutputContractKind::Sink,
            format!(
                "{}not-base64-and-no-pipe",
                crate::runner::REGISTRY_SINK_PREFIX
            ),
        );
        let runner = crate::runner::ExecutorTaskRunner::new(crate::ExecutorAssignmentInbox::new());
        let error = runner
            .stage_rloop_connector("job-bad", &contract, &[])
            .await
            .expect_err("malformed contract must fail the cycle");
        assert!(error.to_string().contains("registry-sink"), "{error}");
    }

    #[test]
    fn rloop_fragment_round_trips_identity() {
        let parsed = parse_rloop_fragment(
            "stream:rloop:job-a|1/3|stream:tw:key=k:time=ts:win=1000:lag=0:agg=count",
        )
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

    /// Routing MUST hash the same bytes the operator persists, because
    /// restore-time key-group redistribution re-hashes the persisted form.
    ///
    /// Routing used to hash `i64::to_be_bytes` while the operator persists
    /// `extract_agg_key(..).to_string()` — ASCII decimal. Two different byte
    /// strings, so a key routed live to subtask A was redistributed on restore
    /// to whichever subtask owns the hash of its *text*. Invisible while
    /// running (every subtask routes identically) and a silent split on
    /// restore. Utf8 keys agreed by coincidence, which is why the only routing
    /// test — Utf8 — never caught it.
    #[test]
    fn int64_routing_agrees_with_the_persisted_group_key() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;

        let schema = StdArc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from((0..64).collect::<Vec<i64>>())) as _],
        )
        .unwrap();

        for row in 0..batch.num_rows() {
            // What the operator writes into its state key, and therefore what
            // redistribution hashes.
            let persisted = krishiv_dataflow::join::extract_agg_key(&batch, 0, row)
                .unwrap()
                .to_string();
            let expected = u32::from(krishiv_state::key_group::key_group_for_key(
                persisted.as_bytes(),
            ));

            // What routing must compute for the same row.
            let mut found = None;
            for subtask in 0..4 {
                let owned = route_batch_by_key_group(&batch, "k", 4, subtask)
                    .unwrap()
                    .owned;
                if let Some(b) = owned {
                    let col = b.column(0);
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    if (0..b.num_rows()).any(|i| arr.value(i) == row as i64) {
                        found = Some(subtask);
                    }
                }
            }
            let owner = found.expect("every row must be owned by exactly one subtask");
            let expected_owner = (0..4)
                .find(|s| {
                    let r = rloop_key_group_range(*s, 4);
                    expected >= r.start() && expected <= r.end()
                })
                .expect("key group falls in some range");
            assert_eq!(
                owner, expected_owner,
                "row {row}: routing sent it to subtask {owner}, but the persisted group \
                 key hashes into subtask {expected_owner}'s range — a restore would move it"
            );
        }
    }

    /// Int32 / Float64 / Bool are declared-legal key types and
    /// `coerce_batch_for_window` casts into them, but routing used to treat
    /// them as unroutable and return "fully owned". With index-partitioned
    /// splits that means each subtask keeps an overlapping key set and
    /// accumulates its own partial aggregate for shared keys.
    #[test]
    fn non_int64_non_utf8_key_types_are_routable() {
        use arrow::array::{BooleanArray, Float64Array, Int32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;

        let cases: Vec<(&str, DataType, arrow::array::ArrayRef)> = vec![
            (
                "int32",
                DataType::Int32,
                StdArc::new(Int32Array::from((0..64).collect::<Vec<i32>>())) as _,
            ),
            (
                "float64",
                DataType::Float64,
                StdArc::new(Float64Array::from(
                    (0..64).map(|i| i as f64 + 0.5).collect::<Vec<f64>>(),
                )) as _,
            ),
            (
                "bool",
                DataType::Boolean,
                StdArc::new(BooleanArray::from(
                    (0..64).map(|i| i % 2 == 0).collect::<Vec<bool>>(),
                )) as _,
            ),
        ];

        for (label, dtype, array) in cases {
            let schema = StdArc::new(Schema::new(vec![Field::new("k", dtype, false)]));
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
            let parallelism = 3;
            let mut owned_total = 0usize;
            let mut routed_total = 0usize;
            for subtask in 0..parallelism {
                let RoutedBatch { owned, routed, .. } =
                    route_batch_by_key_group(&batch, "k", parallelism, subtask).unwrap();
                owned_total += owned.map(|b| b.num_rows()).unwrap_or(0);
                routed_total += routed.iter().map(|(_, b)| b.num_rows()).sum::<usize>();
            }
            // Every subtask sees the whole batch, so across all of them each
            // row is owned exactly once and forwarded by the other P-1.
            assert_eq!(
                owned_total,
                batch.num_rows(),
                "{label}: each row must be owned by exactly one subtask"
            );
            assert!(
                routed_total > 0,
                "{label}: rows outside a subtask's range must be forwarded, not \
                 silently kept — 0 forwarded means the unroutable fallback fired"
            );
        }
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
            let RoutedBatch { owned, routed, .. } =
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

    /// A NULL group key must not be able to kill a long-lived streaming job.
    ///
    /// `extract_agg_key` rejects NULL keys by design, and routing called it
    /// per row with `?` *inside* the run-loop's own input loop — so one NULL
    /// key in one row returned `Err` out of `execute_run_loop_fragment` and
    /// ended the job. NULL is legal data in every source the engine reads;
    /// this made a single such row a permanent outage.
    ///
    /// The rows are excluded (they genuinely cannot be aggregated) and counted
    /// (so the exclusion is not a silent wrong answer), and the surviving rows
    /// route exactly as before.
    #[test]
    fn a_null_group_key_is_quarantined_not_fatal() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("b"),
                    None,
                    Some("c"),
                ])) as _,
                StdArc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5])) as _,
            ],
        )
        .unwrap();

        // Parallelism 1 short-circuits routing entirely, which is exactly why
        // the guard has to sit above that short-circuit: this shape would
        // otherwise carry the NULL row into the window operator and die there.
        let single = route_batch_by_key_group(&batch, "k", 1, 0).unwrap();
        assert_eq!(single.null_key_rows, 2, "both NULL rows counted");
        assert_eq!(
            single.owned.as_ref().map(|b| b.num_rows()),
            Some(3),
            "the three keyed rows survive"
        );

        // And across a real exchange: every keyed row lands on exactly one
        // subtask, and the NULL rows are reported by each.
        let parallelism = 3;
        let mut owned_total = 0usize;
        for subtask in 0..parallelism {
            let routed = route_batch_by_key_group(&batch, "k", parallelism, subtask).unwrap();
            assert_eq!(routed.null_key_rows, 2);
            owned_total += routed.owned.map(|b| b.num_rows()).unwrap_or(0);
        }
        assert_eq!(
            owned_total, 3,
            "the keyed rows are partitioned exactly once across subtasks"
        );
    }

    /// A batch that is *entirely* NULL-keyed must also be survivable — and must
    /// not be reported as "nothing arrived", which is what an untracked empty
    /// result would look like from the outside.
    #[test]
    fn an_all_null_key_batch_yields_no_rows_and_a_full_count() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(StringArray::from(vec![None::<&str>, None, None])) as _,
                StdArc::new(Int64Array::from(vec![1_i64, 2, 3])) as _,
            ],
        )
        .unwrap();

        let routed = route_batch_by_key_group(&batch, "k", 2, 0).unwrap();
        assert!(routed.owned.is_none());
        assert!(routed.routed.is_empty());
        assert_eq!(routed.null_key_rows, 3);
    }
}
