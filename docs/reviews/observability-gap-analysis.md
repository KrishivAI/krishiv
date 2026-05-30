# Observability Gap Analysis — Production Incident Debugging

## Question

> If a production job gives wrong output or stalls, do we expose enough metrics,
> logs, offsets, checkpoint IDs, snapshot IDs, task attempts, and lineage to
> debug it?

## Answer Today: No. Critical gaps exist across the stack.

Below is the diagnostic chain for a stalled/wrong-output job, mapped against what
Krishiv actually emits. Each link is rated: **OK**, **WEAK**, or **MISSING**.

---

## Diagnostic Chain per Incident Type

### Scenario A: "Job stalled — no output for 10 minutes"

| What I need to know | Available? | Where/Why |
|---|---|---|
| Is the job still `Running`? | **OK** | `JobState` via status API |
| How many tasks are running? | **WEAK** | `krishiv_tasks_running` — process-global, no `job_id` label |
| Which stage is active? | **MISSING** | No stage-level state metric |
| What's the executor heartbeat age? | **OK** | `max_executor_heartbeat_age_ticks` in scheduler |
| Is any task stuck at retry? | **WEAK** | `task_retries_total` is counter, no job/stage/task labels |
| What's the watermark (streaming)? | **MISSING** | Watermark exists on `TaskOutputMetadata` and `StreamingTaskState` but no Prometheus gauge |
| Is a source partition lagging? | **MISSING** | Source offsets tracked in checkpoints only; no real-time lag metric |
| Is a sink refusing writes (backpressure)? | **WEAK** | `SinkLatencyTracker` exists but no Prometheus gauge |
| Which partition in shuffle is stuck? | **MISSING** | Shuffle metadata has partitions but no progress metric |
| Is the checkpoint epoch progressing? | **MISSING** | Epoch committed tracked only as internal coordinator state; no metric |
| Are there any hot keys causing skew? | **WEAK** | `HeartbeatHotKeyReport` sent, but no metric emitted from it |

### Scenario B: "Wrong output on batch job"

| What I need to know | Available? | Where/Why |
|---|---|---|
| Which snapshot of the input data was read? | **MISSING** | No snapshot/version recorded in task input metadata |
| Was the job deterministic (same input → same output)? | **MISSING** | No hash of input partitions in task output |
| Were all tasks of a stage attempted? | **WEAK** | `TaskAttemptRef` exists but task attempt history not in metrics |
| Did a partial shuffle write get read? | **WEAK** | Shuffle lease tokens guard this; no event emitted on fence rejection |
| Who submitted the job? | **OK** | `AuditEvent` with principal on `JobSubmitted` |
| What was the SQL? | **OK** | `PlanFragment` in task assignment |
| Which executor ran each partition? | **WEAK** | In `TaskRecord` but not exposed via metrics/API |
| What were the runtime stats (rows, bytes)? | **OK** | `TaskRuntimeStats` on task output |
| Is there a lineage event for this job? | **MISSING** | `OpenLineageEmitter` trait defined but never wired from scheduler |

### Scenario C: "Checkpoint epoch N is corrupted / won't restore"

| What I need to know | Available? | Where/Why |
|---|---|---|
| Which epoch is currently committed? | **WEAK** | `latest_epoch.json` on disk; no metric/API endpoint |
| What's the fencing token for this epoch? | **OK** | In `CheckpointMetadata.fencing_token` |
| What source offsets does epoch N cover? | **OK** | In `CheckpointMetadata.source_offsets` |
| What state snapshots are in epoch N? | **OK** | In `CheckpointMetadata.operator_snapshots` |
| Why was epoch N invalidated? | **MISSING** | `tracing::warn!` logged but no counter or alerting hook |
| Can I generate a replay bundle? | **WEAK** | `generate_replay_bundle()` exists but never called |
| Is the manifest SHA-256 matching? | **OK** | `IntegrityManifest` validated on read |
| Which coordinator wrote this epoch? | **MISSING** | Only `fencing_token`; no `coordinator_id` or process UUID |
| Are there duplicate epochs (split-brain)? | **MISSING** | No detection metric or alert |

---

## Root Causes (per crate)

### `krishiv-metrics`

- **P0 bug**: `render_prometheus()` emits invalid Prometheus format. Three separate
  `# HELP`/`# TYPE` blocks for `krishiv_tasks_total` (one per label value) will
  cause Prometheus scrapes to fail or silently discard metrics.
- **No labels**: All counters/gauges are process-global. Cannot answer "how many
  tasks failed for job X?". Cannot aggregate by `job_id`, `stage_id`, `executor_id`.
- **Missing metric families**: No checkpoint epoch gauge, no watermark gauge, no
  source offset lag gauge, no shuffle partition progress, no state backend size.
- **No histogram support**: No latency buckets for task execution, checkpoint
  commit, shuffle read/write, connector I/O.
- **No structured span conventions**: No mandated `job_id`, `stage_id`, `epoch`,
  `task_id` fields on `tracing` spans.

### `krishiv-scheduler`

- **Metric emission is sparse**: Only `submit_job` and `checkpoint_epoch_commit`
  have dedicated counters. State transitions (`task -> Running/Failed`) only hit
  a generic `tasks_submitted`/`tasks_running` counter.
- **No per-job metric labels**: `inc_tasks_submitted()` is unconditional; doesn't
  carry `job_id` or `stage_id`.
- **Watermark/offset flow ends at `TaskRecord`**: `last_watermark_ms` and
  `last_source_offset` are stored but never surfaced as metrics or structured logs.
- **Event log exists but is not metric-linked**: `EventLogEvent` variants
  (`JobSubmitted`, `TaskFailed`, etc.) are stored but not mirrored to metrics.

### `krishiv-executor`

- **Non-terminal streaming tasks report `Running` with zero metrics**:
  `runner.rs:895-896` sends empty status. No periodic intermediate snapshots of
  watermark, row count, or state size. A streaming task could run for hours with
  zero observability.
- **Shuffle byte counts not tracked on output**: `ExecutorTaskOutput` carries
  partition descriptors but not total bytes written. Available inside
  `ShufflePartitionOutput` but not aggregated at task level.
- **Error fields lack structured context**: `ExecutorError::InvalidAssignment`
  carries a free-form `message` string. No `job_id`, `stage_id`, `task_id`,
  `attempt_id`, `epoch` fields.

### `krishiv-connectors`

- **No source offset lag metric**: Kafka source readers have offsets but no
  gauge for `(latest_broker_offset - consumer_offset)`.
- **No sink commit lag metric**: Sink writers commit offsets but no gauge for
  `(committed_epoch - latest_produced_epoch)`.
- **Error types lack epoch/timestamp fields**: `ConnectorError` variants can't
  be correlated with checkpoint boundaries or wall-clock time.
- **CDC pipeline runs but no per-table progress**: `CdcToLakehousePipeline`
  emits batch-level output but no metric for rows/sec, offset lag, or
  last-committed LSN.

### `krishiv-governance`

- **OpenLineage traits defined but never wired**: `OpenLineageEmitter` has 4
  implementations (NoOp, Logging, HTTP, AsyncHTTP) but no call site emits
  `RunEvent::START`/`COMPLETE`/`FAIL` from the scheduler or executor.
- **Audit logging covers job submit/cancel but not task/checkpoint events**:
  `AuditAction` has `QueryExecuted`, `JobSubmitted`, `JobCancelled`,
  `SavepointCreated`, `SavepointRestored`, `AdminAction`. Missing:
  `TaskAssigned`, `TaskFailed`, `CheckpointCommitted`, `CheckpointAborted`,
  `SinkCommitCompleted`, `StateSnapshotCreated`.

### `krishiv-checkpoint`

- **No metrics**: No counters for epochs committed/failed/aborted. No gauge for
  current epoch. No histogram for commit latency.
- **No coordinator identity in metadata**: `CheckpointMetadata` has `fencing_token`
  but no `coordinator_id` or process UUID — impossible to trace "who wrote this."
- **No snapshot_id lineage**: `iceberg_snapshot_id` is `Option<u64>` but ad-hoc;
  no systematic link between checkpoint epoch and lakehouse snapshot.

### `krishiv-shuffle`

- **No partition-level progress**: `ShuffleMetadata` tracks Pending→Available
  transitions but no metric for "N of M partitions available."
- **No bytes-written metric per job**: `shuffle_bytes_written_total` is a global
  counter with no labels.

### `krishiv-state`

- **No state size gauges**: No metric for key count, byte size, or TTL eviction
  rate per state backend.

---

## Missing Cross-Cutting Infrastructure

1. **`ObservabilityReport`**: No structured dump that captures all relevant state
   (job, tasks, stages, watermarks, offsets, shuffle, state, checkpoint, executor
   health) into a single JSON blob for incident response.

2. **Alerting hooks**: No `alert()` function — all observability is passive
   (metrics must be scraped, logs must be searched). No push-based notification
   path for critical events (fencing token violation, duplicate checkpoint,
   shuffle orphan leaked).

3. **Dead-letter events**: When a task fails permanently, the failure details
   go into `last_failure_reason` on the task record but there's no dead-letter
   queue or structured event emitted.

4. **Replay bundle generation**: `generate_replay_bundle()` exists in
   `krishiv-checkpoint` but is never called from CLI, API, or on failure.

---

## Remediation Plan

See [`diagnostic-observability-plan.md`](./diagnostic-observability-plan.md)
for the crate-by-crate implementation checklist.
