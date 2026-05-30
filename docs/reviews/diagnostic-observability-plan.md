# Diagnostic Observability Plan — Crate-by-Crate

## Phase 1: Metrics Foundation (P0 — blocks all other observability)

### `krishiv-metrics` — Fix Prometheus format + add labeled metrics

**P0 bug fix**:
- [ ] Fix `render_prometheus()`: single `# HELP`/`# TYPE` per metric family, all
  labeled samples after. Current triple-HELP/TYPE is invalid Prometheus.
- [ ] Add unit test that validates output passes a Prometheus parser (or at least
  the structural rule).

**Label support**:
- [ ] Add `KrishivMetrics::with_labels(labels: &[(&str, &str)])` — returns a
  labeled snapshot render path.
- [ ] Add `render_prometheus_labeled()` that emits `name{label="val",...}` format.
- [ ] Support dynamic labels: `job_id`, `stage_id`, `executor_id`, `epoch`.

**New metric families**:
- [ ] `krishiv_checkpoint_epoch{job_id}` — gauge, current committed epoch
- [ ] `krishiv_watermark_ms{job_id}` — gauge, current global low watermark
- [ ] `krishiv_source_offset_lag{job_id,source_id}` — gauge, broker offset - consumer offset
- [ ] `krishiv_sink_commits_total{job_id,sink_id,status}` — counter
- [ ] `krishiv_state_bytes{job_id,backend}` — gauge
- [ ] `krishiv_state_key_count{job_id}` — gauge
- [ ] `krishiv_shuffle_partitions_available{job_id,stage_id}` — gauge
- [ ] `krishiv_task_attempts_total{job_id,stage_id,status}` — counter
- [ ] `krishiv_executor_slots_used{executor_id}` — gauge
- [ ] `krishiv_executor_memory_bytes{executor_id}` — gauge
- [ ] `krishiv_streaming_rows_emitted_total{job_id,task_id}` — counter

**Structured tracing conventions**:
- [ ] Add `pub const SPAN_JOB_ID: &str = "krishiv.job_id"`
- [ ] Add `pub const SPAN_STAGE_ID: &str = "krishiv.stage_id"`
- [ ] Add `pub const SPAN_TASK_ID: &str = "krishiv.task_id"`
- [ ] Add `pub const SPAN_EPOCH: &str = "krishiv.epoch"`
- [ ] Add `pub const SPAN_ATTEMPT_ID: &str = "krishiv.attempt_id"`
- [ ] Add `pub const SPAN_SNAPSHOT_ID: &str = "krishiv.snapshot_id"`
- [ ] Add `pub const SPAN_EXECUTOR_ID: &str = "krishiv.executor_id"`
- [ ] Add `pub fn record_span_fields(span: &Span, job_id: &str, ...)` helper

**gRPC trace context**:
- [ ] Add `tracestate` propagation alongside `traceparent` in `inject_trace_context`
- [ ] Add `tracestate` extraction in `extract_trace_context`

### `krishiv-metrics` — Add `ObservabilityReport`

- [ ] Add `ObservabilityReport` struct containing: job_id, state, stages (with
  task states), watermarks, source offsets, checkpoint epochs, executor health,
  shuffle partition status, state backend stats.
- [ ] Add `ObservabilityReport::from_coordinator(&Coordinator) -> Self`
- [ ] Add `ObservabilityReport::to_json() -> String`
- [ ] Add CLI command: `krishiv diagnose <job_id>`

---

## Phase 2: Wire Metrics Into Scheduler (P0)

### `krishiv-scheduler` — Per-job labeled metrics

- [ ] Replace `global_metrics().inc_tasks_submitted()` with labeled variant
  carrying `job_id` and `stage_id`.
- [ ] On `TaskState::Failed`: emit labeled counter with job_id + attempt count.
- [ ] On `TaskState::Succeeded`: emit labeled counter.
- [ ] On `ExecutorState::Healthy -> Lost`: emit labeled gauge.
- [ ] On `CheckpointEpoch::Committed`: set `krishiv_checkpoint_epoch` gauge.
- [ ] On `CheckpointEpoch::Failed`: increment `krishiv_checkpoint_epochs_total{status=failed}`.
- [ ] On `StreamingTaskState` re-attach: set `krishiv_watermark_ms` gauge.
- [ ] Add `tracing::info!` with structured fields (job_id, stage_id, epoch) on
  every state transition. Current code uses bare strings.

### `krishiv-scheduler` — Wire OpenLineage

- [ ] On `JobState::Succeeded`: emit `RunEvent::COMPLETE` with input/output datasets.
- [ ] On `JobState::Failed`: emit `RunEvent::FAIL`.
- [ ] On `JobSpec::submit_job`: emit `RunEvent::START`.
- [ ] Populate `LineageDataset` from connector metadata in job config.

### `krishiv-scheduler` — Wire Audit Events

- [ ] Add `AuditAction::TaskFailed { job_id, stage_id, task_id, attempt_id }`
- [ ] Add `AuditAction::TaskAssigned { job_id, stage_id, task_id, executor_id }`
- [ ] Add `AuditAction::CheckpointCommitted { job_id, epoch, fencing_token }`
- [ ] Add `AuditAction::CheckpointAborted { job_id, epoch, reason }`
- [ ] Add `AuditAction::StateSnapshotCreated { job_id, operator_id }`
- [ ] Add `AuditAction::SinkCommitCompleted { job_id, sink_id, epoch }`

---

## Phase 3: Wire Metrics Into Executor (P0)

### `krishiv-executor` — Streaming progress snapshots

- [ ] Add `StreamingProgressSnapshot` struct: `task_id`, `watermark_ms`,
  `rows_emitted`, `batches_emitted`, `state_bytes`, `source_offset_lag`,
  `timestamp_ms`.
- [ ] Add periodic timer in `ContinuousWindowExecutor` loop: every N seconds,
  emit `StreamingProgressSnapshot` via `source_throttle_limits` channel or
  dedicated callback.
- [ ] Add `with_progress_reporter(Fn)` to `ExecutorTaskRunner` so progress
  snapshots flow to coordinator heartbeat.
- [ ] In `ExecutorHeartbeat`, add `streaming_progress: Vec<StreamingProgressSnapshot>`.
- [ ] Coordinator applies progress snapshots to metrics + structured logs.

### `krishiv-executor` — Structured error fields

- [ ] Add `job_id`, `stage_id`, `task_id`, `attempt_id` fields to `ExecutorError::InvalidAssignment`.
- [ ] Add `epoch` field to relevant error variants.
- [ ] In `format_failure_message`, include attempt count and executor id.
- [ ] Set `tracing` span fields on all `run_assignment_with` error paths.

### `krishiv-executor` — Executor health metrics

- [ ] Add memory pressure gauge in heartbeat (already in `ExecutorHeartbeat.memory_used_bytes`).
- [ ] Emit `krishiv_executor_slots_used{executor_id}` gauge in heartbeat processing.
- [ ] Add CPU usage gauge (from `/proc/self/stat` or OS crate).

---

## Phase 4: Wire Metrics Into Connectors + State + Shuffle (P1)

### `krishiv-connectors` — Source/sink lag

- [ ] Add `ConnectorMetrics` struct: `rows_read`, `bytes_read`, `offset_lag`, `last_commit_epoch`.
- [ ] Implement for Parquet source: `rows_read` counter.
- [ ] Implement for Kafka source (when live broker wired): `offset_lag` gauge.
- [ ] Implement for Sink: `rows_written`, `bytes_written`, `commit_epoch` gauge.
- [ ] Add `epoch` field to `ConnectorError` for correlation.

### `krishiv-state` — State size gauges

- [ ] Add `StateMetrics` struct: `key_count`, `byte_size`, `ttl_evictions`.
- [ ] Implement for `InMemoryStateBackend`: key count = BTreeMap::len().
- [ ] Implement for `RedbStateBackend`: key count via scan or metadata.
- [ ] Expose via `StateBackend::metrics() -> StateMetrics`.
- [ ] Coordinator collects from executor heartbeat and emits Prometheus gauges.

### `krishiv-shuffle` — Partition-level progress

- [ ] Add `ShuffleMetrics` struct: `partitions_pending`, `partitions_available`,
  `partitions_failed`, `total_bytes_written`.
- [ ] Implement in `ShuffleMetadata` with atomic counters.
- [ ] Expose via coordinator `/metrics` endpoint with `job_id` + `stage_id` labels.
- [ ] Add `krishiv_shuffle_partitions_total{job_id,stage_id,state}` gauge.

---

## Phase 5: Structured Debug Dump (P1)

### `krishiv-checkpoint` — Wire replay bundle

- [ ] Add CLI: `krishiv diagnose <job_id>` → calls `generate_replay_bundle()` +
  `ObservabilityReport` + dumps to stderr/stdout.
- [ ] Add coordinator identity to `CheckpointMetadata`: `coordinator_id` field.
- [ ] Add `event_time` to `CheckpointMetadata` for chronological ordering.
- [ ] Add `krishiv_checkpoint_epochs_total{job_id,status}` counter.

### Cross-cutting — `ObservabilityReport`

- [ ] Implement `ObservabilityReport::from_coordinator()` aggregating:
  - Job state + submitted_at timestamp
  - Per-stage state + task count (by state)
  - Active task assignments (executor, attempt id, start time)
  - Current watermark + source offsets
  - Latest checkpoint epoch + fencing token + timestamp
  - Shuffle partition count (pending/available/failed)
  - Executor pool (healthy/lost/draining count, heartbeat ages)
  - Recent event log entries (last 50)
  - Connector metrics (rows read/written, offset lag)
- [ ] Format as JSON with `serde`.
- [ ] Wire into `krishiv diagnose` CLI.

---

## Implementation Order

1. `krishiv-metrics` — fix Prometheus format P0 bug, add labeled metrics, add
   structured span field constants.
2. `krishiv-scheduler` — wire labeled metrics into job/task/checkpoint state
   transitions.
3. `krishiv-executor` — add streaming progress snapshots, structured error fields.
4. `krishiv-governance` — add new AuditAction variants, wire OpenLineage emitter
   into scheduler job lifecycle.
5. `krishiv-connectors` — add source/sink lag metrics.
6. `krishiv-state` — add state size gauges.
7. `krishiv-shuffle` — add partition progress metrics.
8. `krishiv-checkpoint` — add metrics, coordinator identity, wire replay bundle.
9. Cross-cutting — `ObservabilityReport`, `krishiv diagnose` CLI.

## Validation

After each crate change:
- [ ] `cargo test -p <crate>`
- [ ] `cargo clippy -p <crate> -- -D warnings`
- [ ] Prometheus output format validated against spec
- [ ] Structured log fields verified via `RUST_LOG=krishiv=debug cargo test`
