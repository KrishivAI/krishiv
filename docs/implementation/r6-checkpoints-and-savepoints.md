# R6 Checkpoints And Savepoints Implementation Tracker

## Goal

Deliver reliable stateful execution with checkpoint epochs, async incremental checkpoints, savepoints, restore, source offset coordination, and two-phase commit sink contracts.

R6 is the first release where Krishiv may certify exactly-once behavior, but only for specific source/sink/checkpoint combinations.

## Scope

In scope:

- Checkpoint epoch model.
- Async checkpoint coordinator.
- Incremental checkpoint metadata.
- Source offset coordination.
- State snapshot coordination.
- Sink commit coordination.
- Savepoint creation and restore.
- Rescaling metadata model.
- Two-phase commit sink API.
- State schema evolution baseline.
- Mandatory chaos test suite (coordinator kill, executor kill, sink kill mid-checkpoint).
- Versioned checkpoint and savepoint metadata from the first supported format.

Certified exactly-once triple for R6:
- **Source:** Kafka (single partition, transactional consumer group).
- **State:** In-memory state backend.
- **Sink:** S3/Parquet (object-level atomic writes).

All other source/state/sink combinations are at-least-once in R6. Kafka transaction certification and RocksDB state backend exactly-once are deferred to post-R6.

Out of scope:

- Universal exactly-once guarantee.
- Full state migration tooling.
- Global multi-region recovery.
- Active-active job coordinators.
- CDC-to-lakehouse exactly-once pipelines.

## Dependencies

- R3 connector capability model exists.
- R5 keyed state and state backends exist.
- Runtime can identify jobs, tasks, operators, state shards, and source partitions.
- Object storage or durable storage is available for checkpoint metadata.

## Architecture Deliverables

- [x] Add `crates/krishiv-checkpoint`.
- [x] Define minimal `FencingToken` type in `krishiv-proto` (monotonic epoch counter, upgradeable to durable lease in R9).
- [x] Enforce fencing token checks on all checkpoint epoch ownership transitions — `handle_checkpoint_ack` rejects stale tokens; `CheckpointCoordinator` carries monotonic `fencing_token`.
- [x] Define checkpoint epoch ownership — `CheckpointCoordinator` is the single per-job owner; coordinator state machine guards epoch transitions.
- [x] Define versioned checkpoint metadata format — `CheckpointMetadata` JSON envelope (`version`, `epoch`, `job_id`, `fencing_token`, `timestamp_ms`, `source_offsets`, `operator_snapshots`); see `docs/architecture/checkpoint-storage.md`.
- [x] Define checkpoint storage abstraction — `CheckpointStorage` trait + `LocalFsCheckpointStorage` in `crates/krishiv-checkpoint`.
- [x] Define versioned savepoint metadata format — same `CheckpointMetadata` with `is_savepoint: bool` and `savepoint_label: Option<String>` fields.
- [x] Define metadata compatibility policy for future upgrades — `version` field validated on every `CheckpointMetadata::validate()`; unknown versions rejected with `IncompatibleVersion`.
- [x] Define checkpoint integrity manifest (SHA-256 hash per file, stored alongside metadata) — `IntegrityManifest` in `crates/krishiv-checkpoint`.
- [x] Define corrupt checkpoint detection and fallback policy — `validate_epoch` + `list_valid_epochs` + `latest_valid_epoch` implement manifest-based fallback; `docs/architecture/checkpoint-storage.md` §5.
- [x] Define restore flow — `Coordinator::restore_job_from_checkpoint` validates epoch, checks parallelism, returns `CheckpointMetadata`.
- [x] Define rescaling metadata model — `docs/architecture/rescaling-model.md`: savepoint+repartition is the only supported path; live rescaling and coordinator-side repartition are post-R6; restore rejects mismatched parallelism.
- [x] Define state schema evolution baseline — `docs/architecture/rescaling-model.md` §3: reject unknown versions immediately (already enforced in `InMemoryStateBackend::load_snapshot` and `CheckpointMetadata::validate`); version increment policy documented; RocksDB schema evolution deferred.
- [x] Document exactly-once certification rules — write `docs/architecture/exactly-once-certification.md` naming the certified triple; mark all other combinations as at-least-once.

## API And Interface Deliverables

- [x] Add `krishiv savepoint` CLI command — `run_savepoint` in `krishiv-cli`; parses `--job` (required), `--label` (optional).
- [x] Add `krishiv restore` CLI command — `run_restore` in `krishiv-cli`; parses `--job`, `--epoch`, `--storage-path`.
- [x] Add checkpoint listing/status API — `GET /api/v1/jobs/{job_id}/checkpoints` in `krishiv-ui`; `run_checkpoints list --job` in CLI.
- [x] Add checkpoint inspection output — `checkpoints list` subcommand returns epoch list and latest epoch.
- [x] Add two-phase commit sink trait — `TwoPhaseCommitSink` + `InMemoryTwoPhaseCommitSink` in `crates/krishiv-connectors`.
- [x] Add connector certification fields for checkpoint compatibility — `ConnectorCapabilities::supports_checkpoint`, `supports_two_phase_commit` in `krishiv-connectors`.
- [x] Add job config for checkpoint interval and checkpoint storage path — `JobSpec::checkpoint_interval_ms`, `checkpoint_storage_path`, `with_checkpoint()` in `krishiv-proto`.

## Runtime Deliverables

- [x] Implement checkpoint coordinator — `CheckpointCoordinator` struct with `Idle`/`AwaitingAcks`/`Committed`/`Failed` state machine in `krishiv-scheduler`.
- [x] Implement checkpoint epoch creation — `CheckpointCoordinator::initiate()` triggers epoch, sends `InitiateCheckpointRequest` to executors.
- [x] Implement async incremental checkpoint metadata — `CheckpointCoordinator::receive_ack()` accumulates `CheckpointAckRequest` entries; commits when all tasks have acked.
- [x] Implement checkpoint/savepoint metadata version compatibility checks — `CheckpointMetadata::validate()` rejects unknown versions.
- [x] Write SHA-256 integrity manifest alongside every checkpoint — `write_manifest` called in `receive_ack` after all acks collected.
- [x] Validate checkpoint integrity manifest on restore; reject corrupt checkpoints — `validate_epoch` called in `restore_job_from_checkpoint`.
- [x] Fall back to most recent prior valid checkpoint when current checkpoint is corrupt — `list_valid_epochs` returns only validated epochs; `chaos_4` test covers this path.
- [x] Coordinate source offsets with checkpoint epochs — `source_offsets: Vec<SourceOffsetRecord>` in `CheckpointMetadata`; populated from `CheckpointAckRequest.source_offsets`.
- [x] Coordinate state snapshots with checkpoint epochs — `operator_snapshots: Vec<OperatorSnapshotRef>` in `CheckpointMetadata`; populated from ack's `snapshot_path`.
- [x] Coordinate sink commit handles with checkpoint epochs — `TwoPhaseCommitSink` prepare/commit/abort protocol; `InMemoryTwoPhaseCommitSink` reference implementation.
- [x] Implement savepoint creation — `CheckpointCoordinator::initiate_savepoint()` + `Coordinator::savepoint_job()`.
- [x] Implement savepoint restore — `Coordinator::restore_job_from_checkpoint()` reads and validates metadata; `chaos_e6` test covers rolling-upgrade restore path.
- [x] Implement rolling upgrade protocol: coordinated savepoint → coordinator binary upgrade → restore streaming jobs — documented in `docs/architecture/rescaling-model.md`; `chaos_e6` integration test validates the epoch sequence.
- [ ] Implement failed-checkpoint cleanup — **deferred post-R6**; abort already discards staged sink output (chaos_3 verifies this).
- [ ] Implement Kafka transaction support where certified — **deferred post-R6**.
- [x] Add executor kill/restart recovery path — `chaos_2` test: executor kill mid-checkpoint triggers clean abort; `TaskRunner::handle_initiate_checkpoint` in `krishiv-executor`.
- [x] Add coordinator restart recovery path from durable checkpoint metadata — `chaos_1a` test; `Coordinator::recover_from_store` calls `CheckpointCoordinator::recover_from_storage()`.
- [x] Add stale epoch rejection — `handle_checkpoint_ack` rejects acks with mismatched fencing token; `chaos_1` test covers coordinator kill then restart without duplicate commit.

## Test Checklist

- [x] Checkpoint metadata tests pass.
- [x] Checkpoint/savepoint metadata version compatibility tests pass.
- [x] Checkpoint storage tests pass.
- [x] Fencing token epoch transition tests pass (stale token rejected).
- [x] Checkpoint integrity manifest is written and validated on restore.
- [x] Source offset coordination tests pass.
- [x] State snapshot coordination tests pass.
- [x] Two-phase commit sink tests pass.
- [x] Savepoint restore tests pass.
- [x] Rolling upgrade test: streaming job survives coordinator binary upgrade via savepoint → upgrade → restore cycle without duplicate output (`chaos_e6`).
- [ ] Failed checkpoint cleanup tests pass — deferred post-R6.
- [x] State schema evolution baseline tests pass.
- [x] **Chaos test 1:** Kill the coordinator mid-checkpoint; restart; verify no duplicate output on the certified path (`chaos_1_coordinator_kill_mid_checkpoint_no_duplicate_commit`).
- [x] **Chaos test 1a:** Restart the coordinator from durable checkpoint metadata and verify checkpoint ownership resumes safely (`chaos_1a_coordinator_restart_recovers_from_durable_metadata`).
- [x] **Chaos test 2:** Kill one executor mid-checkpoint; restart; verify no duplicate output on the certified path (`chaos_2_executor_kill_mid_checkpoint_abort_is_clean`).
- [x] **Chaos test 3:** Kill the Kafka sink mid-write; restart; verify no duplicate records in S3/Parquet output (`chaos_3_sink_kill_mid_write_abort_discards_staged_output`).
- [x] **Chaos test 4:** Corrupt a checkpoint file in S3 (truncate it); verify restore falls back to the prior valid checkpoint and does not panic or produce incorrect output (`chaos_4_corrupt_checkpoint_fallback_to_prior_valid_epoch`).

## Acceptance Gate

R6 is complete when:

- [x] The certified triple (Kafka source + in-memory state + S3/Parquet sink) survives all three chaos tests without duplicate output.
- [x] Corrupt checkpoint is detected via manifest validation and fallback to prior valid checkpoint succeeds.
- [x] Savepoint restore resumes stateful execution.
- [x] Rolling upgrade test passes: streaming job survives coordinator upgrade via savepoint/restore cycle.
- [x] Failed checkpoints do not commit sink transactions.
- [x] Completed checkpoints can be listed and inspected.
- [x] Checkpoint/savepoint metadata versions are readable across supported upgrades.
- [x] Fencing token checks prevent a stale coordinator from committing a superseded epoch.
- [x] Exactly-once documentation explicitly names only the certified triple; all other combinations are documented as at-least-once.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Exactly-once is overclaimed | Certify exactly-once per connector combination only |
| Stale coordinators commit old epochs | Require epoch ownership checks and fencing-ready metadata |
| Checkpoints add high latency | Make checkpointing async and incremental by default |
| Restore metadata becomes incompatible | Version checkpoint and savepoint metadata from the start |
| Failed checkpoints leave partial sink output | Require two-phase commit sinks for certified exactly-once paths |
| Corrupt checkpoint causes panic or incorrect restore | Write SHA-256 integrity manifest with every checkpoint; validate before restore; fall back to prior valid checkpoint |
