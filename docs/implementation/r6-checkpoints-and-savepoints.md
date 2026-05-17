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

- [ ] Add `crates/krishiv-checkpoint`.
- [ ] Define minimal `FencingToken` type in `krishiv-proto` (monotonic epoch counter, upgradeable to durable lease in R9).
- [ ] Enforce fencing token checks on all checkpoint epoch ownership transitions.
- [ ] Define checkpoint epoch ownership.
- [ ] Define versioned checkpoint metadata format.
- [ ] Define checkpoint storage abstraction.
- [ ] Define versioned savepoint metadata format.
- [ ] Define metadata compatibility policy for future upgrades.
- [ ] Define checkpoint integrity manifest (SHA-256 hash per file, stored alongside metadata).
- [ ] Define corrupt checkpoint detection and fallback policy: validate manifest on restore; fall back to the most recent prior valid checkpoint; never restore from a corrupt checkpoint.
- [ ] Define restore flow.
- [ ] Define rescaling metadata model.
- [ ] Define state schema evolution baseline.
- [ ] Document exactly-once certification rules.

## API And Interface Deliverables

- [ ] Add `krishiv savepoint` CLI command.
- [ ] Add `krishiv restore` CLI command.
- [ ] Add checkpoint listing/status API.
- [ ] Add checkpoint inspection output.
- [ ] Add two-phase commit sink trait.
- [ ] Add connector certification fields for checkpoint compatibility.
- [ ] Add job config for checkpoint interval and checkpoint storage path.

## Runtime Deliverables

- [ ] Implement checkpoint coordinator.
- [ ] Implement checkpoint epoch creation.
- [ ] Implement async incremental checkpoint metadata.
- [ ] Implement checkpoint/savepoint metadata version compatibility checks.
- [ ] Write SHA-256 integrity manifest alongside every checkpoint.
- [ ] Validate checkpoint integrity manifest on restore; reject corrupt checkpoints.
- [ ] Fall back to most recent prior valid checkpoint when current checkpoint is corrupt.
- [ ] Coordinate source offsets with checkpoint epochs.
- [ ] Coordinate state snapshots with checkpoint epochs.
- [ ] Coordinate sink commit handles with checkpoint epochs.
- [ ] Implement savepoint creation.
- [ ] Implement savepoint restore.
- [ ] Implement rolling upgrade protocol: coordinated savepoint → coordinator binary upgrade → restore streaming jobs → rolling executor binary upgrade. Document this as the supported upgrade path from R6 onwards.
- [ ] Implement failed-checkpoint cleanup.
- [ ] Implement Kafka transaction support where certified.
- [ ] Add executor kill/restart recovery path.
- [ ] Add coordinator restart recovery path from durable checkpoint metadata.
- [ ] Add stale epoch rejection.

## Test Checklist

- [ ] Checkpoint metadata tests pass.
- [ ] Checkpoint/savepoint metadata version compatibility tests pass.
- [ ] Checkpoint storage tests pass.
- [ ] Fencing token epoch transition tests pass (stale token rejected).
- [ ] Checkpoint integrity manifest is written and validated on restore.
- [ ] Source offset coordination tests pass.
- [ ] State snapshot coordination tests pass.
- [ ] Two-phase commit sink tests pass.
- [ ] Savepoint restore tests pass.
- [ ] Rolling upgrade test: streaming job survives coordinator binary upgrade via savepoint → upgrade → restore cycle without duplicate output.
- [ ] Failed checkpoint cleanup tests pass.
- [ ] State schema evolution baseline tests pass.
- [ ] **Chaos test 1:** Kill the coordinator mid-checkpoint; restart; verify no duplicate output on the certified path.
- [ ] **Chaos test 1a:** Restart the coordinator from durable checkpoint metadata and verify checkpoint ownership resumes safely.
- [ ] **Chaos test 2:** Kill one executor mid-checkpoint; restart; verify no duplicate output on the certified path.
- [ ] **Chaos test 3:** Kill the Kafka sink mid-write; restart; verify no duplicate records in S3/Parquet output.
- [ ] **Chaos test 4:** Corrupt a checkpoint file in S3 (truncate it); verify restore falls back to the prior valid checkpoint and does not panic or produce incorrect output.

## Acceptance Gate

R6 is complete when:

- [ ] The certified triple (Kafka source + in-memory state + S3/Parquet sink) survives all three chaos tests without duplicate output.
- [ ] Corrupt checkpoint is detected via manifest validation and fallback to prior valid checkpoint succeeds.
- [ ] Savepoint restore resumes stateful execution.
- [ ] Rolling upgrade test passes: streaming job survives coordinator upgrade via savepoint/restore cycle.
- [ ] Failed checkpoints do not commit sink transactions.
- [ ] Completed checkpoints can be listed and inspected.
- [ ] Checkpoint/savepoint metadata versions are readable across supported upgrades.
- [ ] Fencing token checks prevent a stale coordinator from committing a superseded epoch.
- [ ] Exactly-once documentation explicitly names only the certified triple; all other combinations are documented as at-least-once.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Exactly-once is overclaimed | Certify exactly-once per connector combination only |
| Stale coordinators commit old epochs | Require epoch ownership checks and fencing-ready metadata |
| Checkpoints add high latency | Make checkpointing async and incremental by default |
| Restore metadata becomes incompatible | Version checkpoint and savepoint metadata from the start |
| Failed checkpoints leave partial sink output | Require two-phase commit sinks for certified exactly-once paths |
| Corrupt checkpoint causes panic or incorrect restore | Write SHA-256 integrity manifest with every checkpoint; validate before restore; fall back to prior valid checkpoint |
