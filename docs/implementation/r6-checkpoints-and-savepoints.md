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
- Kafka transaction support where certified.
- State schema evolution baseline.

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
- [ ] Define checkpoint epoch ownership.
- [ ] Define checkpoint metadata format.
- [ ] Define checkpoint storage abstraction.
- [ ] Define savepoint metadata format.
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
- [ ] Coordinate source offsets with checkpoint epochs.
- [ ] Coordinate state snapshots with checkpoint epochs.
- [ ] Coordinate sink commit handles with checkpoint epochs.
- [ ] Implement savepoint creation.
- [ ] Implement savepoint restore.
- [ ] Implement failed-checkpoint cleanup.
- [ ] Implement Kafka transaction support where certified.
- [ ] Add executor kill/restart recovery path.
- [ ] Add stale epoch rejection.

## Test Checklist

- [ ] Checkpoint metadata tests pass.
- [ ] Checkpoint storage tests pass.
- [ ] Source offset coordination tests pass.
- [ ] State snapshot coordination tests pass.
- [ ] Two-phase commit sink tests pass.
- [ ] Savepoint restore tests pass.
- [ ] Failed checkpoint cleanup tests pass.
- [ ] Executor kill/restart tests pass.
- [ ] Duplicate-output prevention tests pass for certified paths.
- [ ] State schema evolution baseline tests pass.

## Acceptance Gate

R6 is complete when:

- [ ] A certified Kafka-to-object-store path survives executor restart without duplicate output.
- [ ] Savepoint restore resumes stateful execution.
- [ ] Failed checkpoints do not commit sink transactions.
- [ ] Completed checkpoints can be listed and inspected.
- [ ] Exactly-once documentation names only certified connector combinations.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Exactly-once is overclaimed | Certify exactly-once per connector combination only |
| Stale coordinators commit old epochs | Require epoch ownership checks and fencing-ready metadata |
| Checkpoints add high latency | Make checkpointing async and incremental by default |
| Restore metadata becomes incompatible | Version checkpoint and savepoint metadata from the start |
| Failed checkpoints leave partial sink output | Require two-phase commit sinks for certified exactly-once paths |
