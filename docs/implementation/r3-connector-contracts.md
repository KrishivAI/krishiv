# R3 Connector Contracts Implementation Tracker

## Goal

Deliver Krishiv's distributed execution foundation and production I/O baseline in two sequential sub-milestones.

**R3.1** delivers the executor binary, gRPC transport, durable metadata store, typed plan nodes, and the first recovery invariants — the infrastructure that all connector and streaming work runs on top of.

**R3.2** builds connector contracts (Parquet, Kafka, S3, catalog) on top of that proven foundation.

**Rule:** R3.2 cannot begin until the R3.1 acceptance gate passes. Connectors built before real executors exist are tested against a simulation, not reality.

## Scope

In scope:

- Executor binary (`krishiv-executor`).
- gRPC transport between coordinator and executor.
- Durable metadata store interface.
- Task attempt IDs and idempotent task status updates.
- Executor leases with heartbeat generation and expiry.
- Durable job event log.
- Kubernetes finalizer cleanup for delete/cancel paths.
- Basic scheduler/executor stability metrics.
- Typed operator nodes in `krishiv-plan`.
- Stage-Local Execution Model documentation.
- Connector traits (`Source`, `Sink`, `Offset`, `CommitHandle`).
- Connector capability flags.
- Parquet reader and writer.
- Kafka source and sink.
- S3-compatible object store integration (unpartitioned; partitioned writes depend on R4).
- Schema registry abstraction.
- Catalog crate (`krishiv-catalog`) with `TableProvider`, `CatalogProvider`, and column statistics.
- At-least-once sink contract.
- CDC design document.
- Connector certification test kit.

Out of scope:

- Full exactly-once certification.
- Kafka transaction certification.
- Iceberg/Delta table support.
- JDBC source/sink.
- CDC implementation beyond design.
- Global connector marketplace or plugin loading.
- Streaming Python UDFs.

## Dependencies

- R1 local execution and DataFusion integration exist.
- R2 distributed submission, coordinator, and executor registry skeleton exist.

---

## R3.1: Distributed Execution Foundation

### Goal

Build the real executor binary, wire transport, durable metadata store, typed plan representation, and restart-safe scheduler semantics. Prove that a real query runs end-to-end from coordinator to executor over the network before any connector work starts.

### Architecture Deliverables

- [x] Add `crates/krishiv-scheduler`.
- [x] Add `crates/krishiv-proto`.
- [x] Add `crates/krishiv-executor` binary crate.
- [x] Add `tonic` gRPC transport layer to `krishiv-proto`.
- [x] Add tonic-shaped coordinator/executor service boundary in `krishiv-proto`.
- [x] Add versioned coordinator/executor transport contracts in `krishiv-proto` for executor registration, heartbeat, task assignment, and task status updates.
- [x] Add task attempt IDs to R3.1 transport task assignments and task status updates.
- [ ] Define stale-attempt rejection and duplicate-status idempotency rules.
- [x] Add executor lease generation to R3.1 registration, heartbeat, task assignment, and task status contracts.
- [ ] Define executor lease model with heartbeat generation and expiry.
- [ ] Define durable job event log events: job submitted, stage planned, task assigned, task started, task succeeded, task failed, executor lost, job cancelled.
- [ ] Define Kubernetes finalizer cleanup path for `KrishivJob` delete/cancel.
- [ ] Define basic scheduler/executor stability metrics: heartbeat age, retry count, task duration, failed assignments.
- [ ] Define `MetadataStore` trait in `krishiv-runtime` (in-memory implementation first).
- [ ] Plug `MetadataStore` into `Coordinator` for durable job/stage/task persistence.
- [ ] Replace `PlanNode` string labels with a typed operator enum in `krishiv-plan`.
- [ ] Add schema propagation through `LogicalPlan` nodes.
- [ ] Add estimated cardinality fields to plan nodes for R4 CBO.
- [x] Write `docs/architecture/stage-local-execution.md` documenting the Stage-Local Execution Model: coordinator assigns partitions, executor runs a full local DataFusion context for its partitions, shuffle moves data between stages. No custom distributed physical operators needed.

### API And Interface Deliverables

- [x] Implement executor registration through the tonic-shaped service boundary (executor → coordinator).
- [x] Expose executor registration through a networked gRPC server/client.
- [ ] Implement task assignment RPC (coordinator → executor).
- [x] Implement task status update through the tonic-shaped service boundary (executor → coordinator).
- [x] Expose task status updates through a networked gRPC server/client.
- [x] Implement executor heartbeat through the tonic-shaped service boundary.
- [x] Expose executor heartbeat through a networked gRPC server/client.
- [x] Include executor lease generation and task attempt ID in coordinator/executor transport contracts.
- [ ] Add status API fields for executor lease age, task attempt, retry count, and last failure reason.
- [ ] Add cancel/delete API path used by Kubernetes finalizers.

### Runtime Deliverables

- [ ] Implement executor task runner loop: receive assignment, create local DataFusion `SessionContext`, register assigned input partitions, execute SQL query, report result and status back to coordinator.
- [ ] Implement executor registration and deregistration on shutdown.
- [ ] Implement crash detection on coordinator side when executor heartbeat stops.
- [ ] Implement task reassignment on executor crash.
- [ ] Add in-memory `MetadataStore` implementation.
- [ ] Persist job, stage, task, attempt, executor lease, and event-log records through `MetadataStore`.
- [ ] Recover coordinator state from `MetadataStore` after process restart.
- [ ] Reject stale task attempts and ignore duplicate status updates safely.
- [ ] Implement `KrishivJob` finalizer cleanup for cancelled/deleted resources.
- [ ] Emit basic scheduler/executor stability metrics.

### Test Checklist

- [ ] gRPC task assignment and status update round-trip tests pass.
- [x] Versioned transport contract unit tests pass.
- [x] Executor binary config and request-construction tests pass.
- [x] Tonic service registration, heartbeat, and task status adapter tests pass.
- [x] Networked registration, heartbeat, and task-status gRPC smoke test passes.
- [x] Executor registers with coordinator and appears in executor registry.
- [ ] Executor deregisters cleanly on shutdown.
- [ ] `MetadataStore` persistence tests pass.
- [ ] Coordinator restart recovery tests pass.
- [ ] Executor lease expiry tests pass.
- [ ] Stale task attempt update tests pass.
- [ ] Duplicate task status update idempotency tests pass.
- [ ] Durable job event log replay tests pass.
- [ ] `KrishivJob` finalizer cleanup tests pass.
- [ ] Operator restart during reconciliation does not duplicate scheduler jobs.
- [ ] Basic stability metrics tests pass.
- [ ] Typed plan operator enum tests pass with schema propagation.
- [ ] End-to-end test: `SELECT 1` runs coordinator → executor via gRPC, result returned.
- [ ] End-to-end test: Parquet file scan runs on executor with DataFusion, result returned to coordinator.
- [ ] Executor crash is detected by coordinator; task is reassigned to another executor.

### Acceptance Gate For R3.1

- [ ] A real SQL query (`SELECT` over a local Parquet file) completes end-to-end: coordinator assigns the task over gRPC, executor runs it via DataFusion, result is returned to the coordinator.
- [ ] Executor crash mid-task is detected and the task is reassigned without manual intervention.
- [ ] Coordinator restart recovers job, stage, task, attempt, executor lease, and event-log state.
- [ ] Stale task attempts and duplicate status updates cannot corrupt job state.
- [ ] Deleting a `KrishivJob` runs finalizer cleanup and does not leave active assignments.
- [ ] `MetadataStore` correctly persists job/task state.
- [ ] Typed plan operator enum passes schema propagation tests.
- [x] Stage-Local Execution Model document is written.
- [ ] Stage-Local Execution Model document is reviewed and approved.

---

## R3.2: Connector Contracts

### Goal

Define connector semantics and certify the first source/sink integrations (Parquet, Kafka, S3) running on real executors from R3.1. R3.2 cannot start until R3.1's acceptance gate passes.

### Architecture Deliverables

- [ ] Add `crates/krishiv-connectors`.
- [ ] Add `crates/krishiv-catalog`.
- [ ] Define `TableProvider` trait in `krishiv-catalog`.
- [ ] Define `Schema` and column statistics model in `krishiv-catalog`.
- [ ] Define `CatalogProvider` trait for table/schema lookup.
- [ ] Implement in-memory catalog backed by DataFusion `SessionContext`.
- [ ] Define connector module boundaries.
- [ ] Define source lifecycle.
- [ ] Define sink lifecycle.
- [ ] Define offset persistence boundary.
- [ ] Define sink commit boundary.
- [ ] Define connector capability flags.
- [ ] Document connector guarantee vocabulary.

### API And Interface Deliverables

- [ ] Define `Source` trait.
- [ ] Define `Sink` trait.
- [ ] Define `Offset` model.
- [ ] Define `CommitHandle` model.
- [ ] Define `ConnectorCapabilities`.
- [ ] Include capability flags: bounded, unbounded, rewindable, transactional, idempotent.
- [ ] Define schema registry abstraction.
- [ ] Add connector configuration validation errors.
- [ ] Add connector certification test harness interface.

### Runtime Deliverables

- [ ] Implement Parquet reader.
- [ ] Implement Parquet writer.
- [ ] Implement S3-compatible object store reads.
- [ ] Implement S3-compatible object store writes (unpartitioned only; partitioned writes depend on R4).
- [ ] Implement Kafka source.
- [ ] Implement Kafka sink.
- [ ] Add source offset tracking.
- [ ] Add at-least-once sink contract.
- [ ] Surface connector capabilities in job metadata.
- [ ] Write CDC design document under `docs/rfcs/`.

### Test Checklist

- [ ] Connector trait unit tests pass.
- [ ] Parquet read/write certification tests pass (running on real R3.1 executors).
- [ ] S3 read/write certification tests pass.
- [ ] Kafka source/sink certification tests pass for supported semantics.
- [ ] Offset serialization tests pass.
- [ ] Connector config validation tests pass.
- [ ] Failure-mode tests document unsupported guarantees.
- [ ] Catalog trait unit tests pass.
- [ ] End-to-end test: Kafka → Parquet pipeline runs on real executors without simulation.

### Acceptance Gate For R3.2

- [ ] Parquet, Kafka, and S3 connectors pass certification tests running on real executors.
- [ ] Every connector declares capability flags.
- [ ] Source offsets are visible in job metadata or logs.
- [ ] At-least-once sink behavior is documented.
- [ ] CDC design is written and linked from the roadmap.
- [ ] Kafka → Parquet pipeline runs end-to-end on real executors.

---

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| R3.1 takes longer than expected | Gate R3.2 on R3.1 acceptance; do not run in parallel |
| Connector semantics diverge | Require capability flags and certification tests |
| Source offsets become connector-specific strings | Use structured offset models with connector-owned payloads only where needed |
| At-least-once behavior is mistaken for exactly-once | Document delivery guarantees per connector and sink mode |
| S3 behavior differs across providers | Test against S3-compatible contract and document provider-specific limitations |
| CDC scope expands too early | Keep R3 CDC to design only |
| Executor binary scope grows too large | Scope to minimal task runner; defer resource isolation and advanced scheduling to R4-R7 |
| gRPC transport adds schema churn | Version RPC messages from the first commit; never break existing fields |
| Catalog abstraction is too narrow for R8 Iceberg | Define `CatalogProvider` generically; keep DataFusion-specific wiring behind an adapter |
