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
- **Two distributed deployment targets (R3.1 must support both):**
  - **Kubernetes**: coordinator and executors run as pods managed by the operator; job submission via `KrishivJob` CRD.
  - **Bare metal / VM**: coordinator and executor binaries started as plain OS processes on any host with TCP connectivity; job submission via `krishiv` CLI pointing at coordinator address directly.
- **Architectural Rule:** Zero Kubernetes API calls (`kube` crate imports) in core runtime crates. Kubernetes API access is limited to `krishiv-operator`, Kubernetes packaging under `k8s/`, and narrowly scoped CLI submission/status paths. Kubernetes is a deployment plugin, not a scheduler dependency. The coordinator has no Kubernetes pod creation or deletion logic.
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
- [x] Define stale-attempt rejection and duplicate-status idempotency rules.
- [x] Add executor lease generation to R3.1 registration, heartbeat, task assignment, and task status contracts.
- [x] Define executor lease model with heartbeat generation and expiry.
- [x] Define durable job event log events: job submitted, stage planned, task assigned, task started, task succeeded, task failed, executor lost, job cancelled.
- [x] Define Kubernetes finalizer cleanup path for `KrishivJob` delete/cancel.
- [x] Define basic scheduler/executor stability metrics: heartbeat age, retry count, task duration, failed assignments.
- [x] Define `MetadataStore` trait in `krishiv-scheduler` with `InMemoryMetadataStore` backend (`SqliteMetadataStore` and `KubernetesMetadataStore` deferred).
- [x] **Decide and document durable MetadataStore backend before restart recovery tests can pass.** `InMemoryMetadataStore` for R3.1 single-process deployments; Sqlite and Kubernetes backends deferred.
- [x] Define `JobSubmitter` trait (`GrpcJobSubmitter` and `KubernetesJobSubmitter` implementations deferred).
- [x] Define `LeaderElection` trait and add `SingleNodeElection` (no-op) for R3.1.
- [ ] Plug `MetadataStore` into `Coordinator` for durable job/stage/task persistence (recovery method added; automatic write-through deferred).
- [ ] Replace `PlanNode` string labels with a typed operator enum in `krishiv-plan`.
- [ ] Add schema propagation through `LogicalPlan` nodes.
- [ ] Add estimated cardinality fields to plan nodes for R4 CBO.
- [x] Write `docs/architecture/stage-local-execution.md` documenting the Stage-Local Execution Model: coordinator assigns partitions, executor runs a full local DataFusion context for its partitions, shuffle moves data between stages. No custom distributed physical operators needed.
- [x] Define graceful executor shutdown protocol: SIGTERM → finish current RecordBatch → deregister via RPC → exit (before SIGKILL terminates the process).
- [x] Define executor deregistration RPC (fast path: best-effort `Draining` heartbeat for R3.1; dedicated `Deregister` RPC deferred).
- [ ] Define task lease token model: coordinator issues a monotonically increasing lease token per task assignment; shuffle/sink writes validate this token before committing, rejecting stale tokens from zombie executors.
- [ ] Define extended executor heartbeat payload: `memory_used_bytes`, `memory_limit_bytes`, `active_task_count` (used by R7 backpressure and R4 scheduler placement).
- [ ] Define job cancellation protocol: coordinator sends `CancelTask` RPC to all assigned executors; executors finish current batch then stop; coordinator marks job `Stopped`, triggers shuffle cleanup, releases executor slots.
- [x] Add Kubernetes `terminationGracePeriodSeconds: 30` to executor pod spec manifests consistent with graceful drain window.
- [ ] Define task timeout model: `task_timeout_seconds` field in `TaskSpec`; executor kills a task that exceeds its timeout and reports `TaskFailed`; coordinator then reassigns according to `max_stage_retries`.
- [ ] Define executor pod launch failure handling: operator monitors executor pod readiness; if a pod is not `Ready` within `executor_startup_timeout_seconds`, the operator deregisters the executor from the coordinator and triggers task reassignment (does not wait for heartbeat timeout).
- [ ] Support `--coordinator <URL>` startup flag on `krishiv-coordinator` and `krishiv-executor` binaries for bare metal / VM deployment (no Kubernetes dependency).
- [x] Document bare metal deployment model: which features are Kubernetes-only (operator, CRDs, NetworkPolicy, K8s HA leader election) vs available on both targets (all core runtime: gRPC, task assignment, heartbeat, ShuffleStore, MetadataStore, CLI).
- [x] Write `docs/architecture/deployment-targets.md` covering both Kubernetes and bare metal deployment models, startup commands, and feature availability matrix.
- [x] Write `docs/security/security-posture.md` (pre-R9 security posture: NetworkPolicy for Kubernetes, firewall rules for bare metal, per-component ServiceAccounts, no credentials in task specs, known limitations).

### API And Interface Deliverables

- [x] Implement executor registration through the tonic-shaped service boundary (executor → coordinator).
- [x] Expose executor registration through a networked gRPC server/client.
- [x] Implement task assignment RPC (coordinator → executor).
- [x] Implement task status update through the tonic-shaped service boundary (executor → coordinator).
- [x] Expose task status updates through a networked gRPC server/client.
- [x] Implement executor heartbeat through the tonic-shaped service boundary.
- [x] Expose executor heartbeat through a networked gRPC server/client.
- [x] Include executor lease generation and task attempt ID in coordinator/executor transport contracts.
- [ ] Add status API fields for executor lease age, task attempt, retry count, and last failure reason.
- [ ] Add cancel/delete API path used by Kubernetes finalizers.
- [ ] Add `CancelTask` RPC from coordinator to executor.
- [ ] Add `Deregister` RPC from executor to coordinator (on graceful shutdown).
- [ ] Extend heartbeat payload to include `memory_used_bytes`, `memory_limit_bytes`, `active_task_count`.
- [ ] Store health snapshot per executor in `ExecutorRegistry` (used by scheduler for memory-aware placement).

### Runtime Deliverables

- [ ] Implement executor task runner loop: receive assignment, create local DataFusion `SessionContext`, register assigned input partitions, execute SQL query, report result and status back to coordinator.
- [x] Add the first executor-side assignment receiver loop backed by an in-memory inbox.
- [x] Add minimal executor task runner skeleton: consume one inbox assignment, report `Running`, validate placeholder fragment metadata, and report terminal status.
- [x] Add first narrow local SQL fragment execution path for `sql: SELECT 1`-style assignments, returning lightweight output metadata without Arrow payloads in control-plane Protobuf.
- [x] Add R3.1 bootstrap `local-parquet:<table>:<path>` input partition registration for executor-local `sql:` fragments, without starting R3.2 connector certification.
- [ ] Implement executor registration and deregistration on shutdown.
- [ ] Implement graceful executor shutdown handler (SIGTERM → finish current RecordBatch → send `Deregister` RPC → exit).
- [ ] Implement `CancelTask` handler on executor: finish current batch, do not start next, send `TaskCancelled` status to coordinator.
- [ ] Implement coordinator `CancelJob`: send `CancelTask` to all assigned executors, wait for acknowledgements, mark job `Stopped`, trigger shuffle cleanup.
- [ ] Implement task lease token issuance on assignment; validate token before any shuffle or sink write.
- [ ] Implement stale lease token rejection in shuffle write path.
- [ ] Implement memory-aware task placement: skip executors above configurable memory threshold when assigning new tasks.
- [ ] Implement task timeout enforcement on executor: kill task and report `TaskFailed` when `task_timeout_seconds` is exceeded.
- [ ] Implement operator-side executor pod launch failure detection: deregister executor if pod not `Ready` within `executor_startup_timeout_seconds`.
- [ ] Implement Kubernetes NetworkPolicy manifest for pre-R9 security posture (restrict coordinator gRPC port to `krishiv` namespace).
- [ ] Implement crash detection on coordinator side when executor heartbeat stops.
- [ ] Implement task reassignment on executor crash.
- [x] Add in-memory `MetadataStore` implementation.
- [ ] Persist job, stage, task, attempt, executor lease, and event-log records through `MetadataStore` (automatic write-through deferred; `recover_from_store` method added to `Coordinator`).
- [x] Recover coordinator state from `MetadataStore` after process restart (`recover_from_store` on `Coordinator`).
- [x] Reject stale task attempts and ignore duplicate status updates safely.
- [x] Implement `KrishivJob` finalizer lifecycle: add finalizer on first observe, remove on deletion after cleanup.
- [x] Emit basic scheduler/executor stability metrics.

### Test Checklist

- [x] gRPC task assignment and status update round-trip tests pass.
- [x] Versioned transport contract unit tests pass.
- [x] Executor binary config and request-construction tests pass.
- [x] Tonic service registration, heartbeat, and task status adapter tests pass.
- [x] Networked registration, heartbeat, and task-status gRPC smoke test passes.
- [x] Executor registers with coordinator and appears in executor registry.
- [x] Executor deregisters cleanly on shutdown via best-effort `Draining` heartbeat (fast path; dedicated `Deregister` RPC deferred).
- [x] Graceful shutdown test: SIGTERM to executor → deregistration heartbeat sent → executor exits cleanly (SIGTERM handler implemented in `heartbeat_loop`).
- [ ] CancelJob test: all executor tasks acknowledge cancellation; job transitions to `Stopped`; no tasks remain running after acknowledgement.
- [ ] Zombie executor test: network partition heals after task reassignment; stale lease token rejected by shuffle write path; only new-assignment output is committed.
- [ ] Extended heartbeat test: memory and task-count fields are populated and stored in `ExecutorRegistry`.
- [ ] Memory-aware placement test: coordinator skips executors above memory threshold when assigning tasks.
- [ ] `MetadataStore` persistence tests pass.
- [ ] Coordinator restart recovery tests pass.
- [x] Executor lease expiry tests pass.
- [x] Stale task attempt update tests pass.
- [x] Duplicate task status update idempotency tests pass.
- [x] Minimal executor task runner lifecycle test passes against the scheduler-backed coordinator service.
- [x] Durable job event log round-trip tests pass (`in_memory_metadata_store_round_trips`).
- [x] `KrishivJob` finalizer add/remove tests pass (`reconcile_adds_finalizer_on_first_observe`, `reconcile_removes_finalizer_on_deletion`).
- [ ] Operator restart during reconciliation does not duplicate scheduler jobs.
- [x] Basic stability metrics tests pass.
- [ ] Typed plan operator enum tests pass with schema propagation.
- [x] End-to-end test: `SELECT 1` runs coordinator → executor via gRPC, result metadata returned.
- [x] End-to-end test: Parquet file scan runs on executor with DataFusion through the networked assignment/status path, with result metadata returned to the runner caller and task status accepted by the coordinator.
- [ ] Executor crash is detected by coordinator; task is reassigned to another executor.

### Acceptance Gate For R3.1

- [ ] A real SQL query (`SELECT` over a local Parquet file) completes end-to-end: coordinator assigns the task over gRPC, executor runs it via DataFusion, result is returned to the coordinator.
- [ ] Executor crash mid-task is detected and the task is reassigned without manual intervention.
- [ ] Graceful executor shutdown completes within `terminationGracePeriodSeconds` without dropping in-flight task output.
- [ ] `CancelJob` stops all executor tasks and transitions job to `Stopped` without orphaned tasks.
- [ ] Stale lease tokens from a zombie executor are rejected by the shuffle write path.
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
- [ ] Define Kafka consumer group offset commit protocol: commit the consumer group offset only after the corresponding output batch is written to the sink (post-write commit). Document the reprocessing window that exists if the executor crashes between output write and offset commit.

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
- [ ] Implement Kafka consumer group offset commit after output write (post-write commit protocol).
- [ ] Test: Kafka source reads from last committed consumer group offset after task reassignment.
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
- [ ] Kafka consumer group offset commit protocol is documented and tested: offset commits after output write, not before.
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
| Graceful shutdown window is too short | Set Kubernetes `terminationGracePeriodSeconds` generously; measure worst-case batch processing time in tests |
| Task lease tokens add coordination overhead | Keep tokens as simple monotonic integers; no network round-trip needed for validation |
| Job cancellation leaves orphaned shuffle data | Wire cancellation cleanup to the same shuffle GC path used on job completion |
