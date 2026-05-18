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
- [x] Define basic scheduler/executor stability metrics: heartbeat age, retry count, task duration, failed assignments (`StabilityMetrics`, `ExecutorHeartbeatAge` in `krishiv-scheduler`).
- [x] Define `MetadataStore` trait in `krishiv-scheduler` with `InMemoryMetadataStore` backend (`SqliteMetadataStore` and `KubernetesMetadataStore` deferred).
- [x] **Decide and document durable MetadataStore backend before restart recovery tests can pass.** `InMemoryMetadataStore` for R3.1 single-process deployments; Sqlite and Kubernetes backends deferred.
- [x] Define `JobSubmitter` trait (`GrpcJobSubmitter` and `KubernetesJobSubmitter` implementations deferred).
- [x] Define `LeaderElection` trait and add `SingleNodeElection` (no-op) for R3.1.
- [x] Plug `MetadataStore` into `Coordinator` for durable job/stage/task persistence (`Coordinator::with_store` builder; `submit_job` and `apply_task_update` write-through to store).
- [x] Replace `PlanNode` string labels with a typed operator enum in `krishiv-plan` (`NodeOp` enum: Scan, Filter, Project, Aggregate, Join, Exchange, Sink, Other; `JoinType` enum).
- [x] Add schema propagation through `LogicalPlan` nodes (`PlanSchema`, `SchemaField`, `FieldType`; `PlanNode::with_output_schema`).
- [x] Add estimated cardinality fields to plan nodes for R4 CBO (`PlanNode::with_estimated_rows` already existed; now paired with schema).
- [x] Write `docs/architecture/stage-local-execution.md` documenting the Stage-Local Execution Model: coordinator assigns partitions, executor runs a full local DataFusion context for its partitions, shuffle moves data between stages. No custom distributed physical operators needed.
- [x] Define graceful executor shutdown protocol: SIGTERM → finish current RecordBatch → deregister via RPC → exit (before SIGKILL terminates the process).
- [x] Define executor deregistration RPC — dedicated `Deregister` RPC implemented (`DeregisterExecutorRequest`/`DeregisterExecutorResponse`, `Coordinator::deregister_executor`, gRPC service handler, and wire helpers in `krishiv-proto` and `krishiv-scheduler`).
- [ ] Define task lease token model: coordinator issues a monotonically increasing lease token per task assignment; shuffle/sink writes validate this token before committing, rejecting stale tokens from zombie executors.
- [x] Define extended executor heartbeat payload: `memory_used_bytes`, `memory_limit_bytes`, `active_task_count` added to `ExecutorHeartbeatRequest` and `ExecutorHeartbeat`; stored as `ExecutorHealthSnapshot` per `ExecutorRecord`.
- [x] Define job cancellation protocol: `Coordinator::push_cancel_job` sends `CancelTask` gRPC to all executors owning running tasks, then marks job `Cancelled` in scheduler state.
- [x] Add Kubernetes `terminationGracePeriodSeconds: 30` to executor pod spec manifests consistent with graceful drain window.
- [x] Define task timeout model: `task_timeout_secs` field in `TaskSpec` and `ExecutorTaskAssignment`; wire field added to `ExecutorTaskAssignment` protobuf; executor enforces with `tokio::time::timeout` and reports `TaskFailed` on expiry.
- [ ] Define executor pod launch failure handling: operator monitors executor pod readiness; if a pod is not `Ready` within `executor_startup_timeout_seconds`, the operator deregisters the executor from the coordinator and triggers task reassignment (does not wait for heartbeat timeout).
- [x] Support `--coordinator <URL>` flag on `krishiv-executor` and add standalone `krishiv-coordinator` binary to `krishiv-scheduler` (`--coordinator-id`, `--grpc-addr`) for bare-metal / VM deployments without Kubernetes.
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
- [x] Add status API fields for executor lease generation, task attempt, and last failure reason (`lease_generation`, `memory_used_bytes`, `memory_limit_bytes`, `active_task_count` in `ExecutorView`; `last_failure_reason` in `TaskView`; retry count deferred).
- [x] Add cancel/delete API path used by Kubernetes finalizers: `reconcile` delete branch calls `coordinator.cancel_job` before stripping finalizer.
- [x] Add `CancelTask` RPC from coordinator to executor (`Coordinator::push_cancel_job` + `wire::task_cancellation_request_to_wire`).
- [x] Add `Deregister` RPC from executor to coordinator: `DeregisterExecutor` RPC in `CoordinatorExecutor` service, `Coordinator::deregister_executor`, and `ExecutorRuntime::deregister_with_grpc_endpoint` updated to call the dedicated RPC.
- [x] Extend heartbeat payload to include `memory_used_bytes`, `memory_limit_bytes`, `active_task_count`.
- [x] Store health snapshot per executor in `ExecutorRegistry` (`ExecutorHealthSnapshot` field on `ExecutorRecord`).

### Runtime Deliverables

- [x] Implement executor task runner loop: receive assignment, create local DataFusion `SessionContext`, register assigned input partitions, execute SQL query, report result and status back to coordinator.
- [x] Add the first executor-side assignment receiver loop backed by an in-memory inbox.
- [x] Add minimal executor task runner skeleton: consume one inbox assignment, report `Running`, validate placeholder fragment metadata, and report terminal status.
- [x] Add first narrow local SQL fragment execution path for `sql: SELECT 1`-style assignments, returning lightweight output metadata without Arrow payloads in control-plane Protobuf.
- [x] Add R3.1 bootstrap `local-parquet:<table>:<path>` input partition registration for executor-local `sql:` fragments, without starting R3.2 connector certification.
- [x] Implement executor deregistration on shutdown: SIGTERM handler calls `deregister_with_grpc_endpoint` (dedicated Deregister RPC) and exits cleanly.
- [x] Implement graceful executor shutdown handler: SIGTERM → `deregister_with_grpc_endpoint` → exit (current DataFusion batch completes naturally before shutdown).
- [x] Implement `CancelTask` handler on executor: `cancel_task` marks task in `cancelled_tasks` set; runner checks after `Running` status, skips execution, sends `TaskCancelled` to coordinator.
- [x] Implement coordinator `CancelJob`: `push_cancel_job` sends `CancelTask` gRPC to all assigned executors and marks job `Cancelled` (shuffle cleanup deferred to R4).
- [ ] Implement task lease token issuance on assignment; validate token before any shuffle or sink write.
- [ ] Implement stale lease token rejection in shuffle write path.
- [x] Implement memory-aware task placement: skip executors above configurable memory threshold when assigning new tasks (`CoordinatorConfig::with_memory_threshold`, `ExecutorRegistry` filters on `health_snapshot.memory_used_bytes`).
- [x] Implement task timeout enforcement on executor: `tokio::time::timeout` wraps `execute_stage_fragment`; on expiry returns `ExecutorError::InvalidAssignment` with timeout message causing `TaskFailed` report.
- [ ] Implement operator-side executor pod launch failure detection: deregister executor if pod not `Ready` within `executor_startup_timeout_seconds`.
- [x] Implement Kubernetes NetworkPolicy manifest for pre-R9 security posture: `k8s/manifests/network-policy.yaml` restricts coordinator gRPC port 9090 to `krishiv-system` namespace; added to kustomization and validated by manifest test.
- [x] Implement crash detection on coordinator side when executor heartbeat stops (`advance_heartbeat_clock` marks timed-out executors `Lost`).
- [x] Implement task reassignment on executor crash: `advance_heartbeat_clock` resets Running tasks on lost executors to `Assigned`; relaunched on next `launch_assigned_task_assignments`.
- [x] Add in-memory `MetadataStore` implementation.
- [x] Persist job, stage, task, attempt, executor lease, and event-log records through `MetadataStore` (`Coordinator::with_store` write-through in `submit_job` and `apply_task_update`).
- [x] Recover coordinator state from `MetadataStore` after process restart (`recover_from_store` on `Coordinator`).
- [x] Reject stale task attempts and ignore duplicate status updates safely.
- [x] Implement `KrishivJob` finalizer lifecycle: add finalizer on first observe, remove on deletion after cleanup.
- [x] Emit basic scheduler/executor stability metrics: `/metrics` endpoint serves live `StabilityMetrics` (running tasks, retries, failed assignments, max heartbeat age) in Prometheus text format.

### Test Checklist

- [x] gRPC task assignment and status update round-trip tests pass.
- [x] Versioned transport contract unit tests pass.
- [x] Executor binary config and request-construction tests pass.
- [x] Tonic service registration, heartbeat, and task status adapter tests pass.
- [x] Networked registration, heartbeat, and task-status gRPC smoke test passes.
- [x] Executor registers with coordinator and appears in executor registry.
- [x] Executor deregisters cleanly via dedicated `Deregister` RPC (`grpc_deregister_transitions_executor_to_removed`; fast-path draining heartbeat also retained).
- [x] Graceful shutdown test: SIGTERM to executor → deregistration heartbeat sent → executor exits cleanly (SIGTERM handler implemented in `heartbeat_loop`).
- [x] CancelJob test: `push_cancel_job` sends `CancelTask` RPC to executors; job transitions to `Cancelled` (`cancel_job_pushes_cancel_rpc_to_executor`).
- [ ] Zombie executor test: network partition heals after task reassignment; stale lease token rejected by shuffle write path; only new-assignment output is committed (deferred — requires R4 shuffle).
- [x] Extended heartbeat test: memory and task-count fields are populated and stored in `ExecutorRegistry` (`extended_heartbeat_stores_memory_snapshot`).
- [x] Memory-aware placement test: coordinator skips executors above memory threshold when assigning tasks (`memory_aware_placement_skips_overloaded_executor`).
- [x] `MetadataStore` persistence tests pass (`metadata_store_persists_job_on_submit`, `metadata_store_persists_task_state_on_update`).
- [x] Coordinator restart recovery tests pass (`coordinator_recovers_submitted_job_from_store`).
- [x] Executor lease expiry tests pass.
- [x] Stale task attempt update tests pass.
- [x] Duplicate task status update idempotency tests pass.
- [x] Minimal executor task runner lifecycle test passes against the scheduler-backed coordinator service.
- [x] Durable job event log round-trip tests pass (`in_memory_metadata_store_round_trips`).
- [x] `KrishivJob` finalizer add/remove tests pass (`reconcile_adds_finalizer_on_first_observe`, `reconcile_removes_finalizer_on_deletion`).
- [x] Operator restart during reconciliation does not duplicate scheduler jobs (`operator_restart_does_not_duplicate_scheduler_jobs`).
- [x] Basic stability metrics tests pass (`stability_metrics_include_heartbeat_age_and_task_counts`).
- [x] Typed plan operator enum tests pass with schema propagation (`plan_node_with_typed_op`, `plan_node_schema_propagation`, `plan_schema_empty_by_default`).
- [x] End-to-end test: `SELECT 1` runs coordinator → executor via gRPC, result metadata returned.
- [x] End-to-end test: Parquet file scan runs on executor with DataFusion through the networked assignment/status path, with result metadata returned to the runner caller and task status accepted by the coordinator.
- [x] Executor crash is detected by coordinator; task is reassigned to another executor (`executor_crash_detected_and_task_reassigned`).
- [x] Dedicated `Deregister` RPC transitions executor to `Removed` over networked gRPC (`grpc_deregister_transitions_executor_to_removed`).
- [x] `ExecutorRuntime::deregister_with_grpc_endpoint` calls the dedicated Deregister RPC over a real gRPC connection, transitioning executor to `Removed` (`deregister_via_grpc_endpoint_transitions_executor_to_removed`).
- [x] `KrishivJob` delete path calls `cancel_job` before stripping finalizer (`reconcile_delete_calls_cancel_job_before_removing_finalizer`).
- [x] Task timeout field propagates from `TaskSpec` through wire protocol to `ExecutorTaskAssignment`; timeout enforcement via `tokio::time::timeout` in executor runner.
- [x] NetworkPolicy manifest restricts coordinator gRPC port to `krishiv-system` namespace (`network_policy_restricts_coordinator_grpc_to_krishiv_namespace`).
- [x] Status API exposes executor `lease_generation`, `memory_used_bytes`, `memory_limit_bytes`, `active_task_count`, and task `last_failure_reason` via `ExecutorView`/`TaskView`.
- [x] `CancelTask` inbox marks task as cancelled; executor runner sends `TaskCancelled` and skips execution (`task_runner_reports_cancelled_when_inbox_cancel_received`).
- [x] `/metrics` serves live `StabilityMetrics` in Prometheus format (`metrics_returns_prometheus_stability_fields`, `metrics_reflects_live_coordinator_state`).
- [x] `krishiv-coordinator` binary CLI parses defaults, explicit flags, and rejects invalid input (`parses_defaults`, `parses_explicit_flags`, `rejects_unknown_flag`, `rejects_invalid_grpc_addr`).

### Acceptance Gate For R3.1

- [x] A real SQL query (`SELECT` over a local Parquet file) completes end-to-end: coordinator assigns the task over gRPC, executor runs it via DataFusion, result is returned to the coordinator.
- [x] Executor crash mid-task is detected and the task is reassigned without manual intervention.
- [x] Graceful executor shutdown completes within `terminationGracePeriodSeconds` without dropping in-flight task output (SIGTERM handler in executor `heartbeat_loop`).
- [x] `CancelJob` stops all executor tasks via `push_cancel_job` RPC and transitions job to `Cancelled`.
- [ ] Stale lease tokens from a zombie executor are rejected by the shuffle write path (deferred — requires R4 shuffle).
- [x] Coordinator restart recovers job, stage, task, attempt, executor lease, and event-log state.
- [x] Stale task attempts and duplicate status updates cannot corrupt job state.
- [x] Deleting a `KrishivJob` runs finalizer cleanup: `cancel_job` is called before finalizer is stripped, leaving no active assignments (`reconcile_delete_calls_cancel_job_before_removing_finalizer`).
- [x] `MetadataStore` correctly persists job/task state.
- [x] Typed plan operator enum passes schema propagation tests.
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
