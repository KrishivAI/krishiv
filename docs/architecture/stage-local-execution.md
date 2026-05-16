# Krishiv Stage-Local Execution Model

**Status:** Draft - R3.1 implementation spec.
**Owner:** Architecture team.
**Linked releases:** R3.1, R3.2, R4, R5, R6.

---

## Purpose

This document defines the distributed execution contract for R3.1. It describes how the coordinator breaks work into stages, assigns partition-local work to replaceable executors, and preserves enough metadata to recover after coordinator or executor failure.

R3.1 should not build a custom distributed physical operator engine. Each executor runs a local DataFusion context for its assigned partitions. Krishiv coordination, scheduling, retries, leases, metadata, and later shuffle/checkpoint services wrap that local execution model.

---

## 1. Core Model

Krishiv distributed execution is stage-local:

1. The coordinator receives a logical or physical DAG.
2. The planner splits that DAG into stages at source, exchange, shuffle, sink, and future stateful boundaries.
3. Each stage is divided into partition-local task assignments.
4. Each executor receives a task assignment for one or more input partitions.
5. The executor creates an isolated local DataFusion execution context for the assignment.
6. The executor registers assigned inputs as DataFusion tables or providers.
7. The executor runs the stage fragment locally and reports status back to the coordinator.
8. Data movement between stages is handled by the shuffle service in R4, not by custom executor-to-executor operator code in R3.1.

This gives Krishiv a simple first distributed runtime while keeping the door open for richer shuffle, streaming, checkpoint, and adaptive execution work.

---

## 2. Non-Goals For R3.1

R3.1 does not implement:

- Certified Parquet, Kafka, or S3 connectors. Those are R3.2.
- A production shuffle service. That is R4.
- Stateful streaming operators. Those are R5.
- Exactly-once checkpoint coordination. That is R6 and must be certified per connector path.
- Multi-coordinator active-active scheduling for the same job.
- Advanced AQE, hot-key splitting, or resource governance.

---

## 3. Components

| Component | R3.1 Responsibility |
|---|---|
| API server / operator | Accepts user jobs and reconciles `KrishivJob` resources into coordinator-owned jobs. |
| JobCoordinator | Owns job truth, stage lifecycle, task attempts, retries, leases, status, and recovery. |
| MetadataStore | Persists job, stage, task, attempt, executor lease, and event-log records. |
| Executor | Runs assigned stage fragments in isolated local DataFusion contexts. |
| Local DataFusion context | Executes SQL fragments and local physical operators for assigned partitions. |
| Connector adapters | Register input partitions or output targets with the local executor context. R3.1 uses minimal inputs; R3.2 certifies real connectors. |
| Shuffle service | Receives stage outputs and serves downstream stage inputs in R4. R3.1 only defines the boundary. |

---

## 3.1 Coordinator/Executor Transport

R3.1 uses generated protobuf and tonic services for the coordinator/executor
network boundary. `krishiv-proto` owns the versioned wire schema and converts it
to Rust domain contracts; `krishiv-scheduler` owns the gRPC server adapter over
the active shared coordinator; `krishiv-executor` owns the gRPC client path for
registration and heartbeat.

The first networked service is `CoordinatorExecutor`:

- `RegisterExecutor`: executor announces id, host, slots, and transport version.
- `ExecutorHeartbeat`: executor refreshes lease generation and reports running attempts.
- `TaskStatus`: executor reports task attempt state back to the coordinator.

Generated protobuf types should remain contained at the transport edge. Scheduler
state continues to use Krishiv typed ids, attempts, leases, and lifecycle enums.

---

## 4. Plan To Stage Mapping

The planner turns a Krishiv plan into a stage graph. A stage is the largest piece of work that can run locally on an executor without requiring a distributed exchange inside the executor process.

Stage boundaries occur at:

- Source boundaries.
- Sink boundaries.
- Exchange or repartition requirements.
- Future shuffle boundaries.
- Future stateful streaming boundaries.
- Future checkpoint barrier coordination points.

Each planned stage records:

- `stage_id`.
- Parent and child stage ids.
- Input partition descriptors.
- Output partitioning contract.
- Output schema.
- Operator kind.
- Estimated cardinality for later R4 cost and AQE work.

R3.1 should prefer a small typed operator enum over string labels so later shuffle, state, and connector code can match on operator semantics safely.

---

## 5. Task Assignment Model

A task assignment is the unit of executor work:

```rust
pub struct TaskAssignment {
    pub job_id: JobId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
    pub executor_id: ExecutorId,
    pub input_partitions: Vec<InputPartition>,
    pub plan_fragment: PlanFragment,
    pub output_contract: OutputContract,
    pub lease_generation: LeaseGeneration,
}
```

The exact Rust types may differ, but these fields are required at the control-plane boundary.

Task states:

| State | Meaning |
|---|---|
| `Pending` | Task is known but not assigned. |
| `Assigned` | Coordinator has assigned the task to an executor attempt. |
| `Running` | Executor acknowledged and started the attempt. |
| `Succeeded` | Attempt completed and output contract is satisfied. |
| `Failed` | Attempt failed and may be retried. |
| `Cancelled` | Coordinator intentionally cancelled the assignment. |
| `Stale` | Attempt lost ownership because a newer attempt or lease generation exists. |

Attempt rules:

- Attempt ids are monotonic per task.
- Only the latest live attempt may move a task to `Running`, `Succeeded`, or `Failed`.
- Duplicate status updates for the same attempt and terminal state are idempotent.
- Status updates from stale attempts are ignored or recorded as stale, but they never change task truth.
- Any side-effecting sink commit must include attempt id and future fencing metadata before it can be certified.

---

## 6. Executor Model

An executor is a replaceable data-plane worker. It never owns durable truth.

For each assignment, the executor:

1. Validates the assignment lease generation.
2. Creates a local DataFusion `SessionContext` or reuses a pooled context with isolated namespaces.
3. Registers assigned input partitions as DataFusion tables, object-store scans, in-memory batches, or future connector providers.
4. Builds the local physical execution fragment.
5. Executes the fragment and streams or collects Arrow `RecordBatch` values.
6. Writes output according to the output contract.
7. Reports task status updates to the coordinator with `attempt_id` and `lease_generation`.

Executor process loss is normal. The coordinator must assume any executor can disappear after receiving an assignment.

---

## 7. Coordinator Model

The coordinator is the only active scheduler for a job. API servers may be active-active, but exactly one `JobCoordinator` owns scheduling for a specific job.

The coordinator owns:

- Job submission and validation.
- Stage graph lifecycle.
- Static task placement in R3.1.
- Task attempt creation.
- Executor lease tracking.
- Heartbeat timeout detection.
- Stage-level retry.
- Durable metadata writes.
- Durable job event log writes.
- Status projection for CLI, API, Web UI, and Kubernetes status.
- Cancel/delete cleanup for `KrishivJob` finalizers.

R3.1 should keep placement static. Adaptive placement, resource queues, hot-key splitting, and multi-tenant governance belong to later releases.

---

## 8. MetadataStore Contract

`MetadataStore` is the coordinator recovery boundary. The first implementation may be in-memory for tests, but the trait must represent durable semantics.

Required persisted entities:

- Job records.
- Stage records.
- Task records.
- Task attempt records.
- Executor lease records.
- Event-log records.
- Partition descriptors.
- Plan fragment metadata.
- Schema metadata.

Required event-log entries:

- `JobSubmitted`.
- `StagePlanned`.
- `TaskAssigned`.
- `TaskStarted`.
- `TaskSucceeded`.
- `TaskFailed`.
- `ExecutorRegistered`.
- `ExecutorLost`.
- `JobCancelled`.
- `JobFinished`.

Recovery uses the latest materialized records plus the event log. Tests must prove replay does not create duplicate jobs, duplicate attempts, or duplicate terminal updates.

---

## 9. Executor Leases And Heartbeats

Every executor registration receives a lease generation. Heartbeats refresh that lease while the executor is healthy.

Lease rules:

- The coordinator stores `executor_id`, `lease_generation`, heartbeat timestamp or tick, and state.
- A heartbeat with an older generation is stale.
- A heartbeat after lease expiry may be rejected and require re-registration.
- When a lease expires, the coordinator marks the executor lost.
- Tasks assigned to a lost executor move to retry handling.
- Reassigned tasks receive new attempt ids.

The status API should expose heartbeat age, lease generation, running task count, and last failure reason.

---

## 10. Failure And Recovery

### 10.1 Executor Crash

1. Executor stops heartbeating.
2. Coordinator marks the executor lease expired.
3. Coordinator records `ExecutorLost`.
4. Coordinator marks running attempts on that executor failed or stale.
5. Coordinator creates new attempts for retryable tasks.
6. Replacement executors receive new assignments.
7. Stale status updates from the old executor are ignored if they arrive later.

### 10.2 Coordinator Restart

1. Coordinator loads jobs, stages, tasks, attempts, leases, and event log from `MetadataStore`.
2. Coordinator reconstructs in-memory scheduling state.
3. Coordinator marks unknown or expired executor leases as lost.
4. Coordinator reassigns unfinished tasks whose attempts have no valid executor lease.
5. Coordinator resumes status projection without creating duplicate scheduler jobs.

### 10.3 Operator Restart

`KrishivJob` reconciliation must be idempotent. The operator uses the Kubernetes resource identity and job id to find existing coordinator state before creating any new scheduler job.

### 10.4 Kubernetes Delete Or Cancel

`KrishivJob` finalizer cleanup must:

- Mark the job cancelled.
- Stop assigning new tasks.
- Cancel active task attempts.
- Release coordinator-owned runtime records.
- Leave enough terminal metadata for audit and status.

---

## 11. Data Movement And Shuffle Boundary

R3.1 defines stage-local execution but does not implement production shuffle.

R4 will add:

- Shuffle writer API.
- Shuffle reader API.
- Shuffle metadata.
- Partitioning model.
- Compression and spill hooks.
- Shuffle garbage collection and orphan detection.

The R3.1 executor must not hide ad hoc distributed data movement inside task execution. Stage outputs should flow through an explicit output contract so R4 can replace local or placeholder outputs with the real shuffle service.

---

## 12. Streaming And Checkpoint Handoff

The stage-local model also constrains later streaming and checkpoint work:

- R5 streaming operators run inside executor-owned stage fragments.
- R5 continuous tasks still use coordinator task ownership, executor leases, and status updates.
- R5 watermarks are operator control records, not scheduler heartbeats.
- R6 checkpoint barriers must include job, stage, task, attempt, and checkpoint epoch identity.
- R6 checkpoint ownership must be fenced so stale attempts cannot acknowledge or commit a checkpoint.

R3.1 should leave explicit extension points for long-running tasks, checkpoint epochs, and future state ownership, but it should not implement those systems early.

---

## 13. Status And Metrics

R3.1 status and metrics should expose enough information to debug instability:

- Job id, stage id, task id, attempt id.
- Executor id and lease generation.
- Heartbeat age.
- Task duration.
- Retry count.
- Failed assignment count.
- Last failure reason.
- Coordinator restart recovery count.
- Stale update count.

These fields are basic stability signals, not the full R9 OpenTelemetry surface.

---

## 14. Example Flow: SQL Over Parquet

1. User submits `SELECT region, count(*) FROM sales GROUP BY region`.
2. Coordinator builds a stage graph.
3. Planner creates input partition descriptors for Parquet files.
4. Coordinator assigns stage tasks to available executors.
5. Each executor registers its Parquet partitions in a local DataFusion context.
6. Each executor runs the local aggregate fragment.
7. Stage outputs are written according to the output contract.
8. Coordinator records task success and advances the stage.
9. R4 shuffle will add a downstream merge stage when repartitioning is required.

---

## 15. Hard Invariants

- Exactly one active `JobCoordinator` schedules a given job.
- Executors are replaceable and never own durable truth.
- `MetadataStore` is the recovery source of truth.
- Attempt ids are monotonic per task.
- Stale attempts cannot change terminal task state.
- Duplicate terminal updates are idempotent.
- Stage-local execution must be deterministic for the same inputs and plan fragment.
- Distributed data movement must pass through explicit output/shuffle contracts.
- Embedded, single-node, and Kubernetes modes must remain semantically aligned for supported features.

---

## 16. R3.1 Acceptance Checklist

- [x] `crates/krishiv-executor` exists.
- [ ] `krishiv-executor` can register with the coordinator.
- [x] Coordinator and executor communicate through a tonic-shaped in-process service boundary for registration and heartbeat.
- [ ] Coordinator and executor communicate through versioned networked gRPC messages.
- [x] R3.1 transport contracts include `attempt_id` and `lease_generation`.
- [ ] Scheduler task status updates are idempotent and reject stale attempts.
- [ ] `MetadataStore` persists job, stage, task, attempt, lease, and event-log records.
- [ ] Coordinator restart reconstructs scheduler state from `MetadataStore`.
- [ ] Executor crash triggers lease expiry and task reassignment.
- [ ] `KrishivJob` finalizer cleanup cancels active assignments.
- [ ] Status API exposes attempt, lease, retry, heartbeat, and failure fields.
- [ ] A local Parquet `SELECT` runs coordinator to executor to coordinator over the R3.1 transport.
