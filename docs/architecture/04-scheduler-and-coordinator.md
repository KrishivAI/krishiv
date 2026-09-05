# Scheduler and coordinator

`krishiv-scheduler` is the control plane. It admits jobs, plans them into
stages and tasks, places tasks on executors, tracks their lifecycle, drives
checkpoints and restores, persists metadata, and serves the gRPC and HTTP
control APIs. This document describes the pieces and the invariants that hold
them together; the data plane they command is `05-executor-and-data-plane.md`.

## Two control-plane roles

| Role | Type | Scope |
|---|---|---|
| **Cluster control plane (CCP)** | `ClusterControlPlane` / `Coordinator` | cluster membership, admission, queues, placement across every job; one leader at a time |
| **Job control plane (JCP)** | `JobCoordinator` (`job_coordinator.rs`) | one job's stage progression, retries, checkpoint barriers; may run inside the CCP process or as a dedicated `krishiv job-coordinator` process |

The `Coordinator` struct is the CCP state machine. It is shared behind
`SharedCoordinator` (an `Arc<RwLock<Coordinator>>`) by the gRPC service, the
HTTP router, the UI, and the Kubernetes operator; every mutation goes through
the write lock and every read through a snapshot (`JobSnapshot`,
`JobDetailSnapshot`, `StageSnapshot`, `TaskSnapshot`, `StabilityMetrics`).
Sharded operation (`coordinator_sharded.rs`) partitions jobs across several
coordinator instances by job id.

## Leadership and fencing

Every durable write is fenced. `leadership.rs` defines the `LeaderElection`
trait — `is_leader()`, `renewal_age()`, `fencing_token()` — with two
implementations: `SingleNodeLeader` (always leader, monotonic token from the
metadata store) and the etcd lease (`etcd_lease.rs`) used by the
`distributed-durable` profile. The fencing token travels on every checkpoint
commit, every metadata write, and every shuffle lease, so a deposed leader that
still believes it is the leader cannot commit anything a successor has moved
past. `/leaderz` reports leadership for Service routing; the liveness probe
runs on a dedicated thread so a wedged scheduling runtime is visible as
"alive but not leader" rather than as a dead pod.

## Job lifecycle

```
JobSpec ─► admission ─► Queued ─► Accepted ─► Planning ─► Running ─► Succeeded
                                                                  ├► Failed
                                                                  └► Cancelled
```

- **Admission** (`admission.rs`): validates the spec (`validate_job`),
  applies namespace quotas and per-queue limits, and rejects with a typed
  `SchedulerError` rather than queueing something that can never run.
- **Queues** (`QueueManager`, `NamespaceQuotaQueueManager`): jobs wait in
  named queues with quotas (`KRISHIV_QUEUE_*` variables; the `KrishivQueue`
  CRD in `14`). The Kubernetes operator's `ReconcileAction::WaitingForExecutors`
  is the visible face of a job that is accepted but not placeable.
- **Planning**: batch SQL jobs are planned by `distributed_batch.rs` into
  stages (`03-planning-and-optimization.md`, "From plan to tasks"). If staged
  planning fails the job falls back to a single `sql:` task and a `WARN` is
  logged — remote execution without scale-out, never a silent local run.
- **Running**: tasks progress `Pending → Assigned → Running → Succeeded |
  Failed | Retrying`. Stage kinds are `ShuffleMap` (output is hash-partitioned
  shuffle data) and `Result` (output is the job result).
- **Terminal**: the job record moves to history (`JobHistoryRecord`, capped by
  `MAX_JOB_HISTORY`); shuffle output and spooled results are reclaimed after a
  grace period (`KRISHIV_JOB_GC_GRACE_SECS`, default 30) so late readers of a
  finished job do not see their data vanish.

## Executors and placement

`heartbeat.rs` owns the `ExecutorRegistry`: a `HashMap<ExecutorId,
ExecutorRecord>` keyed for the hot heartbeat path. Each record carries the
descriptor (host, slots, capability flags), state (`Registered`, `Alive`,
`Draining`, `Lost`), the running task set, the last heartbeat tick, a
`LeaseGeneration`, and an `ExecutorHealthSnapshot` (memory used/limit, active
tasks, CPU, network bytes). Re-registration is idempotent and bumps the lease
generation when the previous incarnation was alive — the new generation is what
lets the coordinator fence stale task reports from the old process. An
executor missing `heartbeat_timeout_ticks` heartbeats is marked lost, its
tasks are re-queued, and `krishiv_executor_lost_total` increments.

Placement policies (`job/scheduler.rs`):

| Scheduler | Policy |
|---|---|
| `StaticScheduler` | round-robin over registered executors |
| `SlotAwareScheduler` | fill free slots, respecting the executor's declared capacity |
| `LocalityScheduler` | prefer the executor holding the upstream shuffle partition (`LocalityPreference`, `LocalityOutcome`, `LocalityTierCounts` for observability) |
| `FairScheduler` | weighted fair share across pools (`PoolSpec`) and namespaces (`NamespaceQuotaSnapshot`, `ResourceUsage`) |

A task assignment carries the job, stage, task and attempt ids, the typed
fragment, the shuffle read/write configuration, the executor's lease
generation, and — for stateful streaming tasks — the key-group range the
subtask owns.

## Failure handling

- **Task failure** retries up to the job's attempt budget with a new
  `AttemptId`; the shuffle store's lease token (`06-shuffle.md`) makes the old
  attempt's late writes unacceptable.
- **Executor loss** re-queues its running tasks and, for `ShuffleMap` output
  that lived only on that executor's disk, regenerates the producing stage
  (`ShuffleRegenOutcome`) within a bounded regeneration budget.
- **Coordinator restart** rebuilds the registry and job table from the
  metadata store (`reset_for_recovery` then replay); running streaming jobs
  receive a `RestoreDirective` that names the checkpoint epoch to resume from
  and the prepared sink transactions to commit or abort (`07`).
- **Consecutive-failure tracking** belongs to the executor *process*: a
  re-registered executor starts from zero.

## Metadata stores

`store.rs` defines the synchronous `MetadataStore` trait and the persisted
record types (`PersistedJobRecord`, `PersistedExecutorDescriptor`,
`ContinuousSnapshot`, `JobHistoryRecord`, `EventLogEvent`). Three backends
match the three durability profiles (`14`):

| Backend | Profile | Notes |
|---|---|---|
| in-memory | `dev-local` | lost on exit |
| RocksDB (`rocksdb_metadata.rs`) | `single-node-durable` | local file; events log rotates at `MAX_EVENTS_LOG_BYTES` = 64 MiB |
| etcd (`etcd_metadata.rs`, feature `etcd`) | `distributed-durable` | one key per record: `/krishiv/jobs/<id>`, `/krishiv/executors/<id>`, `/krishiv/continuous/<id>`, `/krishiv/ivm/<id>[#<chunk>]`, `/krishiv/history/<id>` |

Two etcd facts are load-bearing. IVM snapshots are zstd-compressed and chunked
across `#<index>` keys because a single snapshot exceeded etcd's 1.5 MiB
request ceiling under the HA chaos gate. And all etcd I/O runs on a dedicated
two-thread runtime (`ETCD_RUNTIME`) with the caller blocking on a channel:
driving etcd futures on the scheduling runtime deadlocked every coordinator
under executor churn. The events log is audit-only and never persisted to
etcd; in memory it is a ring buffer bounded by the same 64 MiB, evicting in
1/8 slabs.

## Results

Small task results travel inline in the terminal `TaskStatus`. A result over
the executor's inline threshold (`KRISHIV_INLINE_RESULT_MAX_BYTES`) is streamed
ahead of it as `PushTaskResult` chunks into a **result spool**
(`result_spool.rs`): one Arrow IPC file per task attempt under
`KRISHIV_RESULT_SPOOL_DIR`, capped at `KRISHIV_RESULT_SPOOL_MAX_BYTES` (8 GiB),
`fdatasync`ed every `KRISHIV_RESULT_SPOOL_SYNC_INTERVAL_BYTES` (64 MiB) so
dirty page cache cannot OOM-kill the pod, and deleted on drop. Job results are
served to clients from the spool over Flight.

## Checkpoint coordination

A streaming job with `checkpoint_interval_ms` gets a per-job
`CheckpointCoordinator` (`checkpoint.rs`) whose state machine is `Idle →
AwaitingAcks{epoch} → Committing{epoch} → Committed{epoch} | Failed{epoch,
reason}`. `try_tick` initiates an epoch when the interval elapses;
`barrier_dispatch.rs` / `barrier_client.rs` deliver the barrier to every task;
`barrier_tracker.rs` collects `CheckpointAckRequest`s (operator snapshot refs,
source offsets, prepared sink refs); on quorum the coordinator extracts a
`PendingCommit` under the lock and writes metadata + integrity manifest
outside it. A savepoint is a checkpoint with `pending_savepoint_label` set.
The storage layout and the executor side of the protocol are in `07`.

## Adaptive execution and IVM hosting

The coordinator hosts two engines' control logic directly:

- **AQE** (`coordinator/aqe`, `adaptive.rs`): between batch stages, re-plan
  the next stage from measured shuffle output (`03`).
- **IVM** (`ivm.rs`): the `IvmJobRegistry` holds every incremental job's flow
  in the coordinator process as the single source of truth, offloading ticks
  to a resident executor flow when one is available (`09`).

## Control surfaces

| Surface | File | Purpose |
|---|---|---|
| gRPC `CoordinatorService` | `grpc.rs`, `transport.rs` | executor registration, heartbeats, task status, result push, barrier acks; client job submission |
| HTTP `/api/v1/*` | `coordinator_daemon.rs`, `*_http.rs` | `sql`, `batch-sql`, `jobs/…`, `executors`, `queues`, `events`, `history`, `logs`, `metrics-snapshot`, `bounded-window`, `continuous*`, `ivm/jobs/…`, `openapi.json` |
| probes | `coordinator_daemon.rs` | `/healthz`, `/readyz`, `/leaderz`, `/metrics` |
| console | `krishiv-ui` | `/console` SPA over the same `/api/v1` routes (`11`) |

Authentication for both gRPC and HTTP is in `12-security.md`. The daemon
binaries are `krishiv clusterd` (alias `coordinator`) and
`krishiv job-coordinator`.

## Invariants worth restating

1. The coordinator never executes data-plane work; it plans, places, and
   records. (IVM is the deliberate exception: the flow of record lives here.)
2. Every durable write carries a fencing token and a lease generation.
3. A task's terminal state is decided once; late reports from a superseded
   attempt or lease are rejected, not merged.
4. Metadata durability follows the profile; there is no path that persists
   "sometimes".

## Related

- `05-executor-and-data-plane.md`, `06-shuffle.md`, `07-state-checkpoints-savepoints.md`.
- `../engineering-log/crate-audit-register.md` — the scheduler audit (≈25
  fixes) and its open items.
- `../decisions/0003-task-fragment-encoding.md`.
