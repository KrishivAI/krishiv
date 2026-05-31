# Krishiv Implementation Status

This file is intentionally short. The codebase is the source of truth; use this
as a session handoff note, not as a release-plan archive.

## Current State

- Workspace is a Rust 2024 Cargo workspace with 30 active member crates.
- `krishiv-common` is the shared utility crate currently used by runtime and
  other crates.
- Removed crates still visible in git history include `krishiv-async-util`,
  `krishiv-sql-policy`, and `krishiv-upgrade-tests`.
- Runtime modes are embedded, single-node, and distributed via
  `krishiv-runtime`.
- Runtime placement is explicit via `ExecutionPlacement`: embedded and
  single-node may run `LocalInProcess`, single-node may use a `SingleNodeDaemon`,
  and distributed requires `RemoteClusterRequired`.
- Distributed local fallback is rejected during session/runtime construction
  rather than deferred until query execution.
- Distributed support uses scheduler, executor, proto, Flight SQL, and optional
  Kubernetes operator/manifests.
- The documentation set has been collapsed to `docs/README.md` plus this
  handoff file to avoid stale release-roadmap drift.

## Last Session: Coordinator Cleanup + Architecture Bottlenecks

### Coordinator.jobs elimination (Track B completion)

- Removed `Coordinator.jobs: HashMap<JobId, JobRecord>` — the transitional
  dual-map that shadowed `job_coordinators: HashMap<JobId, Arc<JobCoordinator>>`.
- All job access now routes through `JobCoordinator` using
  `std::sync::RwLock<JobRecord>` for sync-safe access in both sync and async
  contexts via `read_record()` / `write_record()`.
- `find_job` / `find_job_mut` now return `RwLockReadGuard` / `RwLockWriteGuard`.
- Terminal jobs no longer removed from `job_coordinators` on completion (was
  breaking post-terminal queries like snapshots).
- 30+ access sites migrated across coordinator/, tests.

### Architecture bottleneck fixes

1. **FlightClientPool connection reuse** — `do_action()` and `execute_sql()`
   added to `FlightClientPool` with lazy `OnceCell<Channel>`. RemoteExecutionRuntime
   routes all RPCs through a single persistent channel.
2. **ExecutorAssignmentInbox bounded default** — `new()` defaults to 256 capacity.
   `new_unbounded()` preserved for tests.
3. **gRPC channel cache pruning** — `prune_executor_channel()` called from
   `mark_executor_lost` and `advance_heartbeat_clock`.
4. **Parallel job launch** — `drive_pending_task_launches` uses `join_all` for
   concurrent assignment delivery across jobs.
5. **In-process checkpoint 3-phase** — `CheckpointInner::handle_ack` now async
   with extract → I/O → finalize. In-process bridge no longer holds
   `Mutex<Coordinator>` across filesystem writes.
6. **Coordinator lock sharding** — gRPC `register_executor` and `checkpoint_ack`
   handlers use dedicated `executor_inner` / `checkpoint_inner` locks instead of
   the full coordinator `Arc<RwLock<Coordinator>>`.

### Files touched

- `crates/krishiv-scheduler/src/coordinator/mod.rs` — jobs field removed,
  constructor, find_job/find_job_mut, Debug, drive_pending_task_launches
- `crates/krishiv-scheduler/src/coordinator/job_lifecycle.rs` — submit_job,
  cancel_job, apply_task_update
- `crates/krishiv-scheduler/src/coordinator/executor_ops.rs` — channel pruning,
  reset_running_tasks_for_lost_executor
- `crates/krishiv-scheduler/src/coordinator/recovery.rs` — recover_from_store,
  persist_jobs_to_store
- `crates/krishiv-scheduler/src/coordinator/task_assignment.rs` — guard paths
- `crates/krishiv-scheduler/src/coordinator/streaming.rs` — guard paths
- `crates/krishiv-scheduler/src/coordinator/snapshots.rs` — job_snapshot,
  job_detail_snapshot, job_snapshots
- `crates/krishiv-scheduler/src/coordinator/barrier_dispatch.rs` — guard path
- `crates/krishiv-scheduler/src/checkpoint.rs` — receive_ack_async
- `crates/krishiv-scheduler/src/coordinator_sharded.rs` — handle_ack async
  3-phase, finalize_ack, ExecutorInner register/deregister
- `crates/krishiv-scheduler/src/in_process.rs` — checkpoint_ack 3-phase
- `crates/krishiv-scheduler/src/grpc.rs` — register_executor inner lock,
  checkpoint_ack inner lock + 3-phase
- `crates/krishiv-scheduler/src/job_coordinator.rs` — tokio→std RwLock,
  read_record/write_record sync accessors
- `crates/krishiv-scheduler/src/tests.rs` — jobs→job_coordinators migration
- `crates/krishiv-runtime/src/execution_runtime.rs` — FlightClientPool usage,
  RemoteExecutionRuntime pool-based RPCs
- `crates/krishiv-runtime/src/flight_client.rs` — FlightClientPool with
  do_action, execute_sql, OnceCell channel
- `crates/krishiv-executor/src/assignment_inbox.rs` — bounded default
- `crates/krishiv-api/src/dataframe.rs` — DashMap fix, collect routing
- `crates/krishiv-api/src/session.rs` — InProcessCluster optional in
  build_execution_runtime
- `crates/krishiv-exec/src/join.rs` — pre-existing corruption fix
- `crates/krishiv-vector-sinks/Cargo.toml` — added dashmap dep
- `crates/krishiv-vector-sinks/src/lancedb_sink.rs` — missing .await fix

### Validation

```bash
cargo test -p krishiv-scheduler --lib   # 205 passed
cargo test -p krishiv-executor --lib    # 162 passed
cargo test -p krishiv-runtime --lib     # 277 passed
cargo test -p krishiv-api --lib         # 35 passed
```

### Pending

- Sync dance (`sync_executor_to_inner` / `sync_checkpoint_to_inner`) still
  transitional. Full elimination requires outer Coordinator methods to read
  from inner locks directly.

## Next Useful Commands

```bash
cargo check --workspace
cargo test -p krishiv-api --lib
```
