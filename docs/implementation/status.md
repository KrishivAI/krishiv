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
- Durability profiles are explicit via `DurabilityProfile`: `dev-local`,
  `single-node-durable`, and `distributed-durable`.
- The documentation set has been collapsed to `docs/README.md` plus this
  handoff file to avoid stale release-roadmap drift.

## Current Session: In-Process Protocol + Checkpoint Inner Drift

- `SharedCoordinator::new` now seeds `CheckpointInner` from existing coordinator
  checkpoint state instead of starting empty.
- Added `SharedCoordinator::submit_job`, and routed CCP/federation submit paths
  through it so streaming checkpoint coordinators are reflected into the
  sharded checkpoint state.
- gRPC management `list_checkpoints` and `inspect_state` now read
  `checkpoint_inner` directly, reducing dependence on the outer coordinator
  checkpoint snapshot.
- In-process runtime now builds executor/checkpoint inner locks after local
  executor registration, so embedded/single-node direct transport starts from
  the same executor registry state as the coordinator.
- Executor assignment inbox deduplication now keys by
  `(job_id, task_id, attempt_id)` instead of `(task_id, attempt_id)`, fixing
  repeated embedded/single-node jobs that reuse local task ids.
- `trigger_checkpoint_for_job` explicitly drops its job existence guard before
  mutating checkpoint state, satisfying the synchronization-lock lint.
- Added deployment conformance coverage for embedded local execution,
  single-node daemon placement, distributed fake remote placement, Kubernetes
  `kind` smoke-test artifacts, and bare-metal/VM coordinator process mode.
- Added shared `DurabilityProfile` types in `krishiv-common`, re-exported by
  shuffle/state/checkpoint, and wired coordinator-family daemons to validate
  `dev-local`, `single-node-durable`, and `distributed-durable` requirements.

### Validation

```bash
cargo test -p krishiv-scheduler --lib shared_                         # 5 passed
cargo test -p krishiv-executor --lib same_task_attempt_in_different_jobs_is_not_duplicate  # 1 passed
cargo test -p krishiv-runtime --lib collect_batch_sql_multiple_queries                     # 1 passed
cargo test -p krishiv-runtime --lib in_process                                             # 40 passed
cargo test -p krishiv-runtime --lib deployment_conformance                                 # 2 passed
cargo test -p krishiv-scheduler --test r2_k8s_manifests deployment_conformance             # 1 passed
cargo test -p krishiv-common durability                                                    # 2 passed
cargo test -p krishiv-scheduler --lib parses_defaults
cargo test -p krishiv-scheduler --lib parses_single_node_durable_profile_with_required_local_storage
cargo test -p krishiv-scheduler --lib parses_distributed_durable_profile_with_etcd_fencing
cargo test -p krishiv-scheduler --lib rejects_single_node_durable_without_metadata_path
cargo check -p krishiv-shuffle -p krishiv-state -p krishiv-checkpoint
```

### Pending

- Remaining sync dance (`sync_executor_to_inner` / `sync_checkpoint_to_inner`)
  still exists for tick/checkpoint ack compatibility; more outer coordinator
  checkpoint/executor APIs need migration to inner-lock reads/writes before it
  can be removed.
- Durability profiles are now typed and daemon-validated; remaining work is to
  carry them through more executor-side state/checkpoint construction sites.

## Previous Session: Coordinator Cleanup + Architecture Bottlenecks

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
