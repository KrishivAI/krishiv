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

## Current Session: Python API Binding Enhancements & Batch Examples Implementation

- Created a virtual environment and set up Python 3.14 development dependencies (`maturin`, `pytest`, `pandas`, `pyarrow`, `arro3-core`).
- Reclaimed 62 GB of disk space by deleting an abandoned 48 GB subagent worktree and a 14 GB duplicate cargo target directory.
- Fixed a PyO3 type-subclassing bug in `crates/krishiv-python/src/schema.rs` by adding `#[pyo3(signature = (*_args, **_kwargs))]` on `__init_subclass__` to make it compatible with Python 3.14.
- Fixed a session-mode initialization bug in `crates/krishiv-python/src/session.rs` `from_env()` to properly detect and route to local vs. distributed modes when `KRISHIV_MODE` is unset but a coordinator URL is present.
- Fixed a validation bug in `crates/krishiv-python/src/windows.rs` `ensure_watermark_before_window` to allow `max_lateness_ms` to be `0` (valid for strict ordering streams).
- Fixed an Arrow Capsule to Pandas conversion error in `crates/krishiv-python/src/batch.rs` and `query_result.rs` by calling `pyarrow.record_batch` to explicitly cast raw Capsule representation into a standard `pyarrow.RecordBatch` object.
- Exposed and exported missing lakehouse and utility functions (`read_delta`, `read_hudi`, `write_hudi_append`, `write_hudi_upsert`, `make_example_batch`, `apply_state_migration`, `memo_cache_info`, `memo_transform_call`) in `crates/krishiv-python/python/krishiv/__init__.py` and `krishiv.pyi`.
- Implemented and successfully ran all 6 Python batch examples in `/home/code/krishiv/crates/krishiv-python/examples/`:
  - `batch_iot_sensor.py`: Validated sensor average temp, max humidity, and device count SQL aggregates.
  - `batch_ecommerce.py`: Validated customer-order VIP/Standard joins and revenue aggregates.
  - `batch_log_analytics.py`: Validated error rate calculations per microservice.
  - `batch_delta_audit.py`: Validated local Delta table Version 0 and Version 1 time-travel queries.
  - `batch_hudi_ingest.py`: Validated local COW Hudi table snapshot appending and snapshot reading.
  - `batch_sql.py`: Checked general SQL group-by and ordering on parquet dataframes.

- Fixed a type mismatch bug in `/home/code/krishiv/crates/krishiv/examples/stream_continuous_job.rs` where `LocalWindowExecutionSpec::default_count_agg()` returned a `Vec<AggExpr>` instead of a `LocalWindowExecutionSpec`; resolved by using `LocalWindowExecutionSpec::new_test_tumbling(...)`.
- Added a public `.with_state_ttl(5000)` builder method on `Stream` in `/home/code/krishiv/crates/krishiv-api/src/stream.rs` and updated `/home/code/krishiv/crates/krishiv/examples/stream_state_ttl.rs` to call it, resolving a private field compilation error.
- Successfully compiled, executed, and verified all 5 streaming examples in Rust:
  - `stream_transaction_count.rs`: Verified transaction event-time tumbling window counts. Removed an unused import warning.
  - `stream_multi_source.rs`: Verified sliding window aggregation with multi-source watermark lag synchronization.
  - `stream_session_window.rs`: Verified grouping clickstream logs by user-activity inactivity session windows.
  - `stream_continuous_job.rs`: Verified continuous unbounded job submission, live data pushing, and window polling.
  - `stream_state_ttl.rs`: Verified stateful windowed count queries running under event-time state TTL eviction rules.

- Added a `#[new]` python constructor on `PyBatch` in `crates/krishiv-python/src/batch.rs` so that standard `pyarrow.RecordBatch` objects can be wrapped directly via `ks.Batch(pa_batch)`.
- Implemented and successfully ran the Python streaming example `/home/code/krishiv/crates/krishiv-python/examples/stream_transaction_count.py` verifying real-time transaction event-time tumbling count aggregations.

### Validation

```bash
/home/code/krishiv/.venv/bin/python3 crates/krishiv-python/examples/batch_iot_sensor.py
/home/code/krishiv/.venv/bin/python3 crates/krishiv-python/examples/batch_ecommerce.py
/home/code/krishiv/.venv/bin/python3 crates/krishiv-python/examples/batch_log_analytics.py
/home/code/krishiv/.venv/bin/python3 crates/krishiv-python/examples/batch_delta_audit.py
/home/code/krishiv/.venv/bin/python3 crates/krishiv-python/examples/batch_hudi_ingest.py
/home/code/krishiv/.venv/bin/python3 crates/krishiv-python/examples/batch_sql.py
/home/code/krishiv/.venv/bin/python3 crates/krishiv-python/examples/stream_transaction_count.py
/home/code/krishiv/.venv/bin/pytest crates/krishiv-python/python/tests/          # 24 passed, 2 skipped
cargo run -p krishiv --example stream_transaction_count
cargo run -p krishiv --example stream_multi_source
cargo run -p krishiv --example stream_session_window
cargo run -p krishiv --example stream_continuous_job
cargo run -p krishiv --example stream_state_ttl
```

### Pending

- Implementation of the remaining equivalent Python streaming examples.

## Previous Session: Batch Example Ingestion & Execution Verification

- Verified and ran all embedded batch examples one by one.
- **`batch_iot_sensor`**: Verified average temperature, max humidity, and device count aggregations. Fixed unused `Int64Array` compiler warning.
- **`batch_ecommerce`**: Verified VIP/Standard customer segmented revenue joins and sum aggregations.
- **`batch_log_analytics`**: Verified error rate calculations and filter logic per application service.
- **`batch_delta_audit`**: Verified time-travel query capability on Version 0 vs Version 1 (latest) after fixing engine provider deregistration and delta writer log version initialization bugs.
- **`batch_hudi_ingest`**: Verified local Copy-On-Write snapshot query and ingestion flow.
- **`batch_sql`**: Checked standard inline SQL dataframe queries against parquet.
- **`memory_stream`**: Validated memory stream collect and sequence filtering.
- Ran workspace `cargo check` and full test suites for `krishiv-runtime`.

### Validation

```bash
cargo run -p krishiv --example batch_iot_sensor
cargo run -p krishiv --example batch_ecommerce
cargo run -p krishiv --example batch_log_analytics
cargo run -p krishiv --example batch_delta_audit
cargo run -p krishiv --example batch_hudi_ingest
cargo run -p krishiv --example batch_sql
cargo run -p krishiv --example memory_stream
cargo check --workspace
cargo test -p krishiv-runtime --lib                         # 279 passed
```

### Pending

- Validation of all streaming examples in `crates/krishiv/examples/` as the next step.

## Previous Session: In-Process Protocol + Checkpoint Inner Drift

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
