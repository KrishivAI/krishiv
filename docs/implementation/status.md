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

## Current Session: k3s Kubernetes Deployment + All 24 Examples

Installed k3s on Ubuntu 26.04 VPS (11 GiB RAM, 96 GiB disk) and deployed
Krishiv as a 4-pod distributed cluster. All 24 examples (12 Rust + 12 Python)
pass against the k3s cluster.

### k3s cluster setup

- k3s v1.35.5+k3s1 installed (`curl -sfL https://get.k3s.io | ... sh -`)
- Built musl-static binary (`cargo build --target x86_64-unknown-linux-musl`)
  so the image runs on Alpine without a glibc dependency.
- Container image: `localhost/krishiv:local` — `FROM alpine:3.21` + musl binary
  (415 MiB including debug symbols).
- Deployed to namespace `krishiv-system`:
  - `coordinator` (1 pod): gRPC `:9090`, HTTP `:18080`, `--insecure`, JSON metadata
  - `executor` (2 pods, 2 slots each = 4 total): hostPath `/tmp` mount so pods can
    read temp parquet files written by the client.
  - `flight-server` (1 pod): `KRISHIV_COORDINATOR_HTTP` → coordinator HTTP
- NodePort services: flight `:30051`, coordinator HTTP `:30080`

### Bugs fixed during k3s deployment

1. **GLIBC mismatch** — Ubuntu 26.04 binary (glibc 2.43) crashes in Debian
   bookworm containers (glibc 2.36). Fixed by building with musl
   (`x86_64-unknown-linux-musl` target) and using `FROM alpine:3.21`.

2. **reqwest CA cert panic** — `FROM scratch` has no CA certificate store.
   reqwest with rustls panics on startup. Fixed by switching to `FROM alpine:3.21`
   which ships `/etc/ssl/certs/ca-certificates.crt`.

3. **Executor advertises `0.0.0.0:50055`** — the coordinator can't reach executor
   pods via `http://0.0.0.0:50055`. Fixed in `cli.rs`: when bind IP is unspecified
   (`0.0.0.0`), use the configured host for the advertised endpoint. Injected
   `POD_IP` via the Kubernetes downward API so executors advertise their real pod IP.

### Validation

```bash
# Cluster health
curl http://127.0.0.1:30080/readyz        # → ready
curl http://127.0.0.1:30080/api/v1/executors  # → 2 Healthy executors

# All 12 Rust examples (ExIT 0)
KRISHIV_COORDINATOR_URL=http://127.0.0.1:30051 cargo run -p krishiv --example <name>

# All 12 Python examples (exit 0)
KRISHIV_COORDINATOR_URL=http://127.0.0.1:30051 python3 crates/krishiv-python/examples/<name>.py
```

20 coordinator jobs created, 19 Succeeded (1 Cancelled = timed-out from pre-fix
run).

## Previous Session: All 7 Architectural Fixes

All 7 bugs/gaps/bottlenecks identified in the session audit were implemented.

### Changes

1. **`InputPartitionDescriptor::InlineIpc`** — Added proto field (`ipc_bytes = 14`, kind `INLINE_IPC = 6`), Rust domain variant, wire encoding/decoding, executor `read_inline_ipc_partitions`, `register_window_partitions` on coordinator, `window_job_partitions` map in coordinator. Window jobs now pass Arrow IPC bytes as typed task input partitions instead of encoding them in the fragment description string.

2. **Flight server proxy mode** — `FlightExecutionHost` keeps `InProcessCluster` always (needed for continuous streams), but batch SQL and bounded windows are forwarded to the real coordinator in proxy mode. The `coordinator_http` field now correctly separates "where to run batch/window" from "local cluster for continuous streams".

3. **Python `from_env` alignment** — `Session.from_env()` now calls `with_local_cluster(url)` when `KRISHIV_COORDINATOR_URL` is set without `KRISHIV_MODE`, matching the Rust example behavior.

4. **Remove `drive_pending_task_launches` from HTTP handlers** — Removed the inline `drive_pending_task_launches` polling loop from `execute_batch_sql_coordinated`. The background orchestration loop (every 500ms) handles task dispatch; the HTTP handler just submits and waits.

5. **Python batch examples use cluster mode** — All 6 Python batch examples updated from `Session.embedded()` to `Session.from_env()`. When `KRISHIV_COORDINATOR_URL` is set they route through the real coordinator.

6. **Missing Python streaming examples** — Added `memory_stream.py`, `stream_multi_source.py`, `stream_state_ttl.py`. Also added `with_state_ttl(ttl_ms)` on `PyStream` and `state_ttl_ms` field on `StreamPipeline`; added `with_source_id_column` on `PyRelation`.

7. **Circuit breaker observability** — `GET /api/v1/executors` now includes `consecutive_task_failures` per executor so operators can see circuit breaker state without restarting the cluster.

### Validation

All 12 Rust examples and all 12 Python examples pass on a fresh single-node cluster.

```bash
cargo run -p krishiv -- local start --data-dir /tmp/krishiv-cluster
# Rust (all 12): EXIT 0
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example <name>
# Python (all 12): exit 0
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 python3 crates/krishiv-python/examples/<name>.py
# Circuit breaker visible:
curl http://127.0.0.1:18080/api/v1/executors  # → consecutive_task_failures: 0
```

## Previous Session: Python Examples on Single-Node Cluster

All 9 Python examples in `crates/krishiv-python/examples/` pass on the real
single-node cluster (coordinator + executor + flight-server). No bugs found.

- Batch examples use `Session.embedded()` — run locally, no cluster required.
- Streaming examples use `Session.from_env()` with `KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051` — windowing routed through coordinator → executor via the `bounded-window` path implemented in the previous session.
- Rebuilt Python module with `maturin develop` to pick up the new bounded_window coordinator routing.

### Validation

```bash
# Batch (embedded, no cluster needed)
python3 crates/krishiv-python/examples/batch_iot_sensor.py
python3 crates/krishiv-python/examples/batch_ecommerce.py
python3 crates/krishiv-python/examples/batch_log_analytics.py
python3 crates/krishiv-python/examples/batch_sql.py
python3 crates/krishiv-python/examples/batch_delta_audit.py
python3 crates/krishiv-python/examples/batch_hudi_ingest.py

# Streaming (single-node cluster)
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 python3 crates/krishiv-python/examples/stream_transaction_count.py
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 python3 crates/krishiv-python/examples/stream_session_window.py
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 python3 crates/krishiv-python/examples/stream_continuous_job.py
# All 9 exit 0. bounded-window-* jobs visible in GET /api/v1/jobs as Succeeded.
```

## Previous Session: Correct Streaming Architecture — Coordinator → Executor

Fixed the bounded window execution path so streaming examples go through the real
coordinator → executor pipeline (not the flight server's embedded InProcessCluster).

### Changes

- **`crates/krishiv-executor/src/fragment/batch.rs`**: Added `window:<topic>:<spec_b64>:<batches_b64>` fragment handler. Decodes the window spec (JSON+base64) and input batches (Arrow IPC+base64), then runs `krishiv_exec::execute_bounded_window` and returns inline IPC results.
- **`crates/krishiv-executor/Cargo.toml`**: Added `base64 = "0.22"` dependency.
- **`crates/krishiv-scheduler/src/bounded_window.rs`**: New module — `execute_bounded_window_coordinated` submits a `bounded-window` batch job carrying the encoded spec+batches, drives task launches, and waits for inline IPC results (same pattern as `batch_sql.rs`).
- **`crates/krishiv-scheduler/src/bounded_window_http.rs`**: New module — `api_bounded_window` HTTP handler wrapping the above.
- **`crates/krishiv-scheduler/Cargo.toml`**: Added `base64 = "0.22"`.
- **`crates/krishiv-scheduler/src/lib.rs`**: Registered `bounded_window` and `bounded_window_http` modules.
- **`crates/krishiv-scheduler/src/coordinator_daemon.rs`**: Registered `POST /api/v1/bounded-window` route.
- **`crates/krishiv-runtime/src/coordinator_http_client.rs`**: Added `execute_coordinator_bounded_window` — POSTs to the coordinator and returns decoded batches.
- **`crates/krishiv-runtime/src/lib.rs`**: Re-exported `execute_coordinator_bounded_window`.
- **`crates/krishiv-flight-sql/src/lib.rs`**: `BoundedWindow` action handler now checks `coordinator_http_url()`; when set, forwards to coordinator instead of using the embedded InProcessCluster.

### Architecture after fix

```
Client (example)
  → Flight server (port 50051)
      ├─ Batch SQL        → POST /api/v1/batch-sql    → coordinator → executor ✓
      └─ BoundedWindow    → POST /api/v1/bounded-window → coordinator → executor ✓
                                         (fragment: window:<topic>:<spec_b64>:<batches_b64>)
```

Streaming jobs now appear in the coordinator job list as `bounded-window-*` and
are executed by the registered executor. Embedded mode (no coordinator URL) still
runs locally on the flight server's InProcessCluster.

### Validation

All 12 examples pass on a fresh single-node cluster:

```bash
cargo run -p krishiv -- local stop --data-dir /tmp/krishiv-cluster
cargo run -p krishiv -- local start --data-dir /tmp/krishiv-cluster
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example <name>
# All 12 exit 0. bounded-window-* jobs visible in /api/v1/jobs.
```

## Previous Session: All 12 Examples on Real Single-Node Cluster

### Bugs Fixed

1. **`memory_stream.rs`** — missing `ExecutionMode` import.

2. **`local_cluster.rs`** — coordinator spawned without `--insecure`; executor
   could not register via gRPC (denied as unauthenticated). Added `--insecure`
   to the coordinator spawn args in `run_local_start`.

3. **`dataframe.rs` + `session.rs`** — `read_delta_async` / `read_hudi_async`
   produce `sql_query = Some("SELECT * FROM delta_...")`. In remote mode,
   `collect_async` routed that SQL to the coordinator batch-sql API. The
   executor's DataFusion context had no delta/hudi table registered, so the
   job failed/hung. Fix: added `force_local: bool` flag on `DataFrame`; set it
   via `with_force_local()` in both lake-house read methods so collection
   always runs against the local DataFusion plan.

4. **`execution_runtime.rs`** — `RemoteExecutionRuntime::accept_plan` for
   `ExecutionKind::Streaming` plans called `do_action(ExecutePlan)` → flight
   server → coordinator batch-sql HTTP 500. Streaming plans are executed by
   `collect_bounded_window` on the flight server's embedded `InProcessCluster`;
   the coordinator roundtrip is wrong. Fix: return early with a success report
   for streaming plans.

### Validation (single-node cluster: coordinator + executor + flight-server)

```bash
# Start cluster
cargo run -p krishiv -- local start --data-dir /tmp/krishiv-cluster

# Batch examples
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example batch_iot_sensor
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example batch_ecommerce
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example batch_log_analytics
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example batch_sql
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example batch_delta_audit
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example batch_hudi_ingest

# Streaming examples
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example memory_stream
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example stream_transaction_count
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example stream_multi_source
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example stream_session_window
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example stream_continuous_job
KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051 cargo run -p krishiv --example stream_state_ttl
# All 12 exit 0.
```

### Pending

- None. All 12 examples pass on the real single-node cluster.

## Previous Session: Single-Node Run of All Rust Examples

- Fixed a missing `ExecutionMode` import in `crates/krishiv/examples/memory_stream.rs` (was in scope from `krishiv` re-export but absent from the use statement).
- Ran all 12 examples on the embedded single-node cluster — all pass cleanly:
  - **batch_iot_sensor**: avg temp, max humidity, device count per device.
  - **batch_ecommerce**: VIP/Standard revenue segmentation.
  - **batch_log_analytics**: error rate per service.
  - **batch_delta_audit**: time-travel version 0 vs latest.
  - **batch_hudi_ingest**: COW snapshot with 3 users.
  - **batch_sql**: city group-by count.
  - **memory_stream**: collect + sequence-0 filter over in-memory stream.
  - **stream_transaction_count**: event-time tumbling window counts.
  - **stream_multi_source**: sliding window multi-device aggregation.
  - **stream_session_window**: inactivity-gap session grouping.
  - **stream_continuous_job**: unbounded job submit + live push + poll.
  - **stream_state_ttl**: stateful TTL windowed counts.

### Validation

```bash
cargo check -p krishiv --examples   # Finished (0 errors)
cargo run -p krishiv --example batch_iot_sensor
cargo run -p krishiv --example batch_ecommerce
cargo run -p krishiv --example batch_log_analytics
cargo run -p krishiv --example batch_sql
cargo run -p krishiv --example batch_delta_audit
cargo run -p krishiv --example batch_hudi_ingest
cargo run -p krishiv --example memory_stream
cargo run -p krishiv --example stream_transaction_count
cargo run -p krishiv --example stream_multi_source
cargo run -p krishiv --example stream_session_window
cargo run -p krishiv --example stream_continuous_job
cargo run -p krishiv --example stream_state_ttl
```

### Pending

- Implementation of remaining Python streaming examples (stream_continuous_job, stream_session_window).

## Previous Session: Python API Binding Enhancements & Batch Examples Implementation

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
