# Deep Code Review: `krishiv-scheduler`, `krishiv-executor`, `krishiv-proto`, `krishiv-shuffle`

**Scope:** Library source (non-test), core algorithms, architecture, concurrency, and performance.  
**Methodology:** Read `Cargo.toml`, `lib.rs`, and every `src/**/*.rs` file; grep for `TODO|FIXME|GAP|BUG|unwrap|panic!|unsafe`; trace cross-crate call graphs.

---

## 1. `krishiv-scheduler`

### 1.1 Bugs

| # | File | Line | Issue | Severity |
|---|------|------|-------|----------|
| B‑S1 | `checkpoint.rs` | 297 | **BUG‑1:** Epoch hint must be written *after* the manifest seals the snapshot, but the current ordering can commit an incomplete manifest. | Critical |
| B‑S2 | `auth.rs` | 24 | `validate_grpc_auth` is **opt‑in per handler**. Any handler that forgets to call it accepts anonymous traffic. This is a security hole, not a compile‑time guarantee. | High |
| B‑S3 | `etcd_metadata.rs` | 68–79 | `persist()` uses `tokio::task::block_in_place(|| block_on(...))` inside a **synchronous** `MetadataStore` trait method. When called from an async context (e.g. gRPC handler) it blocks the async runtime thread. | High |
| B‑S4 | `cluster_control.rs` | 199–229 | `run_leader_loop` ignores errors from `promote_to_active` / `demote_to_standby` with `let _ =`. If promotion fails, orchestration loops may still be spawned on an invalid state. | Medium |
| B‑S5 | `grpc.rs` | 266 | GAP‑CK‑03: `commit_epoch()` calls synchronous disk I/O (`LocalFsCheckpointStorage`) directly inside an async gRPC handler. | Medium |
| B‑S6 | `coordinator.rs` | 967 | GAP‑CP‑05: Fail‑closed on persist errors is documented but not fully wired; a failed metadata append may leave the job state inconsistent. | Medium |
| B‑S7 | `batch_sql.rs` | 57–93 | Hard‑coded 300 s timeout with a busy‑wait loop (`sleep(50ms)`). No cancellation propagation; the loop keeps polling after `cancel_job` is issued. | Low |

### 1.2 Algorithms

- **Deterministic heartbeat clock** (`heartbeat.rs:132`): `advance_clock` ages every executor uniformly in ticks. Lost detection is `current_tick - last_heartbeat_tick >= timeout_ticks`.
- **Slot‑aware scheduling** (`heartbeat.rs:194`): `schedulable_executors` filters by `can_accept_work()`, non‑zero slots, and optional memory threshold. Returns borrowed descriptors to avoid cloning.
- **Lease generation bump** (`heartbeat.rs:51`): Re‑registration bumps the lease only when the executor was alive; deregistration / mark‑lost always bump. Prevents zombie updates.
- **Barrier dispatch planning** (`barrier_dispatch.rs:67`): Scans `checkpoint_coordinators` in `AwaitingAcks`, collects `Running` tasks with a barrier endpoint, deduplicates via `barrier_dispatch_sent`.
- **Barrier ack tracking** (`barrier_tracker.rs:38`): `record_ack` verifies `epoch` and `job_id`, then checks `expected_tasks.is_subset(received_acks)`.

### 1.3 Architecture

- **`SharedCoordinator`** wraps a `tokio::sync::RwLock<Coordinator>`. All state mutations funnel through this lock.
- **`MetadataStore`** trait is **synchronous** (`fn append_event(&mut self, ...) -> SchedulerResult<()>`). The `EtcdMetadataStore` implementation violates async safety by blocking inside these sync methods.
- **`ClusterControlPlane`** decouples leader election (`SharedLeader`) from the shared coordinator. `SingleNodeLeader` is a local `AtomicU64` wrapper; HA backends (etcd, K8s) are injected via `with_leader`.
- **`JobCoordinator`** scopes per‑job operations but still locks the entire `SharedCoordinator` for every tick.
- **gRPC layering**: `grpc.rs` exposes `CoordinatorExecutorTonicService` which wraps the sync `Coordinator` APIs in async handlers. Auth extraction (GAP‑CP‑08) is stubbed but not wired into every handler.

### 1.4 Gaps

- **GAP‑5** (`tests.rs:3844`, `coordinator.rs:639`): Checkpoint epoch abort cleanup (stale notify/dispatch state).
- **GAP‑CP‑06** (`coordinator.rs:837`): Rebuilding checkpoint coordinators from recovered job specs on restart.
- **GAP‑OB‑01** (`metrics.rs:6`): Only three atomic counters exist (`jobs_submitted`, `checkpoint_epochs`, `tasks_assigned`). No histograms, no per‑job metrics.
- **GAP‑4** (`coordinator.rs:1435`): Channel map must be cloned before the await point; current code holds the read lock across await in some paths.
- **GAP‑CP‑03** (`checkpoint.rs:254`): Fencing token validation before storage commit is not enforced.
- **GAP‑CP‑08** (`grpc.rs:45`): Auth context extraction is not automatically applied to every gRPC handler.
- **GAP‑RT‑04** (`grpc.rs:275`): Management service (savepoint, restore, list checkpoints) is only stubbed.
- **GAP‑3** (`job.rs:545`): Per‑task retry configuration from the stage spec is not wired into the scheduler retry logic.
- **GAP‑SH‑04** (`job.rs:1110`): AQE coalesced partition count is read but not used in stage planning.

### 1.5 Refactoring

1. **Make `MetadataStore` async** (or add an `AsyncMetadataStore` sibling) so `EtcdMetadataStore` can use async etcd clients without blocking the runtime.
2. **Replace `SingleNodeLeader` `SeqCst`** with `Relaxed` ordering; it is process‑local and does not need cross‑thread synchronization guarantees.
3. **Move auth to a tonic interceptor** instead of requiring every handler to call `validate_grpc_auth` manually.
4. **Split `coordinator.rs`** (currently ~1500+ lines) into sub‑modules: `executor_registry.rs`, `job_state.rs`, `checkpoint_coordinator.rs`.
5. **Event‑driven batch SQL** (`batch_sql.rs`): Replace the 50 ms polling loop with a `tokio::sync::Notify` or oneshot channel triggered on job state transition.
6. **Parallel barrier dispatch** (`barrier_dispatch.rs:160`): Dispatch barriers to multiple executors concurrently (`futures::stream::iter(...).buffer_unordered(n)`).

### 1.6 Features / Maturity

| Feature | Status | Notes |
|---------|--------|-------|
| Batch SQL coordination | **Mature** | End‑to‑end via `execute_batch_sql_coordinated`. |
| Executor registry / heartbeat | **Mature** | Lease generation, lost detection, memory filtering. |
| Job lifecycle | **Mature** | Submit, cancel, snapshot, detail queries. |
| Checkpoint coordination | **Partial** | Epoch management exists; fencing validation and abort cleanup are gaps. |
| Barrier dispatch | **Partial** | Planning and tracker are solid; dispatch is serial and auth is missing. |
| Federation HTTP | **Mature** | `federation_http.rs` covers submit/status/cancel. |
| Web UI / metrics | **Basic** | HTML table + Prometheus text format. |
| Admission control | **Basic** | `QuotaQueueManager` works; no CRD backend yet. |
| Adaptive governance | **Types only** | `AdaptiveDecisionKind` defined; no wiring to scheduler logic. |
| Management RPC | **Stub** | Types in `management.rs`; no gRPC implementation. |

### 1.7 Performance

- **`JsonFileMetadataStore::persist`** (`store.rs:161`) rewrites the entire metadata blob (events + jobs) on every `append_event` or `save_job`. Cost is **O(n)** in event log size; will degrade as jobs accumulate.
- **`drive_pending_task_launches`** may hold the `SharedCoordinator` write lock for extended periods while building task assignments.
- **`batch_sql.rs`** busy‑polls every 50 ms, wasting CPU and adding latency.
- **Barrier dispatch** is serial; large jobs with many running tasks will serialize round‑trips.

---

## 2. `krishiv-executor`

### 2.1 Bugs

| # | File | Line | Issue | Severity |
|---|------|------|-------|----------|
| B‑E1 | `runner.rs` | 995–1008 | `initiate_checkpoint_and_deliver_ack` uses `tokio::task::block_in_place` (on multi‑thread runtime) to run sync checkpoint I/O. Blocks the async thread pool. | High |
| B‑E2 | `runner.rs` | 1065 | `initiate_checkpoint_for_job` synthesizes a dummy `ExecutorId::try_new("exec").expect(...)` when `running_attempts` lacks the task. Fragile and leaks a fake identity to the coordinator. | Medium |
| B‑E3 | `fragment/streaming.rs` | 182 | `execute_loop_fragment` panics with `expect("called with wrong prefix")` on a malformed fragment string. Library code should return `Err`. | Medium |
| B‑E4 | `transport.rs` | 301–314 | `ExecutorRuntime::connect_coordinator_client` creates a **new tonic channel per call**. No pooling; can exhaust ephemeral ports under load. | High |
| B‑E5 | `barrier_transport.rs` | 55, 62 | `SharedBarrierInjector` uses `Mutex::lock().unwrap_or_else(|e| e.into_inner())` which continues with poisoned data. Could corrupt barrier state. | Medium |
| B‑E6 | `runner.rs` | 872–878 | `terminal_streaming_task` detection relies on string prefix matching (`starts_with("stream:continuous:")`) and `parse_stream_fragment`. Fragile dispatch logic. | Low |
| B‑E7 | `cli.rs` | 181 | `listener.local_addr().unwrap()` after binding; if the kernel fails to report the bound address, the executor crashes. | Low |

### 2.2 Algorithms

- **Execution model dispatch** (`execution_model.rs:31`): `ExecutionModel::from_fragment` delegates to `krishiv_plan::TypedTaskFragment::decode_or_legacy`, centralizing the batch vs streaming decision.
- **Hash partitioning** (`fragment/batch.rs:277`): Uses `HashPartitioner::new(key_column, num_partitions)` → `partitioner.partition(batch)` → writes per‑partition `ShufflePartition` to the store.
- **Watermark computation** (`fragment/streaming.rs:362`): Scans the event‑time column (`Int64Array`) for the max timestamp, subtracts `watermark_lag_ms`. Returns `None` if column missing or batches empty.
- **Lease‑token anti‑zombie** (shuffle store): Every `write_partition` checks the stored token; stale tokens are rejected before data hits disk.
- **Checkpoint ack fan‑out** (`runner.rs:1021`): Iterates `checkpoint_runners`, looks up real `TaskAttemptRef` from `running_attempts`, falls back to synthetic stage‑0 ids only when necessary.

### 2.3 Architecture

- **`ExecutorTaskRunner`** is a large central struct (~30 fields). It uses a builder pattern (`with_shuffle`, `with_inmem_shuffle`, `with_barrier_injector`, etc.) but the struct itself is a god‑object.
- **`DashMap`** is used for `checkpoint_runners` and `loop_executors` to allow concurrent access across task slots.
- **`SharedLeaseGeneration`** (`grpc_client.rs:15`) is an `Arc<AtomicU64>` shared between the heartbeat loop, `GrpcCoordinatorService`, and the runner. Updates are monotonic (`fetch_max`).
- **`SourceThrottleTable`** (`source_throttle.rs:26`) is an `Arc<DashMap<String, Option<u64>>>` shared between heartbeat loop (writer) and source operators (readers). Enforcement is log‑only; no token bucket yet.
- **Streaming**: True continuous unbounded streaming is not implemented. `stream:loop:` uses a per‑job `ContinuousWindowExecutor` stored in `loop_executors`; `stream:continuous:` delegates to a `ContinuousJobDrainer` trait.

### 2.4 Gaps

- **GAP‑2** (`runner.rs:204`, `fragment/streaming.rs:333`): Watermark is computed and attached to output metadata, but the scheduler does not yet use it for global low‑watermark tracking or downstream stage scheduling.
- **GAP‑6** (`runner.rs:581`, `fragment/streaming.rs:16`): `stream:loop:` fragments use a stateful `ContinuousWindowExecutor`, but the full continuous operator loop (R5.1) is still a simulation (`BarrierSimulator` in `barrier.rs`).
- **GAP‑C3** (`transport.rs:399`): Pooled gRPC client exists (`CoordinatorGrpcPool`) but `ExecutorRuntime` bypasses it with one‑shot `connect_coordinator_client`.
- **GAP‑CP‑09** (`cli.rs:438`): Executor task gRPC server address is configured but not validated against the advertised endpoint.
- **Barrier durability**: `BarrierSimulator` (`barrier.rs:20`) is metadata‑only; no durable snapshot writing yet (deferred to R6).

### 2.5 Refactoring

1. **Use `spawn_blocking` for checkpoint I/O** in `runner.rs:995` instead of `block_in_place`.
2. **Route all coordinator RPCs through `CoordinatorGrpcPool`** to avoid connection churn.
3. **Break up `run_assignment_with`** (`runner.rs:762`) into smaller async functions: `send_running_status`, `execute_fragment`, `send_terminal_status`.
4. **Extract common table registration** (`fragment/common.rs:34`, `fragment/batch.rs:100`, `fragment/streaming.rs:316`) into a single `register_input_partitions` helper.
5. **Replace string prefix matching** in `terminal_streaming_task` with a typed `FragmentKind` enum from `krishiv_plan`.
6. **Remove `expect` in `execute_loop_fragment`**; return `ExecutorError::InvalidAssignment`.

### 2.6 Features / Maturity

| Feature | Status | Notes |
|---------|--------|-------|
| Batch SQL execution | **Mature** | DataFusion integration via `SqlEngine`. |
| Shuffle write (disk) | **Mature** | Parquet via `LocalDiskShuffleStore`; lease tokens protect against zombies. |
| Shuffle write (memory) | **Mature** | `InMemoryShuffleStore` with spill‑to‑disk. |
| Shuffle read (memory) | **Mature** | `InMemoryShuffleStore::read_partition`. |
| Shuffle read (flight) | **Mature** | `FlightShuffleClient` fetches over Arrow Flight. |
| Kafka‑to‑Parquet | **Partial** | Feature‑gated; only in‑memory Kafka source for tests. |
| Streaming windows | **Partial** | Bounded tumbling/sliding/session windows work; continuous streaming is simulated. |
| Checkpoint participation | **Partial** | Acks are sent; state backend snapshot I/O blocks the async thread. |
| Task cancellation | **Mature** | `cancel_task` on inbox + runner checks before execution. |
| Source throttling | **Stub** | Table stores limits; no token‑bucket enforcement. |

### 2.7 Performance

- **Table registration is uncached** (`fragment/batch.rs:100`): Every task re‑registers Parquet tables with the `SqlEngine` even if the same table was used by a previous task on the same executor.
- **`HashPartitioner`** (`partitioner.rs:34`) allocates a `Vec<Vec<u32>>` for every row, then calls `arrow::compute::take` per column. This is expensive for wide schemas.
- **Checkpoint I/O blocks async threads** (`runner.rs:995`), reducing throughput of the multi‑slot runner pool.
- **New gRPC channel per RPC** (`transport.rs:301`) adds TCP handshake latency and port pressure.

---

## 3. `krishiv-proto`

### 3.1 Bugs

| # | File | Line | Issue | Severity |
|---|------|------|-------|----------|
| B‑P1 | `wire.rs` | 786 | `input_partition_descriptor_from_wire` defaults unknown `kind` to `Unspecified` via `unwrap_or`, silently dropping data instead of failing. | Medium |
| B‑P2 | `wire.rs` | 930 | `output_contract_descriptor_from_wire` same pattern as B‑P1. | Medium |
| B‑P3 | `wire.rs` | 585 | `task_output_metadata_from_wire` reconstructs `TaskRuntimeStats` only when *any* stat field is > 0. A job with `input_rows=0` but `output_rows=0` loses the explicit zero, which is semantically fine but inconsistent. | Low |
| B‑P4 | `ids.rs` | — | `try_new` uses regex validation. Called on every wire conversion (e.g. `wire.rs:424`), this is unnecessary overhead for hot paths. | Low |

### 3.2 Algorithms

- **Typed identifier macro** (`ids.rs`): Generates `JobId`, `TaskId`, etc. with a single `String` inner field, `Display`, `PartialEq`, `Eq`, `Hash`, and `TryFrom<&str>` validation.
- **Wire conversion** (`wire.rs`): Each domain type has a `*_to_wire` and `*_from_wire` function. Uses a `required` helper to fail on missing protobuf fields.
- **Version negotiation** (`wire.rs:628`): `TransportVersion` carries `major`/`minor`; compatibility check is `major == CURRENT.major && minor <= CURRENT.minor`.

### 3.3 Architecture

- **Domain / wire separation**: All service traits (`CoordinatorExecutorService`, `ExecutorTaskService`) are defined over domain structs first. `grpc.rs` in each crate adapts them to tonic.
- **Checkpoint ack response** (`wire.rs:1136`) maps four variants (`Accepted`, `StaleEpoch`, `JobNotFound`, `StaleFencingToken`) to protobuf `oneof`.
- **Management types** (`management.rs`) define savepoint/restore/list/inspect requests/responses but have no gRPC server implementation yet.

### 3.4 Gaps

- **GAP‑RT‑04** (`management.rs:5`): Management service is types‑only.
- **GAP‑2** (`executor.rs:136`): Watermark field exists on `ExecutorHeartbeatRequest` but is not populated by the executor nor used by the scheduler.
- **GAP‑3** (`job.rs:156`): `max_task_retries` exists on `TaskSpec` but the scheduler only uses stage‑level retries.

### 3.5 Refactoring

1. **Split `wire.rs`** (~1180 lines) into `wire/task.rs`, `wire/executor.rs`, `wire/checkpoint.rs`, etc.
2. **Replace `unwrap_or` with strict parsing** in `input_partition_descriptor_from_wire` and `output_contract_descriptor_from_wire`; unknown variants should return `WireError`.
3. **Cache compiled regex** or switch to a faster validation (e.g. `id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'`) in `ids.rs`.
4. **Use `Arc<str>`** for repeated identifier strings (`job_id`, `stage_id`) inside wire messages to reduce cloning.

### 3.6 Features / Maturity

| Feature | Status | Notes |
|---------|--------|-------|
| Domain types | **Mature** | IDs, tasks, jobs, checkpoints, executors, lifecycle states. |
| Wire conversions | **Mature** | Bidirectional mapping for all R3.1+ messages. |
| Versioning | **Mature** | Major/minor compatibility check on every message. |
| Management types | **Stub** | Defined but not served. |
| LLM quota/throttle | **Types only** | `LlmThrottleCommand` and `LlmQuotaReport` exist; wiring is minimal. |

### 3.7 Performance

- **`wire.rs` allocations**: Every conversion allocates new `String`s for ids. For high‑frequency heartbeats (every 10 s × many executors) this is unnecessary; `Arc<str>` or string interning would help.
- **`TaskOutputMetadata`** carries inline Arrow IPC as `Vec<Vec<u8>>`. For large batches this can dominate gRPC payload size.

---

## 4. `krishiv-shuffle`

### 4.1 Bugs

| # | File | Line | Issue | Severity |
|---|------|------|-------|----------|
| B‑H1 | `memory_store.rs` | 62–126 | **Race condition in `ensure_memory_capacity`**: reads partition under read lock, spills to disk, then acquires write lock to remove. Another thread may replace the partition in between, causing the spill to **delete the newer partition** (data loss) and undercount `bytes_used`. | **Critical** |
| B‑H2 | `disk_store.rs` | 90 | Temp files from the two‑phase write are not cleaned up on store startup. After a crash, `.tmp.*` files can accumulate indefinitely. | Medium |
| B‑H3 | `object_store.rs` | 143–149 | `put` is not atomic on all object‑store backends. Two concurrent writers can race without the temp‑file + rename guard used by the disk store. | Medium |
| B‑H4 | `partitioner.rs` | 63, 77, etc. | Null keys in the hash column are routed to bucket **0**. Skewed data with many nulls will create hot partitions. | Low |
| B‑H5 | `flight.rs` | 124–155 | `do_get` loads the **entire** partition into a `Vec<RecordBatch>` before encoding into `FlightData`. No streaming for large partitions. | Medium |
| B‑H6 | `shuffle_svc.rs` | 56–76 | HTTP shuffle service (`/shuffle/{job}/{stage}/{partition}`) has **no authentication**; any client on the network can read shuffle data. | High |

### 4.2 Algorithms

- **Two‑phase token validation** (`disk_store.rs:90`): Phase 1 validates/advances token under write lock; Phase 2 writes to a temp file without holding the lock, then re‑acquires read lock to confirm the token is still current before `rename`.
- **LRU‑like spill** (`memory_store.rs:62`): Oldest partitions (by insertion order in `spill_order`) are evicted to disk when `bytes_used + incoming_size > max_bytes`.
- **Hash partitioning** (`partitioner.rs:34`): Supports `Int32`, `Int64`, `Utf8`, `Utf8View`, `LargeUtf8`. Uses `twox_hash::XxHash64` with seed 0.
- **Arrow IPC streaming** (`object_store.rs:65`): Writes `ShufflePartition` as a single Arrow IPC stream file with optional LZ4/Zstd compression.
- **Orphan cleanup** (`orphan.rs:6`): Scans `base_dir` for `.ipc`/`.tmp` files whose job directory is not in the active set; deletes them.

### 4.3 Architecture

- **`ShuffleStore` trait** (`store.rs:26`) uses RPITIT (`impl Future<Output = ShuffleResult<()>> + Send`) so implementations can choose their own sync/async strategy.
- **`LeaseMap`** (`store.rs:62`) is a `Arc<RwLock<BTreeMap<PartitionKey, u64>>>` shared by disk and memory stores.
- **Three implementations**:
  - `LocalDiskShuffleStore` → Parquet files.
  - `InMemoryShuffleStore` → `BTreeMap` under `RwLock` with optional spill to disk.
  - `ObjectStoreShuffleStore` → Arrow IPC in object store (S3, etc.).
- **Arrow Flight service** (`flight.rs:61`) implements only `do_get`; all other methods return `unimplemented`.

### 4.4 Gaps

- **Object store atomicity**: No temp‑file + rename pattern; relies on backend consistency.
- **Temp file cleanup**: No startup GC for `.tmp.*` files in `LocalDiskShuffleStore`.
- **Flight service completeness**: Only `do_get` is implemented; no `do_put`, `get_schema`, etc.
- **Shuffle HTTP auth**: No TLS or token verification.
- **Compression on read**: `flight.rs` does not advertise compression options; `IpcWriteOptions` uses default.

### 4.5 Refactoring

1. **Fix `InMemoryShuffleStore` race**: Use a single `tokio::sync::Mutex` or `std::sync::Mutex` around the entire spill sequence (read partition → write to disk → remove from memory).
2. **Cleanup temp files on startup**: Add a `LocalDiskShuffleStore::cleanup_temp_files()` constructor helper.
3. **Stream `FlightData`** in `flight.rs`: Use `futures::stream::iter(batches).chunks(N).map(...)` or an async reader to avoid loading the whole partition.
4. **Support multi‑column hash keys** in `HashPartitioner`.
5. **Replace `String` in `PartitionId`** with `Arc<str>` to reduce cloning across the executor.

### 4.6 Features / Maturity

| Feature | Status | Notes |
|---------|--------|-------|
| Local disk Parquet store | **Mature** | Atomic writes, lease tokens, compression. |
| In‑memory store | **Mature** | Spill‑to‑disk, LRU eviction, lease tokens. |
| Object store (Arrow IPC) | **Mature** | LZ4/Zstd compression, lease tokens, batch delete. |
| Hash partitioner | **Mature** | 5 key types supported; nulls go to bucket 0. |
| Arrow Flight server | **Partial** | `do_get` works; rest unimplemented. |
| Shuffle HTTP service | **Basic** | No auth, no streaming. |
| Orphan cleanup | **Mature** | Scans and deletes stale `.ipc`/`.tmp` files. |

### 4.7 Performance

- **`InMemoryShuffleStore` contention**: A single `RwLock` guards the entire partition map. High concurrency on shuffle writes will serialize.
- **`ObjectStoreShuffleStore`** writes the whole partition as one IPC stream. For large partitions (>100 MB) this causes a single large allocation and upload.
- **`HashPartitioner`** uses `arrow::compute::take` per column per bucket. For 10k‑row batches and 100 partitions this is 100× `take` calls.
- **`flight.rs`** loads all batches before encoding. A 1 GB partition will reside fully in memory on the Flight server.

---

## 5. Resolution Plan

### Critical (fix before next release)

| ID | Crate | Task | File(s) | Effort |
|----|-------|------|---------|--------|
| C1 | scheduler | **Fix BUG‑1**: Ensure epoch hint is written **after** manifest is sealed in `commit_epoch`. | `checkpoint.rs:297` | 1 day |
| C2 | shuffle | **Fix memory store race**: Hold a single mutex across the entire spill sequence (read → write → remove) to prevent data loss. | `memory_store.rs:62` | 2 days |
| C3 | scheduler | **Fix auth vulnerability**: Replace opt‑in `validate_grpc_auth` with a tonic interceptor that enforces auth on every mutating RPC. | `auth.rs`, `grpc.rs` | 3 days |
| C4 | scheduler | **Fix etcd metadata blocking**: Make `MetadataStore` async or offload `persist` to a blocking thread pool. | `etcd_metadata.rs`, `store.rs` | 2 days |

### High (significant reliability or performance impact)

| ID | Crate | Task | File(s) | Effort |
|----|-------|------|---------|--------|
| H1 | executor | **Use `spawn_blocking` for checkpoint I/O** instead of `block_in_place`. | `runner.rs:995` | 1 day |
| H2 | executor | **Pool all coordinator RPCs** through `CoordinatorGrpcPool`; remove one‑shot channel creation. | `transport.rs:301` | 2 days |
| H3 | shuffle | **Add temp‑file cleanup on startup** for `LocalDiskShuffleStore`. | `disk_store.rs` | 0.5 day |
| H4 | shuffle | **Add auth to shuffle HTTP service** (Bearer token or mTLS). | `shuffle_svc.rs` | 2 days |
| H5 | scheduler | **Parallelize barrier dispatch** to multiple executors. | `barrier_dispatch.rs:160` | 1 day |
| H6 | scheduler | **Replace `batch_sql` polling loop** with event‑driven notification. | `batch_sql.rs:57` | 1 day |

### Medium (code quality, maintainability, partial features)

| ID | Crate | Task | File(s) | Effort |
|----|-------|------|---------|--------|
| M1 | scheduler | **Implement GAP‑CP‑03**: Validate fencing token before storage commit. | `checkpoint.rs:254` | 1 day |
| M2 | scheduler | **Implement GAP‑CP‑05**: Fail‑closed on metadata persist errors. | `coordinator.rs:967` | 1 day |
| M3 | scheduler | **Implement GAP‑CP‑06**: Rebuild checkpoint coordinators after recovery. | `coordinator.rs:837` | 2 days |
| M4 | scheduler | **Split `coordinator.rs`** into sub‑modules for registry, jobs, and checkpoints. | `coordinator.rs` | 2 days |
| M5 | executor | **Refactor `run_assignment_with`** into smaller functions. | `runner.rs:762` | 1 day |
| M6 | executor | **Remove `expect` in `execute_loop_fragment`**; return `Err`. | `fragment/streaming.rs:182` | 0.5 day |
| M7 | executor | **Extract common input‑registration helper** to eliminate duplication between batch and streaming fragments. | `fragment/common.rs`, `fragment/batch.rs`, `fragment/streaming.rs` | 1 day |
| M8 | proto | **Strict wire parsing**: Reject unknown `InputPartitionDescriptorKind` / `OutputContractDescriptorKind` instead of defaulting. | `wire.rs:786, 930` | 0.5 day |
| M9 | proto | **Split `wire.rs`** into per‑domain modules. | `wire.rs` | 1 day |
| M10 | shuffle | **Stream `FlightData`** instead of loading the whole partition. | `flight.rs:124` | 2 days |

### Low (polish, optimizations, minor gaps)

| ID | Crate | Task | File(s) | Effort |
|----|-------|------|---------|--------|
| L1 | scheduler | **Replace `SeqCst`** with `Relaxed` in `SingleNodeLeader`. | `cluster_control.rs:48` | 0.25 day |
| L2 | scheduler | **Use `Arc<str>`** for `job_id` / `stage_id` in `PartitionId` and wire types. | `ids.rs`, `store.rs` | 1 day |
| L3 | scheduler | **Add more metrics**: histograms for task runtime, checkpoint latency, barrier dispatch time. | `metrics.rs` | 1 day |
| L4 | executor | **Cache SQL engine table registrations** per executor instead of per task. | `fragment/batch.rs:100` | 1 day |
| L5 | executor | **Replace string prefix matching** in `terminal_streaming_task` with typed enum. | `runner.rs:872` | 0.5 day |
| L6 | shuffle | **Support multi‑column hash keys** in `HashPartitioner`. | `partitioner.rs` | 2 days |
| L7 | shuffle | **Document null‑key skew** or salt nulls to avoid hot partition 0. | `partitioner.rs:63` | 0.25 day |
| L8 | scheduler | **Add per‑task retry wiring** (GAP‑3). | `job.rs:545` | 1 day |
| L9 | scheduler | **Implement management gRPC** (GAP‑RT‑04). | `grpc.rs:275`, `management.rs` | 3 days |

---

## 6. Cross‑Crate Observations

1. **Blocking inside async is the dominant anti‑pattern**:
   - `krishiv-scheduler`: `EtcdMetadataStore::persist` and `LocalFsCheckpointStorage` (via GAP‑CK‑03).
   - `krishiv-executor`: `initiate_checkpoint_and_deliver_ack` uses `block_in_place`.
   Fix: Audit every `std::fs` and `block_on` call inside async boundaries; replace with `spawn_blocking` or async traits.

2. **Auth is an afterthought**:
   - Scheduler gRPC handlers opt‑in individually.
   - Shuffle HTTP service has no auth at all.
   Fix: Adopt tonic interceptors and axum middleware for uniform enforcement.

3. **Stringly typed dispatch still exists**:
   - `ExecutionModel::from_fragment` and `terminal_streaming_task` rely on string prefixes.
   - `krishiv_plan::TypedTaskFragment` is already defined; migrate all call sites to use the typed enum exclusively.

4. **Metadata store write amplification**:
   - `JsonFileMetadataStore` and `EtcdMetadataStore` rewrite the full snapshot on every event.
   - For R2 (distributed mode) this will become a bottleneck. Consider append‑only event logs or per‑job sharded metadata.

5. **Shuffle temp files and object store races**:
   - Disk store leaves `.tmp.*` files after crashes.
   - Object store lacks atomic rename.
   - These will cause data leaks and consistency issues in long‑running clusters.

---

*End of review.*
