# Krishiv Production Readiness Review

**Date**: 2026-05-29
**Scope**: Full workspace (32 crates, ~109k lines Rust)
**Assessor**: Senior Rust Distributed Systems Engineer
**Context**: This engine runs long-lived compute jobs across many workers where
failures, retries, partial results, duplicate messages, and network partitions
are expected.

---

## Overall Scores

| Dimension | Score | Rationale |
|-----------|-------|-----------|
| **Reliability** | 6/10 | Good state machines, typed IDs, fencing tokens. Gaps: `unwrap()` in prod paths, `<` not `!=` on fence check, no task execution timeout per-SQL, streaming progress invisible. |
| **Performance** | 7/10 | Arrow/DataFusion foundation is solid. Bottlenecks: single `Mutex<Coordinator>`, `block_on` in async, serial spill lock in shuffle. |
| **Maintainability** | 7/10 | Clear crate boundaries, consistent builder pattern. Debt: dependency cycles, string-based plan routing, large lib.rs files. |
| **Idiomatic Rust** | 7/10 | `forbid(unsafe_code)`, typed identifiers, error enums. Gaps: `unwrap()` outside test, `block_on` in async, `std::sync::Mutex` in async context. |
| **Observability** | 4/10 → 6/10 | After this sprint's fixes: valid Prometheus, 10 labeled families, structured span fields, audit expansion. Still missing: per-task timeouts in metrics, continuous checkpoint progress gauge, wired OpenLineage. |

**Overall: 6/10 production readiness. Safe for single-node; needs hardening for multi-tenant distributed.**

---

## CRITICAL Issues (fix before production)

### C1. Fencing token validation uses `<` instead of `!=` — split-brain risk

- **Severity**: Critical
- **File**: `crates/krishiv-checkpoint/src/lib.rs:604-615`
- **Problem**: `validate_fencing_token` returns `Err` only when `metadata.fencing_token < current_token`. A coordinator with a *higher* (future-generation) token is **not** rejected. If a stale coordinator builds checkpoint metadata with `fencing_token=7` but the current active coordinator has `fencing_token=5` (due to a race), the comparison `7 < 5` is `false` and the stale metadata passes validation. This is a split-brain path: two coordinators could both believe they own the same epoch.
- **Why it matters**: In distributed consensus, stale coordinators must be fenced entirely — neither older NOR newer tokens from defunct coordinators should be trusted. The only valid token is the *current* leader's token.
- **Fix**:
```rust
pub fn validate_fencing_token(
    metadata: &CheckpointMetadata,
    current_token: u64,
) -> CheckpointResult<()> {
    if metadata.fencing_token != current_token {
        return Err(CheckpointError::StaleFencingToken {
            stored: metadata.fencing_token,
            current: current_token,
        });
    }
    Ok(())
}
```
- **Note**: This was documented in R11 as item "Change `validate_fencing_token` condition from `<` to `!=`" but the code still has `<`.

---

### C2. `block_on` in async context — potential deadlock

- **Severity**: Critical
- **Files**:
  - `crates/krishiv-runtime/src/lib.rs:427`
  - `crates/krishiv-runtime/src/in_process.rs:212`
  - `crates/krishiv-runtime/src/execution_runtime.rs:288-361`
  - `crates/krishiv-lakehouse/src/delta.rs:kafka_delta::append`
- **Problem**: `execute_batch_sql`, `execute_windowed`, `run_terminal_task`, and `DistributedBackend::execute` all call `block_on()` which creates a nested Tokio runtime. If called from within a Tokio task (e.g., during a CLI `heartbeat_loop` or gRPC handler), this will either panic (cannot start runtime from within runtime) or deadlock (blocking the current thread, preventing progress on other tasks sharing the same worker).
- **Why it matters**: The `InProcessStreamingRuntime` is used by tests and `InProcessCluster`, which are called from Tokio in `coordinator_daemon.rs`. A deadlock here freezes the entire coordinator process in single-node mode.
- **Fix**: Replace all `block_on` calls with proper async. The `run_terminal_task` function is already `RuntimeResult<Vec<RecordBatch>>` — it should return `async fn` and the caller should `await` it. The `DistributedBackend::execute` should be `async fn` with the trait constraint changed to `async_trait`.
```rust
// Instead of:
pub fn execute_batch_sql(&self, query: &str, tables: &[BatchSqlTable]) -> RuntimeResult<Vec<RecordBatch>> {
    let fragment = format!("sql: {query}");
    self.run_terminal_task(&fragment, JobKind::Batch, tables, Vec::new())
}
// Use:
pub async fn execute_batch_sql(&self, query: &str, tables: &[BatchSqlTable]) -> RuntimeResult<Vec<RecordBatch>> {
    let fragment = format!("sql: {query}");
    self.run_terminal_task_async(&fragment, JobKind::Batch, tables, Vec::new()).await
}
```

---

### C3. No per-task execution timeout for SQL queries

- **Severity**: Critical
- **File**: `crates/krishiv-executor/src/runner.rs:911-922`
- **Problem**: Task timeouts only apply when the `JobSpec` includes `with_task_timeout_secs()`. There is no default timeout. If a SQL query hangs (e.g., infinite loop in a UDF, stuck on external I/O, deadlocked DataFusion plan), the task runs forever. The coordinator only detects executor loss via heartbeat timeout, not hung tasks.
- **Why it matters**: In a distributed compute engine, a single hung task blocks the entire stage. The coordinator won't reassign it because the task reports `Running`. All downstream stages wait on shuffle partitions that never materialize.
- **Fix**: Add a default per-task execution timeout in `CoordinatorConfig` (e.g., 1 hour for batch, unbounded for streaming with periodic progress reports). The executor should enforce it via `tokio::time::timeout` wrapping the entire fragment execution.
```rust
// In CoordinatorConfig:
pub fn default_task_timeout_secs(&self) -> Option<u64> {
    Some(self.default_batch_task_timeout_secs) // e.g., 3600
}

// In runner.rs:911:
let effective_timeout = assignment.task_timeout_secs()
    .or(default_config.default_task_timeout_secs())
    .unwrap_or(3600);
```

---

## HIGH Severity Issues

### H1. `unwrap()` in production coordinator paths

- **Severity**: High
- **Files**:
  - `crates/krishiv-scheduler/src/coordinator.rs:305,310` — `expect("coordinator id generation")`
  - `crates/krishiv-executor/src/grpc_client.rs:31` — `unwrap_or_else(|_| LeaseGeneration::initial())`
  - `crates/krishiv-lakehouse/src/delta.rs` — `self.seq.lock().unwrap()`
  - `crates/krishiv-shuffle/src/disk_store.rs` — `unwrap()` in file operations
- **Problem**: These panics crash the process. In a distributed coordinator, crashing on a transient error (like a full disk or a counter overflow) takes down ALL jobs, not just the one that hit the error.
- **Why it matters**: A single `unwrap()` on a lock that another thread poisoned (due to an unrelated panic) cascades the failure to the entire scheduler. The coordinator is the control plane — it must never crash.
- **Fix**: The R11 audit already identified `.lock().unwrap()` patterns. Apply `unwrap_or_else(|p| p.into_inner())` for `std::sync::Mutex` and propagate errors for `tokio::sync::Mutex`.
```rust
// Instead of:
let mut s = store.lock().unwrap(); // coordinator.rs:966 — already fixed with unwrap_or_else
// Fix remaining:
let guard = self.seq.lock().unwrap_or_else(|e| e.into_inner()); // lakehouse/delta.rs
```

---

### H2. Single `tokio::sync::Mutex<Coordinator>` serializes all operations

- **Severity**: High
- **Files**:
  - `crates/krishiv-scheduler/src/coordinator_daemon.rs:236` — `.write().await` acquires exclusive lock
  - `crates/krishiv-scheduler/src/coordinator_daemon.rs:229-240` — tick loop holds write lock for duration of tick
  - `crates/krishiv-scheduler/src/grpc.rs` — every gRPC handler acquires `.write().await`
- **Problem**: The entire coordinator is behind a single `tokio::sync::RwLock<Coordinator>`. Every heartbeat, every task status update, every job submission, every checkpoint ack — all queue behind this lock. Under load, the heartbeat tick loop holds the write lock for the full `coordinator_tick()` duration (which includes checkpoint initiation, task launch, and heartbeat expiry processing). Meanwhile, executor heartbeats are blocked and start timing out, causing cascading executor loss.
- **Why it matters**: At 50 executors × 5-second heartbeats, that's 10 heartbeats/second. If `coordinator_tick()` takes 200ms, heartbeats queue 2 deep. If it takes 500ms+, executors start being marked Lost and tasks are falsely reassigned.
- **Fix**: Split the coordinator into independently-locked subsystems:
```rust
struct Coordinator {
    job_table: Arc<RwLock<JobTable>>,
    executor_registry: Arc<RwLock<ExecutorRegistry>>,
    checkpoint_registry: Arc<RwLock<CheckpointRegistry>>,
    shuffle_metadata: Arc<RwLock<ShuffleMetadata>>,
    event_log: Arc<dyn EventLog>,
}
```
Heartbeat processing only needs `executor_registry.write()` + `job_table.read()`. Task launch only needs `job_table.write()` + `executor_registry.read()`. They can run concurrently.

---

### H3. Non-terminal streaming tasks report `Running` with zero progress

- **Severity**: High
- **File**: `crates/krishiv-executor/src/runner.rs:895-903`
- **Problem**: When `model == ExecutionModel::Streaming && !terminal_streaming_task`, the executor sends `TaskState::Running` with no output metadata, no watermark, no row count, no state size. The coordinator receives `Running` with zero information and cannot:
  - Know if the task is making progress or stuck
  - Track the global low-watermark
  - Decide when to initiate a checkpoint
  - Detect a silent hang (e.g., source Kafka partition has no new data but task is alive)
- **Why it matters**: A streaming job can appear "running" for hours while producing zero output because the source is stuck or the operator is in an infinite loop. Operators cannot detect this without periodic progress reports.
- **Fix**: This sprint already added the `StreamingProgressSnapshot` and `StreamingProgressCallback` infrastructure. Now wire it: in the streaming operator main loop, call `self.progress_callback.on_progress(&snapshot)` every N seconds (default 30). The heartbeat loop then includes these snapshots in `ExecutorHeartbeat.streaming_progress`.

---

### H4. Shuffle lease token uses `<` comparison, allowing stale writes on equal token

- **Severity**: High
- **File**: `crates/krishiv-shuffle/src/memory_store.rs:169`
- **Problem**: The comment says "accept equal or newer tokens" but the code uses `<`. This means `lease_token < expected` rejects only strictly-lower tokens. Equal tokens are accepted. This is correct for the *writer* side (re-writes with the same token are allowed). However, for `register_partition_lease` (line 142-143), the same `<` check means a coordinator with the *same* token can re-register, which is correct monotonic replacement. The issue is that `write_partition` with the same token as `register_partition_lease` creates a race: if executor A registers token=5, executor B writes with token=5, and executor A's write arrives later with token=5, executor A overwrites executor B's data. This is arguably correct (both have the same lease) but surprising — the last writer wins, and there's no ordering guarantee.
- **Why it matters**: In a shuffle, partition data is a deterministic function of the input. If two executors both write the same partition with the same lease token, they should produce identical data. But if they produce different data (due to non-deterministic UDFs or different input partitions), the reader gets whichever arrived last — a non-deterministic result.
- **Fix**: Document this invariant explicitly: "Partitions written under the same lease token are assumed identical. If they differ, the last write wins." Alternatively, add content hashing and reject mismatches.

---

## MEDIUM Severity Issues

### M1. `std::sync::Mutex` used in async code paths

- **Severity**: Medium
- **Files**: `crates/krishiv-executor/src/grpc_client.rs:55,63,72` (the `CoordinatorGrpcPool` wraps `Arc<Mutex<...>>`)
- **Problem**: The `grpc_client.rs:88` uses `tokio::sync::Mutex` but the `SharedLeaseGeneration` handling uses `Arc<AtomicU64>` (correct). Earlier versions used `std::sync::Mutex` in the `client` field, which was already fixed to `tokio::sync::Mutex`. However, other crates still use `std::sync::Mutex`:
  - `krishiv-lakehouse/src/delta.rs` — `MemoryDeltaStore.entries: Mutex<Vec<Vec<u8>>>`
  - `krishiv-executor/src/barrier_transport.rs` — `SharedBarrierInjector.inner: Arc<Mutex<BarrierInjector>>`
- **Why it matters**: Holding `std::sync::Mutex` across an `.await` point causes a deadlock — the Mutex is not `Send`, so the compiler should catch this. But even brief holds before `.await` are risky if the code is later refactored.
- **Fix**: Replace all `std::sync::Mutex` with `tokio::sync::Mutex` in async code paths. For high-contention paths, use `parking_lot::Mutex` which is `Send` and doesn't poison.

---

### M2. String-based plan routing

- **Severity**: Medium
- **File**: `crates/krishiv-runtime/src/plan.rs:15-21`
- **Problem**: `is_streaming_plan()` uses string prefix matching (`starts_with("stream:")`, `contains("krishiv-stream")`). A user SQL query containing the literal text "krishiv-stream" would be misclassified as a streaming plan. The `PhysicalPlan` already has a `kind: ExecutionKind` field — why is string matching needed?
- **Why it matters**: Incorrect plan classification means a batch SQL query could be routed to the streaming executor, or vice versa. The streaming executor may never terminate a batch query.
- **Fix**: Rely exclusively on `ExecutionKind::Streaming` to classify plans. Remove the string prefix matching. If the plan name must encode additional info, use a structured field on `PhysicalPlan` rather than embedding protocol in strings.
```rust
pub fn is_streaming_plan(plan: &PhysicalPlan) -> bool {
    plan.kind() == ExecutionKind::Streaming
}
```

---

### M3. No checkpoint coordinator identity in metadata

- **Severity**: Medium
- **File**: `crates/krishiv-checkpoint/src/lib.rs:99-125` (CheckpointMetadata struct)
- **Problem**: `CheckpointMetadata` has `fencing_token` but no `coordinator_id`. If two coordinators race and both write metadata with the same fencing token (or one writes with a stale token that passes the `<` check), there's no way to determine who authored it.
- **Why it matters**: During incident response, operators need to trace "which coordinator committed this epoch" to understand failover events. Without an identity field, the audit trail is incomplete.
- **Fix**: Add `coordinator_id: String` to `CheckpointMetadata`. Populate from `CoordinatorId`.

---

### M4. Prometheus format was invalid (fixed in this sprint)

- **Severity**: Medium → Resolved
- **File**: `crates/krishiv-metrics/src/lib.rs` (old version)
- **Problem**: Triple HELP/TYPE per metric family broke Prometheus ingestion. **Fixed** — now emits valid format with single HELP/TYPE per family.
- **Status**: Verified with regression test `render_prometheus_single_help_type_per_family`.

---

### M5. OpenLineage trait defined but never wired

- **Severity**: Medium
- **File**: `crates/krishiv-governance/src/lib.rs` (OpenLineage section)
- **Problem**: `OpenLineageEmitter` has 4 implementations (NoOp, Logging, HTTP, AsyncHTTP) but no call site in the scheduler emits `RunEvent::START`/`COMPLETE`/`FAIL`.
- **Why it matters**: Data platform teams require OpenLineage for data discovery, compliance, and debugging. Without it, Krishiv jobs are invisible to data catalogs.
- **Fix**: Wire `RunEvent::START` on job submit, `RunEvent::COMPLETE` on job succeed, `RunEvent::FAIL` on job fail. Populate `LineageDataset::inputs`/`outputs` from connector metadata in `JobSpec`.

---

### M6. `executor_channels` race fixed but still fragile

- **Severity**: Medium → Mitigated
- **File**: `crates/krishiv-scheduler/src/coordinator.rs:286`
- **Problem**: `executor_channels` is now `Arc<tokio::sync::Mutex<HashMap<...>>>` which serializes connect attempts. This was the fix for the double-connect race (R11). However, holding the mutex across `connect().await` means a slow connection blocks ALL task launches to ALL executors for the duration of the TCP handshake (up to 10 seconds).
- **Fix**: Use a `DashMap` keyed by executor endpoint, with each value being an `Arc<tokio::sync::Mutex<Option<Channel>>>`. This allows parallel connects to different executors.
```rust
executor_channels: Arc<DashMap<String, Arc<tokio::sync::Mutex<Option<Channel>>>>>,
```

---

## LOW Severity Issues

### L1. Dependency cycles prevent independent compilation

- **Files**: `Cargo.toml` of `connectors`, `lakehouse`, `exec`, `api`, `flight-sql`, `runtime`
- **Problem**: Two documented cycles: `connectors↔lakehouse↔exec` and `api↔flight-sql↔runtime`. These don't prevent compilation (Rust allows crate cycles) but they do prevent independent crate testing and publishing.
- **Fix**: Extract shared traits into `krishiv-connector-traits` and `krishiv-types`.

### L2. Duplicate types across crates

- **Files**: `krishiv-runtime`, `krishiv-scheduler`, `krishiv-proto`
- **Problem**: `JobState`, `JobStatus`, `TaskSpec`, `TaskReport`, `KeyGroupRange` exist in multiple crates with slightly different fields.
- **Fix**: Canonicalize in `krishiv-proto` and re-export.

### L3. Large `lib.rs` files

- **Files**: `krishiv-exec/src/lib.rs` (1740 lines), `krishiv-checkpoint/src/lib.rs` (2650 lines), `krishiv-lakehouse/src/lib.rs` (large)
- **Fix**: Extract into meaningful modules.

### L4. `block_in_place` used correctly in some places

- **Files**: `krishiv-connectors/src/two_phase_parquet_s3.rs` — uses `tokio::task::block_in_place` for sync file I/O. This is correct per Tokio best practices for CPU-heavy or blocking I/O. Verify all sync I/O in async contexts uses this pattern.

---

## Missing Failure-Mode Tests

| Failure Scenario | Test Exists? | Priority |
|---|---|---|
| Coordinator crash during checkpoint ack collection | Yes (`chaos_1_coordinator_kill_mid_checkpoint_no_duplicate_commit`) | — |
| Executor crash mid-shuffle-write | Yes (lease token rejection tests) | — |
| Network partition (coordinator→executor) | Partial (heartbeat timeout tests) | High |
| Network partition (executor→executor shuffle read) | Missing | High |
| Slow/frozen executor (no heartbeat timeout but no progress) | Missing | High |
| Duplicate task assignment (two coordinators) | Missing (fencing token `<` bug) | Critical |
| Corrupt Arrow IPC data in shuffle | Missing | Medium |
| Checkpoint metadata JSON parse error | Yes (`list_valid_epochs` graceful skip) | — |
| Full disk during shuffle write | Partial (memory spill tests) | Medium |
| Full disk during checkpoint write | Missing | Medium |
| Kafka broker failover mid-consume | Missing (Kafka harness is deterministic) | Medium |
| Large partition skew (10x data on one partition) | Yes (hot key detection tests) | — |
| Concurrent savepoint + checkpoint | Missing | Medium |
| gRPC channel exhaustion (too many concurrent streams) | Missing | Low |
| Metadata store corruption (bit flip in JSON) | Missing | Medium |

---

## Architecture Improvement Recommendations

1. **Split `Coordinator` into sharded subsystems** — Replace `Arc<RwLock<Coordinator>>` with independent `JobManager`, `ExecutorManager`, `CheckpointManager`, each with their own lock. Use `tokio::sync::Notify` for cross-subsystem signaling.

2. **Add backpressure on all unbounded channels** — The `ExecutorAssignmentInbox` uses an unbounded `VecDeque`. Add a max queue depth and reject assignments when full.

3. **Add circuit breakers** — If an executor repeatedly fails tasks, stop assigning to it after N consecutive failures. Currently tasks are retried at stage level which can waste resources.

4. **Add a dead-letter queue** — Permanently failed tasks should emit a structured `TaskFailed` event with full context (SQL, input partition IDs, error, stack) to a dead-letter topic for offline debugging.

5. **Make `ExecutionBackend::execute` async** — The trait is synchronous but blocks on Flight SQL RPCs via `block_on`. Make it `async fn` to match the runtime.

6. **Adopt `thiserror` for error types** — Currently errors use manual `Display` impls. `thiserror` would reduce boilerplate and add `#[from]` for automatic error conversion.

7. **Add a deterministic simulation test framework** — For distributed scheduling, a `toyko`-style deterministic runtime (like FoundationDB's simulation) would catch timing bugs the current tests miss.

---

## Implementation Plan (Ordered by risk)

### Phase 1: Crash Safety (Week 1)
1. Fix `validate_fencing_token` `<` → `!=` (C1)
2. Remove all `unwrap()`/`expect()` in non-test production paths (H1)
3. Add default task execution timeout (C3)
4. Add dead-letter queue for permanently failed tasks

### Phase 2: Correctness Under Concurrency (Week 2)
1. Replace `block_on` with async in `krishiv-runtime` (C2)
2. Split `Coordinator` lock into sharded subsystems (H2)
3. Replace remaining `std::sync::Mutex` in async paths with `tokio::sync::Mutex` (M1)
4. Add `coordinator_id` to `CheckpointMetadata` (M3)

### Phase 3: Observability Wiring (Week 3)
1. Wire streaming progress snapshots into executor heartbeat → coordinator metrics (H3)
2. Wire OpenLineage `RunEvent` emissions from scheduler (M5)
3. Add checkpoint epoch gauge emission in scheduler
4. Wire new `AuditAction` variants (TaskAssigned, TaskFailed, etc.) in scheduler state transitions

### Phase 4: Architecture Cleanup (Week 4)
1. Break dependency cycles (L1)
2. Replace string-based plan routing with typed enum (M2)
3. Unify duplicate types across crates (L2)
4. Add missing failure-mode tests

### Phase 5: Performance (Week 5+)
1. Replace `Arc<Mutex<HashMap<...>>>` for executor channels with `DashMap` sharded locking (M6)
2. Profile and optimize `coordinator_tick()` path
3. Add benchmark suite for scheduler throughput
