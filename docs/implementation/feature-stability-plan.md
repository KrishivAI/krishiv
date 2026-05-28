# Feature Stability Plan

Generated: 2026-05-28  
Scope: every feature in the full feature table, ordered by release target.  
Goal: concrete implementation steps to advance each feature from its current maturity to ✅ Stable.

Features already marked ✅ Stable are listed for completeness with a "maintain" note only.  
🟡 Beta features list the specific gaps to close.  
🔴 Stub features list the full implementation work required.

---

## Already ✅ Stable — Maintain Only

| Feature | Action |
|---------|--------|
| Embedded batch SQL | None. Regression-guard with `cargo test -p krishiv --lib`. |
| Single-node batch SQL | None. Regression-guard with `cargo test -p krishiv-sql --lib`. |
| Tumbling window — bounded | None. Regression-guard with `cargo test -p krishiv-exec --lib`. |
| Watermarking (single-source) | None. Keep `LocalWindowExecutionSpec.source_watermark_lags` wired through all code paths. |
| State — in-memory | None. `InMemoryStateBackend` is not durable by design; document as development/test-only. |
| Shuffle — in-memory | None. Regression-guard with `cargo test -p krishiv-shuffle --lib`. |
| Parquet source/sink | None. Regression-guard with `cargo test -p krishiv-connectors --lib`. |
| UDF — scalar | None. Python UDF panics are correctly surfaced as `UdfError::Panic`. |

---

## R2 — Kubernetes Operator: Pod Creation (BUG-2)

**Feature:** Distributed batch SQL (Flight) + Kubernetes operator  
**Current:** 🟡 Beta — operator reconciler never creates executor Pods; `BUG-2`.

### Gaps
1. `ensure_executor_pods()` in `crates/krishiv-operator/src/reconciler.rs` is a no-op.
2. No `Pod` template rendered from `KrishivJob` spec.
3. Distributed sink path has no idempotent write guard — duplicate output possible if sink is not idempotent.

### Implementation Steps

**Step 1 — Pod creation in operator reconciler**  
File: `crates/krishiv-operator/src/reconciler.rs`  
- Read desired executor replica count from `KrishivJob.spec.executors`.
- List existing executor Pods (label selector: `krishiv.io/job={job_id}`).
- For each missing slot, call `k8s.pods().create(build_executor_pod_spec(...))`.
- `build_executor_pod_spec` renders: image from `KrishivJob.spec.image`, env vars `KRISHIV_COORDINATOR_ADDR`, `KRISHIV_EXECUTOR_SLOTS`, labels `krishiv.io/job` + `krishiv.io/role=executor`.
- Add finalizer `krishiv.io/executor-cleanup` to created pods.

**Step 2 — Pod deletion on job completion/cancellation**  
File: `crates/krishiv-operator/src/reconciler.rs`  
- In the `JobState::Succeeded | JobState::Failed | JobState::Cancelled` branch, delete all pods matching the label selector.
- Remove finalizer only after deletion confirms.

**Step 3 — Idempotent sink guard for distributed batch**  
File: `crates/krishiv-connectors/src/sink.rs` + `crates/krishiv-exec/src/...`  
- Add `idempotent: bool` to `ConnectorCapabilities`.
- In batch output writer: if `!capabilities.idempotent`, wrap write with a dedup token derived from `(job_id, stage_id, attempt_id, partition_id)`.
- Parquet sink: name output file with the dedup token; skip write if file already exists (S3 `if-none-match` or local stat check).

**Step 4 — Tests**  
- `crates/krishiv-operator/src/tests.rs`: add `operator_creates_executor_pods` using `kube-runtime` mock client.
- `crates/krishiv-connectors/src/tests.rs`: add `idempotent_parquet_write_skips_duplicate` test.

**Validation:**
```bash
cargo test -p krishiv-operator --lib -- operator_creates_executor_pods
cargo test -p krishiv-connectors --lib -- idempotent_parquet_write_skips_duplicate
cargo clippy -p krishiv-operator -p krishiv-connectors -- -D warnings
```

---

## R4 — Shuffle: Disk TOCTOU Race (BUG-4)

**Feature:** Shuffle — disk  
**Current:** 🟡 Beta — BUG-4: token TOCTOU race in `disk_store.rs`.

### Gaps
1. `register_lease` checks token then inserts in two separate operations — another thread can interleave.
2. `object_store` orphan cleanup lacks a consistent metadata snapshot before scan + delete.

### Implementation Steps

**Step 1 — Fix disk store TOCTOU (BUG-4)**  
File: `crates/krishiv-shuffle/src/disk_store.rs`  
- Replace the check-then-insert pattern with a single `entry().or_insert_with()` call on the `lease_tokens` `DashMap`.
- Or use `compare_exchange` on an `AtomicU64` per partition slot.
- The winning inserter proceeds; all others return `ShuffleError::StaleLease`.

**Step 2 — Object-store orphan cleanup with consistent metadata view**  
File: `crates/krishiv-shuffle/src/object_store.rs`  
- Before the orphan scan, take a point-in-time snapshot of all active job IDs from the `ShuffleMetadata` store (hold the read lock for the snapshot only, not across the S3 list call).
- Build the `active_set` from the snapshot.
- Scan object-store keys; delete only keys whose job ID is absent from `active_set` AND whose object creation time is older than a configurable grace period (default 30 min).

**Step 3 — AQE hot-key split application**  
File: `crates/krishiv-scheduler/src/adaptive.rs`  
Feature: AQE (partition coalescing) — 🟡 Beta — split decisions logged but never applied.  
- `process_hot_key_reports` currently appends to `AdaptiveDecisionLog` only.
- Add a `pending_repartitions: HashMap<JobId, RepartitionPlan>` to `Coordinator`.
- When a hot key exceeds threshold, emit a `ThrottleCommand` to the affected source task AND record a `RepartitionPlan` specifying the new partition count.
- On the next stage boundary, `job_spec_from_physical_plan` reads the pending repartition and emits extra `Exchange` nodes with the new parallelism.

**Step 4 — Tests**
- `crates/krishiv-shuffle/src/tests.rs`: `disk_store_concurrent_lease_registration_no_toctou` — spawn 16 tasks racing on the same partition slot, assert exactly one wins.
- `crates/krishiv-shuffle/src/tests.rs`: `object_store_orphan_cleanup_skips_active_jobs` — register a job, run orphan cleanup, assert the active job's data survives.
- `crates/krishiv-scheduler/src/tests.rs`: `aqe_hot_key_split_applied_to_next_stage`.

**Validation:**
```bash
cargo test -p krishiv-shuffle --lib
cargo test -p krishiv-scheduler --lib -- aqe_hot_key_split
cargo clippy -p krishiv-shuffle -p krishiv-scheduler -- -D warnings
```

---

## R5 — Stateful Streaming: Unbounded Windows, State, Watermarks

### R5.1 Feature: Tumbling Window — Unbounded (BUG-1)

**Current:** 🟡 Beta — BUG-1 checkpoint epoch sequencing; checkpoint protocol incomplete.

#### Gaps
1. `handle_checkpoint_ack` does not enforce strict epoch ordering — an ack for epoch N+2 can arrive before N+1 is committed.
2. Restore path: `restore_from_checkpoint` in `crates/krishiv-executor/src/runner.rs` reloads state but does not re-initialize operator watermarks and timers from the snapshot.
3. Fencing token is not validated on restore.

#### Implementation Steps

**Step 1 — Strict epoch sequencing in coordinator**  
File: `crates/krishiv-scheduler/src/checkpoint.rs`  
- Add `last_committed_epoch: u64` to `CheckpointCoordinator`.
- In `handle_checkpoint_ack`: reject (return `StaleFencingToken` or new `OutOfOrderEpoch` error) any ACK for `epoch != last_committed_epoch + 1`.
- Only advance `last_committed_epoch` after a full quorum ACK for epoch N.

**Step 2 — Restore watermarks and timers from snapshot**  
File: `crates/krishiv-executor/src/runner.rs`  
- After restoring state via `state_backend.restore_snapshot()`, call `operator.restore_from_state(&mut backend)` which must reload the event-time watermark and all open window accumulators.
- `TumblingWindowOperator::restore_from_state` already has a skeleton; wire it to also restore `current_watermark_ms` from the snapshot key `"watermark"`.

**Step 3 — Fencing token validation on restore**  
File: `crates/krishiv-scheduler/src/lib.rs` (`restore_job_from_checkpoint_with_fencing`)  
- Assert `stored_fencing_token == expected_token` before applying any operator state.
- Return `SchedulerError::FencingTokenMismatch` if tokens differ.

**Step 4 — Tests**
- `crates/krishiv-exec/src/tests.rs`: `tumbling_window_unbounded_checkpoint_epoch_ordering` — verify that an out-of-order ACK is rejected.
- `crates/krishiv-exec/src/tests.rs`: `tumbling_window_restore_preserves_watermark` — checkpoint at epoch 2, restore, assert watermark is correct and no window is re-emitted.

**Validation:**
```bash
cargo test -p krishiv-exec --lib -- tumbling_window
cargo test -p krishiv-scheduler --lib -- checkpoint_epoch
```

---

### R5.2 Feature: Sliding Window → Stable

**Current:** 🟡 Beta — unbounded streaming path and checkpoint integration pending.

#### Gaps
1. `StateBackedSlidingWindowOperator` exists but is not wired into the unbounded streaming path in `crates/krishiv-exec/src/continuous.rs`.
2. No deterministic replay test.
3. No checkpoint round-trip test.

#### Implementation Steps

**Step 1 — Wire into unbounded execution path**  
File: `crates/krishiv-exec/src/continuous.rs` / `operator_runtime.rs`  
- In `build_operator_for_spec`, when `window_kind == WindowKind::Sliding`, return a `StateBackedSlidingWindowOperator` (not the stateless `SlidingWindowOperator`).
- Confirm `state_backend` is always passed through to stateful operators in the continuous execution loop.

**Step 2 — Checkpoint integration**  
File: `crates/krishiv-exec/src/window/sliding.rs`  
- `SlidingWindowOperator::persist_to_state` must snapshot all open buckets keyed by `(key, window_start_ms)`.
- `restore_from_state` must rebuild `open_windows: HashMap<(K, i64), Accumulator>` from snapshot.
- Validate that restore → emit produces identical output to uninterrupted run.

**Step 3 — Tests**
- `sliding_window_unbounded_checkpoint_restore_roundtrip`: push 100 events, checkpoint, restore, push 50 more, assert final output equals uninterrupted run.
- `sliding_window_late_event_within_lag_not_dropped`: late event arrives within `watermark_lag_ms`, assert it is included in the correct bucket.

**Validation:**
```bash
cargo test -p krishiv-exec --lib -- sliding_window
```

---

### R5.2 Feature: Session Window → Stable

**Current:** 🟡 Beta — merge algorithm not formally proven; depends on watermark ordering.

#### Gaps
1. Session merge across partitions with concurrent watermarks is untested under reordering.
2. No formal invariant test for the merge rule: sessions with overlapping gaps must be merged exactly once.

#### Implementation Steps

**Step 1 — Formal merge invariant test**  
File: `crates/krishiv-exec/src/tests.rs`  
- Property test using `proptest`: generate random event sequences with random gaps; assert every output session covers all events that could belong to it and no two output sessions overlap.

**Step 2 — Watermark-ordered merge**  
File: `crates/krishiv-exec/src/window/session.rs`  
- Enforce that `SessionWindowOperator::close_sessions` only emits sessions whose end time is `<= current_watermark - gap_ms`.
- Add `pending_merges: BTreeMap<u64, SessionAccumulator>` sorted by session start for O(log n) merge lookups.

**Step 3 — Checkpoint integration** (same pattern as sliding window above)

**Validation:**
```bash
cargo test -p krishiv-exec --lib -- session_window
```

---

### R5.2 Feature: Watermarking (Multi-Source) → Stable

**Current:** 🟡 Beta — stalled source blocks all windows; no minimum-advance timeout.

#### Gaps
1. `MultiSourceWatermarkTracker` uses `min()` across all sources — one idle source freezes progress.
2. No idle-source timeout configured.

#### Implementation Steps

**Step 1 — Idle source timeout**  
File: `crates/krishiv-exec/src/continuous.rs` (or wherever `MultiSourceWatermarkTracker` lives)  
- Add `source_last_event_ms: HashMap<SourceId, u64>` tracking the last received event timestamp per source.
- Add `idle_timeout_ms: u64` to `LocalWindowExecutionSpec` (default: 30_000).
- In the watermark advance loop: if `now_ms - source_last_event_ms[src] > idle_timeout_ms`, treat the source's watermark as `u64::MAX` for the purpose of the `min()` calculation.
- Emit a `tracing::warn!` when a source is treated as idle.

**Step 2 — Python/Rust API exposure**  
File: `crates/krishiv/src/...` + `crates/krishiv-python/src/relation.rs`  
- Add `.with_idle_source_timeout(ms)` builder to `Relation` and `PyRelation`.

**Step 3 — Tests**
- `multi_source_watermark_idle_source_does_not_block_progress`: two sources, source B goes silent after event 10; assert windows still close based on source A's watermark after the timeout.

**Validation:**
```bash
cargo test -p krishiv-exec --lib -- multi_source_watermark
```

---

### R5.2 Feature: State — redb Durable → Stable

**Current:** 🟡 Beta — single-writer `Mutex` bottleneck under high concurrency.

#### Gaps
1. `RedbStateBackend` uses `Arc<Mutex<redb::Database>>` — all reads and writes serialized through one lock.
2. redb's own read-write transaction model supports concurrent readers; we should use it.

#### Implementation Steps

**Step 1 — Upgrade to redb read transactions**  
File: `crates/krishiv-state/src/redb_backend.rs`  
- Replace `Arc<Mutex<Database>>` with `Arc<Database>` (redb is `Send + Sync`).
- `get()` opens a `ReadTransaction` (no lock needed).
- `put()` / `delete()` open a `WriteTransaction` (serialized by redb internally).
- Remove the outer `Mutex`.

**Step 2 — Batch write API**  
- Add `put_batch(entries: Vec<(K, V)>)` that opens one `WriteTransaction` for all entries.
- `TumblingWindowOperator::persist_to_state` already uses this; confirm sliding/session do too.

**Step 3 — Concurrency test**
- `redb_backend_concurrent_reads_do_not_block`: spawn 32 reader tasks + 1 writer task; assert all readers complete without waiting on the writer.

**Validation:**
```bash
cargo test -p krishiv-state --lib
```

---

### R5.2 Feature: State TTL → Stable

**Current:** 🟡 Beta — not watermark-aware; may evict state needed for late events.

#### Gaps
1. `TtlStateBackend::evict_expired` uses wall-clock time, not event time.
2. Late events within `watermark_lag_ms` can arrive after their TTL has expired.

#### Implementation Steps

**Step 1 — Watermark-aware TTL**  
File: `crates/krishiv-state/src/ttl.rs`  
- Add `watermark_ms: Arc<AtomicU64>` to `TtlStateBackend`.
- Change `evict_expired` to: evict only entries where `entry_event_time + ttl_ms < watermark_ms.load() - watermark_lag_ms`.
- Wire `watermark_ms` update from the streaming executor's watermark advance loop.

**Step 2 — API plumbing**  
File: `crates/krishiv-exec/src/continuous.rs`  
- Pass the current watermark `AtomicU64` into `TtlStateBackend` when building the state backend for a streaming task.

**Step 3 — Test**
- `ttl_does_not_evict_state_within_watermark_lag`: set TTL to 1s, watermark lag to 5s; emit an event 3s old; advance watermark to `now - 4s`; assert the state entry still exists.

**Validation:**
```bash
cargo test -p krishiv-state --lib -- ttl
```

---

## R6 — Checkpointing / Savepoints → Stable

**Current:** 🟡 Beta — BUG-1 epoch sequencing, restore path stubbed, fencing unvalidated.

### Gaps
1. BUG-1: epoch sequencing (see R5.1 tumbling window step 1 above — shared fix).
2. `restore_job_from_checkpoint` does not validate fencing token before applying operator state.
3. Restore path in executor: watermarks and timers are not reloaded.
4. No chaos tests.

### Implementation Steps

**Step 1 — Fencing on restore** (see R5.1 Step 3 above — same fix)

**Step 2 — Full restore path**  
File: `crates/krishiv-executor/src/runner.rs`  
- After loading operator state snapshots, call `operator.restore_event_time_timers(&snapshot)` to rebuild all pending timers.
- Confirm `source_offset` is restored to `checkpoint.source_offsets[source_id]` before re-starting the source poll loop.

**Step 3 — Savepoint creation and restore**  
File: `crates/krishiv-checkpoint/src/lib.rs`  
- `create_savepoint(job_id, epoch)`: copy the latest epoch metadata to a `savepoint/{name}/` prefix in the checkpoint store; mark it as a savepoint (immutable).
- `restore_from_savepoint(savepoint_name)`: read the savepoint metadata, validate fencing token, apply to all operators.

**Step 4 — Chaos tests**  
File: `tests/fault-injection/` (new)  
- `coordinator_kill_mid_checkpoint`: start a streaming job, inject a coordinator kill at epoch N mid-ACK, verify the new coordinator picks up from epoch N-1 (not N), assert no duplicate output.
- `executor_kill_mid_checkpoint`: kill executor mid-snapshot, verify tasks are reassigned and state is restored from the last complete epoch.
- `sink_kill_mid_commit`: kill the sink writer after the checkpoint ACK but before the transaction commit; verify two-phase commit prevents duplicate output.

**Validation:**
```bash
cargo test -p krishiv-checkpoint --lib
cargo test -p krishiv-scheduler --lib -- checkpoint
cargo test --test fault_injection_checkpoint  # new
```

---

## R7 — Resource Governance & Backpressure → Stable

### R7.1 Feature: Resource Governance / Quotas → Stable

**Current:** 🟡 Beta — in-memory tracking complete; no durable persistence across restarts.

#### Gaps
1. `QuotaQueueManager` state is in-memory only; restarts lose all per-namespace usage counters.
2. No recovery test.

#### Implementation Steps

**Step 1 — Durable quota persistence**  
File: `crates/krishiv-scheduler/src/admission.rs`  
- Wire `QuotaQueueManager` to use `MetadataStore` (JSON file or etcd) for persisting `namespace_usage: HashMap<NamespaceId, ResourceUsage>`.
- On `admit()`: read from store, check quota, persist updated usage atomically.
- On job completion/cancellation: decrement usage and persist.

**Step 2 — Recovery on coordinator restart**  
File: `crates/krishiv-scheduler/src/lib.rs` (`recover_from_store`)  
- After recovering job records, rebuild quota usage by summing `job.resource_request` for all running jobs.

**Step 3 — Tests**
- `quota_usage_survives_coordinator_restart`: submit a job, serialize state, create a new coordinator from stored state, assert quota counters are correct.

**Validation:**
```bash
cargo test -p krishiv-scheduler --lib -- quota
```

---

### R7.2 Feature: Backpressure / Flow Control → Stable

**Current:** 🟡 Beta — explicit credit protocol deferred; source throttling is skeleton.

#### Gaps
1. `RateLimiter` in connectors is not wired to actual source poll calls.
2. No end-to-end test for throttle propagation.
3. Explicit credit messages are defined in proto but not sent.

#### Implementation Steps

**Step 1 — Wire rate limiter to source poll**  
File: `crates/krishiv-connectors/src/source.rs`  
- In the `Source::poll_next` default wrapper, call `self.rate_limiter.acquire(batch.num_rows())` before returning each batch.
- `RateLimiter::acquire` sleeps (via `tokio::time::sleep`) until tokens are available.

**Step 2 — Coordinator → executor throttle command**  
File: `crates/krishiv-scheduler/src/grpc.rs` + `crates/krishiv-executor/src/runner.rs`  
- Add `ThrottleCommand` to heartbeat response (already defined in proto).
- Executor task runner: on receiving `ThrottleCommand { source_id, tokens_per_second }`, update the source's `RateLimiter`.

**Step 3 — Credit protocol (explicit)**  
File: `crates/krishiv-proto/proto/.../coordinator_executor.proto`  
- Add `CreditGrant { task_id, credits: u64 }` to `TaskAssignment`.
- Executor: hold sending until credits > 0; decrement on send; request more via heartbeat `credits_consumed`.
- This is the full credit-based protocol deferred from R7.2. Implement as a separate Tokio task per streaming pipeline.

**Step 4 — Tests**
- `backpressure_throttle_reduces_source_rate`: source produces at 10k/s; throttle command sets 1k/s; measure throughput drops to ≤1.2k/s within 2 epochs.

**Validation:**
```bash
cargo test -p krishiv-connectors --lib -- rate_limit
cargo test -p krishiv-scheduler --lib -- throttle
```

---

## R8 — Lakehouse, Python, Flight SQL → Stable

### R8.1 Feature: UDF — Aggregate → Stable

**Current:** 🟡 Beta — merge operation untested in distributed aggregation context.

#### Gaps
1. `AggregateUdf::merge` is defined but never called in the distributed multi-partition aggregation path.
2. No distributed UDAF test.

#### Implementation Steps

**Step 1 — Wire merge in distributed aggregation**  
File: `crates/krishiv-exec/src/aggregate.rs` (or `crates/krishiv-sql/src/udf.rs`)  
- In the partial → final aggregation combine step, call `udaf.merge(partial_states)` to combine partial accumulators from multiple executors.
- Ensure `DataFusion`'s `Accumulator::merge_batch` is correctly delegated to `AggregateUdf::merge`.

**Step 2 — Distributed UDAF test**  
File: `crates/krishiv-exec/src/tests.rs`  
- `distributed_udaf_merge_produces_correct_result`: register a custom sum-of-squares UDAF; run it over 4 partitions; assert the merged result equals a single-partition computation.

**Validation:**
```bash
cargo test -p krishiv-exec --lib -- distributed_udaf
```

---

### R8.1 Feature: UDF — Table-Valued → Stable

**Current:** 🟡 Beta — SQL `CREATE FUNCTION` syntax not wired; manual registration only.

#### Gaps
1. No `CREATE FUNCTION ... RETURNS TABLE` parser path in `krishiv-sql`.
2. UDTFs can only be registered via Rust API (`session.register_udtf(...)`).

#### Implementation Steps

**Step 1 — Parse `CREATE FUNCTION ... RETURNS TABLE`**  
File: `crates/krishiv-sql/src/udf.rs`  
- Intercept `Statement::CreateFunction` from `sqlparser`.
- If `return_type` is a `TABLE(col type, ...)` construct, extract the schema and function body.
- Register the UDTF via `sync_table_udfs`.

**Step 2 — SQL integration test**
- `CREATE FUNCTION explode(arr ARRAY) RETURNS TABLE (val INT) ...` test that uses the UDTF in a `SELECT * FROM explode(ARRAY[1,2,3])`.

**Validation:**
```bash
cargo test -p krishiv-sql --lib -- udtf_sql_create_function
```

---

### R8.1 Feature: Python Bindings → Stable

**Current:** 🟡 Beta — residual `todo!()` in stream binding methods; no PyPI wheel CI.

#### Gaps
1. `PyRelation` stream binding methods contain `todo!()` for some emit modes.
2. No maturin CI pipeline; no `.pyi` stubs.

#### Implementation Steps

**Step 1 — Remove all `todo!()` from stream bindings**  
File: `crates/krishiv-python/src/relation.rs`  
- Audit every method with `rg 'todo!' crates/krishiv-python/`.
- Replace each `todo!()` with a real implementation or a clean `Err(PyNotImplementedError::new_err("..."))` with a documented issue number.

**Step 2 — Maturin build pipeline**  
File: `.github/workflows/python-wheels.yml` (new)  
```yaml
- uses: PyO3/maturin-action@v1
  with:
    target: x86_64-unknown-linux-gnu
    manylinux: manylinux2014
    args: --release --strip -m crates/krishiv-python/Cargo.toml
```
- Publish to TestPyPI on every PR merge; publish to PyPI on tags.

**Step 3 — `.pyi` stubs**  
File: `crates/krishiv-python/krishiv.pyi` (new)  
- Use `pyo3-stub-gen` or handwrite stubs for `Session`, `DataFrame`, `Relation`, `PyQueryResult`, `PyBatch`.

**Step 4 — Tests**
- `cargo test -p krishiv-python --lib` — zero `todo!()` panics.
- Add a Python smoke test in `python/tests/test_stream.py` exercising `tumbling_window` + `collect`.

**Validation:**
```bash
cargo test -p krishiv-python --lib
rg 'todo!' crates/krishiv-python/src/  # must be empty
```

---

### R8.1 Feature: Flight SQL Server → Stable

**Current:** 🟡 Beta — auth token validation is a wiring placeholder; prepared statements not implemented.

#### Gaps
1. `authenticate_request` in `crates/krishiv-flight-sql/src/lib.rs` is a passthrough.
2. `CreatePreparedStatement` / `GetFlightInfoPreparedStatement` return `Unimplemented`.

#### Implementation Steps

**Step 1 — Real auth token validation**  
File: `crates/krishiv-flight-sql/src/lib.rs`  
- Extract the `Authorization: Bearer <token>` header.
- Validate via HMAC-SHA256 against `KRISHIV_FLIGHT_SQL_SECRET` env var (symmetric JWT for now).
- Return `Status::unauthenticated` if invalid.
- Add `KRISHIV_FLIGHT_SQL_SECRET` to the documented configuration options.

**Step 2 — Prepared statement skeleton**  
File: `crates/krishiv-flight-sql/src/lib.rs`  
- `create_prepared_statement`: parse SQL, store in a `HashMap<PreparedStatementHandle, ParsedQuery>` with a UUID handle.
- `get_flight_info_prepared_statement`: look up by handle, return schema info.
- `do_put_prepared_statement_query`: bind parameters, execute, return results.
- Prepared statement handles expire after 10 minutes (configurable).

**Step 3 — Tests**
- `flight_sql_auth_rejects_invalid_token`.
- `flight_sql_prepared_statement_round_trip`.

**Validation:**
```bash
cargo test -p krishiv-flight-sql --lib
```

---

### R8.2 Feature: Iceberg → Stable

**Current:** 🟡 Beta — no multi-writer chaos tests; partition evolution unimplemented; time-travel via snapshot_id only.

#### Gaps
1. No test for concurrent writers conflicting on the same snapshot counter.
2. Partition evolution (add/drop partition fields) not implemented.
3. Time-travel SQL syntax (`VERSION AS OF`, `TIMESTAMP AS OF`) not parsed.

#### Implementation Steps

**Step 1 — Multi-writer chaos test**  
File: `crates/krishiv-lakehouse/src/tests.rs`  
- Spawn 10 concurrent tasks each appending a batch; assert final snapshot count == 10 and all data is visible.
- Inject a simulated conflict on `check_and_append`; assert the losing writer retries successfully.

**Step 2 — Partition evolution**  
File: `crates/krishiv-lakehouse/src/iceberg.rs`  
- Add `add_partition_field(field: PartitionField)` and `drop_partition_field(field_id: i32)` to the `IcebergTable` trait.
- Implement by appending a new `PartitionSpec` to the table metadata and incrementing `spec_id`.
- Existing data retains its original partition spec; new writes use the current spec.

**Step 3 — Time-travel SQL**  
File: `crates/krishiv-sql/src/as_of.rs`  
- Parse `SELECT ... FROM table VERSION AS OF <snapshot_id>` and `TIMESTAMP AS OF '<ts>'`.
- Map to `IcebergScanOptions { snapshot_id: Some(...) }`.
- Add `snapshot_for_timestamp(ts_ms)` to `IcebergTable`: scan snapshot history and return the latest snapshot committed before `ts_ms`.

**Step 4 — Tests**
- `iceberg_partition_evolution_new_writes_use_new_spec`.
- `iceberg_time_travel_returns_correct_snapshot`.

**Validation:**
```bash
cargo test -p krishiv-lakehouse --lib
cargo test -p krishiv-sql --lib -- iceberg_time_travel
```

---

## R9 — Row-Level Security / Policy + Metrics → Stable

### Feature: Row-Level Security / Policy → Stable

**Current:** 🟡 Beta — masking applied only at Flight SQL boundary; embedded queries bypass it; row-level WHERE rewrite not implemented.

#### Gaps
1. `PolicyEnforcingSqlEngine` is only used in `do_get_statement` (Flight SQL path).
2. `Session::sql()` in embedded/single-node mode does not invoke the policy engine.
3. Row-level filter predicates are not pushed into the scan WHERE clause.

#### Implementation Steps

**Step 1 — Policy at all execution boundaries**  
File: `crates/krishiv-api/src/session.rs`  
- Before calling `SqlEngine::execute`, call `policy_engine.check_and_rewrite(sql, &auth_context)?`.
- This applies for embedded, single-node, and distributed modes.

**Step 2 — Row-level WHERE rewrite**  
File: `crates/krishiv-sql-policy/src/lib.rs`  
- `apply_row_predicates(plan, policy)` must inject `Filter` nodes above `Scan` nodes for each matching policy predicate.
- Use DataFusion's logical plan rewriter to insert the filter.
- Add test: a policy that restricts `WHERE region = 'us-east'` is correctly injected for tables matching the policy.

**Step 3 — Tests**
- `embedded_query_respects_row_level_policy`: create a session with a row-level policy; run a query via `session.sql()`; assert filtered rows are not in the result.
- `flight_sql_and_embedded_produce_same_policy_result`: same query, same policy, both paths return identical rows.

**Validation:**
```bash
cargo test -p krishiv-sql-policy --lib
cargo test -p krishiv-api --lib -- policy
```

---

### Feature: Metrics / Observability → Stable

**Current:** 🟡 Beta — OTel exporter stdout-only; trace context not propagated across gRPC.

#### Gaps
1. `KrishivMetrics::init()` hardcodes stdout exporter.
2. No W3C TraceContext propagation in tonic interceptors.

#### Implementation Steps

**Step 1 — Configurable OTel exporter**  
File: `crates/krishiv-metrics/src/lib.rs`  
- If `OTEL_EXPORTER_OTLP_ENDPOINT` is set, use `opentelemetry-otlp` with gRPC transport.
- Otherwise fall back to stdout.
- Support `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` for HTTP transport.

**Step 2 — Trace context propagation across gRPC**  
File: `crates/krishiv-proto/src/services.rs` (tonic interceptors)  
- Client interceptor: inject `traceparent` and `tracestate` headers from the current span context into every outgoing RPC.
- Server interceptor: extract `traceparent`/`tracestate` and set as the parent span context before entering the handler.
- Use `opentelemetry-http` propagator.

**Step 3 — Tests**
- `otel_trace_context_propagated_across_grpc`: start a server with a trace interceptor; make a client call with a parent span; assert the server handler runs within a child span of the same trace.

**Validation:**
```bash
cargo test -p krishiv-metrics --lib
cargo test -p krishiv-proto --lib -- trace_context
```

---

## R10 — Data Quality / Dead-Letter → Stable

**Current:** 🟡 Beta — quality rules defined but not evaluated in streaming loop.

### Gaps
1. `QualityRule` evaluation is only in the batch path.
2. No dead-letter sink wired into the streaming operator loop.

### Implementation Steps

**Step 1 — Wire quality rules into streaming loop**  
File: `crates/krishiv-exec/src/continuous.rs`  
- After each operator processes a batch, pass it through `QualityRuleEvaluator::evaluate(batch)`.
- Route failing rows to `DeadLetterSink::write(failed_rows, rule_id, reason)`.
- Continue processing passing rows.

**Step 2 — Dead-letter sink trait**  
File: `crates/krishiv-connectors/src/sink.rs`  
- Add `DeadLetterSink` trait with `write_rejected(batch: RecordBatch, reason: &str) -> ConnectorResult<()>`.
- Implement `ParquetDeadLetterSink` writing to `<output_path>/dead_letter/`.
- Implement `LogDeadLetterSink` for development use.

**Step 3 — Tests**
- `streaming_quality_rule_routes_failing_rows_to_dead_letter`: stream 100 events; 10 fail a null-check rule; assert output has 90 rows and dead-letter has 10.

**Validation:**
```bash
cargo test -p krishiv-exec --lib -- quality_rule_streaming
cargo test -p krishiv-connectors --lib -- dead_letter
```

---

## R12 — Kafka Source/Sink → Stable (from 🔴 Stub)

**Current:** 🔴 Stub — all operations return `Unsupported`; `kafka-runtime` feature does not exist.

### Implementation Steps

**Step 1 — Feature gate**  
File: `crates/krishiv-connectors/Cargo.toml`  
```toml
[features]
kafka = ["dep:rdkafka"]

[dependencies]
rdkafka = { version = "0.36", features = ["tokio"], optional = true }
```

**Step 2 — Kafka source**  
File: `crates/krishiv-connectors/src/kafka.rs`  
- `KafkaSource` implements `Source`:
  - `open(config)`: create `rdkafka::consumer::StreamConsumer` with provided bootstrap servers, group ID, and topic list.
  - `poll_next()`: call `consumer.stream().next().await`; deserialize bytes to `RecordBatch` using schema.
  - `current_offset()`: return `HashMap<partition_id, offset>` from `consumer.position()`.
  - `seek(offsets)`: call `consumer.seek()` for rewind/replay.
- Capabilities: `unbounded: true, rewindable: true, transactional: false`.

**Step 3 — Kafka sink**  
File: `crates/krishiv-connectors/src/kafka.rs`  
- `KafkaSink` implements `Sink`:
  - `open(config)`: create `rdkafka::producer::FutureProducer`.
  - `write_batch(batch)`: serialize each row to bytes (JSON or Avro) and produce.
  - `commit()`: call `producer.flush(timeout)`.
- Capabilities: `idempotent: false` (at-least-once); `transactional: true` when `enable.idempotence=true` is configured.

**Step 4 — Watermark-aware streaming path**  
File: `crates/krishiv-connectors/src/kafka.rs`  
- `KafkaSource::poll_next` must extract the event-time field specified in `WatermarkSpec` and update the source-level watermark tracker.

**Step 5 — Tests (in-memory harness)**  
File: `crates/krishiv-connectors/src/tests.rs`  
- Use `InMemoryCdcEventSource` as a Kafka harness substitute.
- `kafka_source_produces_correct_batches`: push 1000 records; assert batch count and total rows.
- `kafka_sink_writes_all_batches`: write 1000 records; assert all arrive at the mock consumer.
- `kafka_source_seek_replays_from_offset`: seek to offset 500; assert only records 500+ are returned.

**Step 6 — Connector certification**  
- Run the existing `connector_certification_test_kit` against `KafkaSource` and `KafkaSink`.
- All capability-declared tests must pass.

**Validation:**
```bash
cargo test -p krishiv-connectors --lib --features kafka
cargo test -p krishiv-connectors --lib --features kafka -- kafka_source
```

---

## R14 — Kafka CDC (Debezium) → Stable (from 🔴 Stub)

**Current:** 🔴 Stub — `run_with_source()` event loop stubbed; `parse_debezium_envelope()` silently swallows malformed JSON.

### Implementation Steps

**Step 1 — Fix silent JSON swallowing**  
File: `crates/krishiv-connectors/src/cdc.rs` (or wherever `parse_debezium_envelope` lives)  
- Return `Err(ConnectorError::Cdc("malformed Debezium envelope: ..."))` on any JSON parse failure.
- Log the raw bytes (truncated to 512 bytes) at `tracing::warn!` level for debuggability.

**Step 2 — Real `run_with_source` event loop**  
File: `crates/krishiv-connectors/src/cdc.rs`  
```rust
pub async fn run_with_source<S: CdcEventSource>(
    source: &mut S,
    sink: &mut dyn LakehouseTable,
    config: &CdcConfig,
    shutdown: CancellationToken,
) -> ConnectorResult<()> {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            event = source.next_event() => {
                let event = event?;
                let batch = parse_debezium_envelope(&event.payload)?;
                sink.merge(batch, &config.merge_key).await?;
                source.commit_offset(event.offset).await?;
            }
        }
    }
    Ok(())
}
```

**Step 3 — Schema evolution in CDC**  
- On schema change events (`"op": "r"` with new schema field list), call `sink.evolve_schema(new_schema)`.
- Buffer events during schema migration.

**Step 4 — Tests**
- `cdc_malformed_envelope_returns_error`: pass a truncated JSON payload; assert `Err(ConnectorError::Cdc(...))`.
- `cdc_run_with_source_processes_all_events`: inject 100 insert/update/delete events; assert lakehouse table reflects correct final state.
- `cdc_schema_evolution_adds_new_column`: send events before and after a column addition; assert new column appears in output.

**Step 5 — Incremental computation / live tables** (R14 companion feature, 🔴 Stub)  
File: `crates/krishiv-exec/src/live_table.rs` (new)  
- `LiveTableOperator` wraps a `run_with_source` CDC loop and materializes results to an `IcebergTable`.
- SQL: `CREATE LIVE TABLE foo AS SELECT ... FROM kafka_topic` parsed in `crates/krishiv-sql/src/live_table.rs`.
- Wire `LiveTableOperator` into the streaming execution plan at `NodeOp::LiveTable`.

**Validation:**
```bash
cargo test -p krishiv-connectors --lib -- cdc
cargo test -p krishiv-exec --lib -- live_table
```

---

## R16 — CEP (Pattern Matching) → Stable (from 🔴 Stub)

**Current:** 🔴 Stub — operator not wired into streaming execution.

### Implementation Steps

**Step 1 — CEP operator core**  
File: `crates/krishiv-cep/src/lib.rs`  
- Implement NFA-based pattern matcher:
  - `PatternSpec`: ordered list of `PatternEvent { name, predicate }` with `within: Duration` timeout.
  - `NfaState`: current state per in-progress match attempt.
  - `PatternMatchOperator::process(event)`: advance all live NFA states; emit complete matches; discard timed-out states.
- Output schema: one row per complete match with columns for each named event in the pattern.

**Step 2 — Wire into streaming execution**  
File: `crates/krishiv-exec/src/continuous.rs`  
- In `build_operator_for_spec`, handle `NodeOp::CepPattern { spec }` by returning `PatternMatchOperator::new(spec)`.
- Wire into `operator_runtime.rs` dispatch table.

**Step 3 — SQL / API surface**  
File: `crates/krishiv-sql/src/cep.rs` (new)  
- Parse `MATCH_RECOGNIZE` SQL clause (SQL:2016 subset): `PATTERN`, `DEFINE`, `MEASURES`, `WITHIN`.
- Map to `PatternSpec` and insert `NodeOp::CepPattern` into the physical plan.

**Step 4 — Tests**
- `cep_pattern_detect_abc_sequence`: stream of events A, B, C interleaved with noise; assert only complete A→B→C sequences within 10s are emitted.
- `cep_pattern_timeout_discards_incomplete_match`: A arrives but no B within timeout; assert no output.
- `cep_sql_match_recognize_round_trip`: parse a `MATCH_RECOGNIZE` query; assert plan contains `NodeOp::CepPattern`.

**Validation:**
```bash
cargo test -p krishiv-cep --lib
cargo test -p krishiv-sql --lib -- match_recognize
cargo test -p krishiv-exec --lib -- cep_pattern
```

---

## R17 — AI Features → Stable

### Feature: AI — Embeddings → Stable

**Current:** 🟡 Beta — rate limiting for external APIs is basic.

#### Gaps
1. `EmbeddingClient` uses a simple token bucket; no retry on 429; no per-model rate limit tracking.

#### Implementation Steps
- Replace ad-hoc rate limiting with `tower::ServiceBuilder` + `tower::limit::RateLimit`.
- Add exponential backoff retry on HTTP 429 or 503: max 5 retries, base 1s, cap 32s.
- Track per-model usage (OpenAI has per-model TPM/RPM limits); expose a `RateLimitReport` via `GET /api/v1/ai/rate-limits`.
- Test: `embedding_rate_limit_retries_on_429`: mock HTTP server returns 429 twice then 200; assert the result is returned after two retries.

**Validation:**
```bash
cargo test -p krishiv-ai --lib -- embedding_rate_limit
```

---

### Feature: AI — RAG → Stable

**Current:** 🟡 Beta — vector store connectors deferred.

#### Gaps
1. `RAG_VECTOR_SINKS` global exists but Qdrant and pgvector connectors are not certified.
2. No end-to-end RAG pipeline test (chunk → embed → store → query).

#### Implementation Steps
- Certify `QdrantSink` and `PgvectorSink` through `connector_certification_test_kit`.
- Add `LanceDbSink` as a local-only alternative for embedded mode.
- Implement end-to-end test: chunk a document, embed via `EmbeddingUdf`, store in `QdrantSink`, query via `rag_query()`, assert relevant chunks are returned.

**Validation:**
```bash
cargo test -p krishiv-vector-sinks --lib -- certification
cargo test -p krishiv-ai --lib -- rag_end_to_end
```

---

### Feature: AI — LLM UDFs → Stable

**Current:** 🟡 Beta — token cost estimates approximate.

#### Gaps
1. Cost estimates use hardcoded per-token prices; actual usage is not tracked from API responses.

#### Implementation Steps
- Extract `usage.prompt_tokens` + `usage.completion_tokens` from each API response.
- Accumulate in `LlmCostTracker` per-session; expose via `session.llm_cost_report()`.
- Write to `KrishivMetrics` counters `llm_tokens_prompt_total` and `llm_tokens_completion_total`.
- Test: `llm_udf_token_cost_matches_api_response`: mock API returns `usage: {prompt_tokens: 10, completion_tokens: 20}`; assert `cost_report.total_tokens == 30`.

**Validation:**
```bash
cargo test -p krishiv-ai --lib -- llm_cost
```

---

## R18 — Delta Lake → Stable (from 🔴 Stub)

**Current:** 🔴 Stub — `write_delta()` returns placeholder; `MERGE INTO` incomplete.

### Implementation Steps

**Step 1 — Integrate `delta-rs`**  
File: `crates/krishiv-lakehouse/Cargo.toml`  
```toml
deltalake = { version = "0.17", features = ["s3"], optional = true }
```

**Step 2 — `DeltaTable` implementation**  
File: `crates/krishiv-lakehouse/src/delta_lake.rs`  
- `read(options)`: use `deltalake::open_table(path).await` + `DeltaOps::scan()` filtered by `snapshot_id` or `timestamp`.
- `write(batch, mode)`: use `DeltaOps::write(vec![batch]).with_save_mode(mode).await`.
- `merge(source_batch, key)`: implement `MERGE INTO` via `DeltaOps::merge()`.

**Step 3 — `MERGE INTO` SQL**  
File: `crates/krishiv-sql/src/merge.rs`  
- Wire `Statement::Merge` to `DeltaTable::merge()`.
- Support `WHEN MATCHED THEN UPDATE SET ...` and `WHEN NOT MATCHED THEN INSERT ...`.

**Step 4 — Tests**
- `delta_lake_write_read_roundtrip`.
- `delta_lake_merge_upserts_correctly`.
- `delta_lake_time_travel_returns_older_version`.

**Validation:**
```bash
cargo test -p krishiv-lakehouse --lib --features delta
cargo test -p krishiv-sql --lib -- merge_into_delta
```

---

## R18 — Hudi → Stable (from 🔴 Stub)

**Current:** 🔴 Stub — core operations unimplemented.

### Implementation Steps

**Step 1 — Hudi core operations**  
File: `crates/krishiv-lakehouse/src/hudi.rs`  
- `read(options)`: scan the Hudi `_hoodie_commit_time` timeline; apply incremental read if `start_commit` is specified.
- `append(batch)`: write as a new commit to the Hudi Copy-on-Write (COW) table; update the timeline.
- `upsert(batch, record_key, precombine_field)`: deduplicate via the `precombine_field`; write CoW update.
- Schema evolution: Hudi uses Avro schema; map Arrow schema to Avro on write.

**Step 2 — Tests**
- `hudi_append_and_read_roundtrip`.
- `hudi_upsert_deduplicates_by_record_key`.
- `hudi_incremental_read_returns_only_new_commits`.

**Validation:**
```bash
cargo test -p krishiv-lakehouse --lib --features hudi
```

---

## Post-R20 — Schema Registry → Stable (from 🔴 Stub)

**Current:** 🔴 Stub — not implemented.

### Implementation Steps (deferred, but planned here for completeness)

**Step 1 — Schema registry API**  
File: `crates/krishiv-schema-registry/src/lib.rs`  
- `SchemaRegistry` trait: `register(subject, schema) -> SchemaId`, `lookup(subject) -> Schema`, `get_by_id(id) -> Schema`, `evolve(subject, new_schema) -> Result<SchemaId, CompatibilityError>`.
- Compatibility modes: `BACKWARD`, `FORWARD`, `FULL`.

**Step 2 — Local backend**  
- `LocalSchemaRegistry`: in-memory + JSON file persistence. Suitable for development and single-node use.

**Step 3 — Confluent Schema Registry compat backend**  
- `ConfluentSchemaRegistryClient`: implements the trait by proxying to a Confluent-compatible HTTP API.
- Wire into `KafkaSource` and `KafkaSink` Avro serialization.

**Step 4 — Tests**
- `schema_registry_backward_compat_rejects_breaking_change`.
- `schema_registry_confluent_compat_round_trip` (uses a mock HTTP server).

---

## Multi-Source Stream Joins → Stable

**Current:** 🟡 Beta — stream-table join state grows unbounded; interval join timing unspecified.

### Implementation Steps

**Step 1 — TTL integration for stream-table join**  
File: `crates/krishiv-exec/src/join.rs`  
- `StreamTableJoinOperator` holds a `state: StateBackend` for the stream-side buffer.
- Wire `TtlStateBackend` as the backing store with TTL derived from `watermark_lag_ms * 2` (configurable).
- On watermark advance, call `state.evict_expired()`.

**Step 2 — Interval join timing specification**  
File: `crates/krishiv-exec/src/join.rs`  
- Document `IntervalJoinOperator`'s `lower_bound` and `upper_bound` semantics: for a left event at time `t`, match right events in `[t + lower_bound, t + upper_bound]`.
- Add a test asserting the exact timing semantics.

**Step 3 — Tests**
- `stream_table_join_state_bounded_by_ttl`: run a 10k event stream; assert state size stays below threshold after watermark advances.
- `interval_join_timing_semantics`: assert only events within the specified interval are matched.

**Validation:**
```bash
cargo test -p krishiv-exec --lib -- stream_table_join
cargo test -p krishiv-exec --lib -- interval_join
```

---

## Implementation Priority Order

The following sequencing respects release dependencies and critical-path ordering:

| Priority | Feature | Release | Blocker For |
|----------|---------|---------|-------------|
| P0 | Shuffle disk TOCTOU (BUG-4) | R4 | R5 state correctness |
| P0 | K8s operator pod creation (BUG-2) | R2 | Distributed batch SQL stable |
| P0 | Tumbling window epoch sequencing (BUG-1) | R5 | All checkpoint work |
| P1 | Restore path: watermarks + timers | R6 | Checkpointing stable |
| P1 | Kafka source/sink (🔴 Stub → real) | R12 | CDC, live tables |
| P1 | Multi-source watermark idle timeout | R5.2 | Sliding/session window stable |
| P1 | State TTL watermark awareness | R5.2 | All stateful streaming |
| P1 | Row-level security at all boundaries | R9 | Security certification |
| P2 | Sliding window checkpoint integration | R5.2 | — |
| P2 | Session window merge invariant | R5.2 | — |
| P2 | redb concurrent reads | R5.2 | High-throughput streaming |
| P2 | Resource governance durable persistence | R7.1 | Multi-tenant production |
| P2 | Backpressure credit protocol | R7.2 | — |
| P2 | UDF: UDAF distributed merge test | R8.1 | — |
| P2 | UDF: UDTF SQL CREATE FUNCTION | R8.1 | — |
| P2 | Python bindings: remove todo!() | R8.1 | PyPI publishing |
| P2 | Flight SQL: real auth + prepared stmts | R8.1 | — |
| P2 | Iceberg: partition evolution + time-travel | R8.2 | Delta/Hudi parity |
| P3 | OTel propagation across gRPC | R9 | — |
| P3 | Data quality in streaming loop | R10 | — |
| P3 | AQE hot-key split application | R4 | — |
| P3 | Kafka CDC: real run_with_source loop | R14 | Live tables |
| P3 | Incremental computation / live tables | R14 | — |
| P4 | CEP wiring | R16 | — |
| P4 | AI rate limiting + RAG connectors | R17 | — |
| P4 | Delta Lake delta-rs integration | R18 | — |
| P4 | Hudi core operations | R18 | — |
| P5 | Schema registry | Post-R20 | — |

---

## Validation Summary

Run these commands to verify each release tier:

```bash
# R2-R4 gates
cargo test -p krishiv-operator --lib
cargo test -p krishiv-shuffle --lib
cargo test -p krishiv-scheduler --lib -- aqe

# R5-R6 gates
cargo test -p krishiv-exec --lib -- tumbling_window sliding_window session_window
cargo test -p krishiv-state --lib
cargo test -p krishiv-checkpoint --lib

# R7 gates
cargo test -p krishiv-scheduler --lib -- quota throttle
cargo test -p krishiv-connectors --lib -- rate_limit

# R8 gates
cargo test -p krishiv-exec --lib -- distributed_udaf
cargo test -p krishiv-sql --lib -- udtf_sql_create_function
cargo test -p krishiv-python --lib
cargo test -p krishiv-flight-sql --lib
cargo test -p krishiv-lakehouse --lib

# R9 gates
cargo test -p krishiv-sql-policy --lib
cargo test -p krishiv-metrics --lib

# R12+ gates
cargo test -p krishiv-connectors --lib --features kafka
cargo test -p krishiv-connectors --lib -- cdc
cargo test -p krishiv-cep --lib

# Full workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
```
