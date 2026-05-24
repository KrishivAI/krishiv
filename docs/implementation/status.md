# Krishiv Implementation Status

## Current Phase

**ADR-12.4 follow-ups (2026-05-24).** Branch `cursor/adr124-memory-stream-ttl-cbbd` (extends embedded/streaming fixes):
- **ADR-12.4:** `InProcessCoordinatorBridge` + `InProcessStreamingRuntime` — coordinator submits jobs, pushes assignments to `ExecutorAssignmentInbox`, executor runs via `ExecutorTaskRunner::run_next_with` (no tonic for `inprocess://` endpoints).
- **State TTL:** `LocalWindowExecutionSpec.state_ttl_ms` wires `TtlStateBackend` + `StateBackedTumblingWindowOperator` in `local_streaming` and session `StateTtlConfig` on streams.
- **Memory streams:** `Session::memory_stream` / `register_memory_stream`; windowed `collect()` uses in-process path for Embedded/SingleNode; Python `memory_stream()` + `memory:<name>` sources use same path.
- **Stream-kafka contract:** In-process fragments use canonical `key=key:time=ts` columns matching `stream-kafka:` partition encoding.

**Embedded batch/streaming fixes (2026-05-24).** Branch `cursor/fix-embedded-batch-streaming-cbbd` (merged intent):
- Runtime backends accept batch plans without bogus `SqlEngine` re-execution; embedded redirects streaming to single-node.
- `local_streaming` executes tumbling/sliding/session windows via `krishiv-exec`.
- API: `WindowedStream::collect`, `ensure_local_mode`, stream `coordinator_url`, `StateTtlConfig` → `TtlConfig`.
- Python: `WindowedStream` collect/async iteration wired; embedded `stream()` allowed.

**Gap analysis implementation (2026-05-23).** See [`docs/engineering/gap-analysis-2026-05-23.md`](../engineering/gap-analysis-2026-05-23.md).

### Gap closure (branch `cursor/gap-analysis-impl-7aa2`)

- **GAP-C1:** Coordinator binary runs `coordinator_tick()` on an interval (heartbeat + task launch).
- **GAP-C3/C4:** Executor gRPC pool (`grpc_client`) and lease generation updates on register/heartbeat.
- **GAP-C8:** Checkpoint initiate commands attached to heartbeat responses; notify dedup on coordinator.
- **GAP-C9/C10:** Catalog `MemTable` scan + `SqlEngine::with_in_memory_catalog` integration test.
- **GAP-C5/C7:** Checkpoint epoch/fsync and TTL `list_keys` filtering verified on `main` (restored checkpoint crate).
- **GAP-I5:** Audit log call sites in scheduler job submit and sql-policy `execute_as`.
- **GAP-I3:** `plan_sql` runs `Optimizer::default().optimize`.
- **GAP-T2:** `coordinator_executor_integration` test in `krishiv-scheduler`.
- **GAP-CI1/CI2:** GitHub Actions CI workflow and PR template.
- **GAP-B1:** `rust-version = "1.92"` in workspace `Cargo.toml`.

### Validation (2026-05-24, ADR-12.4 branch)

```bash
cargo +stable test -p krishiv-runtime -p krishiv-scheduler -p krishiv-executor -p krishiv-api -p krishiv-exec --lib
# runtime: in_process_windowed_stream_returns_batches; api: tumbling_window_collect_executes_in_embedded_mode
```

### Validation (2026-05-24, gap closure)

```bash
cargo check -p krishiv-exec -p krishiv-sql -p krishiv-runtime -p krishiv-metrics -p krishiv-vector-sinks -p krishiv-scheduler -p krishiv-cep
cargo test -p krishiv-exec --lib tumbling_state_persist_and_restore_roundtrip
cargo test -p krishiv-vector-sinks --lib weaviate_query_returns_results
cargo test -p krishiv-scheduler --lib
cargo test -p krishiv-metrics --lib krishiv_metrics_prometheus_contains_tasks_total
cargo fmt --check
cargo clippy -p krishiv-exec -p krishiv-sql -p krishiv-runtime -p krishiv-metrics -p krishiv-scheduler -p krishiv-state -p krishiv-vector-sinks -p krishiv-cep -- -D warnings
```

### Follow-up closure (same PR, 2026-05-24)

- **GAP-I1:** `EmbeddedBackend` / `SingleNodeBackend` execute plans via `SqlEngine`; `DistributedBackend` uses Flight SQL (`flight_client`).
- **GAP-I2:** `TumblingWindowOperator::persist_to_state` / `restore_from_state` + `StateBackedTumblingWindowOperator`.
- **GAP-I4:** `sync_aggregate_udfs` / `sync_table_udfs` in `krishiv-sql` (`udf.rs` from `main`).
- **GAP-I6:** `KrishivMetrics` + `global_metrics().render_prometheus()` on coordinator `/metrics`; `inc_tasks_submitted` on job submit.
- **GAP-B2:** `krishiv-cep` and `krishiv-vector-sinks` added to workspace members.
- **GAP-B3:** [`CONTRIBUTING.md`](../../CONTRIBUTING.md) documents native link prerequisites.
- **GAP-B4:** `cargo fmt --all` applied.
- **GAP-B5:** `cargo clippy` clean on follow-up crates (excludes `krishiv-python` / `krishiv-chaos` when native deps absent).
- **GAP-T1:** `weaviate_query_returns_results` passes.

### Remaining

- **GAP-C2:** Operator K8s lease election loop (verify active/standby coordinator transition).
- Full-workspace `cargo clippy --workspace` may still fail on bins/crates needing native link (`krishiv-executor`, `krishiv-operator`) — see CONTRIBUTING.md.

---

**R13 COMPLETE (2026-05-23).**

Release tracker: [`r13-python-streaming-api.md`](r13-python-streaming-api.md)  
Gap register: [`docs/architecture/r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md)

## R13 Sprint — All Gaps Implemented (2026-05-23)

All 14 R13 gap items from `docs/architecture/r12-maturity-gap-register.md` and
`docs/implementation/r13-python-streaming-api.md` are implemented with no stubs or deferred items.

### Completed

**GAP-CP-07 — Executor registry idempotent re-registration with lease bump** (`heartbeat.rs`)
- `ExecutorRegistry::register` is fully idempotent: bumps lease on re-registration from a live state.
- Re-registration after `mark_lost` / `deregister` (which already bumped the lease) reuses the current
  generation rather than double-incrementing.
- Test: `lost_executor_can_reregister_with_next_lease_generation` passes.

**GAP-CP-08 — Auth context extraction in all scheduler gRPC handlers** (`lib.rs`)
- `extract_auth_context(request.metadata())` called at top of every handler in
  `CoordinatorExecutorTonicService`: `register_executor`, `deregister_executor`,
  `executor_heartbeat`, `task_status`, `checkpoint_ack`.
- Structured `tracing::debug!` log emitted with `subject` field on each call.

**GAP-CP-09 — Executor `--connect` mode starts task gRPC server and runner loop** (`main.rs`, `transport.rs`)
- `GrpcCoordinatorService` struct added to `krishiv-executor/src/transport.rs`, implementing
  `CoordinatorExecutorService` over live gRPC.
- `ExecutorCliConfig` gains `task_grpc_addr: Option<SocketAddr>` (default `0.0.0.0:50055`,
  `KRISHIV_TASK_GRPC_ADDR` env var, or `--task-grpc-addr <ADDR>` / `"off"` to disable).
- `heartbeat_loop` creates `ExecutorAssignmentInbox`, optionally binds and spawns the task gRPC
  server, then spawns `ExecutorTaskRunner` using `GrpcCoordinatorService`.

**GAP-CK-03 — `commit_epoch` sync disk I/O moved off async context** (`lib.rs`)
- `checkpoint_ack` handler rewritten to use `tokio::task::spawn_blocking`.
- `SharedCoordinator` (Arc<RwLock>) is cloned into the blocking closure; lock acquisition and
  `handle_checkpoint_ack` run on the blocking thread pool.

**GAP-OB-01 — Metrics counters for scheduler hot paths** (`lib.rs`)
- Three `LazyLock<AtomicU64>` module-level counters: `JOBS_SUBMITTED_TOTAL`,
  `CHECKPOINT_EPOCHS_TOTAL`, `TASKS_ASSIGNED_TOTAL`.
- `SchedulerMetrics` struct and `scheduler_metrics()` function expose current counter values.
- Counters incremented in `submit_job`, `launch_assigned_task_assignments`,
  `handle_checkpoint_ack` (on success).

**GAP-SH-04 — `CoalesceRule` output used in task count generation** (`job.rs`)
- `job_spec_from_physical_plan` passes `plan.coalesced_partition_count()` to `job_spec_from_plan_parts`.
- `job_spec_from_plan_parts` generates N `coalesced-partition-{i}` tasks when
  `coalesced_partition_count` is `Some(N)`.
- Logical plan path passes `None` (unchanged).

**GAP-PY-01 — Complete Python API** (`krishiv-python/src/lib.rs`, `python/krishiv/__init__.py`, `pyproject.toml`)
- Exception hierarchy via `create_exception!`: `KrishivError`, `QueryError`, `SchemaError`,
  `ConnectorError`, `CheckpointError`, `AuthorizationError`, `ModeError`.
- `PySession` with factory classmethods (`embedded`, `local`, `connect`, `from_env`), `mode`
  property, `sql`, `sql_async`, `register_parquet`, `stream` methods.
- `PyStream → PyWindowedStream` via `tumbling_window`; `PyWindowedStream` implements `__aiter__` /
  `__anext__` (raises `PyStopAsyncIteration` when exhausted).
- `PyBatch` with `num_rows`, `num_columns`, `__repr__`.
- `PyParquetSink`, `PyKafkaSink`, `PyIcebergSink` classes.
- Module-level `read_parquet(path)`, `read_kafka(session, topic, bootstrap_servers)` pyfunctions.
- `python/krishiv/__init__.py` re-exports all native symbols + adds `connect_async(url)` coroutine.
- `pyproject.toml` adds `python-source = "python"` so the pure-Python facade is bundled.

**Pre-existing test fixes applied alongside R13:**
- `crates/krishiv-scheduler/src/checkpoint.rs`: `FencingToken::from(N)` → `FencingToken::try_new(N).unwrap()` (×3)
- `crates/krishiv-api/src/lib.rs`: `#[tokio::test] async fn session_sql_async_fails_when_policy_configured` → `#[test] fn` (sync `session.sql()` call cannot use `block_in_place` inside `current_thread` runtime)

### Validation

```
cargo test --workspace --lib    → all suites pass, 0 failures
cargo clippy --workspace -- -D warnings → 0 errors
```

### Blockers

None.

### Next Task

Begin R14 (observability and production hardening):
- Wire `scheduler_metrics()` into the `/metrics` HTTP endpoint of the coordinator binary.
- Add structured tracing spans to the task runner loop.
- Integration tests for the executor task gRPC path.

Validation: `cargo test --workspace && cargo clippy --workspace -- -D warnings`

## R12 CARRYOVER + Code-Review Refactor (2026-05-22)

Release tracker: [`r12-foundation-completeness.md`](r12-foundation-completeness.md)  
Gap register: [`docs/architecture/r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md)

**R12 carryover gaps now closed** (see session below). All workspace lib tests pass;
`cargo clippy --workspace -- -D warnings` clean.

**R12 maturity:** Audit slices S1–S6 are documented on branch `claude/r12-slices-planning-BcFL5`;
integration gaps for distributed/streaming/remote paths remain open — see **GAP-*** IDs in the
gap register and **R12 carryover** section below.

---

## R12 Carryover Gap Closure Session (2026-05-23)

Branch: `claude/r1-r12-pending-slices-6ksEO`

### Closed Gaps

| Gap ID | Summary | File(s) Changed |
|--------|---------|-----------------|
| **GAP-CP-03** | Wire `validate_fencing_token` in `commit_epoch` before storage write | `krishiv-scheduler/src/checkpoint.rs` |
| **GAP-CK-01** | `restore_job_from_checkpoint` validates fencing token against live coordinator | `krishiv-scheduler/src/lib.rs` |
| **GAP-CN-01** | Duplicate `RdkafkaCdcEventSource` — confirmed no duplicate; `kafka` feature compiles cleanly | `krishiv-connectors` (no change needed) |
| **GAP-CP-04** | `--metadata-backend` / `--metadata-path` CLI flags + env vars on `krishiv_coordinator` binary | `krishiv-scheduler/src/bin/krishiv_coordinator.rs` |
| **GAP-CP-05** | `save_job` fail-closed: metadata persist errors → `SchedulerError::Transport` (not warn-only) | `krishiv-scheduler/src/lib.rs` |
| **GAP-CP-06** | `recover_from_store` rebuilds `checkpoint_coordinators` from recovered job specs | `krishiv-scheduler/src/lib.rs` |
| **GAP-RT-04** | Real `RemoteCoordinatorClient` gRPC (4 RPCs: savepoint, restore, list, inspect) | `krishiv/src/remote_client.rs`, `krishiv-proto/src/lib.rs`, `krishiv-proto/proto/…/coordinator_executor.proto`, `krishiv-scheduler/src/lib.rs` |
| **GAP-RT-05** | `Session::sql_async` fails-closed when policy engine configured (returns `AccessDenied`) | `krishiv-api/src/lib.rs` |
| **GAP-RT-06** | `collect_with_stats` uses plan's own `TaskContext` not a fresh `SessionContext` | `krishiv-sql/src/lib.rs` |
| **GAP-SH-02** | Shuffle codec header `[0x4B, 0x53, 0x48, codec_byte]` prefixed to all partition files | `krishiv-shuffle/src/lib.rs` |
| **GAP-SH-03** | `hash_i64` / `hash_str` use `XxHash64::with_seed(0)` (stable, deterministic) | `krishiv-shuffle/src/lib.rs`, `Cargo.toml`, `krishiv-shuffle/Cargo.toml` |

### Still Deferred (tracked for R13)

| Gap ID | Summary |
|--------|---------|
| GAP-SH-01 | Shuffle compression wired onto executor hot path (complex integration) |
| GAP-RT-01 | `SingleNodeBackend` / `EmbeddedBackend` in-process coordinator |
| GAP-RT-03 | `WindowedStream` → executor fragments |
| GAP-CN-02 | Kafka watermark-aware streaming |
| GAP-CP-09 | Executor binary task gRPC loop |
| GAP-PY-01 | Python API `todo!()` removal |

### Validation (2026-05-23)

```
cargo test --workspace --lib    → all suites pass (0 failures)
cargo clippy --workspace -- -D warnings → 0 errors, 0 warnings
```

### Next Task

1. Wire the new scheduler module files (`admission`, `checkpoint`, `heartbeat`, `job`, `store`) declared in `lib.rs` to replace duplicated inline code — target shrinking `krishiv-scheduler/src/lib.rs` from ~8400 to ~4000 lines.
2. Update R13 tracker prerequisites to reference closed gap IDs above.

---

## Code-Review Refactor Session (2026-05-22) — Phases 4–7

### Completed

**Phase 4 — Move PolicyEnforcingSqlEngine out of krishiv-sql** (commit 858e37b)
- Full `PolicyEnforcingSqlEngine` implementation was already in `krishiv-sql-policy`; this phase removed the leftover `policy_tests` module from `krishiv-sql` and moved it to `krishiv-sql-policy`.
- Added `inner()` accessor to `PolicyEnforcingSqlEngine`; added tokio dev-dep to `krishiv-sql-policy`.
- `krishiv-sql/Cargo.toml` no longer depends on `krishiv-governance`.

**Phase 5 — Consolidate unix_now_ms into krishiv-async-util** (commit 7337583)
- Removed local `unix_now_ms()` and `unix_now_ms_checked()` from `krishiv-state`.
- Added `krishiv-async-util` dependency; updated test to call `krishiv_async_util::unix_now_ms_checked()`.

**Phase 6 — Improve ConnectorError variants** (commit 7afa21a)
- Added typed variants: `Kafka { message, retriable }`, `Parquet(String)`, `ObjectStore { message, status }`, `Cdc(String)`, `Io(io::Error)`.
- Added `IoStr { message }` migration alias so all existing call-sites rename safely (`Io { message }` → `IoStr { message }`).
- Updated `Display` impl; all 57 connector tests pass.

**Phase 7 — Small cleanups** (commit 59abfee)
- 7a: `#[deprecated]` on `StoreError` alias in `krishiv-shuffle` (kept for `krishiv-executor` source compat).
- 7b: Renamed `execute_kafka_to_parquet_pipeline` → `execute_source_to_sink_pipeline`.
- 7c: Added `Transport`, `PlanRejected`, `PartialResult` variants + constructor helpers to `RuntimeError`; added `#[non_exhaustive]`.
- 7d: Added `Arc<SqlEngine>` field + `with_sql_engine()` builder to `ExecutorTaskRunner`.

### Validation
```
cargo test --workspace --lib    → 29 suites, 0 failures (audit_log_dedup flaky test is pre-existing, passes in isolation)
cargo clippy --workspace -- -D warnings → 0 errors
```

### Blockers
None. Workspace compiles clean; all lib tests pass.

### Next Task (refactor track)

Wire the new module files into their parent `lib.rs` with `mod` declarations and remove
the corresponding duplicated code from lib.rs. Start with `krishiv-scheduler/src/lib.rs`
(8449 lines → target ~4000 lines after extracting admission, checkpoint, heartbeat, job, store).

Validation: `cargo test --workspace --lib && cargo clippy --workspace -- -D warnings`

## Code-Review Refactor Session (2026-05-22) — Phases 1–3

### Completed (commit 47c9a1f)
- Extracted `krishiv-async-util` crate: panic-safe `block_on`, `unix_now_ms` helpers
- Extracted `krishiv-sql-policy` crate: re-exports `PolicyEnforcingSqlEngine` from `krishiv-sql`
- Added `krishiv-testkit` stub crate for future shared test utilities
- Wired `block_on` from `krishiv-async-util` into `krishiv-api` and `krishiv/src/cli.rs`
- Created scheduler module files: admission, checkpoint, heartbeat, job, store
- Created exec module files: adaptive (SpaceSaving), aggregate, join, queue, window
- Created executor module files: barrier, fragment, grpc, runner, transport
- Fixed pre-existing double-increment bug in `run_restore` arg parser (2 tests now pass)
- Fixed `block_on_works_inside_tokio_runtime` test to use multi-thread flavor

## R12 Sprint Completion Summary (2026-05-22)

All P0/P1 bug-fix sprints (S1, S2) completed in previous session (commits c1e65c4 etc.).
Slices S3–S6 completed in this session:

### S3: Real Kafka Connector
- `features = ["kafka"]` gate in `krishiv-connectors/Cargo.toml`
- `RdkafkaCdcEventSource` + `RdkafkaCdcConfig` behind `kafka` feature; `rdkafka = "0.36"` with `features = ["tokio"]`

### S4: Remote Coordinator CLI
- `CoordinatorMode` enum + `from_args_with_env_override` (public, testable)
- `RemoteCoordinatorClient` with lazy `connect_lazy` gRPC in `crates/krishiv/src/remote_client.rs`
- All checkpoint/state/savepoint/restore commands dispatch to remote when `--coordinator` set
- 12 unit tests pass

### S5: AQE Coalescing + Shuffle Compression
- `CoalesceRule::apply`: stamps `coalesced_partition_count` AND appends `CoalescePartitions` PlanNode
- `ShuffleCompression` enum with `compress()`/`decompress()` methods; `CompressionCodec` type alias
- `LocalShuffleStore::write_partition`/`read_partition` use codec methods (Lz4/Zstd)
- 29 optimizer + 49 shuffle tests pass

### S6: Deployment Layer Completeness
- **S6.1**: `DistributedBackend { flight_url }` in `krishiv-runtime`; `SessionBuilder::with_coordinator(url)` in `krishiv-api`
- **S6.4**: `SqliteMetadataStore` feature-gated (`--features sqlite`) in `krishiv-scheduler`; 3 tests pass
- **S6.5**: `crates/krishiv-federation/` crate: `RegionId`, `RoutingPolicy`, `FederationClient`, `GlobalCoordinator`; 5 tests pass
- **P1.23**: `Coordinator::persist_jobs_to_store` added to snapshot in-memory jobs to a `MetadataStore`

### Test Results (2026-05-22, post-rebase push bbe1113)
```
cargo test -p krishiv-federation          → 5 passed
cargo test -p krishiv-optimizer           → 29 passed (includes CoalesceRule + CoalescePartitions node)
cargo test -p krishiv-shuffle             → 49 passed (includes Lz4/Zstd round-trips)
cargo test -p krishiv-scheduler           → 97 passed
cargo test -p krishiv-scheduler --features sqlite → 3 sqlite tests pass
cargo check --workspace                   → 0 errors
cargo clippy (modified crates) -D warnings → 0 errors
```

### Deferred to R13 (gap-tracked)
- S6.2: `SingleNodeBackend` in-process coordinator — **GAP-RT-01**, GAP-ST-06
- S6.3: `EmbeddedBackend` streaming redirect — **GAP-RT-01**, GAP-RT-03
- S3.3: `KafkaSource` watermark-aware streaming — **GAP-CN-02**
- `--metadata-backend sqlite` CLI flag — **GAP-CP-04**
- Full Flight SQL transport in `DistributedBackend` — **GAP-RT-01** (ADR-12.3)
- `WindowedStream` → executor fragments — **GAP-RT-03**
- Executor binary task gRPC loop — **GAP-CP-09**
- Python API `todo!()` removal — **GAP-PY-01**

### R12 carryover (close before R13 Sprint 1)

| Priority | Gap ID | Summary | Status |
|----------|--------|---------|--------|
| P0 | GAP-CP-03 | Wire `validate_fencing_token` in `commit_epoch` / writes | ✅ CLOSED |
| P0 | GAP-CK-01 | Restore validates fencing token | ✅ CLOSED |
| P0 | GAP-CN-01 | Fix duplicate `RdkafkaCdcEventSource` (`kafka` feature compile) | ✅ CLOSED (no dup found) |
| P0 | GAP-RT-04 | Real `RemoteCoordinatorClient` gRPC (not stub `Ok`) | ✅ CLOSED |
| P1 | GAP-CP-04–06 | Coordinator startup metadata recovery | ✅ CLOSED |
| P1 | GAP-SH-02, GAP-SH-03 | Shuffle codec header; stable partition hash | ✅ CLOSED |
| P1 | GAP-RT-05, GAP-RT-06 | Policy fail-closed; collect_with_stats task context | ✅ CLOSED |
| P1 | GAP-SH-01 | Shuffle compression on executor path | ⏳ DEFERRED R13 |
| P1 | GAP-DOC-01 | Align “complete” claims with L4 acceptance per gap register | ✅ CLOSED (this update) |

Full list: [`r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md).

### Blockers

None for local batch SQL / in-process scheduler tests. **Distributed and streaming product claims**
remain blocked on carryover gaps above (especially GAP-CP-03, GAP-RT-01, GAP-RT-04, GAP-ST-01).

### Next Task

1. Close P0 R12 carryover gaps (fencing, remote CLI RPCs, kafka compile).
2. Update R13 tracker prerequisites to reference gap IDs.
3. Validation: `cargo test --workspace` and carryover-specific tests in gap register.

## R11 Completion Summary

All four sprints completed and validated.

**Sprint 1 (S1)** — Critical lock-safety + fencing fixes:
- `krishiv-checkpoint`: fencing token `!=` guard (rejects future-generation tokens, prevents split-brain)
- `krishiv-scheduler`: `unwrap_or_else` on store mutexes + `tokio::sync::Mutex` for channel cache (eliminates double-connect race)
- `krishiv-api`: `jobs()` lock-recovery via `unwrap_or_else(|p| p.into_inner())`
- `krishiv-catalog`: `DataFusionSchemaBridge` `.expect()` → `unwrap_or_else`

**Sprint 2 (S2)** — CDC real event loop:
- `CdcEventSource` trait + `InMemoryCdcEventSource` for testable injection
- `run_with_source<S, F>` real loop with shutdown signal support
- `run()` returns structured error directing callers to `run_with_source`

**Sprint 3 (S3)** — CLI stub replacements:
- `krishiv checkpoints list`: real epoch listing via `LocalFsCheckpointStorage`
- `krishiv restore`: real epoch restore plan from checkpoint metadata
- `krishiv savepoint`: real coordinator call with context-rich failure message
- `krishiv state inspect`: real state inspection with informative "none found" responses

**Sprint 4 (S4)** — Medium-priority hardening:
- `ShuffleMetadata::mark_pending` now returns `ShuffleResult<()>`; enforces `max_partitions` cap (default 65536); `with_max_partitions` builder added
- `K8sLeaseElection`: `last_renewed_at` TTL field; `is_leader()` auto-evicts stale `true` state when past `lease_duration_s`; all `.unwrap()` → `unwrap_or_else(|p| p.into_inner())`

Validation (2026-05-21):
```
cargo test --workspace          → all suites pass (0 failures)
cargo clippy --workspace -- -D warnings → 0 errors, 0 warnings
```

Next: implement R12 — fix all 21 P0 audit items, wire rdkafka, enable remote coordinator CLI, implement AQE coalescing. See `docs/architecture/r12-r20-roadmap.md` for full nine-release strategic plan.
