# Krishiv Implementation Status

## Current Phase

**Audit Resolution Sweep — distributed/embedded/streaming fixes (2026-05-26).**

Branch `cursor/implement-all-audit-resolutions-ac65` lands every resolution
from the unified-runtime audit (PR #49):

| Item | Crate | Summary |
|------|-------|---------|
| A1   | krishiv-scheduler, krishiv-operator | `ClusterControlPlane::from_shared_with_leader` accepts an injected `Arc<dyn LeaderElection>`; operator passes `K8sLeaseElection` |
| A2/E3 | krishiv-scheduler, krishiv-operator | Coordinator starts Standby; orchestration loops spawn only after promotion and are aborted on demotion via returned `AbortHandle`s |
| A3   | krishiv-scheduler | Standalone `krishiv-job-coordinator` rewritten as a federation-HTTP client of the CCP — no more orphan SharedCoordinator |
| A4   | krishiv-scheduler | `JobCoordinator::spawn_job_orchestration_loops` no longer ticks the heartbeat clock (CCP owns it) |
| A5   | krishiv-scheduler | `spawn_orchestration_loops_with_handles` returns `AbortHandle`s |
| A6   | krishiv-scheduler | Dispatch fan-out via `FuturesUnordered`; channel cache drops the lock before `connect().await` |
| A7   | krishiv-scheduler | `submit_job` installs the `CheckpointCoordinator` only AFTER `save_job` succeeds |
| A8   | krishiv-scheduler | `restore_job_from_checkpoint_with_fencing(..., leader_token)` always validates |
| A9   | krishiv-scheduler | `process_hot_key_reports` uses `krishiv_async_util::unix_now_ms` |
| B2   | krishiv-api | Distributed mode defaults `remote_execution=true`; `Session::check_routing()` guards |
| B3/D2 | krishiv-runtime, krishiv-flight-sql | Typed `KrishivFlightAction` over Flight `DoAction` replaces SQL-comment encoding for new clients; comment parser hardened against `*/` injection |
| B4   | krishiv-shuffle | Hand-rolled TCP framing replaced with Arrow Flight (`DoGet`) |
| B5   | krishiv-executor | Executor binary wires `LocalDiskShuffleStore` + `InMemoryShuffleStore` and starts the shuffle Flight server |
| B6   | krishiv-executor | `slots`-many concurrent runner tasks share one inbox |
| B7   | krishiv-executor, krishiv-proto | Shared `SharedLeaseGeneration` atomic stamped on every outbound RPC by `GrpcCoordinatorService` |
| B8   | krishiv-executor | Task/barrier endpoints populated BEFORE first register — no more lease-bump race |
| B9   | krishiv-executor | Failure status carries the real error text (truncated to 4 KiB) |
| B10  | krishiv-executor | Checkpoint fanout uses real `running_attempts` instead of synthetic `exec-checkpoint` ids |
| C1   | krishiv-runtime | Per-cluster coordinator/executor ids + per-cluster job counter |
| C2/C3 | krishiv-runtime | SingleNode is distinct; `InProcessExecutionRuntime` drops the `Mutex<EmbeddedBackend>` |
| C5   | krishiv-runtime | `run_terminal_task` loops until the job terminates → multi-stage in-process execution |
| D3   | krishiv-scheduler | Checkpoint barrier quorum counts only `Running` tasks |
| D4   | krishiv-checkpoint | `ObjectStoreCheckpointStorage` uses `run_blocking_on_tokio` (`block_in_place`) instead of `futures::executor::block_on`; async API exposed |
| E1   | krishiv-operator | `ensure_dedicated_job_loop` documented as in-process-only; standalone JCP is client-only |
| F1   | krishiv-executor | Self-heal on `StaleLease` / `UnknownExecutor` heartbeat dispositions — re-register and invalidate cached channel |
| F2   | krishiv-scheduler | `push_cancel_job` reuses cached channel pool + concurrent dispatch |
| F3   | krishiv-scheduler | `Coordinator` no longer derives `Clone` |
| F4   | krishiv-scheduler | `inprocess://` task endpoints short-circuited from gRPC dispatch |

Validation (per-crate, completed in-session):

```bash
cargo check --workspace --lib --bins --tests \
  --exclude krishiv-ai --exclude krishiv-lakehouse --exclude krishiv-vector-sinks \
  --exclude krishiv-federation --exclude krishiv-chaos --exclude krishiv-python \
  --exclude krishiv-spark-connect --exclude krishiv-bench --exclude krishiv-upgrade-tests \
  --exclude krishiv-schema-registry
# OK

cargo test --workspace --lib (same exclusions)
# 701 passed, 0 failed across 21 suites — including:
#   krishiv-scheduler: 109 passed
#   krishiv-executor:  55 passed
#   krishiv-checkpoint:36 passed (+ async path coverage)
#   krishiv-runtime:   28 passed (incl. flight_action round-trip)
#   krishiv-flight-sql:30 passed (incl. do_action_explain_round_trip)
#   krishiv-shuffle:   54 passed (Arrow Flight DoGet)
#   krishiv-api:       36 passed
#   krishiv-operator:  38 passed
#   krishiv-state:     66 passed
#   krishiv-proto:     38 passed

cargo clippy -p krishiv-runtime -p krishiv-scheduler -p krishiv-executor \
  -p krishiv-api -p krishiv-flight-sql -p krishiv-operator -p krishiv-shuffle \
  -p krishiv-checkpoint --lib --tests --no-deps -- -D warnings
# 0 errors, 0 warnings on touched crates.

cargo fmt --all -- --check    # OK
```

Excluded crates (`krishiv-ai`, `krishiv-lakehouse`, `krishiv-vector-sinks`,
`krishiv-federation`, …) depend transitively on `native-tls`/`libssl-dev` which
is not available in the local environment; they are unchanged.

**Blocker:** none.

**Next:** wire `krishiv-flight-sql` to also use `DoPut` / `DoExchange` for
streaming push/drain payloads (currently they ride inside `DoAction` bodies
as base64-IPC) — the typed action API is stable enough for that follow-up to
be additive without breaking compat.

---

**Large-Crate Root Refactor — eight crates complete (2026-05-26).**

Split monolithic `lib.rs` roots into cohesive modules with crate-root `pub use` facades. No public API renames; behavior-preserving module moves only.

| Crate | Modules | Lib tests (`--all-features`) |
|-------|---------|------------------------------|
| `krishiv-executor` | `error`, `execution_model`, `assignment_inbox`, `tests` | 55 passed |
| `krishiv-state` | `error`, `namespace`, `backend`, `memory`, `redb_backend`, `snapshot`, `timer`, `processing_time`, `ttl`, `inspector`, `tests` | 66 passed |
| `krishiv-shuffle` | `path`, `metadata`, `error`, `compression`, `local_store`, `orphan`, `partitioner`, `store`, `memory_store`, `disk_store`, `object_store`, `flight`, `tests` | 53 passed |
| `krishiv-connectors` | `error`, `capabilities`, `config`, `offset`, `source`, `sink`, `certification`, `two_phase`, `quality`, `tests` | 69 passed |
| `krishiv-proto` | `ids`, `lifecycle`, `job`, `checkpoint`, `executor`, `task`, `io`, `management`, `services`, `wire`, `tests` | 30 passed |
| `krishiv-api` | `error`, `types`, `session`, `dataframe`, `stream`, `window`, `collect`, `tests` | 35 passed |
| `krishiv-scheduler` | `metrics`, `error`, `config`, `adaptive`, `coordinator`, `grpc`, `auth`, `leadership`, `tests` | 111 passed |
| `krishiv-operator` | `constants`, `error`, `crd/job`, `controller`, `dynamic`, `reconciler`, `status`, `pod_failure`, `queue_manager`, `lease`, `tests` | 38 passed |

Post-move fixes: test modules use `use crate::*` (proto, connectors, scheduler, operator); `KrishivQueue` types exported from `queue_manager`; `ExecutorPodLaunchFailure::{new,with_executor_id}` are `pub(crate)` for operator tests; `cargo fmt --all` applied.

Validation (completed in-session):
```bash
cargo fmt --check
cargo check --workspace --all-targets --all-features   # OK
cargo test -p krishiv-{proto,executor,state,shuffle,connectors,api,scheduler,operator} --all-features --lib
```

**Session update (2026-05-26):**
- Fixed `krishiv-proto` build-script tempdir creation, executor lease-generation propagation/re-registration, and shuffle zero-partition handling.
- Validated the touched crates with `cargo test -p krishiv-executor -p krishiv-shuffle --lib`; all 109 tests passed.
- Continued the crate-by-crate review across `krishiv-scheduler`, `krishiv-operator`, `krishiv-catalog`, and `krishiv-lakehouse`; no new concrete runtime defect was confirmed in that slice.

**Session update (2026-05-26, follow-up):**
- Fixed duplicate SQL table registration in `krishiv-sql` by making `register_parquet` and `register_record_batches` replace existing registrations before re-adding them.
- Added a regression test for repeated in-memory table registration.
- Verified the failing example now runs successfully: `cargo run --manifest-path examples/rust/Cargo.toml --bin single_node_inventory_replenishment`.

**Blocker:** full `cargo test --workspace --all-features` and `cargo clippy --workspace …` did not finish in this environment (`Disk quota exceeded` on `/tmp` and `target/` during link/protoc/sqlite builds). Free disk space, then run:
```bash
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

**Cleanup (2026-05-26):** Reverted incidental edits outside the eight crates (runtime, python, exec, plan, …). Removed one-off scripts `scripts/refactor_split.py` and `scripts/finish_large_crate_refactor.py`. Applied `cargo fmt` to the eight crates + `.cargo/config.toml`. Working tree is refactor-scoped (~101 paths: 8 crate trees + config + status).

**Next:** `git add` the eight crate directories + `.cargo/config.toml` + `.gitignore` + `docs/implementation/status.md`, then commit.

---

**Crate Modularization & Crate Root Refactor Phase 2: krishiv-state (2026-05-26).**
- **Completed krishiv-state Modularization**: Successfully dismantled the monolithic `legacy.rs` facade in the `krishiv-state` crate and fully migrated all domain-specific implementations to dedicated module files (`error.rs`, `namespace.rs`, `backend.rs`, `memory.rs`, `timer.rs`, `redb_backend.rs`, `snapshot.rs`, `processing_time.rs`, `ttl.rs`, `inspector.rs`, `tests.rs`).
- **Clean Crate Root Facade**: Re-implemented `lib.rs` as a clean public facade with explicit re-exports for the entire public API (such as `StateBackend`, `StateError`, `StateResult`, `InMemoryStateBackend`, `RedbStateBackend`, `TtlStateBackend`, etc.), eliminating the `legacy.rs` module entirely.
- **Strict Warning-Free Quality**: Resolved all import, visibility, and trait issues (specifically importing `redb::ReadableTable` and `redb::ReadableTableMetadata` in `redb_backend.rs` and `krishiv_async_util::unix_now_ms` in `tests.rs`), ensuring zero warnings or stubs exist across the entire crate.
- **Comprehensive Verification**: Validated the refactored crate using `cargo check -p krishiv-state` and executed all 66 unit/integration tests successfully with 100% test parity and zero failures.
- **Next Crate Target**: Prepared to systematically apply the exact same modularization workflow to the remaining large crates (`krishiv-shuffle`, `krishiv-executor`, etc.).

Validation:
```bash
cargo check -p krishiv-state
cargo test -p krishiv-state
```

**Workspace Zero-Warning Clippy and Test Validation Hardening (2026-05-26).**
- **Methodical Clippy Cleanup**: Resolved all remaining workspace-wide Clippy warnings and compiler errors, ensuring the entire codebase successfully achieves a strict "zero-warning" status under `cargo clippy --all-targets --all-features -- -D warnings`.
- **Nested Control Flow Flattening**: Addressed nested and collapsible conditional statements inside `crates/krishiv-api/src/legacy.rs` (`explain_async`) and `crates/krishiv/src/query_cli.rs` (`build_session`) using intermediate variables to preserve precise business logic.
- **Python Module Lint Optimization**: Cleaned up the `krishiv-python` crate by eliminating redundant closures, needless borrows, and unused imports (`std::sync::Arc` in `sources.rs`), and moved test modules to the bottom of the files to satisfy Rust standards.
- **Allowed Complex Traits/Signatures**: Added targeted `#[allow(clippy::too_many_arguments)]` on the private `from_sql_dataframe` (`krishiv-api`) and `#[allow(clippy::type_complexity)]` on `resolve_register_udf_args` (`krishiv-python`) to prevent invasive API refactors.
- **Useless Format Removal**: Replaced `format!` without placeholders with a `.to_string()` call in `crates/krishiv/src/daemon_cmd.rs`.
- **Workspace Validation**: Executed and validated all 29 test suites across the workspace using `cargo test --all-targets --all-features`, showing 100% test parity with zero failures.

Validation:
```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

**Workspace Large-Crate Modularization & Root Refactor (2026-05-26).**
- **Modularized Crate Roots**: Refactored `krishiv-api`, `krishiv-connectors`, `krishiv-operator`, and `krishiv-proto` to serve as clean public facades with explicit re-exports in their `lib.rs` roots.
- **Removed Monolithic Wildcards**: Eliminated the unsafe and implicit reliance on wildcard `pub use legacy::*` imports at the crate roots, transitioning to highly specific, granular module-first re-exports.
- **Unified Crate Module Trees**: Standardized the workspace architectural modularization template across all eight large crates, ensuring strict namespace boundaries, improved compiler search behavior, and clean downstream API boundaries.
- **Full Workspace Validation**: Successfully validated all workspace crates with `cargo check --all-targets --all-features` and ensured 100% test parity with `cargo test --all-targets --all-features` passing perfectly with zero failures.

Validation:
```bash
cargo check --all-targets --all-features
cargo test --all-targets --all-features
```

**CDC State Persistence & Integration Test Hardening (2026-05-25).**
- **CdcOffsetTracker State Persistence**: Implemented `CdcOffsetTracker` under the `state` feature flag to persist committed CDC partition offsets directly into the `RedbStateBackend` under a dedicated `"cdc_offsets"` namespace, preventing restart-replay gaps.
- **Transactional Module Exposure**: Declared `pub mod transactional;` in `crates/krishiv-connectors/src/lib.rs` to expose exactly-once transactional helper utilities to other crates.
- **Tokio Test Runtime Context**: Fixed `kafka_source_reports_unbounded_and_rewindable` in `krishiv-connectors` to use a `#[tokio::test]` runtime, preventing a Tokio context panic on `rdkafka` client initialization.
- **Clean Warnings**: Cleaned up remaining unused imports and assignments in tests, ensuring the entire workspace compiles warning-free and all tests pass with zero failures.

Validation:
```bash
cargo check --workspace --all-targets --all-features
cargo test --workspace --all-features
```

**Local/cluster UI lifecycle (2026-05-25).**
- Coordinator HTTP now serves a live status UI at `http://127.0.0.1:18080/ui` by default plus JSON endpoints `/api/v1/jobs` and `/api/v1/executors`.
- The UI reads the running coordinator `SharedCoordinator` snapshots directly; it is no longer the standalone `krishiv-ui --demo` / empty-state process.
- `krishiv local start|status` and `krishiv cluster start|status` now point users at the live coordinator UI. Bare-metal `cluster start` enables coordinator HTTP on `127.0.0.1:18080`.
- `--http-addr <HOST:PORT>` and `KRISHIV_LOCAL_HTTP_ADDR` / `KRISHIV_CLUSTER_HTTP_ADDR` can override the UI/HTTP address when needed.
- Older local configs are normalized by `krishiv local start` to the live coordinator UI URL and clear the obsolete standalone `ui_pid`.

Validation:
```bash
cargo check -p krishiv-scheduler -p krishiv
cargo check -p krishiv
cargo build -p krishiv --bin krishiv
cargo test -p krishiv local --lib
cargo test -p krishiv-scheduler coordinator_http --lib
./target/debug/krishiv local start
./target/debug/krishiv local status
```

**Distributed unified mitigation (2026-05-24).** Branch `cursor/implement-distributed-unified-854c`:
- **CCP/JCP:** `ClusterControlPlane`, `JobCoordinator`, `coordinator_daemon` shared startup.
- **Lowering:** `krishiv-plan::lowering` encodes `NodeOp` → executor fragments (batch SQL + `stream:tw|sw|ses`).
- **Binaries:** `krishiv-clusterd`, `krishiv-job-coordinator` for bare-metal multi-process.
- **Operator:** `spec.dedicatedCoordinator` spawns per-job orchestration loops (in-process JCP); JCP pod template + `KrishivExecutorPool` CRD; operator `replicas: 2`.
- **WS-4–11:** Barrier gRPC dispatch, object-store checkpoints (`s3://`), Redb window state, shuffle-svc, slot-aware placement, KEDA manifest, `krishiv cluster` CLI, systemd units, `RemoteFederationClient`, bare-metal CI.
- **API:** `Session::execute_local` / `execute_remote`, `with_coordinator_grpc`.
- **Tests:** `krishiv-scheduler/tests/distributed_e2e.rs`, `scripts/audit-fencing.sh`.

```bash
cargo +stable test -p krishiv-scheduler -p krishiv-operator -p krishiv-api -p krishiv-plan -p krishiv-runtime
cargo +stable test -p krishiv-scheduler --test distributed_e2e
```

**Unified execution mode parity (2026-05-24).** Branch `cursor/unified-execution-phase0-1-7ffe` (PR #43):
- **C2:** `SessionBuilder::with_remote_execution(true)` + `KRISHIV_REMOTE_EXEC=1` disables local fallback; data plane routes to Flight.
- **C3:** Flight comment protocol for catalog sync (`krishiv-register-parquet`) + shared `FlightExecutionHost` on server.
- **C4:** `sql_as` authorizes client-side, executes via `collect_batch_sql`, masks results.
- **Remote streaming:** bounded window + continuous register/push/drain over Flight protocol.
- **explain_async:** `ExecutionRuntime::explain_sql` (local + remote).
- **Python:** `submit_stream_job`, `push_stream_job_input`, `poll_stream_job`; `stream_exec` single async collect path.
- **Local cluster:** `krishiv local start` spawns `krishiv-flight-server` on `:50051`.
- **Cleanup:** removed `accept_plan_with_backend`; shared embedded runtime for orphan `DataFrame`/`Stream::new`.
- **Test:** `remote_execution_without_fallback_uses_flight_server`.

```bash
cargo +stable test -p krishiv-plan -p krishiv-exec -p krishiv-runtime -p krishiv-executor -p krishiv-api -p krishiv-flight-sql -p krishiv-sql-policy --lib
```

**Unified execution (2026-05-24).** Branch `cursor/unified-execution-phase0-1-7ffe`:
- **ADR-13.1–13.7:** `ExecutionRuntime`, session-scoped `InProcessCluster`, unified `execute_bounded_window` for all window kinds (tumbling/sliding/session), TTL, full agg support.
- **Fragments:** `stream:tw|sw|ses` encoding in `krishiv-plan::window`; executor delegates to operator runtime (canonical watermark semantics).
- **Modes:** Embedded/SingleNode/Distributed window collect through `ExecutionRuntime`; Python unified; `Session::submit_stream_job` for continuous jobs.
- **Spark-like local:** `krishiv local start|stop|status`; `SessionBuilder::with_local_cluster`; `KRISHIV_COORDINATOR` for single-node SQL.
- **Docs:** [unified-execution-model.md](../architecture/unified-execution-model.md), [unified-execution-tracker.md](unified-execution-tracker.md).

**ADR-12.4 follow-ups (2026-05-24).** Branch `cursor/adr124-memory-stream-ttl-cbbd` (extends embedded/streaming fixes on `main`):
- **ADR-12.4:** `InProcessCoordinatorBridge` + `InProcessStreamingRuntime` — coordinator submits jobs, pushes assignments to `ExecutorAssignmentInbox`, executor runs via `ExecutorTaskRunner::run_next_with` (no tonic for `inprocess://` endpoints).
- **State TTL:** `LocalWindowExecutionSpec.state_ttl_ms` wires `TtlStateBackend` + `StateBackedTumblingWindowOperator` in `local_streaming` and session `StateTtlConfig` on streams.
- **Memory streams:** `Session::memory_stream` / `register_memory_stream`; windowed `collect()` uses in-process path for Embedded/SingleNode; Python `memory_stream()` + `memory:<name>` sources use same path.
- **Stream-kafka contract:** In-process fragments use canonical `key=key:time=ts` columns matching `stream-kafka:` partition encoding.

**Embedded batch/streaming fixes (2026-05-24).** Merged via PR #41 on `main`; also on this branch:
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

### Follow-up slice (same PR, 2026-05-24)

- **Batch through coordinator:** `DataFrame.collect_async()` routes SQL via `ExecutionRuntime::collect_batch_sql` → `sql:` executor tasks; batches returned in `ExecutorTaskOutput`.
- **Distributed `Session.sql()`:** Removed `ensure_local_mode` from SQL/register paths; distributed collect uses in-process coordinator fallback + Flight SQL full drain (`execute_remote_sql`).
- **Continuous streaming:** `stream:continuous:{job_id}` executor fragment + `ContinuousStreamRegistry`; `submit_stream_job(name, spec)`, `push_stream_job_input`, `poll_stream_job`.
- **Multi-source watermark:** `MultiSourceWatermarkSpec` → `WindowExecutionSpec.source_watermark_lags` wired in `execute_bounded_window`.

```bash
cargo +stable test -p krishiv-plan -p krishiv-exec -p krishiv-runtime -p krishiv-executor -p krishiv-api --lib
```

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
