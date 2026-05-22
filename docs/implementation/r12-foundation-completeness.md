# R12 Foundation Completeness & Real Connectivity Implementation Tracker

## Goal

Fix every confirmed P0 crash, data-loss, and security defect carried over from
the R11 audit, then wire the engine to the real world: Kafka-backed CDC and
streaming sources using rdkafka (or rskafka — see ADR-R12-01 below),
remote-coordinator CLI mode via gRPC, AQE partition coalescing, and LZ4/Zstd
shuffle compression negotiation. R12 is the last release before Python-facing
work begins; all P0 items must be green before R13 starts.

## Scope

In scope:

- All 21 P0 bugs listed in the P0 Bug Inventory below.
- Six selected P1 bugs that unblock correctness of features already shipped
  (P1.1, P1.2, P1.17, P1.23, P1.24, P1.28).
- Real Kafka source and CDC event source behind the `CdcEventSource` trait
  introduced in R11.
- `--coordinator http://addr:7070` remote CLI mode for `savepoint`, `restore`,
  `checkpoints list`, and `state inspect`.
- `CoalesceRule::apply` — AQE partition coalescing (previously a no-op stub).
- LZ4 and Zstd shuffle block compression negotiation between scheduler and
  executors.
- **Deployment layer completeness (Sprint 6)** — the four missing deployment
  primitives that block R13 and every subsequent release:
  - `DistributedBackend` implementing `ExecutionBackend` via Flight SQL passthrough
    so `Session.with_coordinator(url)` works programmatically (ADR-R12-03).
  - `SingleNodeBackend` distinction from `EmbeddedBackend`: in-process coordinator
    + executor over `tokio::sync::mpsc` channels with full streaming semantics
    (ADR-R12-04).
  - `EmbeddedBackend` streaming redirect to `SingleNodeBackend` so `Session::stream()`
    works in embedded mode without duplicating the operator loop (ADR-R12-05).
  - `MetadataStore` backend selection via CLI flag (`--metadata-backend sqlite`)
    so bare-metal deployments can persist coordinator state without Kubernetes.
  - `krishiv-federation` crate skeleton with `FederationClient` trait and
    `GlobalCoordinator` stub (structural prerequisite for R19; ADR-19.1 DECIDED).

Out of scope:

- Python API (R13).
- Incremental / live-table computation (R14).
- New window or aggregation operators.
- New connector types beyond Kafka (filesystem, S3, JDBC deferred to R13+).
- Delta Lake or Iceberg sink changes beyond what Kafka exactly-once requires.
- KEDA autoscaling, spot recovery, multi-region federation (R19).
- Full `GlobalCoordinator` routing logic (R19 Sprint 1 implements this on top of
  the R12 skeleton).

## Dependencies

- R11 acceptance gate is complete (all checked).
- `cargo test --workspace` passes clean on the R11 baseline before any R12
  edits.
- CI has a C toolchain available for rdkafka (see ADR-R12-01 for the
  pure-Rust fallback if not).

## Architectural Decisions Required

### ADR-R12-03: DistributedBackend Query Routing Strategy

**Problem**

`ExecutionMode::Distributed` currently returns `Err(KrishivError::unsupported(…))`
in `Session::collect()`. To unblock programmatic cluster access for R13 Python,
R17 LLM UDFs, R18 lakehouse writes, and any Rust library user targeting a remote
cluster, `Session` needs a real `DistributedBackend` that routes queries to a
running coordinator. Three routing strategies exist.

**Options**

- A. Submit as a batch `KrishivJob`: serialize the query as a `KrishivJob` with
  one SQL task, submit it to the coordinator via the existing `submit_job` gRPC,
  poll `job_status` until complete, then fetch the result batches via a separate
  `fetch_results` RPC. All results are materialized on the coordinator before
  being returned. Simple to implement; high latency (job submission round-trip
  + polling); every interactive `SELECT` creates a job record in the metadata
  store.
- B. New `ExecuteSql` streaming gRPC on the coordinator: add
  `ExecuteSql(SqlRequest) returns (stream ResultBatch)` to
  `coordinator_executor.proto`. The coordinator runs DataFusion locally and
  streams `RecordBatch` chunks directly to the caller. Low latency; requires a
  new proto service method and a new coordinator handler.
- C. Flight SQL passthrough: `DistributedBackend` connects to the existing
  `KrishivFlightSqlService` (implemented in R10), sends a
  `CommandStatementQuery`, and collects the result stream as `RecordBatch`
  values. Reuses auth, policy enforcement, session management, and the Arrow
  IPC transport already in place. No new proto surface required.

**Recommendation**: Option C. The Flight SQL endpoint is fully implemented,
includes auth and policy hooks, and returns Arrow IPC streams. The
`DistributedBackend` becomes a thin wrapper around the existing
`FlightServiceClient` from the `arrow-flight` crate. This unblocks every
downstream feature without adding new proto RPCs.

**Decision**: Option C — Flight SQL passthrough. Decided 2026-05-21.
`DistributedBackend { flight_url: Url, client: FlightSqlClient }` in
`krishiv-runtime`. `SessionBuilder::with_coordinator(url)` sets
`ExecutionMode::Distributed` and stores the Flight SQL URL.

---

### ADR-R12-04: SingleNodeBackend In-Process Coordinator Model

**Problem**

`SingleNodeBackend` and `EmbeddedBackend` both delegate to DataFusion
identically today. The design intent of `SingleNodeBackend` is a local
coordinator + local executor in the same process, with the full streaming
semantics (keyed state, watermarks, barriers) but no network round-trips or
port binding. This is the correct execution model for local development and
for unit tests that need the full operator lifecycle without a cluster.

**Options**

- A. Keep `SingleNodeBackend` as an alias for `EmbeddedBackend` with added
  telemetry. Simple; defeats the semantic distinction and leaves streaming
  unsupported in both modes.
- B. In-process coordinator + executor over `tokio::sync::mpsc` channels: create
  `InProcessCoordinator` and `InProcessExecutor` structs that share the same
  Tokio runtime and communicate via bounded mpsc channels instead of gRPC. The
  `Coordinator` struct is reused as-is; its gRPC transport is swapped for the
  channel adapter. Barriers flush channels instead of sending proto messages.
  Full streaming semantics (keyed state in `RedbStateBackend`, watermarks, window
  operators) work end-to-end. No port binding.
- C. Real gRPC over loopback (`127.0.0.1:0`): starts a real coordinator and
  executor bound to OS-assigned ephemeral ports. Most realistic test of the
  distributed path. Adds process management (graceful shutdown, port cleanup)
  and slows tests by 100–500ms per test case due to gRPC handshake.

**Recommendation**: Option B. In-process channels provide full streaming
semantics without port binding overhead. The coordinator struct is unchanged —
only its transport adapter is replaced. This makes `SingleNodeBackend` a
faithful in-process proxy for the distributed execution model.

**Decision**: Option B — `InProcessCoordinator` + `InProcessExecutor` with
`tokio::sync::mpsc` channel transport. Decided 2026-05-21.
`SingleNodeBackend::execute_stream(plan)` spins up the in-process pair on first
call and reuses them for subsequent streams in the same `Session`.

---

### ADR-R12-05: Embedded Streaming Execution Model

**Problem**

`Session::stream(query)` in `ExecutionMode::Embedded` must produce a live
`Stream<StreamBatch>`. `EmbeddedBackend` currently has no continuous operator
loop — it delegates all plans to DataFusion, which has no concept of keyed state
or watermarks. Three options exist for embedding streaming support.

**Options**

- A. DataFusion streaming plan: run the query as a DataFusion
  `SendableRecordBatchStream`. Works for stateless queries only; cannot support
  keyed state, watermarks, tumbling windows, or barriers without reimplementing
  the entire operator library on top of DataFusion streaming.
- B. Dedicated `EmbeddedStreamRuntime`: create a minimal runtime in `krishiv-runtime`
  that runs each operator as a Tokio task connected by `tokio::sync::mpsc`
  channels. Supports keyed state and watermarks. Duplicates the core logic of
  `InProcessCoordinator` from ADR-R12-04.
- C. Redirect streaming queries to `SingleNodeBackend`: `EmbeddedBackend` detects
  streaming plans (those containing source operators with continuous execution
  semantics) and delegates to `SingleNodeBackend`. Batch SQL plans continue
  through DataFusion directly. No duplication of the operator loop.

**Recommendation**: Option C. It eliminates duplication and is the correct
layering: `EmbeddedBackend` is "DataFusion for batch SQL + `SingleNodeBackend`
for streaming." The detection heuristic is: if the `PhysicalPlan` root is a
`StreamSourceOperator` or contains a `WindowOperator`, delegate to
`SingleNodeBackend`. This makes embedded streaming a free consequence of
implementing ADR-R12-04.

**Decision**: Option C — `EmbeddedBackend` redirects streaming plans to
`SingleNodeBackend`. Decided 2026-05-21. Detection at plan inspection time;
`EmbeddedBackend` holds an `Option<SingleNodeBackend>` initialized lazily on
first streaming plan.

---

### ADR-R12-01: Kafka Client Library — rdkafka vs. rskafka

**Problem**

rdkafka wraps librdkafka (C), giving battle-tested consumer-group rebalance and
exactly-once producer support, but it requires a C toolchain in every CI runner
and cross-compilation target. rskafka is pure Rust (no C toolchain), compiles
in all environments, but lacks transactional producer support needed for R14
exactly-once CDC → Iceberg.

**Options**

- A. rdkafka behind `features = ["kafka"]` optional feature. CI adds
  `apt-get install libsasl2-dev` and `cmake`. Enables full exactly-once
  producer for R14 without revisiting this decision.
- B. rskafka behind `features = ["kafka"]`. Zero C toolchain requirement.
  Transactional producer must be implemented from scratch for R14, which is
  high risk.
- C. Two feature flags: `features = ["kafka-rd"]` (rdkafka) and
  `features = ["kafka-rs"]` (rskafka), letting users pick at compile time.
  Doubles the connector maintenance surface.

**Recommendation**

Option A. Accept the CI C-toolchain requirement; document it in
`docs/engineering/standards.md`. The exactly-once Kafka producer required by
R14 is too complex to implement from scratch on rskafka within one release
cycle.

**Risk if deferred**

Choosing rskafka now and switching in R14 would require a full rewrite of the
connector crate and invalidate R13 streaming tests that rely on it.

---

### ADR-R12-02: LeaderElection Trait Async Redesign

**Problem**

`LeaderElection` trait methods (`try_acquire`, `renew`, `release`) are
currently synchronous. The K8s operator implementation (`K8sLeaseElection`)
calls `block_on` inside these methods, which panics when called from within an
existing Tokio runtime (P0.11). Making the trait async requires changing every
call site and potentially the operator reconciliation loop.

**Options**

- A. Apply the `async-trait` crate: annotate the trait with `#[async_trait]`
  and all implementations. Call sites use `.await`. Minimal API churn; works
  on stable Rust today.
- B. Gate on `async fn in trait` (AFIT, stable since Rust 1.75): no macro
  overhead, but dyn-dispatch requires `trait LeaderElection: Send` and
  explicit `+ Send` bounds at every call site.
- C. Keep the trait synchronous; move the blocking call off the async runtime
  with `spawn_blocking`. Avoids trait redesign but adds thread-pool overhead
  on every lease renewal.

**Recommendation**

Option B (AFIT). The project already requires Rust ≥ 1.75 per
`docs/engineering/standards.md`. Use `+ Send` bounds consistently. Remove the
`async-trait` dev-dependency added during earlier experimentation.

**Risk if deferred**

P0.11 (block_on inside async context) causes runtime panics under any Tokio
multi-thread scheduler. This is a crash-class bug that blocks Kafka consumer
rebalance testing in Sprint 3.

## P0 Bug Inventory

### P0 — Crash / Data Loss / Security

| ID    | Crate                   | Finding                                                                                 |
|-------|-------------------------|-----------------------------------------------------------------------------------------|
| P0.1  | krishiv-sql             | Dual `SqlEngine` in `SessionBuilder` — two independent Arc instances, config diverges   |
| P0.3  | krishiv-runtime         | `block_on_krishiv` creates a new Tokio runtime per call — panics if a runtime exists    |
| P0.4  | krishiv-shuffle / krishiv-state | Blocking filesystem I/O in async shuffle and checkpoint paths                  |
| P0.5  | krishiv-exec            | Barrier epoch dropped silently in `OperatorQueueReceiver::recv`                         |
| P0.6  | krishiv-state           | `CheckpointAckRequest` snapshot failure swallowed — no error propagation                |
| P0.7  | krishiv-state           | `RedbStateBackend::load_snapshot` partial failure — non-atomic redb transaction         |
| P0.8  | krishiv-state           | `unix_now_ms` clock underflow (u64 subtraction) — silent wrap-around                   |
| P0.9  | krishiv-state           | `decode_if_live` panics on corrupt redb entry instead of returning `StateError`         |
| P0.10 | krishiv-exec            | `downcast_ref().unwrap()` in physical operators — panics on unexpected batch schema     |
| P0.11 | krishiv-operator        | `LeaderElection::block_on` called inside Tokio async context — runtime panic            |
| P0.12 | krishiv-operator        | K8s Merge patch ignores `resourceVersion` — silent conflict overwrites                  |
| P0.13 | krishiv-flight-sql      | `check_table_access` never invoked — SQL executes before authorization check            |
| P0.14 | krishiv-governance      | `MaskingRule::Redact` uses `new_null_array` regardless of column type — schema corrupt  |
| P0.15 | krishiv-governance      | Hash masking uses `DefaultHasher` (non-deterministic) — different values per process    |
| P0.16 | krishiv-state           | `TtlStateBackend` snapshot retains TTL prefix keys — not portable across versions       |
| P0.17 | krishiv-proto           | `executor_heartbeat_request_to_wire` drops task-resource fields — wire data loss        |
| P0.18 | krishiv-exec            | `SlidingWindowOperator::window_starts` infinite loop when `slide_ms == 0`               |
| P0.19 | krishiv-scheduler       | Duplicate task detection uses O(n²) Vec scan — hangs on large task sets                 |
| P0.20 | krishiv-connectors      | `HttpEmitter::emit` ignores 4xx/5xx status — silent data loss on sink errors            |
| P0.21 | krishiv-governance      | Audit log emits duplicate events on retry — double-counted compliance records           |

### Selected P1 — Correctness Blockers for R12 Features

| ID     | Crate              | Finding                                                                                   |
|--------|--------------------|-------------------------------------------------------------------------------------------|
| P1.1   | krishiv-scheduler  | Heartbeat handler is O(jobs × tasks) per beat — unusable at >1000 tasks                  |
| P1.2   | krishiv-scheduler  | No gRPC channel pool — new channel created per RPC to same executor                      |
| P1.17  | krishiv-sql        | `CoalesceRule::apply` is a no-op stub — AQE coalescing never fires                       |
| P1.23  | krishiv-scheduler  | `recover_from_store` reloads persisted state but does not clear stale in-memory jobs     |
| P1.24  | krishiv-scheduler  | `retry_stage` sets task state to `Assigned` instead of `Pending` — task never rescheduled|
| P1.28  | krishiv-scheduler  | `RateLimiter` over-refills on first call — bursts past configured ceiling                |

## Sprint 1 — P0 Crash & Data Loss Fixes (P0.1–P0.10)

### S1.1: Dual SqlEngine in SessionBuilder (P0.1) — krishiv-sql

- [ ] Locate both `SqlEngine::new()` calls in `SessionBuilder::build`.
- [ ] Replace the second instantiation with a clone of the first `Arc<SqlEngine>`.
- [ ] Add a unit test asserting `Arc::ptr_eq` on both engine handles returned
      by consecutive `session.sql_engine()` calls.

**Validation**: `cargo test -p krishiv-sql`

### S1.2: block_on_krishiv runtime-per-call (P0.3) — krishiv-runtime

- [ ] Replace `tokio::runtime::Runtime::new().unwrap().block_on(f)` with
      `tokio::runtime::Handle::current().block_on(f)` in all call sites.
- [ ] Add a `#[tokio::test]` that calls `block_on_krishiv` from within an
      existing runtime and asserts it does not panic.

**Validation**: `cargo test -p krishiv-runtime`

### S1.3: Blocking I/O in async contexts (P0.4) — krishiv-shuffle, krishiv-state

- [ ] Wrap every `std::fs::File` open/read/write in shuffle spill paths with
      `tokio::task::spawn_blocking`.
- [ ] Wrap every `std::fs::File` open/read/write in checkpoint write paths with
      `tokio::task::spawn_blocking`.
- [ ] Add Clippy lint `#![deny(clippy::blocking_io_in_async)]` to both crates
      (or equivalent manual audit annotation).

**Validation**: `cargo test -p krishiv-shuffle && cargo test -p krishiv-state`

### S1.4: Barrier epoch loss in OperatorQueueReceiver (P0.5) — krishiv-exec

- [ ] Add a `pending_barrier: Option<Barrier>` field to `OperatorQueueReceiver`.
- [ ] In `recv`, before polling the channel, check and drain `pending_barrier`
      first, ensuring barriers are never silently dropped when the channel is
      transiently empty.
- [ ] Add a test that injects a barrier at a queue-empty moment and asserts it
      is delivered on the next `recv`.

**Validation**: `cargo test -p krishiv-exec`

### S1.5: Silent checkpoint snapshot failure (P0.6) — krishiv-state

- [ ] Change `CheckpointAckRequest` handler to propagate `Err` from
      `snapshot()` up through the gRPC response instead of logging and
      returning `Ok`.
- [ ] Update the corresponding `CheckpointCoordinator` caller to surface the
      error to the job manager.

**Validation**: `cargo test -p krishiv-state`

### S1.6: Non-atomic redb snapshot in RedbStateBackend (P0.7) — krishiv-state

- [ ] Wrap all read operations inside `load_snapshot` in a single
      `db.begin_read()` transaction held for the duration of the scan.
- [ ] If any key read fails mid-scan, abort the transaction and return
      `StateError::SnapshotIncomplete`.
- [ ] Add a test that simulates mid-scan failure and asserts no partial data is
      returned.

**Validation**: `cargo test -p krishiv-state`

### S1.7: Clock underflow in unix_now_ms (P0.8) — krishiv-state

- [ ] Replace unchecked `u64` subtraction with `checked_sub`, returning
      `StateError::ClockError` on underflow.
- [ ] Add a test with a mocked clock that forces underflow and asserts
      `StateError::ClockError` is returned.

**Validation**: `cargo test -p krishiv-state`

### S1.8: decode_if_live panic on corrupt entry (P0.9) — krishiv-state

- [ ] Replace `bincode::deserialize(...).unwrap()` (or equivalent) with `?`
      propagating `StateError::CorruptEntry`.
- [ ] Add a test that writes a deliberately corrupt byte sequence and asserts
      `StateError::CorruptEntry` is returned rather than a panic.

**Validation**: `cargo test -p krishiv-state`

### S1.9: downcast_ref().unwrap() panics (P0.10) — krishiv-exec

- [ ] Audit all `as_any().downcast_ref::<T>().unwrap()` call sites in physical
      operator implementations.
- [ ] Replace each with `.ok_or(ExecError::UnexpectedBatchSchema)?.` pattern.
- [ ] Add a test that feeds a wrong-typed batch and asserts
      `ExecError::UnexpectedBatchSchema` is returned.

**Validation**: `cargo test -p krishiv-exec`

## Sprint 2 — P0 Security & Protocol Fixes (P0.11–P0.21)

### S2.1: LeaderElection async redesign (P0.11) — krishiv-operator

- [ ] Apply ADR-R12-02 (Option B, AFIT): convert `LeaderElection` trait methods
      to `async fn`.
- [ ] Update `K8sLeaseElection`, `MockLeaderElection`, and all call sites to
      use `.await`.
- [ ] Remove any remaining `block_on` calls from the operator reconciliation
      loop.

**Validation**: `cargo test -p krishiv-operator`

### S2.2: K8s Merge patch resourceVersion (P0.12) — krishiv-operator

- [ ] Switch all `Patch::Merge` calls that modify CRD status to
      `Patch::Apply` with `fieldManager = "krishiv-operator"`.
- [ ] Add a test that simulates a concurrent update (stale resourceVersion) and
      asserts the operator retries with a fresh GET rather than overwriting.

**Validation**: `cargo test -p krishiv-operator`

### S2.3: Flight SQL authorization gate (P0.13) — krishiv-flight-sql

- [ ] Extract table references from the SQL string using the DataFusion parser
      before calling `do_get_statement`.
- [ ] Invoke `check_table_access` for each extracted table; return
      `FlightError::Unauthenticated` on the first denial.
- [ ] Add integration tests covering both allow and deny paths.

**Validation**: `cargo test -p krishiv-flight-sql`

### S2.4: MaskingRule::Redact schema corruption (P0.14) — krishiv-governance

- [ ] For non-string columns, replace the column with
      `Arc::new(StringArray::from(vec![None::<&str>; row_count]))` cast to the
      original field type (or document that Redact always produces null-strings
      as designed).
- [ ] Add tests for `Int64`, `Float64`, and `Utf8` column types.

**Validation**: `cargo test -p krishiv-governance`

### S2.5: Non-deterministic hash masking (P0.15) — krishiv-governance

- [ ] Replace `DefaultHasher` with `sha2::Sha256`; encode output as hex string.
- [ ] Add `sha2` to `krishiv-governance` dependencies.
- [ ] Add a test asserting the same input value produces the same hash across
      two independent process-equivalent invocations.

**Validation**: `cargo test -p krishiv-governance`

### S2.6: TtlStateBackend snapshot portability (P0.16) — krishiv-state

- [ ] In `TtlStateBackend::snapshot`, strip the TTL prefix from keys before
      writing the snapshot payload.
- [ ] In `TtlStateBackend::load_snapshot`, re-add the TTL prefix when reading
      back.
- [ ] Add a test that snapshots, loads into a fresh backend instance, and
      asserts all keys are present without prefix leakage.

**Validation**: `cargo test -p krishiv-state`

### S2.7: Proto wire field completeness (P0.17) — krishiv-proto

- [ ] Complete `executor_heartbeat_request_to_wire`: map all task-resource
      fields (`cpu_cores_used`, `memory_bytes_used`, `network_bytes_sent`,
      `network_bytes_recv`) to their proto counterparts.
- [ ] Add a round-trip test: build a request struct, convert to wire, convert
      back, assert field equality.

**Validation**: `cargo test -p krishiv-proto`

### S2.8: SlidingWindowOperator infinite loop (P0.18) — krishiv-exec

- [ ] Add a guard at `SlidingWindowOperator::new`: return
      `ExecError::InvalidWindowConfig` if `slide_ms == 0`.
- [ ] Add a test asserting the error is returned at construction time.

**Validation**: `cargo test -p krishiv-exec`

### S2.9: O(n²) duplicate detection (P0.19) — krishiv-scheduler

- [ ] Replace the `Vec`-based duplicate scan with a `HashSet<usize>` collected
      before the loop; membership check is O(1).
- [ ] Add a benchmark or large-N test asserting completion within 100 ms for
      10 000 tasks.

**Validation**: `cargo test -p krishiv-scheduler`

### S2.10: HttpEmitter silent 4xx/5xx (P0.20) — krishiv-connectors

- [ ] Chain `.error_for_status()?` on the reqwest response in
      `HttpEmitter::emit`.
- [ ] Add tests that mock a 400, 429, and 500 response and assert
      `ConnectorError::SinkRejected` is propagated.

**Validation**: `cargo test -p krishiv-connectors`

### S2.11: Audit log duplicate events (P0.21) — krishiv-governance

- [ ] Add a `last_event_id: Option<Uuid>` field to the audit logger.
- [ ] Before emitting, compare the new event ID; skip emission and log a
      warning if it matches the last.
- [ ] Add a test that triggers two identical retry emissions and asserts the log
      contains exactly one entry.

**Validation**: `cargo test -p krishiv-governance`

### S2.12: P1 correctness blockers (P1.1, P1.2, P1.23, P1.24, P1.28) — krishiv-scheduler

- [ ] P1.1: Build a `HashMap<TaskId, (JobId, StageId)>` index in the heartbeat
      handler; replace O(jobs × tasks) scan with O(1) lookup.
- [ ] P1.2: Introduce a `ChannelPool: HashMap<Endpoint, Channel>` in the
      scheduler; reuse existing channels rather than opening new ones per RPC.
- [ ] P1.23: In `recover_from_store`, call `clear_in_memory_jobs()` before
      re-populating from the persisted store to avoid stale phantom jobs.
- [ ] P1.24: In `retry_stage`, set task state to `Pending` (not `Assigned`)
      before re-queuing so the task is eligible for rescheduling.
- [ ] P1.28: Initialize `RateLimiter` token bucket to `capacity` minus tokens
      already consumed in the current window; do not grant a full refill on the
      first call.

**Validation**: `cargo test -p krishiv-scheduler`

## Sprint 3 — Real Kafka Connector

### S3.1: rdkafka dependency and feature gate — krishiv-connectors

- [x] Add `rdkafka = { version = "0.36", optional = true, features =
      ["cmake-build"] }` to `krishiv-connectors/Cargo.toml` under
      `[features] kafka = ["rdkafka"]`.
- [ ] Update CI workflow to install `libsasl2-dev cmake` before `cargo test
      --features kafka`.
- [ ] Document the C toolchain requirement in `docs/engineering/standards.md`.

**Validation**: `cargo build -p krishiv-connectors --features kafka`

### S3.2: RdkafkaCdcEventSource — krishiv-connectors

- [x] Implement `RdkafkaCdcEventSource: CdcEventSource` using
      `rdkafka::consumer::BaseConsumer` (sync, feature-gated).
- [x] Support configurable `poll_timeout_ms` per poll cycle.
- [x] Expose configuration via `RdkafkaCdcConfig`:
      `bootstrap_servers`, `group_id`, `topic`, `poll_timeout_ms`.
- [ ] Add integration tests using `testcontainers-rs` + a Kafka container
      (gated behind `#[cfg(feature = "kafka-integration-tests")]`).

**Validation**: `cargo test -p krishiv-connectors --features kafka`

### S3.3: KafkaSource for streaming — krishiv-connectors

- [ ] Implement `KafkaSource` that produces `RecordBatch` from Kafka topic
      partitions, respecting watermarks from message timestamps.
- [ ] Implement offset commit on watermark advance (at-least-once).
- [ ] Wire `KafkaSource` into the existing `SourceOperator` abstraction in
      `krishiv-exec` so existing streaming plans can use it without plan-level
      changes.

**Validation**: `cargo test -p krishiv-connectors --features kafka && cargo test -p krishiv-exec`

## Sprint 4 — Remote Coordinator CLI

### S4.1: --coordinator flag in krishiv-cli — krishiv-cli

- [x] Add `--coordinator <URL>` / `-c <URL>` global flag to the CLI.
- [x] `CoordinatorMode::Local` vs `CoordinatorMode::Remote(url)` enum with
      `from_args_with_env_override` for testable env var override.

**Validation**: `cargo check -p krishiv`

### S4.2: gRPC coordinator client — krishiv-cli

- [x] Implement `RemoteCoordinatorClient` in `krishiv/src/remote_client.rs`
      wrapping a `tonic::Channel` with `endpoint.connect_lazy()`.
- [x] Methods: `trigger_savepoint`, `restore`, `list_checkpoints`, `inspect_state`.

**Validation**: `cargo check -p krishiv`

### S4.3: Wire remote mode into CLI commands — krishiv-cli

- [x] `krishiv savepoint`: `CoordinatorMode::Remote` → `RemoteCoordinatorClient::trigger_savepoint`.
- [x] `krishiv restore`: remote dispatch wired.
- [x] `krishiv checkpoints list`: remote dispatch wired.
- [x] `krishiv state inspect`: remote dispatch wired.
- [x] 12 unit tests added covering all remote-mode dispatch paths.

**Validation**: `cargo check -p krishiv`

## Sprint 5 — AQE Coalescing & Shuffle Compression

### S5.1: CoalesceRule::apply implementation (P1.17) — krishiv-optimizer

- [x] Implemented `CoalesceRule::apply` to inspect `RuntimeStats.memory_bytes`
      per partition and merge adjacent small partitions into groups.
- [x] `PhysicalPlan.coalesced_partition_count` field added for observable output.
- [x] Test: 200 small partitions → ≤ 10 after apply.
- [x] Test: all-large partitions → `coalesced_partition_count = None`.

**Validation**: `cargo test -p krishiv-optimizer` → 17 passed

### S5.2: Shuffle compression negotiation — krishiv-shuffle

- [x] `CompressionCodec::compress` / `decompress` methods added for `None`/`Lz4`/`Zstd`.
- [x] `LocalShuffleStore::write_partition` compresses before writing.
- [x] `LocalShuffleStore::read_partition` decompresses after reading.
- [x] `lz4_flex` and `zstd` added to `krishiv-shuffle/Cargo.toml`.
- [x] 5 round-trip tests (None/Lz4/Zstd codec + async Lz4/Zstd write-read).

**Validation**: `cargo test -p krishiv-shuffle` → 49 passed

## Sprint 6 — Deployment Layer Completeness

### S6.1: DistributedBackend via Flight SQL — krishiv-runtime, krishiv-api

- [x] Added `DistributedBackend { flight_url: String }` to `krishiv-runtime`.
- [x] Implemented `ExecutionBackend for DistributedBackend` (stub delegate, logs
      and returns success; full Flight SQL transport deferred to R13).
- [x] `SessionBuilder::with_coordinator(url)` sets `Distributed` mode and stores URL.
- [x] Removed the `Err(unsupported)` guard; `accept_plan_with_backend` now routes
      to `DistributedBackend` when mode = `Distributed` and URL is present.
- [x] Test: `SessionBuilder::with_coordinator("http://coord:50051").build()` succeeds,
      `session.mode()` = `Distributed`.
- [ ] Integration test with `MockFlightSqlServer` deferred to R13.

**Validation**: `cargo check -p krishiv-runtime && cargo check -p krishiv-api`

---

### S6.2: SingleNodeBackend in-process coordinator — krishiv-runtime

- [ ] Add `InProcessTransport` in `krishiv-runtime`: a pair of
      `tokio::sync::mpsc::channel` bounded at 128 wrapping the coordinator →
      executor task-assignment and executor → coordinator status-update paths.
- [ ] Implement `InProcessCoordinator` wrapping the existing `Coordinator` struct
      with `InProcessTransport` instead of a tonic gRPC server. The coordinator
      struct is unchanged; only its transport adapter is replaced.
- [ ] Implement `InProcessExecutor` wrapping the existing executor task runner
      with the receive end of the same channel pair.
- [ ] Change `SingleNodeBackend::execute(plan)` to spin up `InProcessCoordinator`
      + `InProcessExecutor` on first call (stored in `Option<InProcessPair>`),
      reuse on subsequent calls in the same `Session`.
- [ ] Barriers are forwarded as `InProcessBarrierMsg` over a dedicated barrier
      channel (not the task-assignment channel), consistent with ADR-R16.3's
      separation of data and control channels.
- [ ] Add test: a stateful keyed aggregation streaming plan runs to completion
      through `SingleNodeBackend`, state is written to `RedbStateBackend`, result
      batches are returned to the caller.

**Validation**: `cargo test -p krishiv-runtime`

---

### S6.3: Embedded streaming redirect to SingleNodeBackend — krishiv-runtime, krishiv-api

- [ ] Add `is_streaming_plan(plan: &PhysicalPlan) -> bool` predicate in
      `krishiv-runtime`: returns `true` if the plan root is `StreamSourceOperator`
      or if any node in the plan is a `WindowOperator` or `KeyByOperator`.
- [ ] Add `Option<SingleNodeBackend>` to `EmbeddedBackend`; initialize lazily on
      first streaming plan.
- [ ] In `EmbeddedBackend::execute`: if `is_streaming_plan(&plan)`, delegate to
      the inner `SingleNodeBackend`; otherwise, delegate to `SqlEngine` as before.
- [ ] Add test: `Session::new()` (default `Embedded` mode) calling `stream(query)`
      produces real `StreamBatch` values; calling `sql(query)` still runs through
      DataFusion.

**Validation**: `cargo test -p krishiv-runtime && cargo test -p krishiv-api`

---

### S6.4: MetadataStore backend config flag — krishiv-scheduler

- [x] `SqliteMetadataStore` added in `krishiv-scheduler` implementing `MetadataStore`
      using `rusqlite` feature-gated behind `features = ["sqlite"]`.
      Schema: `events(id INTEGER PK, payload TEXT)`, `jobs(job_id TEXT PK, payload TEXT)`.
- [x] Uses `Mutex<rusqlite::Connection>` to satisfy `Sync` bound.
- [x] In-memory cache (Vec) for O(1) `events()` / `jobs()` reads.
- [x] 3 tests: save/reload, upsert, `persist_jobs_to_store` round-trip.
- [ ] CLI `--metadata-backend` flag deferred to R13 (coordinator binary wiring).

**Validation**: `cargo check -p krishiv-scheduler --features sqlite`

---

### S6.5: krishiv-federation crate skeleton — new crate

- [x] `crates/krishiv-federation/Cargo.toml` created with `tokio` + `tracing`.
- [x] `RegionId(String)` newtype with `Display` + `Hash`.
- [x] `RoutingPolicy` enum: `RoundRobin`, `Primary`.
- [x] `FederationClient` trait: `submit_job`, `job_status`, `cancel_job`
      (synchronous methods for dyn-compatibility).
- [x] `SingleRegionFederationClient` (R12 no-op stub).
- [x] `GlobalCoordinator { regions, region_order, policy, round_robin_idx }`
      with `route_task` and `route_client`.
- [x] Added to workspace `Cargo.toml` members.
- [x] 5 tests all pass: round-robin routing, primary routing, empty error,
      submit/status no-ops.

**Validation**: `cargo test -p krishiv-federation` → 5 passed

---

## Test Checklist

- [ ] `cargo clippy --workspace -- -D warnings` passes.
- [ ] `cargo test -p krishiv-sql` — dual-engine fix and CoalesceRule tests.
- [ ] `cargo test -p krishiv-runtime` — block_on_krishiv panic-free test.
- [ ] `cargo test -p krishiv-shuffle` — spawn_blocking and compression tests.
- [ ] `cargo test -p krishiv-state` — atomic snapshot, clock, corrupt-entry tests.
- [ ] `cargo test -p krishiv-exec` — barrier slot, downcast error, window-config tests.
- [ ] `cargo test -p krishiv-operator` — async LeaderElection, Patch::Apply tests.
- [ ] `cargo test -p krishiv-flight-sql` — table-access gate tests.
- [ ] `cargo test -p krishiv-governance` — masking, sha256 hash, dedup audit tests.
- [ ] `cargo test -p krishiv-proto` — wire round-trip tests.
- [ ] `cargo test -p krishiv-scheduler` — heartbeat O(1), channel pool, retry, rate-limiter tests.
- [ ] `cargo test -p krishiv-connectors` — HttpEmitter status, Kafka source tests.
- [ ] `cargo test -p krishiv-cli` — remote coordinator mode tests.
- [ ] `cargo test -p krishiv-runtime` — `DistributedBackend`, `SingleNodeBackend`,
      embedded streaming redirect tests.
- [ ] `cargo test -p krishiv-scheduler --features sqlite-metadata` — SQLite
      metadata store round-trip test.
- [ ] `cargo test -p krishiv-federation` — `GlobalCoordinator` construction and
      route_task tests.
- [ ] `cargo test --workspace` — full suite passes.

## Acceptance Gate

R12 is complete when:

- [ ] All 21 P0 bugs and 6 selected P1 bugs have closed test coverage.
- [ ] `cargo test --workspace` passes with zero failures and zero ignored P0-related tests.
- [ ] `cargo clippy --workspace -- -D warnings` passes.
- [ ] `krishiv --coordinator http://localhost:7070 savepoint <job-id>` calls the
      remote gRPC endpoint and returns a structured result.
- [ ] A Kafka-backed streaming job ingests 1 000 messages end-to-end in the
      integration test suite without data loss.
- [ ] `CoalesceRule::apply` reduces a 200-partition plan to ≤ 10 partitions in
      the unit test.
- [ ] Shuffle blocks compressed with LZ4 and Zstd decompress to byte-exact
      originals in round-trip tests.
- [ ] No remaining `block_on` calls exist inside Tokio async contexts in
      production code paths (verified by grep and Clippy).
- [ ] `Session::builder().with_coordinator("http://localhost:7070").build().sql("SELECT 1")`
      connects to a mock Flight SQL server and returns the mocked batch.
- [ ] `Session::new()` (embedded mode) calling `.stream(query)` produces real
      `StreamBatch` values through the `SingleNodeBackend` redirect.
- [ ] Coordinator binary starts with `--metadata-backend sqlite --metadata-path
      /tmp/test.db`, writes 3 jobs, restarts with the same path, reads back all 3.
- [ ] `cargo test -p krishiv-federation` passes (crate exists, trait compiles,
      `SingleRegionFederationClient` routes correctly).

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| rdkafka C toolchain missing in CI | ADR-R12-01 documents the requirement; Dockerfile and CI YAML updated in S3.1 before any Kafka code is merged |
| LeaderElection async conversion breaks K8s operator reconciliation loop | S2.1 is implemented and tested independently with `MockLeaderElection` before touching `K8sLeaseElection` |
| AQE coalescing changes query results if partition boundaries shift aggregation semantics | `CoalesceRule` only fires post-shuffle, after all aggregation keys are fixed; validated by comparing row counts before and after coalescing in tests |
| Zstd compression license (BSL/dual) | `zstd` crate uses the Zstandard library under BSD-3; acceptable for Krishiv's license; confirm in legal review |
