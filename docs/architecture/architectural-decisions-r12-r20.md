# Architectural Decision Records: R12–R20

## Introduction

This document consolidates every architectural decision and known bottleneck
identified during the planning of Krishiv releases R12 through R20. Each record
describes a concrete engineering choice that affects the design of one or more
crates, introduces a structural dependency, or creates a risk that could block
a downstream release if resolved incorrectly. The records are scoped to decisions
that cannot be deferred inside a sprint — they must be explicitly chosen before
implementation begins because the wrong option would require a crate-level rewrite
to correct.

Each ADR carries a status: OPEN (the team has not yet chosen an option and
implementation is blocked until they do), PROPOSED (the planning team leans
toward a specific option based on the analysis below, but the decision has not
been formally recorded), or DECIDED (the team has committed to an option and
implementation may proceed). When a decision moves to DECIDED, the "Decision"
field below the options list is filled in with the chosen option and the date.
Every sprint plan in the release tracker files (`docs/implementation/r*.md`)
references the relevant ADR ID; an OPEN ADR with a sprint depending on it is
an immediate escalation. The goal of this document is to make the complete
decision surface visible so that the engineering team can schedule ADR reviews
as explicit work items rather than discovering blockers mid-sprint.

## Decision Index Table

| ADR ID | Title | Release | Status | Risk Level |
|--------|-------|---------|--------|------------|
| ADR-12.1 | Kafka Client Library | R12 | PROPOSED | HIGH — blocks real Kafka connector in Sprint 3 |
| ADR-12.2 | LeaderElection Trait Async Migration | R12 | PROPOSED | HIGH — P0.11 crash blocks K8s operator |
| ADR-12.3 | DistributedBackend Query Routing | R12 | DECIDED | HIGH — blocks R13 `ks.Session.connect()` and all programmatic cluster access |
| ADR-12.4 | SingleNodeBackend In-Process Coordinator | R12 | DECIDED | HIGH — blocks streaming in embedded/single-node mode |
| ADR-12.5 | Embedded Streaming Execution Model | R12 | DECIDED | MEDIUM — blocks `Session::stream()` in default mode |
| ADR-12.6 | Scheduler God-File Decomposition | R12 | DECIDED | HIGH — `krishiv-scheduler/src/lib.rs` at 7 971 lines blocks any structural work |
| ADR-12.7 | ExecutionBackend Async Migration | R12 | DECIDED | HIGH — sync trait forces `block_on` inside Tokio; causes nested-runtime panics |
| ADR-12.8 | PhysicalPlan → Real Operator Wiring | R12 | DECIDED | HIGH — plan is name-only; `execute()` never runs actual operators |
| ADR-13.1 | Python/Tokio Async Bridge | R13 | DECIDED | HIGH — blocks asyncio-native streaming API |
| ADR-13.2 | Python Schema Metaclass | R13 | DECIDED | MEDIUM — blocks schema-declared sources |
| ADR-14.1 | Incremental View Delta Storage | R14 | DECIDED | HIGH — blocks live table refresh correctness |
| ADR-14.2 | Memoization Key Storage | R14 | DECIDED | MEDIUM — blocks `@ks.transform(memo=True)` |
| ADR-15.1 | Spark Connect Implementation Scope | R15 | PROPOSED | HIGH — determines sprint capacity for R15 |
| ADR-15.2 | dbt Adapter Transport | R15 | PROPOSED | MEDIUM — affects dbt adapter connection model |
| ADR-16.1 | CEP Engine Scope for R16 | R16 | PROPOSED | MEDIUM — determines operator complexity |
| ADR-16.2 | State Rescaling Algorithm | R16 | DECIDED | HIGH — wrong choice breaks restore correctness |
| ADR-16.3 | gRPC Barrier Message Format | R16 | DECIDED | HIGH — blocks full exactly-once barrier transport |
| ADR-17.1 | LLM UDF Execution Isolation | R17 | PROPOSED | HIGH — wrong choice crashes streaming executors |
| ADR-17.2 | Vector Store Sink Consistency | R17 | PROPOSED | MEDIUM — affects embedding pipeline correctness |
| ADR-18.1 | delta-rs Tokio Integration | R18 | PROPOSED | HIGH — nested runtime panic on first Delta write |
| ADR-18.2 | MERGE INTO SQL Implementation | R18 | PROPOSED | HIGH — wrong option corrupts data on upsert |
| ADR-19.1 | Multi-Region Metadata Consistency | R19 | DECIDED | CRITICAL — all federation code depends on this |
| ADR-19.2 | Spot Recovery Checkpoint Timing | R19 | PROPOSED | HIGH — data loss on spot eviction |
| ADR-20.1 | Portal Frontend Deployment | R20 | PROPOSED | MEDIUM — blocks CI pipeline for portal work |

---

## R12: Foundation Completeness & Real Connectivity

### ADR-12.1: Kafka Client Library

**Status**: PROPOSED

**Problem statement**

Krishiv's streaming and CDC stacks require a production-grade Kafka client.
Two options exist in the Rust ecosystem: `rdkafka` (wraps librdkafka, a C
library) and `rskafka` (pure Rust). The choice affects CI infrastructure,
cross-compilation targets, and the feasibility of exactly-once Kafka producer
semantics needed for R14 CDC-to-Iceberg pipelines.

**Options**

- A. `rdkafka` behind `features = ["kafka"]`. The `rdkafka` crate wraps
  librdkafka, which provides battle-tested consumer-group rebalance, transactional
  producer, and SASL/SSL support. Requires a C toolchain (`cmake`, `libsasl2-dev`)
  in every CI runner and cross-compilation environment. Enables exactly-once
  Kafka producer for R14 without revisiting this decision.
- B. `rskafka` behind `features = ["kafka"]`. Pure Rust; no C toolchain
  dependency; compiles in all environments including musl targets. Does not
  support transactional producer. Implementing a transactional producer on top
  of `rskafka` for R14 exactly-once is high risk and would consume a full
  sprint.
- C. Dual feature flags `kafka-rd` (rdkafka) and `kafka-rs` (rskafka), letting
  users choose at compile time. Doubles the connector maintenance surface;
  integration tests must run twice; no single blessed path for enterprise
  support.

**Recommendation**: Option A. Accept the C toolchain requirement; document it
in `docs/engineering/standards.md` and update CI YAML to install `libsasl2-dev`
and `cmake` before any Kafka feature is compiled.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

If Option A: CI gains a C toolchain step; cross-compilation to musl (e.g., AWS
Lambda custom runtime) requires a static build of librdkafka; document this in
the deployment guide. R14 exactly-once producer is feasible without a full
connector rewrite.

**Risk if deferred**

Choosing `rskafka` and switching to `rdkafka` in R14 requires a full rewrite
of `krishiv-connectors`'s Kafka module and invalidates all R13 streaming
integration tests that depend on it. The decision must be DECIDED before Sprint
3 of R12 begins.

---

### ADR-12.2: LeaderElection Trait Async Migration

**Status**: PROPOSED

**Problem statement**

`LeaderElection` trait methods (`try_acquire`, `renew`, `release`) are
synchronous. The Kubernetes operator implementation calls `block_on` inside
these methods, which panics when called from within an existing Tokio runtime
— the root cause of P0.11. Making the trait async requires changing every
call site and the operator reconciliation loop.

**Options**

- A. Apply the `async-trait` crate: annotate with `#[async_trait]` and use
  `.await` at all call sites. Minimal API churn; works on all stable Rust today.
  Adds a heap allocation per async call due to the macro's `Box<dyn Future>`
  output.
- B. Use native `async fn` in traits (AFIT, stable since Rust 1.75): no macro
  overhead; dyn-dispatch requires explicit `+ Send` bounds at every call site
  that stores a `Box<dyn LeaderElection>`. The project already targets Rust ≥ 1.75.
- C. Keep the trait synchronous; move the blocking call to `spawn_blocking` on
  every lease renewal. Avoids trait redesign but adds thread-pool overhead on
  every leader tick and does not fully resolve the nested runtime panic if any
  internal path calls `block_on` after the move.

**Recommendation**: Option B (AFIT). Rust ≥ 1.75 is the project minimum;
use `+ Send` bounds consistently. Remove the `async-trait` dev-dependency added
during earlier experimentation.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

All implementations of `LeaderElection` (`K8sLeaseElection`, `MockLeaderElection`,
and `EtcdLeaderElection` in R19) must use `async fn`. All call sites must use
`.await`. The `async-trait` dependency is removed from `krishiv-operator`.

**Risk if deferred**

P0.11 causes runtime panics under any Tokio multi-thread scheduler on every
leader-election tick. This is a crash-class defect that blocks Kafka consumer
rebalance testing in Sprint 3 of R12 and affects every subsequent release that
uses `krishiv-operator`.

---

### ADR-12.3: DistributedBackend Query Routing Strategy

**Status**: DECIDED

**Problem statement**

`ExecutionMode::Distributed` currently returns `Err(KrishivError::unsupported(…))`
in `Session::collect()`. Every downstream release that targets a remote cluster
programmatically — R13 `ks.Session.connect()`, R17 LLM UDFs, R18 lakehouse
writes, R20 portal — requires a working `DistributedBackend`. The backend must
route queries to a running coordinator and stream results back to the caller
without requiring a separate job submission lifecycle.

**Options**

- A. Submit as a batch `KrishivJob` via the existing `submit_job` gRPC: serialize
  the query, submit it, poll `job_status` until complete, fetch results. All
  results are materialized on the coordinator side before being returned.
  Simple implementation; high latency (polling round-trips); every interactive
  `SELECT` creates a job record in the metadata store.
- B. New `ExecuteSql(SqlRequest) returns (stream ResultBatch)` streaming gRPC on
  the coordinator: the coordinator runs DataFusion locally and streams `RecordBatch`
  chunks directly. Low latency but requires a new proto service method and a new
  coordinator handler — additional surface to maintain.
- C. Flight SQL passthrough: `DistributedBackend` connects to the existing
  `KrishivFlightSqlService` (implemented in R10), sends a `CommandStatementQuery`,
  and collects the result stream as Arrow `RecordBatch` values. Reuses all existing
  auth, policy enforcement, session management, and Arrow IPC transport.

**Recommendation**: Option C. The Flight SQL endpoint is fully implemented and
battle-tested. `DistributedBackend` becomes a thin wrapper around the
`FlightSqlClient` from `arrow-flight`. No new proto RPC surface is required.

**Decision**: Option C — Flight SQL passthrough. Decided 2026-05-21.
`DistributedBackend { flight_url: Url, client: FlightSqlClient }` in
`krishiv-runtime`. `SessionBuilder::with_coordinator(url)` sets
`ExecutionMode::Distributed` and stores the Flight SQL URL. The
`ExecutionMode::Distributed => Err(unsupported)` guard in `krishiv-api` is
removed in R12 Sprint 6.

**Consequences**

The Flight SQL endpoint must be reachable from the calling process. For
Kubernetes deployments, the coordinator service must expose the Flight SQL port
(default 50051) in the `coordinator-service.yaml`. For bare-metal deployments,
the coordinator must bind to the Flight SQL port in addition to the gRPC
coordinator port. The `with_coordinator` URL targets the Flight SQL port; a
separate `with_grpc_coordinator` builder method is added for CLI and operator
use cases that target the gRPC port directly.

**Risk if deferred**

Deferring past R12 means the entire R13 Python API (`ks.Session.connect()`) has
no distributed mode. Python users would be limited to embedded/single-node
execution. Every subsequent release (R14 CDC, R17 LLM UDFs, R18 lakehouse) that
assumes cluster execution from Python would be unbuildable.

---

### ADR-12.4: SingleNodeBackend In-Process Coordinator Model

**Status**: DECIDED

**Problem statement**

`SingleNodeBackend` and `EmbeddedBackend` delegate to DataFusion identically
today. The design intent is a local coordinator + local executor in the same
process with full streaming semantics (keyed state, watermarks, barriers) but
no network round-trips or port binding. Without a real distinction, streaming
does not work in either mode. This is the foundational deployment mode for
local development and for unit tests that require the full operator lifecycle.

**Options**

- A. `SingleNodeBackend` as an alias for `EmbeddedBackend` with telemetry. No
  meaningful distinction; streaming remains unsupported.
- B. In-process coordinator + executor over `tokio::sync::mpsc` channels:
  `InProcessCoordinator` wraps the existing `Coordinator` struct with a channel
  transport adapter instead of tonic gRPC. `InProcessExecutor` uses the receive
  end of the same channels. No port binding; full streaming semantics (keyed
  state in `RedbStateBackend`, watermarks, window operators, barriers forwarded
  as `InProcessBarrierMsg` over a dedicated barrier channel).
- C. Real gRPC over loopback (`127.0.0.1:0`): starts a real coordinator and
  executor on OS-assigned ephemeral ports. Most realistic test of the distributed
  path but adds 100–500ms per test case due to gRPC handshake and process
  management.

**Recommendation**: Option B. In-process channels provide full streaming
semantics at zero port overhead. The `Coordinator` struct is reused unchanged —
only its transport adapter is replaced. This makes `SingleNodeBackend` a
faithful in-process proxy for the distributed execution model in unit tests
and local development.

**Decision**: Option B — `InProcessCoordinator` + `InProcessExecutor` with
`tokio::sync::mpsc` channel transport. Decided 2026-05-21. Barriers use a
dedicated barrier channel consistent with ADR-16.3's separation of data and
control channels. Implemented in R12 Sprint 6; reused by R16 streaming
certification tests and R13 `ks.Session` in embedded mode.

**Consequences**

The `Coordinator` and `Executor` structs must be refactored to accept a generic
`Transport` type parameter (or a trait object) so that the gRPC transport and
the in-process channel transport share the same logic. This refactor is bounded
to `krishiv-scheduler` and `krishiv-executor` and does not affect the public API.

**Risk if deferred**

Without `SingleNodeBackend` implementing real streaming, `Session::stream()` in
any local mode produces nothing. This blocks R13 Sprint 5 (asyncio-native
streaming) because there is no in-process streaming runtime to back the Python
`async for batch in stream.window()` loop.

---

### ADR-12.5: Embedded Streaming Execution Model

**Status**: DECIDED

**Problem statement**

`Session::stream(query)` in `ExecutionMode::Embedded` (the default mode) must
produce a live `Stream<StreamBatch>`. `EmbeddedBackend` currently has no
continuous operator loop. Three approaches exist for adding streaming support
without duplicating the operator logic that already exists in `SingleNodeBackend`
(per ADR-12.4).

**Options**

- A. DataFusion streaming plan: run the query as a `SendableRecordBatchStream`.
  Supports stateless queries only; cannot support keyed state, watermarks, or
  tumbling windows without reimplementing the entire streaming operator library
  on top of DataFusion.
- B. Dedicated `EmbeddedStreamRuntime`: a minimal runtime in `krishiv-runtime`
  running each operator as a Tokio task over `tokio::sync::mpsc` channels.
  Supports full streaming semantics but duplicates the core logic of
  `InProcessCoordinator` from ADR-12.4.
- C. Redirect streaming plans to `SingleNodeBackend`: `EmbeddedBackend` detects
  streaming plans (plans rooted at `StreamSourceOperator` or containing
  `WindowOperator` / `KeyByOperator`) and delegates to the inner
  `SingleNodeBackend`. Batch SQL plans continue through DataFusion directly.

**Recommendation**: Option C. It eliminates duplication: if ADR-12.4 is
implemented correctly, Option C is free. `EmbeddedBackend` becomes "DataFusion
for batch SQL + `SingleNodeBackend` for streaming." The detection heuristic is
plan-level, not query-text-level, so it is robust to SQL queries that contain
streaming semantics embedded in subqueries.

**Decision**: Option C — `EmbeddedBackend` redirects streaming plans to an inner
`Option<SingleNodeBackend>` initialized lazily on first streaming plan.
Decided 2026-05-21. The `is_streaming_plan(plan)` predicate is the detection
boundary; it lives in `krishiv-runtime` and is tested independently.

**Consequences**

Users who call `Session::new().stream(query)` (default embedded mode) get full
streaming semantics without specifying a deployment mode. This is the correct
ergonomics for local development: streaming "just works" in the default session.
The flip side: the first streaming call in a default session incurs the
`InProcessCoordinator` initialization latency (~10ms). Acceptable for local dev;
production workloads should use `ExecutionMode::Distributed` explicitly.

**Risk if deferred**

Without this redirect, `Session::stream()` in embedded mode returns an empty
stream or panics. R13's `ks.read_parquet().window().agg()` chain (Sprint 3)
would silently produce no output in local test runs, making the Python API
appear broken in the most common developer workflow.

---

### ADR-12.6: Scheduler God-File Decomposition

**Status**: DECIDED

**Problem statement**

`crates/krishiv-scheduler/src/lib.rs` is 7 971 lines and contains 47+ public
types spanning at least six independent concerns: job/stage/task lifecycle
(`JobRecord`, `StageRecord`, `TaskRecord`, `JobSnapshot`, `StageSnapshot`,
`TaskSnapshot`), executor registry (`ExecutorRegistry`, `ExecutorRecord`,
`ExecutorHealthSnapshot`), checkpoint coordination (`CheckpointCoordinator`,
`CheckpointCoordinatorState`), metadata persistence (`MetadataStore`,
`InMemoryMetadataStore`, `JsonFileMetadataStore`), leader election
(`LeaderElection`, `SingleNodeElection`), and the gRPC service adapters
(`CoordinatorExecutorTonicService`, `CoordinatorExecutorGrpcService`). The
file is too large to navigate safely; adding any new coordinator capability
risks merge conflicts with unrelated parallel work and makes the blast radius
of every edit unnecessarily large.

**Options**

- A. In-place `mod` files: keep `krishiv-scheduler` as one crate but split
  `lib.rs` into `src/coordinator.rs`, `src/metadata.rs`, `src/checkpoint.rs`,
  `src/registry.rs`, `src/election.rs`, `src/job.rs`, `src/grpc.rs`.
  `lib.rs` becomes a thin re-export file of ~80 lines. No public API change;
  no `Cargo.toml` change; compile and test commands are unchanged.
- B. Extract separate crates: create `krishiv-metadata` (MetadataStore family),
  `krishiv-election` (LeaderElection), `krishiv-checkpoint-coordinator`
  (CheckpointCoordinator). Strongest isolation; each crate has its own version
  and CI boundary. Migration cost is high: every import path changes across
  six other crates, and the gRPC adapters currently inline job/stage/task types
  from the same file, so the extraction order is non-trivial.
- C. Feature-gated modules: wrap groups of types behind Cargo features so that
  unit tests can compile a leaner subset. No file reorganization; just adds
  `#[cfg(feature = "...")]` attributes. Does not address the navigation
  problem; the file stays 7 971 lines.

**Recommendation**: Option A. Module files give 95% of the benefit (navigability,
independent diff, parallel editing) at near-zero migration cost. Option B is the
right end state but belongs to a dedicated crate-restructuring release, not a
stability sprint. Option C is theatre.

**Decision**: Option A — split `lib.rs` into module files within `krishiv-scheduler`.
`lib.rs` declares `pub mod coordinator`, `pub mod metadata`, `pub mod checkpoint`,
`pub mod registry`, `pub mod election`, `pub mod job`, `pub mod grpc`; each module
file re-exports only the types currently exposed in the public API. Public paths
are unchanged. Decided 2026-05-21. Implemented in R12 Sprint 7.

**Consequences**

`lib.rs` drops from 7 971 to ~120 lines. Every new structural addition to the
coordinator has a clear home. Parallel sprint work on metadata (Sprint 4) and
checkpoint (Sprint 6) can proceed without conflicting diffs.

**Risk if deferred**

Every sprint that adds coordinator capability (SQLite metadata store in Sprint 4,
InProcessCoordinator in Sprint 6) is merged into the same file, compounding the
conflict surface. Deferring past R12 makes the eventual split a full-sprint effort
rather than a one-day mechanical move.

---

### ADR-12.7: ExecutionBackend Async Migration

**Status**: DECIDED

**Problem statement**

`ExecutionBackend::execute()` in `crates/krishiv-runtime/src/lib.rs:258` is
declared synchronous:

```rust
fn execute(&mut self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport>;
```

The callers — `Session::collect()`, `Session::explain()`, `Session::stream()` —
are all `async fn` running inside a Tokio runtime. Any non-trivial implementation
of `execute()` must call async Tokio primitives (DataFusion `execute_logical_plan`,
the `InProcessCoordinator` channel send/recv, or the `FlightSqlClient` stub). The
current workaround is to wrap these calls in `futures::executor::block_on()`, which
panics when called inside a Tokio async context — this is the root cause of P0.19
(`nested Tokio runtime panic`) identified in the R11 audit.

**Options**

- A. Keep sync signature; use `tokio::task::block_in_place` in each impl: wrapping
  the async body inside `block_in_place(|| Handle::current().block_on(...))`.
  Works in `multi_thread` Tokio runtime; panics in `current_thread` runtime (used
  in tests and in embedded mode). Defers the trait contract fix indefinitely;
  every new backend impl must remember the `block_in_place` pattern.
- B. `async-trait` crate: annotate the trait with `#[async_trait]` and change the
  signature to `async fn execute(...)`. `async-trait` erases to
  `Pin<Box<dyn Future<...> + Send + '_>>` under the hood. All three backend impls
  become naturally async. Requires `async-trait` in `krishiv-runtime`'s
  `Cargo.toml`; adds one `Box` allocation per call (acceptable for a control-plane
  operation that submits a query, not a hot data path).
- C. Return a `Pin<Box<dyn Future<...>>>` directly (manual, no crate): same semantics
  as Option B without the `async-trait` dependency. Identical runtime behaviour;
  more verbose trait definition (`fn execute(...) -> BoxFuture<'_, RuntimeResult<...>>`).

**Recommendation**: Option B. `async-trait` is the de-facto standard pattern in
the Rust async ecosystem (used by `tower`, `axum`, `datafusion`). The `Box`
allocation is immaterial on a query-submission boundary. Option C achieves the
same result but forces every impl to write `Box::pin(async move { ... })` manually.

**Decision**: Option B — `#[async_trait]` on `ExecutionBackend`. Decided 2026-05-21.
Migration: add `async-trait = "0.1"` to `krishiv-runtime/Cargo.toml`; annotate
the trait; update all three impls (`EmbeddedBackend`, `SingleNodeBackend`, and
the new `DistributedBackend` from ADR-12.3). Remove all `block_on` / `block_in_place`
workarounds. Implemented in R12 Sprint 7 before ADR-12.3/12.4/12.5 work begins.

**Consequences**

Callers in `Session` can `await` the backend call directly. The nested-runtime
panic (P0.19) is eliminated structurally rather than papered over per call site.
All future `ExecutionBackend` implementors automatically get the correct async
contract.

**Risk if deferred**

Any `DistributedBackend` or `InProcessCoordinator` implementation that wraps
`FlightSqlClient` / Tokio channels will hit the nested-runtime panic immediately
on the first test. ADR-12.3 and ADR-12.4 work cannot ship if this is not resolved
first.

---

### ADR-12.8: PhysicalPlan → Real Operator Wiring

**Status**: DECIDED

**Problem statement**

`PhysicalPlan` (in `crates/krishiv-plan/src/lib.rs:387`) carries only a name,
kind (`Batch`/`Streaming`/`Incremental`), and a `Vec<PlanNode>`. `PlanNode`
carries a string label and an optional `NodeOp` enum. `lower_to_physical()` in
`crates/krishiv-exec/src/lib.rs:58` produces placeholder nodes labelled
`"physical:{original_id}"` — no operator parameters, no type information. When
`ExecutionBackend::execute(plan)` is called, both `EmbeddedBackend` and
`SingleNodeBackend` ignore the plan content entirely and return
`ExecutionReport::accepted()`. The real operator structs (`TumblingWindowOperator`,
`SlidingWindowOperator`, `HashJoin`, `StreamTableJoin`, etc. in `krishiv-exec`)
exist but are never instantiated from a plan. This means no query ever executes
any data logic — the engine is a no-op behind a typed façade.

**Options**

- A. Extend `NodeOp` variants: add one variant per operator type to the existing
  `NodeOp` enum in `krishiv-plan` (`NodeOp::TumblingWindow(WindowConfig)`,
  `NodeOp::HashJoin(JoinConfig)`, `NodeOp::Scan(ScanConfig)`, etc.). `lower_to_physical()`
  maps each `LogicalPlan` node's `NodeOp` to the corresponding physical `NodeOp`
  variant. `execute()` pattern-matches on `NodeOp` to construct real operator
  structs. Single source of truth; no new crates; no dependency cycle (operator
  config types live in `krishiv-plan`, operator impls live in `krishiv-exec`).
- B. `PhysicalOperatorTree` in `krishiv-exec`: `lower_to_physical()` returns a
  new type `PhysicalOperatorTree` (defined in `krishiv-exec`) that holds actual
  operator structs. `ExecutionBackend::execute()` signature changes to accept this
  tree type. Avoids putting config structs in `krishiv-plan`; cleaner separation.
  Requires changing `ExecutionBackend` trait signature (which is already being
  changed in ADR-12.7) and every call site in `Session`.
- C. String-encoded operator parameters: encode operator config in the `PlanNode`
  label field as a JSON or DSL string; `execute()` parses the string to reconstruct
  config. Avoids any schema change; maximally brittle; untestable without a full
  parse/serialize cycle.

**Recommendation**: Option A. `NodeOp` already exists precisely for this purpose
(it has placeholder variants showing the intent). Adding real variants to it is
the intended path. Option B adds a second plan type that `Session` must manage and
is only justified if `krishiv-plan` becomes a stable public API crate with external
consumers that cannot accept operator-specific config structs — not a current
constraint.

**Decision**: Option A — extend `NodeOp` with concrete operator variants.
Decided 2026-05-21. Migration order: (1) define config structs for the five most
critical operators (`ScanConfig`, `FilterConfig`, `ProjectConfig`, `TumblingWindowConfig`,
`HashJoinConfig`) in `krishiv-plan`; (2) extend `NodeOp` with corresponding variants;
(3) update `lower_to_physical()` to populate them from `LogicalPlan`; (4) update
`execute()` in `EmbeddedBackend` to dispatch to DataFusion for batch ops and to
`SingleNodeBackend` for streaming ops using the populated `NodeOp`. Implemented in
R12 Sprint 7.

**Consequences**

For the first time, a `Session::collect("SELECT 1")` query will execute real
DataFusion logic rather than returning `ExecutionReport::accepted()`. All subsequent
releases (R13 Python, R14 CDC, R16 exactly-once) can rely on queries actually
running. The `lower_to_physical()` / `NodeOp` interface becomes the stable
plan–executor contract, meaning future operators only need a new `NodeOp` variant
and a corresponding impl in `execute()`.

**Risk if deferred**

Every R13 Python integration test that calls `session.sql("SELECT ...")` and
checks the result will fail because the result is always `ExecutionReport::accepted()`
regardless of the query. The Python API becomes untestable end-to-end. This bottleneck
is the single most critical gap between the current codebase and a working engine.

---

## R13: Python-First Streaming API

### ADR-13.1: Python/Tokio Async Bridge

**Status**: DECIDED

**Problem statement**

Krishiv's Rust runtime uses Tokio. Python users call Krishiv from both
synchronous scripts and from `asyncio` coroutines. The bridge between Python's
asyncio event loop and Tokio's runtime must be safe, correct, and ergonomic.
Three architectures exist; the choice affects every async Python API surface in
R13 and later releases.

**Options**

- A. `pyo3-asyncio` crate: provides macros and utilities for bridging PyO3
  async functions with both `asyncio` and Tokio. Handles event loop detection,
  coroutine wrapping, and `await` translation. Adds a compile-time dependency;
  the crate has not had a release since 2023 and lags PyO3 API changes — first
  PyO3 minor upgrade will break it.
- B. Manual `Future` bridging: implement a custom `TokioRuntime` singleton in
  `krishiv-python` that `spawn`s Tokio tasks from synchronous Python calls and
  uses `tokio::sync::oneshot` channels to return results. Full control; no external
  crate dependency; in practice this reimplements what pyo3-asyncio does without
  the test suite. The hard part — `__await__` protocol on the Python side and waker
  registration — is not avoided by implementing manually.
- C. Dedicated Tokio runtime thread with `Python::attach()` per batch delivery:
  `ks.Session` holds an `Arc<TokioRuntimeThread>` (a real OS thread with its own
  `Runtime::new()`) started at `connect_async()` and joined at `__del__`. When a
  batch is ready inside Tokio, `spawn_blocking` moves GIL acquisition off Tokio
  worker threads; the batch is constructed inside `Python::attach()` and posted to
  the user's asyncio loop via `asyncio.run_coroutine_threadsafe`. This is the
  production pattern used by `polars`, `delta-rs` Python bindings, and `lance`.

**Recommendation**: Option C. `pyo3-asyncio` (A) is abandoned; manual bridging
(B) reimplements it without the test coverage. The dedicated runtime thread
approach (C) keeps Tokio workers free for I/O, never holds the GIL on a worker
thread, and is battle-tested in the broader PyO3 ecosystem. Implementation note:
hold a `Arc<AtomicBool>` shutdown flag and check it inside every `spawn_blocking`
before calling `Python::attach()` — this prevents a panic when Python GC drops
the `Session` while a delivery is in flight.

**Decision**: Option C — dedicated Tokio runtime thread with `spawn_blocking` +
`Python::attach()` per batch delivery. Decided 2026-05-21. Implementation begins
in R13 Sprint 1 (`krishiv-python` crate scaffold); asyncio-native streaming API
wired in Sprint 5.

**Consequences**

The choice determines the implementation pattern for every `async def` in
the Python API (`session.sql_async()`, `stream.collect_async()`,
`live.run_async()`). Switching after R13 ships would require changing every
Python async call site and invalidating third-party integrations.

**Risk if deferred**

Without a resolved async bridge, no async Python API can be implemented.
R13's asyncio-native streaming and Jupyter integration are the primary
differentiators from the R8 beta Python API. Deferring past Sprint 1 compresses
the sprint window for all async work.

---

### ADR-13.2: Python Schema Metaclass

**Status**: DECIDED

**Problem statement**

`ks.Schema` subclasses declare column types as class-level annotations
(`class OrderEvent(ks.Schema): order_id: str`). At runtime, Krishiv must
extract these annotations, map them to Arrow data types, and enforce schema
validation on incoming data. Three Python metaprogramming patterns are available
for this.

**Options**

- A. `__init_subclass__`: when a user defines a subclass of `ks.Schema`, Python
  calls `ks.Schema.__init_subclass__` on the new class. The base implementation
  reads `__annotations__`, builds the Arrow schema, and stores it as a class
  attribute. Simple; no metaclass required; works with `dataclasses` and standard
  inheritance. Requires Python 3.6+.
- B. Metaclass: `class Schema(metaclass=SchemaMeta)`. The metaclass's `__new__`
  method intercepts class creation and processes annotations. More powerful (can
  control `__repr__`, `__eq__`, and field ordering); harder to compose with other
  metaclasses — if users mix `ks.Schema` with another metaclass-based library,
  metaclass conflicts arise and are hard to debug.
- C. Dataclass-based: annotate the base `Schema` class with `@dataclasses.dataclass`;
  user subclasses are also dataclasses automatically. Use `dataclasses.fields()`
  to introspect the schema. Loses flexibility for custom field descriptors such as
  `ks.DateTimeUtc` with timezone metadata or `ks.Json` with schema validation.

**Recommendation**: Option A (`__init_subclass__`). It matches the roadmap API
(`class MySchema(ks.Schema): …`) without metaclass composition hazards. The
type mapping is defined once in `krishiv-python/src/schema.rs` and called from
the PyO3 `__init_subclass__` binding: `str → Utf8`, `int → Int64`,
`float → Float64`, `bool → Boolean`, `datetime → TimestampMicrosecond(UTC)`,
`bytes → LargeBinary`, `Optional[T] → nullable(T)`. Schema is resolved at
class-definition time, making mismatches startup errors rather than runtime
errors at the first batch.

**Decision**: Option A — `__init_subclass__` hook with PyO3-backed Arrow schema
resolution. Decided 2026-05-21. The type mapping table lives in
`krishiv-python/src/schema.rs`; the Python `ks.Schema` base class is implemented
in R13 Sprint 2.

**Consequences**

The schema metaclass choice determines the internal Arrow type mapping code,
the Python error messages on schema mismatch, and the shape of `.pyi` type
stubs. Changing this after R13 ships would break user-defined schema classes.

**Risk if deferred**

Without a resolved schema model, `ks.read_kafka(schema=OrderEvent)` and
`ks.read_parquet(schema=OrderEvent)` cannot be implemented. All schema-declared
sources and sinks in R13 depend on this decision.

---

## R14: Incremental Computation & CDC Lakehouse

### ADR-14.1: Incremental View Delta Storage

**Status**: DECIDED

**Problem statement**

Live table incremental refresh requires storing the delta (changed rows) between
refresh cycles. The storage format for these deltas determines correctness,
performance, and integration complexity with the Iceberg and CDC connectors.

**Options**

- A. Iceberg equality-delete files: represent deleted or updated rows as
  equality-delete manifests in the live table's own Iceberg snapshot. No separate
  storage system required. Reading the live table merges base data with equality
  deletes at scan time — O(delete file count) scan overhead that grows unboundedly
  between compactions. Not suitable for > 10k CDC events/second; Iceberg's own
  documentation recommends equality-delete files only for low-frequency updates.
- B. Internal Krishiv change log: CDC deltas are appended to a `redb` embedded
  log (embedded/single-node mode) or a Kafka compacted topic (distributed mode).
  Decouples CDC ingestion throughput from Iceberg commit frequency. A background
  compaction task (configurable interval, default 60s) merges the log into the
  base Iceberg table and truncates the log. `redb` writes survive coordinator
  restart because the database file is on durable local storage and is included
  in the coordinator checkpoint.
- C. Delta Lake merge files: represent incremental changes as Delta Lake
  transaction log entries. Reuses R18 Delta Lake infrastructure but forces the
  lakehouse to Delta format for a feature whose primary output is an Iceberg
  table — cross-format coupling that contradicts the Iceberg-first strategy.

**Recommendation**: Option B. The internal change log decouples the two
fundamentally different rates: CDC ingestion (potentially 100k events/second)
from Iceberg commit frequency (coarse, every 30–60s). `redb` for embedded mode
avoids any external service dependency; a Kafka compacted topic for distributed
mode reuses infrastructure that is already required for the CDC source.
Compaction is time-based (default 60s) with a configurable event-count high-water
mark for bursty workloads. The `redb` database is checkpointed as part of the
coordinator checkpoint so it survives restarts.

**Decision**: Option B — internal Krishiv change log (`redb` for embedded,
Kafka compacted topic for distributed). Decided 2026-05-21. The `CREATE LIVE TABLE`
physical planner (R14 Sprint 1) must reference the `ChangeLogStore` trait from the
start; the `redb` implementation is delivered in Sprint 1, Kafka implementation
in Sprint 3.

**Consequences**

Option A (equality-delete files) couples the live table refresh closely to
the Iceberg snapshot model and requires implementing compaction scheduling.
Option B (redb) introduces a second stateful store on the coordinator, which
must be included in checkpoint/restore.

**Risk if deferred**

Without a delta storage decision, the `CREATE LIVE TABLE ... REFRESH ON CHANGE`
plan cannot be implemented. This blocks all incremental computation work in R14
and the CDC-to-Iceberg exactly-once story.

---

### ADR-14.2: Memoization Key Storage

**Status**: DECIDED

**Problem statement**

Function-level memoization (`@ks.transform(memo=True)`) requires a persistent
store mapping input content hashes to output rows. The store must survive
coordinator restarts (so that unchanged rows are not re-processed after a
pipeline restart) and must be accessible from executor processes.

**Options**

- A. `redb` (embedded key-value, per-coordinator): each coordinator process holds
  a `redb` database mapping `(namespace, function_name, input_hash) → output_row`.
  Fast local reads; no network round-trips; not shared across coordinators in a
  multi-region setup (R19).
- B. Redis (distributed): a Redis instance (or Redis Cluster) shared across all
  coordinators. Fast reads across nodes; requires an additional Redis deployment;
  adds operational complexity; a Redis outage causes all memoized transforms to
  re-run (degraded correctness, not a crash).
- C. S3-backed manifest: memoization keys and output row locations are written
  as Parquet manifest files on S3. Durable; no additional service; slow for
  high-frequency lookups (S3 GET latency ~10ms vs. `redb` ~0.1ms). Suitable
  for batch pipelines where memoization lookups are infrequent.

**Recommendation**: Option A. Covering the function source text in the hash is
a correctness requirement — a logic change must invalidate cached results.
`inspect.getsource()` is the standard Python mechanism; its output is hashed on
the Python side and passed to Rust as a `[u8; 32]`. Storage in `redb` (embedded)
or a Kafka compacted topic (distributed) keeps the dependency graph flat — no
Redis deployment required in any mode. The `redb` memo store must be benchmarked
at 1M+ keys before Sprint 5 declares `rag_index()` stable.

**Decision**: Option A — SHA-256 of `(function_source_bytes || schema_json ||
data_bytes)`, stored in `redb` under the `memo:` key namespace. Decided
2026-05-21. Implemented in R14 Sprint 2; shared with R17's `ks.rag_index()`
incremental re-indexing path.

**Consequences**

Option A (redb) makes memoization coordinator-local; multi-region deployments
must replicate the memo store or accept cache misses on failover. Option B
(Redis) adds an operational dependency. Option C (S3) limits memoization to
batch workloads.

**Risk if deferred**

Without a resolved memo store, the `@ks.transform(memo=True)` decorator cannot
be implemented correctly. CocoIndex-competitive incremental AI pipelines —
Krishiv's primary R14 differentiator — depend on this feature.

---

## R15: Spark SQL & Ecosystem Compatibility

### ADR-15.1: Spark Connect Implementation Scope

**Status**: PROPOSED

**Problem statement**

The Spark Connect protocol covers 200+ proto message types representing the full
Spark SQL and DataFrame API surface. Implementing full coverage in R15 is not
feasible. The scope of Spark Connect support must be bounded to a set that
covers the most common enterprise migration use cases while remaining achievable
in one release.

**Options**

- A. Full Spark Connect proto coverage (200+ message types): correct for any
  PySpark code; requires 3–6 months of protocol implementation work. Not
  feasible in R15's sprint window.
- B. TPC-H subset (22 benchmark queries): implement only the Spark SQL plan
  nodes and DataFrame operations required to execute TPC-H Q1–Q22. Covers the
  most common analytical patterns (filter, join, aggregate, window, sort). Does
  not cover UDFs, ML functions, or streaming plans. A Sail-style approach:
  focus on the highest-value 20% of the API surface.
- C. Sail-style DataFusion transpilation: instead of implementing the Spark
  Connect proto server, parse PySpark plans and transpile them to DataFusion
  logical plans at the client level. Requires a transpilation layer in the
  Python client; the coordinator never speaks Spark Connect directly. Faster
  to implement but less compatible with tools that speak native Spark Connect
  (Databricks SDK, dbt Spark adapter).

**Recommendation**: Option B (TPC-H subset). Deliver the 22 TPC-H queries
correctly and document the coverage scope clearly. This satisfies the primary
enterprise migration use case (analytical SQL) and provides a concrete acceptance
gate. Extend coverage in subsequent point releases based on user demand.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

Users with PySpark code that uses UDFs, ML functions, or Structured Streaming
must wait for extended coverage. The acceptance gate becomes TPC-H Q1–Q22
correctness, not full API parity. The dbt adapter (ADR-15.2) must use Flight
SQL rather than Spark Connect to avoid coupling to the limited Spark Connect
implementation.

**Risk if deferred**

Attempting full Spark Connect coverage without bounding scope will make R15
late and under-deliver on every feature. A clear scope decision enables focused
sprint planning.

---

### ADR-15.2: dbt Adapter Transport

**Status**: PROPOSED

**Problem statement**

The Krishiv dbt adapter must connect to the coordinator to submit SQL queries
and retrieve results. Two transport options exist: implement the adapter over
Flight SQL (already available in Krishiv from R8.1) or implement a dedicated
dbt protocol server.

**Options**

- A. Flight SQL: the dbt adapter connects to `krishiv-flight-sql` using the
  Arrow Flight SQL JDBC/ODBC-compatible protocol. DBeaver, Tableau, and any
  tool with a Flight SQL driver can use the same endpoint. No new server code
  required. Flight SQL does not natively support `INFORMATION_SCHEMA` queries
  required by dbt's `get_columns_in_relation` macro — these must be emulated.
- B. Dedicated dbt protocol server: implement a lightweight HTTP server in
  `krishiv-scheduler` that speaks the dbt Adapter API (a simple JSON RPC
  interface used by dbt-core). More control over response format; required
  by dbt-core if the adapter is a "remote" type. Adds a new server component
  that must be deployed and maintained separately from Flight SQL.

**Recommendation**: Option A (Flight SQL). Reuse the existing Flight SQL
endpoint. Implement `INFORMATION_SCHEMA` emulation in `krishiv-sql` to satisfy
dbt's schema introspection queries. This minimises new server-side code and
keeps the connector surface unified.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

The dbt adapter connects via the Arrow Flight SQL driver; users must install
the `dbt-krishiv` adapter package and configure the Flight SQL endpoint in
`profiles.yml`. `INFORMATION_SCHEMA` emulation must be implemented in `krishiv-sql`
before the dbt adapter can be tested end-to-end.

**Risk if deferred**

Without a transport decision, the dbt adapter cannot be implemented. The
`profiles.yml` format, connection parameters, and adapter test suite all depend
on the chosen transport.

---

## R16: Advanced Stateful Streaming & Exactly-Once

### ADR-16.1: CEP Engine Scope for R16

**Status**: PROPOSED

**Problem statement**

Complex Event Processing (CEP) requires matching event sequences against
temporal patterns. Full NFA-based CEP engines (as in Apache Flink) support
arbitrary patterns with Kleene closure, negation, and time constraints. A
simpler state machine covers the primary use cases (`begin`, `followed_by`,
`within`) at a fraction of the implementation cost.

**Options**

- A. Full NFA (Non-deterministic Finite Automaton) implementation: supports
  Kleene operators (`one_or_more`, `optional`), negation (`not_followed_by`),
  and unlimited branching. Correct for all CEP patterns. 2–3 months of
  implementation and correctness testing. Not feasible within R16's sprint window
  alongside temporal joins and exactly-once certification.
- B. Simple state machine: implement `begin`, `followed_by`, `where` (per-event
  filter), `within` (time window), and `not_followed_by` (optional, common
  pattern). Covers fraud detection, session detection, and sequence anomaly
  patterns — the 80% case for enterprise streaming. Kleene operators deferred
  to R16.1 or R17.
- C. Embed an existing CEP library: use a Rust CEP crate if one exists. At the
  time of planning, no production-ready Rust CEP library covers the required
  feature set; this option is not available.

**Recommendation**: Option B (simple state machine) for R16. Deliver
`begin → followed_by → where → within` correctly with test coverage before
adding Kleene and negation. Document the unsupported patterns clearly so users
know what to expect.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

CEP patterns requiring Kleene closure (`one_or_more`, `optional`) are not
supported in R16. The fraud-detection acceptance test must be designed using
only `begin/followed_by/within` patterns.

**Risk if deferred**

Attempting full NFA scope in R16 alongside temporal joins and exactly-once
certification will make all three features late or incomplete. The sprint plan
explicitly budgets for Option B scope.

---

### ADR-16.2: State Rescaling Algorithm

**Status**: DECIDED

**Problem statement**

When a job is restored with a different parallelism (e.g., 4 → 8 partitions),
the keyed state must be redistributed across the new task slots. The algorithm
choice affects correctness (no key lost, no key duplicated), performance
(time to redistribute), and implementation complexity.

**Options**

- A. Consistent hashing: keys are assigned to partitions using a consistent
  hash ring. When the number of partitions changes, only a fraction of keys
  migrate (O(K/N) where K is key count, N is partition count change). Fast;
  no full state scan required. Risk: if the original job did not use consistent
  hashing for key assignment, the rescaling breaks the keyed-state contract.
  Requires the original partitioning strategy and the rescaling strategy to be
  the same hash function.
- B. Key-group rescaling (Flink-style): each key is assigned to a key-group
  (0 to max_parallelism-1) based on `hash(key) % max_parallelism`. Rescaling
  redistributes whole key-groups rather than individual keys. The key-group
  assignment is stable regardless of the current parallelism as long as
  `max_parallelism` is fixed at job creation. Clean, proven in production (Flink
  uses this approach). Requires recording `max_parallelism` in the checkpoint
  metadata.
- C. Broadcast + filter: on restore, all state is broadcast to all new task
  slots; each task filters to keep only the keys that hash to its partition.
  Simple to implement; requires reading the full state on every restore (O(total
  state)). Not feasible for large state sizes; not a production approach.

**Recommendation**: Option B (key-group rescaling). It is the proven production
approach and is correct for arbitrary parallelism changes as long as
`max_parallelism` is recorded at job creation. Implement `max_parallelism` as
a `JobSpec` field (default 128) before implementing the rescaling algorithm.

**Decision**: Option B — key-group hashing with 32768 key groups and a fixed
`max_parallelism` recorded at job creation. Decided 2026-05-21. The `StateBackend`
trait must expose `key_group_range() -> RangeInclusive<u16>` from Sprint 1 of
R16. Checkpoint paths are named `{job_id}/{epoch}/{kg_start}-{kg_end}.sst`, not
by task ID. `max_parallelism` defaults to 128 in `JobSpec`; jobs must not be
rescaled beyond `max_parallelism` (the coordinator enforces this at submission).

**Consequences**

`max_parallelism` must be a required (or defaulted) field in `JobSpec`. All
checkpoints must include `max_parallelism` in their metadata. Jobs created
before this field was added cannot be rescaled (they lack `max_parallelism`
metadata). The 32768 group count allows exact 1:1 task-to-group assignment at
max parallelism and ≥ 64 groups per task at typical parallelism (8–512 tasks).

**Risk if deferred**

Implementing rescaling with Option A (consistent hashing) when the job's
original partitioning was modulo-based produces incorrect state redistribution —
some keys are assigned to the wrong task slot after restore, causing silent data
corruption in stateful aggregations.

---

### ADR-16.3: gRPC Barrier Message Format

**Status**: DECIDED

**Problem statement**

Full exactly-once across distributed executors requires a checkpoint barrier
that flows through the execution graph via gRPC. The barrier must carry enough
information for each operator to know which epoch to checkpoint, and for the
coordinator to know when all operators have acknowledged. The protobuf schema
for barrier injection, forwarding, and acknowledgment has not been defined.

**Options**

- A. Minimal barrier: `Barrier { epoch: u64, job_id: String }`. Operators
  forward this unchanged; coordinator collects acks. Simple; does not carry
  alignment information for operators with multiple upstream edges — joins and
  merges receive barriers from multiple upstreams and cannot correctly align
  without knowing how many to wait for.
- B. Aligned barrier: `Barrier { epoch: u64, job_id: String, checkpoint_id: String, barrier_kind: BarrierKind, timestamp_ms: i64 }` sent over a dedicated `BarrierService` bidirectional streaming RPC. Operators with multiple upstream edges track received-barrier counts per epoch and forward only after all upstream barriers for the same epoch are received. Correct for joins and merges; the dedicated RPC ensures barrier delivery is never blocked by data-channel backpressure.
- C. Full Flink-compatible barrier: includes alignment mode (`AT_LEAST_ONCE` vs
  `EXACTLY_ONCE`), channel state sizes, and recovery metadata. Correct and
  extensible; significantly more complex; requires a full barrier alignment state
  machine in each operator. Over-engineered for R16 scope.

**Recommendation**: Option B with a dedicated `BarrierService` RPC. Option A
fails for any topology with joins or fan-in merges. Option C is over-engineered
for the R16 certification test topology. The dedicated RPC (not piggybacked on
the data or heartbeat channel) ensures barrier delivery is unaffected by data
channel backpressure — exactly the condition where checkpoints matter most.
The `BarrierAck` reply carries an optional `StateHandle` so the coordinator
knows each task's checkpoint URI before declaring the epoch complete.

**Decision**: Option B — aligned `CheckpointBarrier` proto over a dedicated
`BarrierService` bidirectional RPC. Decided 2026-05-21. Proto schema:

```protobuf
message CheckpointBarrier {
  uint64 epoch = 1;
  string job_id = 2;
  string checkpoint_id = 3;
  BarrierKind kind = 4;   // CHECKPOINT or SAVEPOINT
  int64 timestamp_ms = 5;
}
message BarrierAck {
  uint64 epoch = 1;
  string job_id = 2;
  string task_id = 3;
  optional StateHandle state_handle = 4;
}
service BarrierService {
  rpc BarrierStream(stream CheckpointBarrier) returns (stream BarrierAck);
}
```

This proto must be committed to `krishiv-proto` before R16 Sprint 1 subtask S1.2.

**Consequences**

The barrier proto schema is part of the `krishiv-proto` public wire format.
Once committed, changing it requires a proto version bump and a migration step
in the checkpoint protocol. This decision has long-lived consequences.

**Risk if deferred**

Without a defined barrier format, the gRPC barrier transport (replacing the
R6 in-process simulation) cannot be implemented. This blocks exactly-once
Kafka→Kafka and Kafka→Parquet certification, which are the primary acceptance
gates of R16.

---

## R17: AI/ML Native Data Platform

### ADR-17.1: LLM UDF Execution Isolation

**Status**: PROPOSED

**Problem statement**

LLM UDFs call external APIs (OpenAI, Anthropic) or run local models
(HuggingFace Transformers). API calls are blocking I/O; local model inference
is CPU/GPU intensive. Both must run in a Tokio executor without blocking
async worker threads. Additionally, a panicking UDF must not crash the streaming
executor process.

**Options**

- A. `spawn_blocking` for API-backed UDFs: wrap the HTTP API call in
  `tokio::task::spawn_blocking`. Safe for Tokio; adds one thread-hop per
  inference call. Panic in the spawned thread is caught by Tokio's
  `JoinHandle::await` and returned as an error — the executor process does not
  crash. For local model inference (HuggingFace), `spawn_blocking` is also
  safe; a dedicated thread pool (Rayon) can be used for CPU-bound inference.
- B. Subprocess isolation: run each UDF in a separate subprocess that communicates
  via Arrow IPC over stdin/stdout or Unix sockets. A panicking or crashing
  subprocess does not affect the executor. Adds subprocess spawn overhead
  (50–200ms per batch). Correct for untrusted or third-party UDFs; overly
  conservative for trusted first-party LLM API calls.
- C. In-process async for API-backed UDFs: use `reqwest` (already async) directly
  on a Tokio worker thread. No thread-hop; maximum throughput for API calls.
  Risk: if the reqwest call blocks (e.g., on a DNS timeout), it starves the
  Tokio worker thread. Additionally, a panic in the UDF closure (e.g., on
  unexpected API response parsing) crashes the executor.

**Recommendation**: Option A for API-backed UDFs (spawn_blocking with a dedicated
thread pool for rate-limit management) and Option A for local model inference
(Rayon thread pool, separate from Tokio's). Option B is reserved for untrusted
third-party UDFs that users install without Krishiv review.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

All LLM UDFs run on a thread pool, not on Tokio worker threads. The thread pool
size is configurable via `LlmUdfConfig.thread_pool_size`. Rate limiting is
implemented inside the spawned closure using a shared `Arc<Semaphore>`.

**Risk if deferred**

Option C (in-process async without spawn_blocking) blocks Tokio workers on DNS
and connection timeouts, causing backpressure cascades across all operators in
the executor. This is the same defect class as P0.4 (blocking I/O in async
contexts), which caused a P0 audit finding in R12.

---

### ADR-17.2: Vector Store Sink Consistency

**Status**: PROPOSED

**Problem statement**

Vector store sinks (Qdrant, Pinecone, pgvector, Weaviate) do not support
two-phase commit. If a streaming executor fails mid-batch after writing some
embeddings to the vector store but before committing the Kafka offset, the
embeddings are written but the offset is not advanced. On restart, the same
embeddings are generated and written again, producing duplicate vectors.

**Options**

- A. At-least-once: accept that duplicate embeddings may exist in the vector
  store after recovery. For embedding-based semantic search, duplicate vectors
  reduce search quality but do not cause data corruption. Simple to implement;
  no idempotency key required.
- B. Idempotent upsert with epoch key: include the checkpoint epoch number and
  the source row's primary key in each vector store entry (e.g., as metadata
  fields `_krishiv_epoch` and `_krishiv_row_id`). On retry, use the vector
  store's upsert API (if available) to overwrite the previous embedding for
  the same `_krishiv_row_id`. Requires the vector store to support upsert by
  a user-defined key (Qdrant, Pinecone, and pgvector all support this; Weaviate
  has an optional ID mode).
- C. Pre-flight duplicate check: before writing, query the vector store for the
  existence of each embedding by `_krishiv_row_id`. Skip writes for IDs that
  already exist. Correct; adds read-before-write latency (N reads for a batch
  of N embeddings). Not feasible at high throughput.

**Recommendation**: Option B (idempotent upsert). Include `_krishiv_epoch` and
`_krishiv_row_id` as mandatory metadata fields in all vector store sink writes.
Use the vector store's native upsert API. For vector stores without upsert
support (rare), fall back to Option A with a logged warning.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

All vector store sink connectors must accept a `upsert_key` field in their
configuration. The `_krishiv_epoch` and `_krishiv_row_id` metadata fields are
reserved in all vector store collections managed by Krishiv.

**Risk if deferred**

Choosing Option A (at-least-once) results in duplicate embeddings in production
vector stores after every executor failure or rolling upgrade. Enterprise users
building RAG applications will observe degraded search quality that is difficult
to diagnose without inspecting the vector store directly.

---

## R18: Storage Format Unification & Time Travel

### ADR-18.1: delta-rs Tokio Runtime Integration

**Status**: PROPOSED

**Problem statement**

`delta-rs` exposes an async API but internally creates its own Tokio runtime.
Calling any `delta-rs` async method from within Krishiv's existing Tokio
multi-thread scheduler will panic with "cannot start a runtime from within a
runtime" — the same defect class as P0.3 (fixed in R12). `delta-rs` does not
currently expose a runtime-injection API that would allow sharing Krishiv's
`Handle::current()`.

**Options**

- A. Wrap all `delta-rs` calls in `tokio::task::spawn_blocking`: the blocking
  thread has no surrounding runtime, so `delta-rs`'s internal `Runtime::new()`
  does not panic. Adds one thread-hop per operation (estimated 0.5–2ms). All
  Delta reads, writes, and MERGE operations go through this path.
- B. Configure `delta-rs` to use `Handle::current()` by patching its runtime
  initialisation. Requires `delta-rs` to expose a `RuntimeConfig` API that does
  not exist in any released version. Not feasible without forking `delta-rs`.
- C. Use `delta-rs`'s synchronous API in a Rayon thread pool. The sync API
  covers reads fully but does not cover the full write path (merge, schema
  evolution) in current `delta-rs` releases.

**Recommendation**: Option A (spawn_blocking). The only universally safe option
across all `delta-rs` operations. Document the thread-hop latency as a known
performance tradeoff. Revisit if `delta-rs` publishes a runtime-injection API.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

All `delta-rs` calls in `krishiv-lakehouse` are wrapped in `spawn_blocking`.
Delta checkpoint writes will be measurably slower than native async Arrow writes.
A CI lint check (grep for `await` on `delta` / `deltalake` call sites outside
`spawn_blocking`) must be added to prevent regressions.

**Risk if deferred**

Any `delta-rs` async call made directly on a Tokio worker thread will panic in
production. The first `df.write_delta()` call from a user script would crash
the executor process.

---

### ADR-18.2: MERGE INTO SQL Implementation Strategy

**Status**: PROPOSED

**Problem statement**

DataFusion does not support `MERGE INTO` DML as a built-in plan node.
Implementing a fully general `MERGE INTO` as a DataFusion `LogicalPlan` variant
with a corresponding physical operator is a 2–3 month effort. However,
`MERGE INTO` is required for Delta Lake upsert and CDC-to-Iceberg pipelines
in R18.

**Options**

- A. Implement `MERGE INTO` as a new DataFusion `LogicalPlan` node and physical
  operator. Correct semantics; unified SQL behaviour across all formats.
  2–3 months of work; not feasible in R18.
- B. Rewrite `MERGE INTO` as `DELETE` followed by `INSERT`. Incorrect semantics
  for `UPDATE WHEN MATCHED` clauses — the DELETE removes the row before UPDATE
  can observe its current value. Produces data corruption on partial-update
  merge patterns.
- C. Format-specific dispatch: route `MERGE INTO` on Delta tables to `delta-rs`'s
  native `DeltaOps::merge` API; route `MERGE INTO` on Iceberg tables to
  Iceberg's native equality-delete + append commit. Parse `MERGE INTO` SQL in
  Krishiv's parser and dispatch based on the target table format. Covers 90%
  of real-world use cases. Explicitly unsupported for non-Delta/Iceberg targets
  until R20.

**Recommendation**: Option C for R18. Format-specific merge with correct
semantics for the two primary formats. Schedule Option A (unified DataFusion
plan node) as an ADR target for R20.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

`MERGE INTO` on non-Delta/Iceberg tables returns `MergeTargetUnsupportedError`
with a clear message. The same `MERGE INTO` SQL syntax has different internal
execution paths for Delta vs. Iceberg — this must be documented in the SQL
reference.

**Risk if deferred**

Omitting `MERGE INTO` entirely blocks CDC-to-Delta and CDC-to-Iceberg upsert
pipelines. Option B's incorrect semantics produce data corruption under
partial-update merge patterns, which is a data-loss-class defect.

---

## R19: Multi-Region, Autoscaling & Cloud-Native

### ADR-19.1: Multi-Region Metadata Consistency Model

**Status**: DECIDED

**Problem statement**

The multi-region coordinator federation must agree on which jobs exist, their
current state, and which region is authoritative for each job. This is a
fundamental distributed systems design decision that determines every data
structure in `krishiv-federation`. It must be decided before any federation
code is written.

**Options**

- A. Strong consistency via Raft (openraft crate): all metadata writes go through
  a Raft leader; followers replicate synchronously. High write latency for
  cross-region operations (bounded by leader-to-follower RTT, typically 50–200ms).
  No split-brain. Significant implementation complexity.
- B. Region-local metadata with async replication (eventual consistency): fast
  local writes; split-brain possible during network partitions — two regions
  may simultaneously believe they are authoritative for the same job, producing
  duplicate task scheduling and potential data loss.
- C. Separate global control plane from regional data planes: a new global
  coordinator tier (Postgres-backed, single writer) owns the job catalog and
  region assignment. Regional coordinators own task execution within their region.
  The global coordinator is the single source of truth. No Raft required for R19.
  HA for the global coordinator uses `krishiv-etcd` (Sprint 4) on bare-metal or
  cloud-provider managed Postgres.

**Recommendation**: Option C. The clean separation avoids Raft complexity while
maintaining correctness. The global coordinator is a single writer with Postgres
optimistic locking. This ADR must be DECIDED before Sprint 1 of R19.

**Decision**: Option C — separate global control plane (single-writer,
Postgres-backed) from regional data planes. Decided 2026-05-21.

Architecture:
- A new `GlobalCoordinator` mode in `krishiv-scheduler` owns the job catalog,
  job state, and region assignment in a Postgres schema with optimistic locking.
- Regional coordinators are stateless task dispatchers: they receive task
  assignments from the global coordinator and report execution status back. They
  hold no durable state independently.
- HA for the global coordinator: managed Postgres (cloud deployments) or the
  `krishiv-etcd` leader election implemented in R19 Sprint 4 (bare-metal).
- The `krishiv-federation` crate defines a `FederationClient` trait with methods
  `submit_job`, `cancel_job`, `job_status`, `list_jobs`, `route_task(job_id) → RegionUrl`.
- Option A (Raft) remains the correct long-term path for a fully peer-to-peer
  control plane and is scheduled as a future ADR beyond R20.

**Consequences**

Option C requires a new global coordinator process (or mode in `krishiv-scheduler`)
and a Postgres schema for the job catalog. Regional coordinators become stateless
task dispatchers that report to the global coordinator. The current single-coordinator
deployment model must evolve to support both global and regional coordinator roles.

**Risk if deferred**

Writing any federation code before this decision produces incompatible data
models. Option B (async replication) cannot be refactored to Option C (global
tier) without a full rewrite of the metadata storage and locking layer. This is
the highest-risk open ADR in the entire R12–R20 roadmap.

---

### ADR-19.2: Spot Recovery Checkpoint Timing

**Status**: PROPOSED

**Problem statement**

Kubernetes sends `SIGTERM` before pod eviction with a configurable grace period
(default 30s). The checkpoint must complete within this window. For large state
(10GB+ RocksDB state for session windows), a full checkpoint cannot complete
within 60 seconds over a WAN link. Spot recovery correctness depends on the
R16 incremental checkpointing implementation.

**Options**

- A. Checkpoint at SIGTERM reception: start a full checkpoint when `SIGTERM`
  is received. Correct for small state; races with the eviction deadline for
  large state. High risk of partial checkpoints for streaming jobs with
  significant state accumulation.
- B. Continuous incremental checkpointing (R16 RocksDB incremental): on `SIGTERM`,
  only the delta since the last incremental checkpoint needs to be flushed.
  Typically seconds of data (< 50MB delta) regardless of total state size.
  Correct and feasible at all state sizes. Requires R16 incremental checkpointing
  to be complete.
- C. Checkpoint only job metadata on `SIGTERM`: save watermark, epoch, and Kafka
  offsets to etcd or Redis. Replay from the last Kafka offset on restart. Correct
  for Kafka source offsets; loses in-memory window state accumulated since the
  last full checkpoint.

**Recommendation**: Option B. R16's RocksDB incremental checkpointing is the
direct prerequisite. If R16 is not complete when R19 begins, scope spot recovery
to small-state jobs only (< 500MB total state) with a documented size limit.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

The `SIGTERM` handler in `krishiv-executor` calls the incremental checkpoint
trigger from R16 rather than initiating a full checkpoint. The `terminationGracePeriodSeconds`
for executor pods defaults to 60s in the Helm chart; this must be documented
as a minimum value — reducing it below the incremental checkpoint flush time
risks partial checkpoints.

**Risk if deferred**

Without Option B, spot eviction of any executor with non-trivial state causes
data loss (reprocessing from the last committed checkpoint epoch). This makes
spot instances unusable for the stateful streaming workloads that drive the
cost-saving case for spot placement in R19.

---

## R20: Enterprise Platform & Ecosystem

### ADR-20.1: Portal Frontend Deployment

**Status**: PROPOSED

**Problem statement**

The React-based self-serve portal requires a build step (npm/Node.js) that
produces static assets. These assets must be served by the Krishiv coordinator's
HTTP server. The deployment model determines how operators deploy the portal
and how developers iterate on it during development.

**Options**

- A. Embed React build artifacts in the Rust binary via `rust-embed` / `include_bytes!`.
  Single binary deployment. CI runs `npm run build` before `cargo build`. Dev
  iteration is slow (full Rust rebuild after each React change) — mitigated by
  a `KRISHIV_DEV_ASSETS_DIR` environment variable that reads assets from disk
  in development mode.
- B. Serve static files from a separate S3 bucket or CDN: CI uploads the React
  build to S3; the Rust server redirects portal requests to the CDN URL. Fast
  dev iteration; requires a CDN in every deployment environment including
  bare-metal and Docker Compose — adds operational complexity for the quick-start
  scenario.
- C. Rust WebAssembly (WASM) frontend using Leptos or Yew: no npm or Node.js
  build toolchain. The WASM frontend component ecosystem (DAG visualisation,
  charting libraries) is not mature enough for R20's portal scope.

**Recommendation**: Option A (embed in binary). Distribution simplicity is the
correct priority for a managed service that targets operators wanting a
one-command deployment. Add `KRISHIV_DEV_ASSETS_DIR` for development hot-reload.
The ADR must be DECIDED before Sprint 2 of R20 begins.

**Decision**: _To be filled in when the team formally records this as DECIDED._

**Consequences**

The CI pipeline gains a Node.js 20 install step and `npm run build` before
`cargo build`. The `krishiv-portal` crate has a `build.rs` that invokes the
npm build. The Rust binary size increases by the size of the React bundle
(estimated 2–5MB compressed). The `KRISHIV_DEV_ASSETS_DIR` env var is documented
as the development workflow entry point.

**Risk if deferred**

Without a deployment decision, the CI pipeline cannot be extended to include
the npm build step. This blocks all portal development work in R20's Sprint 2
onward. If the decision is not made before Sprint 2 begins, the portal will
be incomplete at the R20 acceptance gate.
