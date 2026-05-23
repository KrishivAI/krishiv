# Krishiv Codebase Gap Analysis

**Date:** 2026-05-23
**Scope:** Full workspace audit — 35 crates, build/test/lint validation, roadmap alignment
**Rust toolchain:** 1.95.0 (stable); workspace declares `rust-version = "1.89"`, `edition = "2024"`
**Related:** [`gap-mitigation-plan.md`](gap-mitigation-plan.md), [`../architecture/r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md)

---

## Executive Summary

The Krishiv workspace compiles (`cargo check --workspace` passes) and the vast
majority of unit/integration tests pass. However, the analysis reveals **6
categories of gaps** spanning build hygiene, correctness, integration wiring,
test coverage, documentation honesty, and deferred infrastructure. The existing
`gap-mitigation-plan.md` (P0–P3 items) and `r12-maturity-gap-register.md`
(GAP-* items) are thorough but have not been fully closed.

This document synthesizes findings from a fresh build/test/lint run against
`main` and cross-references them against the roadmap trackers (R1–R18 complete,
R4–R10 unchecked items, R11 acceptance gate incomplete).

### Key Numbers

| Metric | Value |
|--------|------:|
| Workspace crates | 35 (in `Cargo.toml`) + 2 out-of-workspace (`krishiv-cep`, `krishiv-spark-connect`) |
| `cargo check --workspace` | **PASS** (warnings only) |
| `cargo clippy --workspace -- -D warnings` | **FAIL** — 3 crates with errors (`krishiv-state`, `krishiv-governance`, `krishiv-lakehouse`) |
| `cargo fmt --check` | **FAIL** — 395 diffs across 27 crates |
| `cargo test --workspace` | **FAIL** — 3 crates fail to link (`krishiv-chaos`, `krishiv-python`, `krishiv-executor` binary), 1 test failure (`krishiv-vector-sinks`) |
| Tests passing (excluding link failures) | ~730 pass, 1 fail, 1 ignored |

---

## Category 1: Build and Toolchain Gaps

### GAP-B1: Workspace declares `rust-version = "1.89"` but `iceberg` crate requires 1.92+

**Severity:** Medium
**Finding:** The `Cargo.toml` declares `rust-version = "1.89"` but `iceberg = "0.9.1"` requires `rustc 1.92`. Anyone following the declared MSRV will get a confusing error.
**Resolution:** Update `rust-version` to `"1.92"` or pin `iceberg` to a version compatible with 1.89. Recommend updating since stable (1.95.0) is needed for full compilation.

### GAP-B2: Two crates exist on disk but are excluded from workspace

**Severity:** Low
**Finding:** `krishiv-cep` and `krishiv-spark-connect` have source code in `crates/` but are not listed in `[workspace] members` in `Cargo.toml`. Their code is never compiled, tested, or linted.
**Resolution:**
- `krishiv-cep`: Add to workspace if ready, or document as experimental/deferred.
- `krishiv-spark-connect`: Referenced in `gap-mitigation-plan.md` P0-14 but never compiled. Add to workspace behind a feature flag, or move to `experimental/` directory.

### GAP-B3: Three crates fail to link during `cargo test`

**Severity:** High
**Finding:**
- `krishiv-python` — Missing Python development headers (PyO3 link failure). Requires `python3-dev` system package.
- `krishiv-executor` — Binary target fails to link (likely C++ stdlib dependency from `fastembed` or ONNX transitive deps).
- `krishiv-chaos` — Same linker failure pattern as executor.
**Resolution:**
- Document required system packages in `README.md` or a `CONTRIBUTING.md`.
- Add CI job that installs `python3-dev`, `libssl-dev`, `pkg-config`, `protobuf-compiler`, `cmake`.
- Consider `#[cfg(feature)]` gating for heavy native deps (ONNX/fastembed).

### GAP-B4: `cargo fmt --check` fails with 395 diffs across 27 crates

**Severity:** Medium
**Finding:** Formatting is not enforced. Worst offenders: `krishiv-python` (40 diffs), `krishiv` (38), `krishiv-scheduler` (37), `krishiv-lakehouse` (37), `krishiv-sql` (36), `krishiv-connectors` (35).
**Resolution:** Run `cargo fmt --all` and commit. Add `cargo fmt --check` as a CI gate.

### GAP-B5: `cargo clippy --workspace -- -D warnings` fails on 3 crates

**Severity:** Medium
**Finding:**
- `krishiv-state`: collapsible `if` statement
- `krishiv-governance`: unused mutable variable
- `krishiv-lakehouse`: 8 issues — `DeltaStore` trait has `len` without `is_empty`, plus collapsible `if` statements
**Resolution:** Fix all clippy warnings. These are mechanical fixes (collapsible ifs, add `is_empty`, remove `mut`).

---

## Category 2: Test Gaps

### GAP-T1: `weaviate::tests::weaviate_query_returns_results` fails

**Severity:** High
**Finding:** The only failing test in the passing-crate suite. Per `gap-mitigation-plan.md` P0-11, `WeaviateSink::query_nearest` always returns empty results because the GraphQL response body is parsed into an unused variable.
**Resolution:** Implement proper GraphQL response parsing in `krishiv-vector-sinks/src/weaviate.rs`.

### GAP-T2: No integration test suite for distributed execution path

**Severity:** High
**Finding:** While unit tests exist for coordinator, executor, and proto crates individually, there is no end-to-end integration test that:
1. Starts a coordinator
2. Registers an executor
3. Submits a job
4. Verifies task completion and result delivery
The coordinator binary (`krishiv_coordinator`) fails to link in test mode.
**Resolution:** Create an in-process integration test in `tests/integration/` that exercises the full coordinator→executor→result path using the in-process service adapters.

### GAP-T3: `krishiv-testkit` is nearly empty (90 lines, 0 tests)

**Severity:** Low
**Finding:** Per `gap-mitigation-plan.md` P2-6, this crate provides only basic `make_batch`, `MockSource`, `MockSink`, and `TestSession` stubs. Most crates define their own test helpers.
**Resolution:** Populate with shared test utilities. Low priority — deduplication improves maintenance but doesn't affect correctness.

### GAP-T4: No `cargo test --workspace` CI gate

**Severity:** High
**Finding:** The `status.md` lists selective per-crate test commands but no full workspace test pass. The link failures in 3 crates (GAP-B3) prevent workspace-wide testing.
**Resolution:** Either fix all link failures or establish a CI script that runs `cargo test --workspace --exclude krishiv-chaos --exclude krishiv-python --exclude krishiv-executor` as a gate, with separate jobs for the excluded crates with proper system dependencies.

---

## Category 3: Correctness and Safety Gaps

These items from the existing `gap-mitigation-plan.md` remain unresolved. The status of each is verified by this fresh analysis.

### GAP-C1: Coordinator binary never drives heartbeat or task launch ticks (P0-4)

**Severity:** Critical
**Status:** **UNRESOLVED** — Confirmed via code inspection. `krishiv-scheduler/src/bin/krishiv_coordinator.rs` starts a gRPC server but never calls `advance_heartbeat_clock()` or `launch_assigned_task_assignments()` in a tick loop.
**Resolution:** Add two `tokio::spawn` tick loops in the coordinator binary as described in gap-mitigation-plan P0-4.

### GAP-C2: K8s leader election never wired (P0-5)

**Severity:** Critical
**Status:** **UNRESOLVED** — `K8sLeaseElection` is implemented in `krishiv-operator/src/lib.rs` but `main.rs` never instantiates or runs it. Multiple operator replicas run as permanently active.
**Resolution:** Wire `K8sLeaseElection` in operator `main()` with lease renewal loop.

### GAP-C3: Executor creates new TCP connection per gRPC call (P0-7)

**Severity:** High
**Status:** **UNRESOLVED** — `GrpcCoordinatorService` in `krishiv-executor/src/transport.rs` calls `connect()` in every method.
**Resolution:** Hold client in a `tokio::sync::Mutex<Option<Client>>` with lazy initialization.

### GAP-C4: Executor never updates lease generation from coordinator (P0-8)

**Severity:** High
**Status:** **UNRESOLVED** — `lease_generation` stays at `initial()` forever.
**Resolution:** Update `lease_generation` from heartbeat/register responses.

### GAP-C5: Checkpoint fencing — multiple issues (P1-1, P1-2, P1-3)

**Severity:** High
**Status:** **PARTIALLY RESOLVED** — `validate_fencing_token` semantics were adjusted in the gap-mitigation branch, but epoch monotonicity guard (P1-2) and fsync-before-rename (P1-3) remain unresolved on main.
**Resolution:**
- Add epoch monotonicity check before `write_epoch_metadata`.
- Add `sync_all()` + parent directory `sync_all()` to `LocalFsCheckpointStorage`.

### GAP-C6: Shuffle — no spill-to-disk, partition cap unenforced (P1-4)

**Severity:** High
**Status:** **UNRESOLVED** — `InMemoryShuffleStore` has no memory limit. `max_partitions` field exists but is never checked.
**Resolution:** Add memory threshold with spill to `LocalDiskShuffleStore`. Enforce `max_partitions` in every `write_partition` call.

### GAP-C7: State TTL `list_keys` returns expired keys (P1-7)

**Severity:** Medium
**Status:** **UNRESOLVED** — `TtlStateBackend::list_keys` delegates without filtering.
**Resolution:** Filter expired keys in `list_keys` and `list_namespaces`.

### GAP-C8: Checkpoint ACK never delivered from executor (P1-17)

**Severity:** High
**Status:** **UNRESOLVED** — `TaskRunner::handle_initiate_checkpoint` produces a `CheckpointAckRequest` but the executor main loop has no code path to deliver it via gRPC.
**Resolution:** Wire ACK delivery through `GrpcCoordinatorService::checkpoint_ack()`.

### GAP-C9: `DataFusionTableBridge::scan` returns `EmptyExec` (P0-9)

**Severity:** High
**Status:** **UNRESOLVED** — Every SQL query through the Krishiv catalog returns zero rows.
**Resolution:** Replace `EmptyExec` with `MemoryExec` for in-memory tables and `ParquetExec` for Parquet-backed tables.

### GAP-C10: `krishiv-catalog` not wired into `krishiv-sql` (P0-10)

**Severity:** High
**Status:** **UNRESOLVED** — `SqlEngine` bypasses catalog-registered tables.
**Resolution:** Add `krishiv-catalog` as dependency of `krishiv-sql` and register bridge with DataFusion SessionContext.

---

## Category 4: Integration Wiring Gaps

### GAP-I1: Runtime backends are accept-only stubs (GAP-RT-01)

**Severity:** Critical
**Finding:** `EmbeddedBackend`, `SingleNodeBackend`, and `DistributedBackend` in `krishiv-runtime` all return `ExecutionReport { accepted: true }` without executing plans. Actual execution happens in `krishiv-api`/`krishiv-sql`, but the backends themselves do nothing.
**Resolution:** Either:
- Accept that backends are routing stubs and rename/document accordingly.
- Wire `SingleNodeBackend` to run plans through `SqlEngine`, and `DistributedBackend` to dispatch via Flight SQL.

### GAP-I2: Streaming operators not backed by `StateBackend` (GAP-ST-01)

**Severity:** Critical
**Finding:** `TumblingWindowOperator` in `krishiv-exec` uses in-memory `HashMap`, not the `StateBackend` trait from `krishiv-state`. Checkpoint/restore of streaming state is impossible.
**Resolution:** Wire windowed operators to use `StateBackend::put`/`get` for aggregate storage. Required before any checkpoint correctness claim.

### GAP-I3: Optimizer rules are real but not wired into execution (P2-3, P2-4)

**Severity:** Medium
**Finding:** `ProjectionPruningRule`, `PredicatePushdownRule`, `ConstantFoldingRule`, `CoalesceRule` exist in `krishiv-optimizer` with tests, but no execution path calls `optimizer.optimize()` on plans before execution.
**Resolution:** Add optimizer pass in `SqlEngine::plan_sql()` or `Session::execute()` before handing plans to the backend.

### GAP-I4: `AggregateUdf` and `TableUdf` not bridged to DataFusion (P1-21)

**Severity:** Medium
**Finding:** Only `ScalarUdf` is bridged to DataFusion via `sync_scalar_udfs()`. UDAF and UDTF registrations are silently ignored.
**Resolution:** Add `sync_aggregate_udfs()` and `sync_table_udfs()` functions in `krishiv-sql/src/udf.rs`.

### GAP-I5: Audit log has zero call sites in production code (P0-15)

**Severity:** High
**Finding:** `audit_log()` exists in `krishiv-governance` but is never called from `krishiv-sql-policy`, `krishiv-scheduler`, `krishiv-flight-sql`, or any other execution path.
**Resolution:** Add audit call sites in SQL execution (allow/deny), job submission, and Flight SQL `do_get`.

### GAP-I6: OTel metrics API entirely absent (P0-16)

**Severity:** Medium
**Finding:** `krishiv-metrics` only implements tracing spans. No `MeterProvider`, counters, or histograms. No Prometheus endpoint.
**Resolution:** Add `opentelemetry` metrics feature, create `KrishivMetrics` struct with counters/histograms, expose Prometheus handler.

### GAP-I7: `PolicyEnforcingSqlEngine` bypassed in `Session::sql()` (GAP-RT-05)

**Severity:** High
**Finding:** Direct `sql()` calls bypass policy enforcement. Only `sql_as()` applies policies.
**Resolution:** If policy is configured, route all SQL through `sql_as()` or return `AccessDenied` when principal is missing.

---

## Category 5: Roadmap Alignment Gaps

### GAP-R1: R4 Shuffle And Batch AQE — all checklist items unchecked

**Severity:** N/A (in-progress)
**Finding:** The R4 tracker shows 0/21 checklist items completed. However, implementation reality is ahead: `krishiv-shuffle` and `krishiv-optimizer` crates exist with hash partitioning, compression, spill stubs, and optimizer rules.
**Resolution:** Update R4 tracker to reflect actual implementation progress. Items like "Add `krishiv-shuffle` crate" and "Add `krishiv-optimizer` crate" are done but unchecked.

### GAP-R2: R5 Stateful Streaming Core — all items unchecked but partial implementation exists

**Severity:** N/A (in-progress)
**Finding:** `krishiv-state` crate exists with in-memory and redb backends, TTL, timers, migration. `krishiv-exec` has `TumblingWindowOperator`, `SlidingWindowOperator`, `SessionWindowOperator`. These map to several R5.1 and R5.2 checklist items.
**Resolution:** Update R5 tracker to reflect actual progress.

### GAP-R3: R11 acceptance gate — `cargo test --workspace` does not pass

**Severity:** High
**Finding:** R11 acceptance gate requires "cargo test --workspace passes with zero failures." Currently 3 crates fail to link and 1 test fails.
**Resolution:** Fix link failures (system dep documentation + CI), fix `weaviate_query_returns_results` test.

### GAP-R4: R11 checklist items — most still unchecked

**Severity:** Medium
**Finding:** The R11 tracker has 13 checklist items, of which only 1 is checked (CLI dispatch merged). Lock-poisoning recovery, fencing token fixes, CDC real loop, and CLI commands remain unchecked.
**Resolution:** These are the logical next implementation targets after gap mitigation is merged.

### GAP-R5: R12–R18 trackers claim items complete that have stub implementations

**Severity:** High (documentation honesty)
**Finding:** Per GAP-DOC-01 in the maturity gap register, several tracker items are marked done where implementations are L1–L2 maturity (types defined, unit tests pass) but L4 (binary/CLI integration) is missing.
**Resolution:** Add maturity layer annotations to tracker checkboxes. A checked item should specify whether it's L2 (isolated test) or L4 (end-to-end).

---

## Category 6: Infrastructure and CI Gaps

### GAP-CI1: No CI configuration in repository

**Severity:** High
**Finding:** No `.github/workflows/`, `.gitlab-ci.yml`, or equivalent CI configuration exists. All validation is manual.
**Resolution:** Add GitHub Actions workflow with:
1. `cargo fmt --check`
2. `cargo clippy --workspace -- -D warnings`
3. `cargo check --workspace`
4. `cargo test --workspace` (with appropriate exclusions and system deps)

### GAP-CI2: No PR template

**Severity:** Low
**Finding:** No `PULL_REQUEST_TEMPLATE.md` exists.
**Resolution:** Add template requiring: checklist update, test validation command, and gap-ID references for known-issue fixes.

### GAP-CI3: Missing system dependency documentation

**Severity:** Medium
**Finding:** Building requires `libssl-dev`, `pkg-config`, `protobuf-compiler`, `cmake`, and `python3-dev` (for krishiv-python). None of these are documented.
**Resolution:** Add build prerequisites section to `README.md` and/or a `CONTRIBUTING.md`.

### GAP-CI4: Example binary name collision

**Severity:** Low
**Finding:** `memory_stream` example target exists in both `krishiv-api` and `krishiv` crates, producing a Cargo warning about same output filename.
**Resolution:** Rename one of the examples (e.g., `krishiv-api`'s to `api_memory_stream`).

---

## Priority Resolution Matrix

### Immediate (fix before next release)

| ID | Gap | Effort | Crates affected |
|----|-----|--------|-----------------|
| GAP-B4 | `cargo fmt --all` | Trivial | All |
| GAP-B5 | Fix clippy errors | Trivial | krishiv-state, krishiv-governance, krishiv-lakehouse |
| GAP-T1 | Fix weaviate test | Small | krishiv-vector-sinks |
| GAP-B1 | Fix `rust-version` | Trivial | Root Cargo.toml |
| GAP-CI4 | Rename colliding example | Trivial | krishiv-api or krishiv |

### Short-term (next sprint)

| ID | Gap | Effort | Crates affected |
|----|-----|--------|-----------------|
| GAP-C1 | Wire coordinator tick loops | Small | krishiv-scheduler |
| GAP-C3 | Fix executor connection pooling | Small | krishiv-executor |
| GAP-C4 | Fix executor lease update | Small | krishiv-executor |
| GAP-C9 | Fix catalog scan EmptyExec | Medium | krishiv-catalog |
| GAP-C10 | Wire catalog into SQL | Medium | krishiv-sql, krishiv-catalog |
| GAP-I5 | Wire audit log call sites | Medium | krishiv-governance, krishiv-sql-policy, krishiv-scheduler |
| GAP-I7 | Enforce policy in `Session::sql()` | Medium | krishiv-api |
| GAP-CI1 | Add CI configuration | Medium | Repository root |
| GAP-CI3 | Document system deps | Small | README.md |
| GAP-B2 | Decide on excluded crates | Small | Cargo.toml |

### Medium-term (next 2 sprints)

| ID | Gap | Effort | Crates affected |
|----|-----|--------|-----------------|
| GAP-C2 | Wire K8s leader election | Medium | krishiv-operator |
| GAP-C5 | Checkpoint fencing completeness | Medium | krishiv-checkpoint |
| GAP-C6 | Shuffle spill + partition cap | Medium | krishiv-shuffle |
| GAP-C7 | State TTL list_keys filter | Small | krishiv-state |
| GAP-C8 | Wire checkpoint ACK delivery | Medium | krishiv-executor |
| GAP-I1 | Backend execution wiring | Large | krishiv-runtime, krishiv-sql |
| GAP-I2 | Wire streaming ops to StateBackend | Large | krishiv-exec, krishiv-state |
| GAP-I3 | Wire optimizer into execution | Medium | krishiv-sql, krishiv-optimizer |
| GAP-I4 | Bridge UDAF/UDTF to DataFusion | Medium | krishiv-sql, krishiv-udf |
| GAP-I6 | Add OTel metrics | Medium | krishiv-metrics |

### Long-term (backlog)

| ID | Gap | Effort | Notes |
|----|-----|--------|-------|
| GAP-R1–R5 | Update roadmap trackers | Small | Documentation accuracy |
| GAP-T2 | E2E distributed integration test | Large | Requires executor link fix |
| GAP-T3 | Populate testkit | Small | Deduplication |
| GAP-B3 | Fix all link failures | Medium | System dep + feature gating |

---

## Validation Commands

```bash
# Build gate
cargo check --workspace

# Lint gate
cargo clippy --workspace -- -D warnings

# Format gate
cargo fmt --check

# Test gate (full — requires system deps)
cargo test --workspace

# Test gate (conservative — excludes link-failure crates)
cargo test --workspace \
  --exclude krishiv-chaos \
  --exclude krishiv-python \
  --exclude krishiv-executor

# Per-subsystem validation
cargo test -p krishiv-scheduler --lib
cargo test -p krishiv-exec --lib
cargo test -p krishiv-state --lib
cargo test -p krishiv-checkpoint --lib
cargo test -p krishiv-shuffle --lib
cargo test -p krishiv-connectors --lib
cargo test -p krishiv-lakehouse --lib
cargo test -p krishiv-sql --lib
cargo test -p krishiv-vector-sinks --lib
```

---

## Cross-References

| Document | Relationship |
|----------|-------------|
| [`gap-mitigation-plan.md`](gap-mitigation-plan.md) | P0–P3 item list; this analysis confirms unresolved status |
| [`../architecture/r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md) | GAP-* register; this analysis adds build/CI/test gaps |
| [`../implementation/status.md`](../implementation/status.md) | Current phase tracking |
| [`../implementation/r11-stability-correctness-cli.md`](../implementation/r11-stability-correctness-cli.md) | R11 acceptance gate (requires `cargo test --workspace` pass) |
| [`standards.md`](standards.md) | Engineering standards referenced throughout |
