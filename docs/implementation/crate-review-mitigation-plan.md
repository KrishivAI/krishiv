# Crate Review Mitigation Plan — A+ and Stable Maturity

## Overview

This plan addresses all ~250 issues found in the comprehensive crate-by-crate code
review of all 32 workspace crates.imp The goal is to bring every crate to **A+ grade**
and **stable maturity** by fixing all bugs, gaps, and standards violations.

**Current state (2026-05-28):** ~25 Critical/High, ~60 Medium, ~165 Low/Info issues.

---

## Phase 1: Critical Correctness Bugs (Week 1)

**Goal:** Eliminate all correctness/data-integrity/security bugs.

### 1.1 — Window Aggregate Output Type Bug [KRISHIV-EXEC]

**Crates:** `krishiv-exec`
**Files:** `window/tumbling.rs:325`, `window/sliding.rs:258`, `window/session.rs:287`
**Severity:** Critical — every AVG window aggregation produces wrong type
**Fix:**
- In `build_window_record_batch` (all three window files), dispatch on `AggFunction::Avg`
  to use `finalized_avg` (returns `f64`) and output `DataType::Float64` instead of
  `finalized_value` (returns `i64`) and `DataType::Int64`.
- Add a `finalized_value_for_expr` helper that inspects `AggExpr.function` and calls
  either `finalized_value` or `finalized_avg`.
- Add unit tests for Avg in tumbling, sliding, and session windows.

**Files to modify:**
- `crates/krishiv-exec/src/aggregate.rs` — add `finalized_value_for_expr`
- `crates/krishiv-exec/src/window/tumbling.rs` — use helper
- `crates/krishiv-exec/src/window/sliding.rs` — use helper
- `crates/krishiv-exec/src/window/session.rs` — use helper

### 1.2 — LRU Promotion Bug [KRISHIV-EXEC]

**File:** `crates/krishiv-exec/src/memo.rs:51-53`
**Severity:** Critical — cache entries evicted prematurely
**Fix:**
- On `store()`, when key already exists in `map`, also promote it in `order`:
  `order.retain(|k| k != &key); order.push_back(key.clone());`
- Add test: `store(a); store(b); store(a); assert!(lookup(b).is_some()); lookup(a).is_some()`.

### 1.3 — Empty Group Aggregate Semantics [KRISHIV-EXEC]

**File:** `crates/krishiv-exec/src/aggregate.rs:188-196`
**Severity:** Critical — Min/Max/Avg return 0/0.0 for empty groups instead of NULL
**Fix:**
- `finalized_value` for `Min` → return `i64::MIN` when `!has_value[i]` (or use Arrow null).
- `finalized_value` for `Max` → return `i64::MAX` when `!has_value[i]`.
- `finalized_avg` → return `f64::NAN` when `!has_value[i]`.
- Add test for empty-group count, sum, min, max, avg.

### 1.4 — Avro Type Mapping Bugs [KRISHIV-SCHEMA-REGISTRY]

**File:** `crates/krishiv-schema-registry/src/lib.rs:228-229`
**Severity:** Critical — data corruption for binary payloads; schema mismatch for floats
**Fix:**
- `Schema::Float` → `DataType::Float32` (not `Float64`)
- `Schema::Bytes` → `DataType::Binary` (not `Utf8`)
- Add round-trip test with Float and Bytes fields.

### 1.5 — Security Policy Bypass [KRISHIV-SQL-POLICY]

**File:** `crates/krishiv-sql-policy/src/lib.rs:45`
**Severity:** Critical — `inner()` exposes unguarded SQL execution
**Fix:**
- Change `inner()` from `pub` to `pub(crate)`.
- If external callers need access, add a `#[doc(hidden)]` attribute and document that
  callers must enforce policy externally.
- Add test verifying `execute_as` enforces policy while direct `inner().sql()` is not
  accessible from outside the crate.

### 1.6 — Join Column Masking [KRISHIV-SQL-POLICY]

**File:** `crates/krishiv-sql-policy/src/lib.rs:266`
**Severity:** Critical — join queries get wrong masking
**Fix:**
- `masking_rule_for_field` should accept table-qualified column names (`table.column`).
- When processing join results, match rules against both unqualified and
  fully-qualified column names.
- Add test: two tables with same column name but different masking rules.

### 1.7 — Merge Iceberg Memory No-Op [KRISHIV-SQL]

**File:** `crates/krishiv-sql/src/lakehouse/merge.rs:139`
**Severity:** Critical — MERGE INTO never writes to target
**Fix:**
- Either implement real merge (write merged batches back to the table), or rename to
  `dry_run_merge` and add prominent doc comments.
- Given R18 scope, implement: load source + target → join → write merged result back.
- Add test verifying rows are actually modified.

### 1.8 — Rate Limiter Token Bucket [KRISHIV-AI]

**File:** `crates/krishiv-ai/src/embed/openai.rs:38-42`
**Severity:** Critical — rate limiter broken after first exhaustion
**Fix:**
- Replace `self.tokens = self.requests_per_minute as f64` with
  `self.tokens = (self.tokens + elapsed_secs * rate_per_sec).min(capacity)`.
- Add test: exhaust tokens, sleep, verify gradual refill.

### 1.9 — Owner References Empty UID [KRISHIV-OPERATOR]

**File:** `crates/krishiv-operator/src/pod_manager.rs:176-189`
**Severity:** Critical — Kubernetes GC broken
**Fix:**
- Set `uid` from the `KrishivJob` CRD object metadata (`.metadata.uid`).
- If UID is unavailable (CRD mock mode), skip owner references entirely rather than
  creating them with empty UID.
- Add test with mock UID propagation.

### 1.10 — Wire Data Loss in Proto [KRISHIV-PROTO]

**Files:** `crates/krishiv-proto/src/wire.rs:196-243, 277-308`
**Severity:** Critical — heartbeat fields silently dropped
**Fix:**
- Wire encode/decode for `streaming_task_states`, `hot_key_reports`, `trace_context`
  in heartbeat request.
- Wire encode/decode for `checkpoint_commands`, `trace_context` in heartbeat response.
- Note: these may already be partially fixed in the production-readiness sweep (item 1.5
  in status.md). Verify current state before implementing.
- Add round-trip tests for all heartbeat fields.

### 1.11 — 127.0.0.0 Loopback Bug [KRISHIV]

**File:** `crates/krishiv/src/cluster_cmd.rs:181`
**Severity:** High
**Fix:** `127.0.0.0` → `127.0.0.1`

### 1.12 — Lease-Generation Race [KRISHIV-EXECUTOR]

**File:** `crates/krishiv-executor/src/cli.rs:326-357`
**Severity:** High
**Fix:**
- Only update `shared_lease` after successful re-registration, not from stale response.
- Add test simulating stale lease response.

### 1.13 — Hardcoded Key Group Range [KRISHIV-EXECUTOR]

**File:** `crates/krishiv-executor/src/barrier_grpc.rs:63-64`
**Severity:** High
**Fix:**
- Accept `key_group_range` as a constructor parameter.
- In `ExecutorTaskRunner`, pass the assigned key group range from the task assignment.
- Default to `0..32767` only for single-node mode.

---

## Phase 2: Security & Data Integrity (Week 2)

**Goal:** Fix all security issues, data-integrity gaps, and standards violations.

### 2.1 — SQL Injection Surface Cleanup

**Crates:** `krishiv-sql`, `krishiv-connectors`, `krishiv-vector-sinks`
**Status:** Partially fixed in Phase 1 of production-readiness sweep. Verify all
`validate_table_name`, `validate_class_name`, `validate_identifier` are in place.

### 2.2 — View Name Extraction [KRISHIV-SQL]

**File:** `crates/krishiv-sql/src/lib.rs:474`
**Fix:** Replace `" from "` substring matching with `sqlparser` AST walk
(`visit_relations`).

### 2.3 — CEP SQL Parsing [KRISHIV-SQL]

**File:** `crates/krishiv-sql/src/cep_sql.rs:107`
**Fix:** Use `sqlparser` AST instead of string matching for `MATCH_RECOGNIZE`.

### 2.4 — `times()` Silent No-Op [KRISHIV-CEP]

**File:** `crates/krishiv-cep/src/pattern.rs:80-83`
**Fix:** Return `Err(UnsupportedCombinator::ExactCount)` or store `n` and enforce.

### 2.5 — `unwrap()` in Library Code [KRISHIV-CEP]

**File:** `crates/krishiv-cep/src/matcher.rs:72`
**Fix:** Replace `state.partial.as_mut().unwrap()` with `if let Some(partial)`.

### 2.6 — Credential Protection

**Status:** Partially fixed (Debug redaction in connectors/vector-sinks). Verify:
- `krishiv-ai`: API key storage in `openai.rs` — use `secrecy` crate or `Zeroize`.
- All `Debug` impls on config types redact secrets.

### 2.7 — Mutex Poison Recovery Consistency

**Crates:** `krishiv`, `krishiv-exec`, `krishiv-state`, `krishiv-ai`, `krishiv-executor`
**Fix:** Unify to `.unwrap_or_else(|e| e.into_inner())` everywhere except test code.
Create a shared `lock_or_recover` helper if needed.

### 2.8 — Validate Epoch Memory Pressure [KRISHIV-CHECKPOINT]

**File:** `crates/krishiv-checkpoint/src/lib.rs:496-505`
**Fix:** Stream-hash files using `BufReader` + `Sha256::update` instead of
reading entire files into `Vec<u8>`.

### 2.9 — S3 Prefix Ignored [KRISHIV-CHECKPOINT]

**File:** `crates/krishiv-checkpoint/src/storage_uri.rs:48`
**Fix:** Thread the parsed URI path component through as the storage prefix.

---

## Phase 3: Architecture & API Correctness (Week 3)

**Goal:** Fix all API design issues, duplicate code, and inconsistent patterns.

### 3.1 — Deduplicate Window Watermark Logic [KRISHIV-EXEC]

**Files:** `operator_runtime.rs`, `continuous.rs`
**Fix:** Extract `max_event_time_ms`, `max_event_time_ms_for_source`,
`advance_effective_watermark` into a shared `watermark.rs` module.

### 3.2 — Deduplicate State-Backed Window Operators [KRISHIV-EXEC]

**File:** `window/state_tumbling.rs`
**Fix:** Create a generic `StateBackedWindowOperator<W: WindowOperator>` trait and
implement it once for all three window kinds.

### 3.3 — Fix Sliding Window Performance [KRISHIV-EXEC]

**File:** `window/sliding.rs:163-178`
**Fix:** Compute `window_starts` arithmetically:
```
first = (event_time / slide) * slide
iterate: s = first, s -= slide while s + size > event_time
```
This is O(size/slide) but computed with a formula, not a while loop.

### 3.4 — StreamTable Join Optimization [KRISHIV-EXEC]

**File:** `join.rs:391-395`
**Fix:** Cache the hash map across `process_batch` calls. Store
`Arc<HashMap<...>>` on `StreamTableJoin` and rebuild only when the table side changes.

### 3.5 — AqeRule API Consistency [KRISHIV-OPTIMIZER]

**Fix:** Change `AqeRule::apply` to return `Option<PhysicalPlan>` (matching
`OptimizerRule`), eliminating the clone-before-every-rule pattern.

### 3.6 — Multi-Aggregate Encoding [KRISHIV-PLAN]

**File:** `window.rs:96`
**Fix:** Already fixed in production-readiness sweep (item 2.11). Verify.

### 3.7 — Plan Crate Error Type [KRISHIV-PLAN]

**Fix:** Add `PlanError` enum with variants for parse errors, encode errors,
validation errors. Replace all `Result<_, String>` and `Option` returns.

### 3.8 — Proto Management Type Cleanup [KRISHIV-PROTO]

**Fix:**
- Remove duplicate management types from `services.rs`.
- Use `JobId` instead of `String` for `job_id` in management types.
- Add wire conversion functions for management types.
- Remove dead `label_opt()` doc comment.

### 3.9 — Proto Duplicate Heartbeat Types [KRISHIV-PROTO]

**Fix:** Deprecate `ExecutorHeartbeat` (executor.rs) in favor of
`ExecutorHeartbeatRequest` (task.rs), or unify them.

### 3.10 — Scheduler RwLock Scope [KRISHIV-SCHEDULER]

**File:** `coordinator.rs`
**Fix:** Narrow lock scope in methods that hold write guard across I/O. Acquire lock,
clone needed data, release lock, perform I/O, re-acquire lock to update state.

### 3.11 — Flight SQL Policy Engine Sharing [KRISHIV-FLIGHT-SQL]

**File:** `lib.rs:93-102`
**Fix:** Create `SqlEngine` once during server construction and share via `Arc`.

---

## Phase 4: Performance & Concurrency (Week 4)

**Goal:** Fix all performance issues and concurrency hazards.

### 4.1 — Tokio Blocking Violations

**Crates:** `krishiv-shuffle`, `krishiv-state`, `krishiv-checkpoint`
**Files:**
- `object_store.rs` — IPC encoding on async thread
- `redb_backend.rs` — sync redb ops on async thread
- `lib.rs` (checkpoint) — `run_blocking_on_tokio` per-call runtime creation
**Fix:** Ensure all blocking I/O goes through `spawn_blocking`.

### 4.2 — Block-On Panic in Library Code [KRISHIV-ASYNC-UTIL]

**File:** `src/lib.rs:23-24`
**Fix:** Make `block_on` return `Result<T, Box<dyn Error>>` or cache the runtime
via `LazyLock`.

### 4.3 — Ordering Over-Specification [KRISHIV-EXECUTOR]

**File:** `grpc_client.rs:27,36`
**Fix:** `SeqCst` → `Acquire`/`Release` for `SharedLeaseGeneration`.

### 4.4 — Mutex Held Across Connect [KRISHIV-EXECUTOR]

**File:** `grpc_client.rs:84-103`
**Fix:** Use double-check pattern: lock, check, unlock, connect, re-lock, store.

### 4.5 — Shutdown Ordering [KRISHIV-EXECUTOR]

**File:** `cli.rs:292,398`
**Fix:** `Relaxed` → `Release` (store) and `Acquire` (load) for shutdown flag.

### 4.6 — TTL State Performance [KRISHIV-STATE]

**File:** `ttl.rs`
**Fix:**
- `purge_expired`: batch deletes in a single transaction if backend supports it.
- `list_keys`: avoid per-key `get` by having the backend expose TTL-aware iteration.

### 4.7 — Key Group Lookup O(1) [KRISHIV-STATE]

**File:** `key_group.rs`
**Fix:** Replace linear scan with `task_idx = (key_group * parallelism) / NUM_KEY_GROUPS`.

### 4.8 — Processing Timer Cancellation O(N) [KRISHIV-STATE]

**File:** `processing_time.rs`
**Fix:** Add identity-index `HashMap` mirroring `InMemoryTimerService`.

### 4.9 — Snapshot Count Validation [KRISHIV-STATE]

**File:** `snapshot.rs:55`
**Fix:** Cap `count` to 1,000,000 or use `try_reserve`.

### 4.10 — Heartbeat Backoff [KRISHIV-EXECUTOR]

**File:** `cli.rs:388-393`
**Fix:** Add exponential backoff on heartbeat failure (1s → 2s → 4s → 30s max).

---

## Phase 5: Test Coverage & Documentation (Week 5)

**Goal:** Fill all test gaps and add missing documentation.

### 5.1 — Critical Test Gaps

| Crate | Missing Tests | Priority |
|-------|---------------|----------|
| `krishiv-exec` | Avg in all window types, empty group aggregates, LRU promotion | Critical |
| `krishiv-exec` | `chunk.rs` zero coverage | High |
| `krishiv-plan` | Multi-agg encoding, session window, sliding window round-trip | High |
| `krishiv-plan` | `r17.rs`, `streaming.rs` zero coverage | High |
| `krishiv-cep` | `PartitionedCepMatcher`, boundary events, multi-key | High |
| `krishiv-checkpoint` | Path traversal, concurrent access | Medium |
| `krishiv-state` | `purge_expired`, watermark-TTL interaction, concurrent access | Medium |
| `krishiv-scheduler` | Barrier timeout, concurrent barrier streams | Medium |
| `krishiv-operator` | Lease state TTL eviction, owner reference UID propagation | Medium |
| `krishiv-ai` | Rate limiter refill, unknown model fallback | Medium |
| `krishiv-schema-registry` | Avro round-trip, error paths, schema cache | Medium |
| `krishiv-connectors` | CDC error paths, sink flush/commit | Medium |
| `krishiv-sql` | Window broadcasting incompatible lengths, view name extraction | Medium |
| `krishiv-optimizer` | `ConstantFoldingRule`, `Cost` struct | Low |
| `krishiv-bench` | TPC-H empty-run guard | Low |

### 5.2 — Documentation Gaps

| Crate | Missing | Priority |
|-------|---------|----------|
| `krishiv-exec` | `block_on` panic docs, `EmitMode` dead field removal | Medium |
| `krishiv-proto` | `label_opt()` dead doc, `TransportVersion` compatibility semantics | Medium |
| `krishiv-async-util` | `block_on` current-thread panic, `unix_now_ms` error behavior | Low |
| `krishiv-plan` | `IntervalJoinSpec` bounds semantics | Low |
| `krishiv-cep` | `times()` limitation, window edge cases | Low |
| All crates | Doc-tests on public APIs | Low |

### 5.3 — Integration Test Gaps

| Test | Crate | Priority |
|------|-------|----------|
| Concurrent two-writer race in disk store | `krishiv-shuffle` | High |
| Multi-stream barrier with timeout | `krishiv-scheduler` | Medium |
| Checkpoint barrier integration with fencing | `krishiv-scheduler` | Medium |
| CDC → Iceberg with real Kafka | `krishiv-connectors` | Medium |
| End-to-end policy enforcement through Flight SQL | `krishiv-flight-sql` | Medium |

---

## Phase 6: Workspace Standards Compliance (Week 6)

**Goal:** Bring all crates to full workspace convention compliance.

### 6.1 — Missing `[lints] workspace = true`

| Crate | File |
|-------|------|
| `krishiv-checkpoint` | `Cargo.toml` |
| `krishiv-operator` | `Cargo.toml` |
| `krishiv-chaos` | `Cargo.toml` |
| `krishiv-bench` | `Cargo.toml` |
| `krishiv-upgrade-tests` | `Cargo.toml` |
| `krishiv-ui` | `Cargo.toml` |

**Fix:** Add `[lints] workspace = true` to each.

### 6.2 — Missing `rust-version.workspace = true`

Same crates as 6.1 (chaos, bench, upgrade-tests).

**Fix:** Add `rust-version.workspace = true` to each.

### 6.3 — Error Type Standards Violations

| Crate | Issue | Fix |
|-------|-------|-----|
| `krishiv-async-util` | `expect()` in `block_on` | Return `Result` |
| `krishiv-plan` | No error enum | Add `PlanError` |
| `krishiv-runtime` | `.expect()` in `flight_protocol.rs` | Return `RuntimeResult` |
| `krishiv-scheduler` | `Result<(), String>` in `cli.rs` | Use `SchedulerError` |
| `krishiv-ui` | `Result<Self, String>` in `main.rs` | Add `UiError` |
| `krishiv-metrics` | `Result<_, String>` in `init()` | Add `MetricsError` |
| `krishiv-catalog` | HTTP errors → `InvalidSchema` | Add `Http` variant |
| `krishiv-shuffle` | `From<io::Error>` discards source | Store `io::Error` |

### 6.4 — Workspace Dependency Consistency

| Crate | Issue | Fix |
|-------|-------|-----|
| `krishiv-ai` | `tiktoken-rs` pinned, not workspace | Add to workspace deps |
| `krishiv-chaos` | `tokio` not workspace ref in dev-deps | Use workspace ref |
| `krishiv-bench` | `criterion` not workspace ref | Add to workspace deps |
| `krishiv-bench` | `tokio` not workspace ref in dev-deps | Use workspace ref |
| `krishiv-python` | `pyo3-arrow` pinned, not workspace | Add to workspace deps |
| `krishiv-udf` | `async-trait` unused dep | Remove |

### 6.5 — `#[non_exhaustive]` on Public Enums

Add `#[non_exhaustive]` to: `Cost`, `RuntimeStats`, `CoalesceAdvice`,
`SplitPlanAdvice`, `FileStats`, `JobStatusUpdate`.

---

## Phase 7: Remaining Low-Priority Polish (Week 7)

### 7.1 — Code Duplication Cleanup

| Area | Fix |
|------|-----|
| `flight_protocol.rs` + `flight_action.rs` IPC encode/decode | Extract shared module |
| `flight_client.rs` + `coordinator_http_client.rs` URL normalization | Extract shared utility |
| `fragment/batch.rs` partition registration 3× | Extract shared helper |
| `state_tumbling.rs` 3× wrapper structs | Generic `StateBackedWindowOperator<W>` |

### 7.2 — Minor Correctness Fixes

| File | Fix |
|------|-----|
| `krishiv` `stream_cmd.rs:284` | "non-negative" → "positive" for u64 |
| `krishiv` `cli.rs:27` | Typo `KRISHV` → `KRISHIV` |
| `krishiv` `daemon_cmd.rs:191` | Typo `KRIVHIV` → `KRISHIV` |
| `krishiv-exec` `side_output.rs:39` | Use `saturating_sub` for overflow |
| `krishiv-exec` `schema_normalize.rs:101` | Extend `is_widen` list |
| `krishiv-exec` `interval_join.rs:42` | Document edge case for negative bounds |
| `krishiv-exec` `barrier_align.rs:32` | Validate `input_count > 0` |
| `krishiv-shuffle` `shuffle_svc.rs:47` | `println!` → `tracing::info!` |
| `krishiv-shuffle` `partitioner.rs:130` | Type mismatch error, not `Io` |
| `krishiv-ai` `embed/huggingface.rs:24` | Error on unknown model name |
| `krishiv-ai` `chunk/markdown.rs:43` | Pre-compute byte offsets |
| `krishiv-checkpoint` `lib.rs:358` | `Relaxed` → `AcqRel` |
| `krishiv-runtime` `lib.rs:104` | `&mut self` → `&self` on `ExecutionBackend` |
| `krishiv-runtime` `stream_exec.rs:38` | Replace SQL heuristics with enum |
| `krishiv-operator` `main.rs:226` | Fix unconditional `demote_to_standby` |
| `krishiv-operator` `pod_manager.rs:176` | Fix `checked_abs` misuse |
| `krishiv-governance` `lib.rs:213` | Bounded eviction for audit dedup map |
| `krishiv-scheduler` `federation_http.rs` | Real `JobSpec` deserialization |
| `krishiv-scheduler` `barrier_tracker.rs` | Add pending barrier timeout |
| `krishiv-scheduler` `tests.rs` | `block_on` → `#[tokio::test]` |
| `krishiv-scheduler` `store.rs` | Atomic JSON write (temp+rename) |

### 7.3 — Orphan Cleanup Improvements [KRISHIV-SHUFFLE]

**File:** `orphan.rs`
**Fix:** Also scan for `.tmp` files from crashed writes. Remove empty parent directories.

### 7.4 — Dead Code Removal

| Crate | Item |
|-------|------|
| `krishiv-async-util` | Unused `serde` dep in `krishiv-cep` |
| `krishiv-proto` | `CheckpointInitiateResponse` unused |
| `krishiv-proto` | `TaskAssignment` likely unused |
| `krishiv-exec` | `ExecError::UnexpectedBatchSchema` never constructed |
| `krishiv-exec` | `compare_key_parts` `#[allow(dead_code)]` |
| `krishiv-exec` | `CepKeyState.last_event_ms` dead store |
| `krishiv-state` | `StoreResult` alias vestigial |
| `krishiv-shuffle` | `ShuffleMetadata` unused by any store impl |
| `krishiv-executor` | `StreamingNotImplemented` version refs will rot |

---

## Execution Order

```
Week 1: Phase 1 (Critical bugs)       — 13 items
Week 2: Phase 2 (Security/integrity)   — 9 items
Week 3: Phase 3 (Architecture/API)     — 11 items
Week 4: Phase 4 (Performance/concur)   — 10 items
Week 5: Phase 5 (Tests/docs)           — ~50 test items + docs
Week 6: Phase 6 (Workspace compliance) — ~25 config items
Week 7: Phase 7 (Polish/cleanup)       — ~30 minor items
```

---

## Acceptance Criteria

For each crate to reach **A+ / Stable**:

1. **Zero critical/high bugs** — all correctness, security, and data-integrity issues resolved.
2. **Zero `unwrap()`/`expect()` in non-test library code** — all panics converted to `Result`.
3. **Proper error types at all public boundaries** — no `Result<_, String>` in public APIs.
4. **`[lints] workspace = true`** in every `Cargo.toml`.
5. **`unsafe_code = "forbid"`** enforced (via workspace lint + per-crate `#![forbid]`).
6. **Zero `cargo clippy --workspace -- -D warnings`** failures.
7. **Zero `cargo test --workspace`** failures.
8. **Test coverage**: every public function has at least one test; critical paths have
   edge-case and failure-mode tests.
9. **No dead code warnings** (unless explicitly `#[allow(dead_code)]` with tracking issue).
10. **Doc comments** on all public types and functions.
11. **`#[non_exhaustive]`** on all public enums that may gain variants.
12. **Source error preservation** via `std::error::Error::source()` on all error types.
13. **No `tokio::task::block_in_place` in current-thread runtime** (documented or avoided).
14. **No silent data loss** — all wire conversions handle all fields.
15. **No security bypass paths** — policy enforcement cannot be circumvented.

---

## Verification

After each phase, run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Final A+ verification:

```bash
# Zero warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings 2>&1 | grep -c "warning"
# Expected: 0

# Zero test failures
cargo test --workspace --all-features 2>&1 | grep "test result"
# Expected: all "0 failed"

# Zero expect/unwrap in non-test code
rg '\.(unwrap|expect)\(' --type rust crates/*/src/*.rs crates/*/src/**/*.rs \
  | grep -v '#\[' | grep -v 'test' | grep -v '_test' | grep -v 'benches/'
# Expected: 0 matches (excluding test/bench code)
```
