# Crate-by-Crate Stability Resolution Plan

Generated: 2026-05-29
Scope: fresh code review of all 32 workspace crates against current source.
Goal: concrete, prioritized work to move every crate to ✅ Stable maturity.
Maturity scale: 🔴 Stub / 🟡 Beta / ✅ Stable.

This plan supersedes the snapshot in `crate-review-mitigation-plan.md` (2026-05-28).
It was produced by reading current code crate-by-crate, so several items the prior
plan and `status.md` list as "fixed" are re-verified below — including some that are
**not actually present in the code**.

---

## 1. Maturity Dashboard

| Crate | Grade | Headline blocker to Stable |
|-------|-------|----------------------------|
| krishiv-proto | ✅ Stable | None (enforce no-Arrow/DataFusion dep in CI). |
| krishiv-async-util | ✅ Stable | `block_on` panics under current-thread runtime (hardening). |
| krishiv-metrics | ✅ Stable* | Server-side trace context not re-parented → broken distributed traces. |
| krishiv-catalog | ✅ Stable* | No table create/drop/commit/snapshot APIs in REST client. |
| krishiv-udf | ✅ Stable* | Narrow `ScalarValue` coverage; no E2E distributed UDAF test. |
| krishiv-plan | ✅ Stable* | `encode_stream_fragment` drops multi-source watermark fields. |
| krishiv-scheduler | 🟡 Beta | Auth allow-anonymous default; lossy federation wire model. |
| krishiv-executor | 🟡 Beta | **Fail-open**: unsupported fragments report `Succeeded`; timers not checkpointed. |
| krishiv-operator | 🟡 Beta | Lease optimistic-concurrency uses empty `resourceVersion` (split-brain). |
| krishiv-exec | 🟡 Beta | Continuous path: StateBacked ops unwired w/o TTL, no idle-source timeout, no checkpoint. |
| krishiv-optimizer | 🟡 Beta | String-based predicate splitting/column matching → wrong pushdown. |
| krishiv-runtime | 🟡 Beta | Inherits exec streaming gaps; `list_jobs` panics on poisoned lock. |
| krishiv-sql | 🟡 Beta | Iceberg MERGE is dry-run-only; AS OF Timestamp dropped; regex MERGE parser. |
| krishiv-api | 🟡 Beta | All-or-nothing policy routing; masking on raw query relations. |
| krishiv (facade/CLI) | 🟡 Beta | Inherits sql gates; surprising policy routing via `--api-key`. |
| krishiv-sql-policy | 🟡 Beta | RLS via string subquery-wrap (errors on projected queries), not Filter injection. |
| krishiv-state | 🟡 Beta | TTL write-time expiry is wall-clock, not watermark-aware. |
| krishiv-shuffle | 🔴→🟡 | **Path-traversal validation & disk fsync claimed-fixed but ABSENT**; cross-process fencing missing. |
| krishiv-checkpoint | 🟡 Beta | Savepoint create/restore is metadata-flag-only (no operations). |
| krishiv-connectors | 🟡 Beta | SASL creds unredacted in Debug; "transactional" Kafka is in-memory sim; no rate limiter. |
| krishiv-lakehouse | 🟡 Beta | Delta overwrite writes no `remove` action → corrupt version time-travel. |
| krishiv-schema-registry | 🟡 Beta | No compatibility modes; `Box::leak` memory leak in protobuf path. |
| krishiv-cep | 🟡 Beta | Linear matcher (not NFA); no per-stage predicate eval; `max_gap_ms` unenforced. |
| krishiv-python | 🟡 Beta | `.pyi` covers 14/25 classes; error mapping loses detail. |
| krishiv-flight-sql | 🟡 Beta | `do_get` buffers full result in memory (OOM); no DoPut/DoExchange. |
| krishiv-governance | 🟡 Beta | **Case-sensitive masking → PII leak** for `SSN`/`Password_Hash` etc. |
| krishiv-ai | 🟡 Beta | LSH hashes raw f32 bits (misses near-dupes); no dollar-cost tracking. |
| krishiv-vector-sinks | 🟡 Beta | **SQL injection** via unvalidated pgvector table name; weaviate GraphQL injection. |
| krishiv-ui | 🟡 Beta | No auth on status/topology routes. |
| krishiv-bench | 🟡 maintained | Bench-only crate; OK. |
| krishiv-chaos | 🟡 maintained | Functional fault injector; OK. |
| krishiv-upgrade-tests | 🟡 maintained | Forward-compat tests present; OK. |

`*` = effectively Stable; remaining item is hardening/scope rather than a correctness blocker.

---

## 2. Security & Data-Integrity Blockers (do first)

These are correctness/security defects that should gate any "stable" claim.

| # | Crate | Defect (file:line) | Fix |
|---|-------|--------------------|-----|
| S1 | krishiv-governance | `column_masking_rule` case-sensitive `SENSITIVE.contains(&column)` (`src/lib.rs:129-130`) — `SSN`, `Password_Hash`, `Credit_Card` bypass masking and leak | Lowercase column + sensitive list before compare; make table-aware. |
| S2 | krishiv-vector-sinks | pgvector interpolates `table_name` into CREATE/INSERT/DELETE/SELECT (`pgvector.rs:54-138`); weaviate interpolates `class_name`+vector into GraphQL (`weaviate.rs:109-110`) | Add `validate_identifier()` (`^[A-Za-z_][A-Za-z0-9_]*$`) in all sink constructors; parameterize weaviate query. |
| S3 | krishiv-executor | Unsupported batch fragment returns `placeholder()` → `runner.rs:879-903` reports `Succeeded` (`fragment/batch.rs:175`) — silent no-op / data loss | Return `Err(ExecutorError::InvalidAssignment)`; add regression test asserting `Failed`. |
| S4 | krishiv-shuffle | `validate_safe_id()` + disk `sync_all()` are claimed fixed in `status.md:282,311` but **do not exist** in code; `job_id`/`stage_id` flow raw into paths (`disk_store.rs:51-56`, `local_store.rs:58-59`, `object_store.rs:46-52`) | Implement `validate_safe_id` at every id ingress; add `sync_all()` before rename + parent-dir fsync. |
| S5 | krishiv-connectors | `KafkaCdcConfig` derives `Debug` with plaintext `sasl_password`/`sasl_username` (`cdc.rs:727-743`) | Manual `Debug` redacting secrets; audit `KafkaConfig`. |
| S6 | krishiv-lakehouse | Local Delta overwrite deletes Parquet but writes no `remove` action (`local_delta.rs:98-106`) → older version logs reference deleted files; version time-travel silently returns partial/empty data | Emit `remove` actions; retain files until vacuum (or adopt delta-rs). |
| S7 | krishiv-operator | Lease patch uses `resource_version` from `unwrap_or_default()` → empty string makes optimistic-concurrency an unconditional write (`lease.rs:209-228`) — split-brain leader takeover | Fail/retry when `resource_version` is `None`; never patch with empty version. |
| S8 | krishiv-schema-registry | `Box::leak` per non-string protobuf value (`lib.rs:455,459`) — unbounded memory leak | Build `StringArray` from owned `String`s. |

**Validation gate:** add a regression test per item; run
`cargo test -p krishiv-governance -p krishiv-vector-sinks -p krishiv-executor -p krishiv-shuffle -p krishiv-connectors -p krishiv-lakehouse -p krishiv-operator -p krishiv-schema-registry --lib`.

---

## 3. Correctness Blockers (streaming & query)

| # | Crate | Defect (file:line) | Fix |
|---|-------|--------------------|-----|
| C1 | krishiv-exec | StateBacked sliding/session/tumbling operators only used when `state_ttl_ms.is_some()`; otherwise stateless ops (`continuous.rs:85,112,138`) — unbounded jobs lose state | In `build_operator` always build StateBacked variants (default ephemeral backend), mirroring the bounded path. |
| C2 | krishiv-exec | Continuous watermark tracker has no idle-source policy (`continuous.rs:38-57`) — one stalled source freezes all windows | Build `multi` with idle policy and call `apply_idle_source_policy()` each drain (as `operator_runtime.rs:72` does). |
| C3 | krishiv-exec | `ContinuousWindowExecutor` never checkpoints (`continuous.rs:216-326`) — crash loses open windows | Add `checkpoint()` delegating to operator `persist_to_state`; invoke from runtime drain loop. |
| C4 | krishiv-exec | Interval join is arrival-order dependent & over-evicts for asymmetric bounds (`interval_join.rs:54-87`) | Evaluate with fixed left/right orientation; eviction horizon `wm - max(|lower|,|upper|)`. |
| C5 | krishiv-optimizer | Predicate pushdown splits conjuncts on literal `" AND "` and matches columns by unqualified suffix (`lib.rs:642-645,771-780`) — wrong filters / wrong scan | Operate on structured predicate AST; resolve column ownership by qualified name; add fixpoint iteration. |
| C6 | krishiv-sql | Iceberg `MERGE INTO` is dry-run only — counts but no writeback (`lakehouse/merge.rs:139-243`); AS OF Timestamp parsed then dropped (`lakehouse/providers.rs:251-265`); regex MERGE parser | Implement iceberg writeback or return `Unsupported`; wire Timestamp→`snapshot_for_timestamp`; replace regex MERGE with sqlparser AST. |
| C7 | krishiv-sql | `CREATE FUNCTION ... RETURNS TABLE` registers `StubTableUdf` returning empty batch (`create_function_ddl.rs:177-190`); UDTF bridge ignores args (`udf.rs:257-268`) | Compile/dispatch body or surface as schema-only stub; convert literal args to `ScalarValue`. |
| C8 | krishiv-sql-policy | RLS = string subquery-wrap `SELECT * FROM (q) WHERE pred` (`lib.rs:197-219`) — fails to resolve predicate columns not in projection | Inject `Filter` above `Scan` in the logical plan; mask by source-column provenance. |
| C9 | krishiv-state | TTL `put` computes `expires_at` from wall-clock `unix_now_ms()` even with watermark set (`ttl.rs:118-122`) | Use watermark-aware `now_ms()` for write-time expiry; add `ttl_does_not_evict_state_within_watermark_lag` test. |
| C10 | krishiv-lakehouse | MERGE/Hudi upsert read+rewrite entire base file per commit (`delta_lake.rs:90,137`, `hudi.rs:218-308`); `typed_key` collides across int widths (`delta_lake.rs:213`); `ArrayFormatter` `.expect()` panics (`delta_lake.rs:225`) | File-level pruning; include concrete `DataType` in key prefix; propagate error instead of panic. |
| C11 | krishiv-checkpoint | `create_savepoint`/`restore_savepoint` not implemented — only `is_savepoint` fields exist (`lib.rs:117-120`) | Implement copy-to-immutable-savepoint-prefix + fenced restore. |

---

## 4. Per-Crate Resolution Checklists

### ✅ Already Stable — maintain / harden only

- **krishiv-proto** — Add a CI `cargo tree` check asserting no `arrow`/`datafusion` dependency; add an exhaustiveness round-trip test for `OutputContractDescriptor`.
- **krishiv-async-util** — Align `block_on` with checkpoint's `run_blocking_on_tokio` flavor check (avoid current-thread panic, `lib.rs:29-32`); add a no-runtime fallback-path test.
- **krishiv-metrics** — Fix `extract_trace_context` to set a real remote parent via an OTel propagator (`grpc.rs:40-48`); add a cross-process parent/child linkage test.
- **krishiv-catalog** — Add table create/drop/commit + snapshot/time-travel read APIs to `GenericRestCatalog` (`iceberg_rest.rs:106-215`); add a distinct `Transport` error variant.
- **krishiv-udf** — Broaden `ScalarValue` (Float32/temporal/decimal/list) or document the supported subset; add an E2E distributed UDAF merge test; pin UDAF state encoding (length-prefixed).
- **krishiv-plan** — Make `encode_stream_fragment` carry `source_watermark_lags`/`source_id_column` (or move to serde); add fragment round-trip + lowering tests for each `WindowKind`.

### 🟡 Beta — ordered path to Stable

**krishiv-scheduler**
1. Flip gRPC auth to deny-by-default in production; gate anonymous behind explicit `--insecure`/dev flag (`auth.rs:24-27`).
2. Make federation submit lossless or reject unsupported `JobSpec` fields (`federation_http.rs:32-54`).
3. Mark the federation/`SELECT 1` known issue closed in `status.md` (already fixed at `federation_http.rs:84-103`).

**krishiv-executor**
1. Fail closed on unsupported batch/streaming fragments (`fragment/batch.rs:175`) — **S3**.
2. Persist/restore event-time + processing-time timers in checkpoint snapshots (`runner.rs:480-511`).
3. Attempt-fenced, staged-then-commit object-store sink writes (`fragment/common.rs:241-258`).
4. Distinguish "no state" from "snapshot unsupported" and fail closed (`runner.rs:502`).

**krishiv-operator**
1. Require a concrete `resourceVersion` for Lease patches — **S7** (`lease.rs:209`).
2. Stop swallowing reconcile errors in delete/failure paths; log + requeue (`reconciler.rs:151,164`).
3. Replace `unreachable!()` status stub with a safe no-op (`main.rs:175`).
4. Add a leader-election race test (stale holder cannot take over a live lease).

**krishiv-exec** — **C1–C4** then:
5. Track per-key high-water flushed boundary so late-but-within-lag events don't re-open a window and emit duplicates (`tumbling.rs:242`, `sliding.rs:235`, `session.rs:233`).
6. Make `MemoCache` true LRU on hit or relabel as FIFO (`memo.rs:33-54`).
7. Implement or document-as-helper the temporal join (`temporal_join.rs`).

**krishiv-optimizer** — **C5** then add tests for literal-`AND`, ambiguous join columns, and cascading pushdown→empty-projection pruning; generalize pushdown beyond Filter-above-Scan.

**krishiv-runtime**
1. Add registry checkpoint/restore once exec exposes it (`continuous_stream.rs:72-94`).
2. Replace `list_jobs` `.expect()` poison panic with `RuntimeResult` (`continuous_stream.rs:111`).
3. Audit reachable `RuntimeError::Unsupported`/"stub" task paths (`lib.rs:53,67,95,212,239`).

**krishiv-sql** — **C6, C7** then implement `supports_filters_pushdown` on Delta/Hudi scan providers (`lakehouse/providers.rs:87-225`); route CREATE/REFRESH/DROP LIVE TABLE through `SqlEngine::sql` and implement refresh (`live_table.rs:172`); emit typed `NodeOp::CepPattern` from `cep_sql.rs`.

**krishiv-api**
1. Apply policy per-principal on every SQL path instead of all-or-nothing hard-fail (`session.rs:543-566`).
2. Mask against rewritten output schema, not raw query relations (`session.rs:632-647`).
3. Implement or explicitly error `unbounded_memory_stream` (`session.rs:771`); fix Hudi multi-batch metric aggregation (`session.rs:702-739`); remove vestigial `OnceLock` (`session.rs:104`).

**krishiv (facade/CLI)**
1. Make CLI policy routing explicit — force `--api-key` when keys configured, clear error otherwise (`query_cli.rs:216-220`).
2. Replace `unreachable!()` daemon fallthrough with an error (`daemon_cmd.rs:28`).
3. Inherits krishiv-sql Stable gates.

**krishiv-sql-policy** — **C8** then switch `is_select_query` to AST statement type (`lib.rs:171-195`); de-duplicate local/remote authorization paths (`lib.rs:66-146`).

**krishiv-state** — **C9** then reconcile the stale `Arc<Mutex<Database>>` plan note with the bare-`Database` design; batch/background `purge_expired` to remove per-key-txn latency (`ttl.rs:234-254`).

**krishiv-shuffle** — **S4** first, then:
2. Cross-process object-store fencing via conditional PUT (if-none-match/etag) keyed on lease token (`object_store.rs:143`).
3. Wire orphan cleanup to a metadata-driven point-in-time active-job snapshot; treat equal-token re-commit to an existing final path as a no-op (`disk_store.rs:170-187`).
4. Add the named `disk_store_concurrent_lease_registration_no_toctou` and `object_store_orphan_cleanup_skips_active_jobs` tests.

**krishiv-checkpoint** — **C11** then make object-store writes atomic (temp-key+rename) or document manifest-sealed atomicity (`object_store.rs:46-57`); switch epoch deletion to batched `delete_stream` (`object_store.rs:108-128`).

**krishiv-connectors** — **S5** then:
2. Replace in-memory transactional Kafka simulation with real rdkafka transactions, or stop advertising exactly-once (`transactional_kafka.rs:21-91`).
3. Implement + wire a token-bucket `RateLimiter` into source poll (`source.rs`).
4. Route malformed CDC envelopes to a dead-letter sink instead of dropping (`cdc_router.rs:78`) / aborting (`cdc.rs:378,473`).

**krishiv-lakehouse** — **S6, C10** then add `snapshot_for_timestamp` time-travel (`as_of.rs:10`); provide an FS-backed two-phase commit + real (or clearly-scoped) Iceberg backend (`two_phase.rs:33`, `iceberg_fs.rs:83`).

**krishiv-schema-registry** — **S8** then implement compatibility modes (BACKWARD/FORWARD/FULL); handle the Confluent protobuf message-index prefix (`lib.rs:205-213`); return a correct `arrow_schema()` for Avro (`lib.rs:156`).

**krishiv-cep**
1. Replace the linear matcher with an NFA (quantifiers, negation, branching) (`pattern.rs:81-119`).
2. Move per-stage predicate evaluation into the matcher to cover `DEFINE` (`matcher.rs:36-55`).
3. Enforce per-stage `max_gap_ms` and surface timed-out partials (`matcher.rs:46`).

**krishiv-python**
1. Bring `.pyi` to 100% class/function coverage; CI-gate new unannotated `#[pyclass]`/`#[pyfunction]` (`python/krishiv/krishiv.pyi`).
2. Map each `KrishivError` variant to a dedicated exception with a `code` (`errors.rs:31-37`).
3. Clean the test-mock `todo!()` so the zero-`todo!` gate passes (`lib.rs:203,206`).

**krishiv-flight-sql**
1. Stream `RecordBatch`es incrementally in `do_get` instead of buffering the full result (`lib.rs:352-358`).
2. Implement or formally scope-out DoPut/DoExchange.
3. Remove the `expect` panic path in auth (`lib.rs:132`).

**krishiv-governance** — **S1** then make masking + table-access table-aware and case-normalized (`lib.rs:117,123-135`); add a config-driven production policy hook.

**krishiv-ai**
1. Implement real SimHash/random-projection LSH (`dedup.rs:41-65`).
2. Add a per-model price table; aggregate USD cost from prompt/completion tokens (`llm/mod.rs:42`).
3. Close the `krishiv_async_util` plan item as N/A (not used here).

**krishiv-vector-sinks** — **S2** then parameterize weaviate GraphQL; extend idempotency certification to real backends in CI (`certification.rs:10-24`).

**krishiv-ui**
1. Add auth (reuse `krishiv-governance::AuthProvider`) or formally scope UI to a trusted network (`lib.rs:103-119`).
2. Confirm read-only scope for Stable; expand error-path handler tests.

---

## 5. `status.md` Accuracy Corrections

The review found claims in `status.md` that are **not present in current code**. These
must be corrected so the tracker reflects reality:

| status.md claim | Reality | Action |
|-----------------|---------|--------|
| L282 (Phase 1.1): shuffle `validate_safe_id()` added to `store.rs` + applied to disk/object/local/path | No such symbol exists in `crates/krishiv-shuffle/src` | Re-open as **S4**; correct the log entry. |
| L311 (3.22): disk_store `sync_all()` after Parquet write | No `sync_all`/`sync_data` in `disk_store.rs` | Re-open as **S4**; correct the log entry. |
| L595-623 (feature plan R9): RLS WHERE-rewrite "not implemented" | It *is* implemented, but as a fragile string subquery-wrap, not Filter injection | Update to "partial — string-wrap; Filter injection pending" (**C8**). |
| feature-stability-plan R5.2: redb `Arc<Mutex<Database>>` bottleneck | Code uses a bare `Database` (redb MVCC) | Mark the bottleneck item stale/closed. |
| Federation "ignores spec_json / runs SELECT 1" | Fixed at `federation_http.rs:84-103` | Mark closed. |

Confirmed still-open items from prior review (correctly tracked): object-store
shuffle fencing process-local; two-phase Parquet recovery overwrite; executor
placeholder success; checkpoint timers not snapshotted; lakehouse full
materialization.

---

## 6. Suggested Execution Phasing

| Phase | Theme | Items | Gate |
|-------|-------|-------|------|
| P0 | Security & data-integrity | S1–S8 | per-crate `--lib` tests + regression test per item |
| P1 | Streaming correctness | C1–C4, C9, C11 | `cargo test -p krishiv-exec -p krishiv-state -p krishiv-checkpoint --lib` |
| P2 | Query correctness | C5–C8, C10 | `cargo test -p krishiv-sql -p krishiv-optimizer -p krishiv-sql-policy -p krishiv-lakehouse --lib` |
| P3 | Surface hardening | flight-sql streaming, python `.pyi`, ui auth, metrics trace-context, scheduler auth default | `cargo test -p krishiv-flight-sql -p krishiv-python -p krishiv-ui -p krishiv-metrics -p krishiv-scheduler --lib` |
| P4 | Integration depth | connectors transactions/rate-limit, lakehouse time-travel/Iceberg backend, schema-registry compat modes, cep NFA, catalog table lifecycle | feature-gated + external-system tests |
| P5 | Stable maintenance | proto/async-util/udf/plan hardening; CI invariant checks | `cargo test --workspace && cargo clippy --workspace -- -D warnings` |

**Definition of done for "Stable" per crate:** no fail-open/silent-loss paths;
no unredacted secrets or injection surfaces; critical paths have regression tests;
crate-boundary invariants hold; and the per-crate checklist above is cleared.
