# Architecture, Reusability, Scalability & Technical Debt Review – 2026-05 Follow-up (Incremental)

**Date**: 2026-05-30  
**Review Type**: Targeted incremental follow-up (Phase 1, Slice 1)  
**Baseline**: Deep prior review in `review_report.md` (root) + `docs/implementation/crate-review-mitigation-plan.md` + executed P0-P2 fixes recorded in `docs/implementation/status.md` (as of 2026-05-30) + fresh observability work (`docs/reviews/observability-gap-analysis.md`).  
**Scope of this slice** (per approved plan): 
- Changed / recently expanded files (especially `krishiv-metrics` — part of the observability audit and uncommitted changes on entry).
- Async / Tokio discipline violations (direct check against `docs/engineering/standards.md`).
- Dependency graph / module boundary accuracy (vs `docs/architecture/crate-map.md`).
- High-signal debt patterns via grep (unwrap/expect in lib code, god modules, stringly keys, DashMap usage).
- Focus on the exact dimensions requested: module boundaries, dependency graph, reusability, scalability, Rust ecosystem best practices.

**Explicit Rules Applied** (per AGENTS.md / CLAUDE.md / approved plan):
- Incremental deltas **only** — no re-audit of the 250 prior findings.
- No unrelated refactors or code changes in this review phase (documentation + analysis only; any real P0 would be escalated separately).
- Builds on existing artifacts; produces one durable doc + status.md checkpoint.
- Small durable unit: this slice + validation command.

**Primary References**:
- `docs/engineering/standards.md` (Tokio discipline, crate boundaries, error handling, Arrow model, no blanket exactly-once promises).
- `docs/architecture/crate-map.md` + subagent dependency exploration (plan mode).
- `review_report.md` (god objects, specific bugs/gaps still relevant for coordinator 1805 LOC / runner 1430 LOC).
- Current `docs/implementation/status.md` (recent hardening wins: async isolation in some paths, lease/fencing, fail-closed executor, etc.).
- `docs/architecture/krishiv-vs-spark-and-flink.md` (scalability notes on per-job coordinator model).

---

## 1. krishiv-metrics (New / Recently Expanded Observability Surface)

**Purpose** (from code):
- Centralized OTel + tracing initialization (`init`, `MetricsHandle`, `TracerExporter`, OTLP/stdout/NoOp paths).
- Process-wide `KrishivMetrics` registry (global `OnceLock`) with counters/gauges for tasks, shuffle, queues, per-job checkpoint epochs, watermarks, task attempts, executor slots, source lag, streaming rows, state backend sizes, shuffle partitions.
- Prometheus text renderer (`render_prometheus`) with strict "exactly one HELP + TYPE per family" (recent P0 fix validated in tests).
- W3C trace context propagation helpers (`current_traceparent`, `current_tracestate`) + tonic interceptors (`inject_trace_context`, `extract_trace_context` in `grpc.rs`).
- Structured span field constants (`SPAN_JOB_ID`, `SPAN_EPOCH`, etc.) + `record_span_fields` helper.
- Job cleanup (`remove_job`) to prevent unbounded DashMap growth.

**Issues Identified in This Incremental Pass** (new/expanded code + uncommitted changes):
- **Large monolithic implementation** (`src/lib.rs`): The `KrishivMetrics` impl is ~550+ lines in one file with 20+ `DashMap` fields + one giant `render_prometheus` method (hundreds of lines of repetitive key formatting + BTreeMap collection + string building). This is the same god-object pattern flagged in prior review for coordinator/runner.
- **Stringly-typed keys everywhere**: Most per-job metrics use `format!("{job_id}:{stage_id}")` or `"{job_id}:{source_id}"` as DashMap keys + manual `split_once(':').unwrap_or(...)` parsing in the renderer (multiple sites, e.g. lines ~593, 637, 655, 710 in the version at review time). Fragile, error-prone, no type safety.
- **Cleanup relies on prefix retain scans**: `remove_task_attempt_counters`, `remove_shuffle_partition_counters`, `remove_job` do full `retain` with `starts_with` on every key. Works for moderate job counts but will degrade under long-lived clusters with thousands of historical jobs/stages (linear scan cost + lock contention on DashMaps).
- **Some `unwrap_or` in hot render path** (renderer is called for /metrics scraping). Not panic-level but inconsistent with "prefer explicit error types" in standards.
- **Beta API surface** is documented ("may change between minor releases") — good, but the large internal struct makes future evolution harder without breaking render output or adding more string keys.
- **No histograms or latency distributions yet** (only counters/gauges). Matches the "WEAK" / "MISSING" ratings in the parallel observability gap analysis for backpressure, watermark lag, shuffle partition stuck states, etc.

**Refactoring Opportunities** (reference prior god-object suggestions in `review_report.md`):
- Extract a small `MetricKey` or use nested DashMaps / `dashmap` with composite keys where possible.
- Split `render_prometheus` into per-family helpers or a builder.
- Consider a proper metrics facade (e.g., behind a trait) so the Prometheus renderer is one consumer; future OTLP/StatsD/etc. become easier.
- Add bounded retention or epoch-based cleanup for historical job keys instead of full prefix scans on every job completion.

**Technical Debt Estimate**: **Medium-High** (new surface, high visibility for production debugging, but growing monolithic + stringly keys will hurt maintainability and the exact use case it was built for — incident response).

**Positive Notes**:
- Follows `#![forbid(unsafe_code)]`.
- Recent P0 fix for "exactly one HELP/TYPE per family" is properly tested.
- Structured span fields + W3C propagation are exactly what the observability audit called for.
- `global_metrics()` + lazy init is clean for the process-wide pattern.

---

## 2. Async / Tokio Discipline Violations (Standards.md Compliance)

**Relevant Standard** (`docs/engineering/standards.md`):
> "Do not block Tokio worker threads with long-running compute, file scans, RocksDB operations, or network copies."
> "Keep CPU-heavy operator execution separate from async control-plane I/O."
> "Use `spawn_blocking` for ..."

**Findings (incremental grep + file reads in this slice)**:
- **Still present in connectors** (two-phase commit sinks — critical path for exactly-once lakehouse writes):
  - `crates/krishiv-connectors/src/two_phase.rs:256,288`
  - `crates/krishiv-connectors/src/two_phase_parquet_s3.rs:107,124`
  - Use `tokio::task::block_in_place(move || { ... })` around synchronous Parquet / filesystem work.
- **krishiv-api/src/session.rs** still has multiple `block_on(...)` calls (lines ~442, 464, 541, 560, 578, 664 in the version inspected). These are in the public `Session` API surface used by Python and CLI.
- Recent status work (2026-05-30) did wrap Hudi writes and some two-phase paths with `spawn_blocking` / `block_in_place` in targeted places — partial win, but the pattern is not consistently eliminated and new blocking sites can still appear.
- `krishiv-metrics` init itself is mostly async-safe (OTLP builder), but any caller that does heavy work inside a tracing layer or exporter callback could still cause issues (not a direct violation in the crate yet).

**Impact**:
- Direct violation of published engineering standards.
- Risk of Tokio worker starvation, latency spikes, and "mysterious hangs" under load — exactly the class of production incident the observability work is trying to make debuggable.
- Particularly dangerous in the executor runner pool and connector sink paths (high concurrency).

**Technical Debt Estimate**: **High** (recurring, standards violation, affects the "production readiness" narrative of the current sprint).

---

## 3. Module Boundaries & Dependency Graph Accuracy

**Documented Intent** (`docs/architecture/crate-map.md`):
- Strict one-way dependencies.
- Low-level crates (state, shuffle, plan, proto) must not depend on `krishiv-api` or the `krishiv` facade.
- Public API surface kept narrow; heavy logic stays in implementation crates.

**Actual State (from Cargo.toml + source imports + plan-mode subagent exploration)**:
- **No violations of the "low must not depend on high" rule** — good. Grep confirmed no `krishiv-api` or top-level `krishiv` deps in low-level crates.
- **Documented graph is outdated / incomplete**:
  - `krishiv-metrics` is now wired into `krishiv` facade and several mid-tier crates (not shown in the old diagram).
  - `krishiv-runtime` pulls `krishiv-scheduler`, `krishiv-executor`, `krishiv-sql`, `krishiv-state`, `krishiv-exec`, etc. (far broader than the doc sketch).
  - `krishiv-executor` pulls `krishiv-sql`, `krishiv-ai`, `krishiv-cep`, `krishiv-lakehouse`, `krishiv-state`, `krishiv-shuffle`, `krishiv-metrics`, etc.
  - `krishiv-api` pulls a wide set (state, exec, lakehouse, governance, sql-policy, udf) beyond the narrow "plan + runtime + sql" in the doc.
- These extra edges are **mostly intentional** for the unified execution model (in-process paths, Flight passthrough, etc.) and the "one shared runtime" invariant.
- However, the drift means the crate-map.md no longer serves as an accurate architectural north star or onboarding document. New contributors (or future refactorers) will be surprised by the actual coupling.
- `krishiv` facade + `distributed` module re-exports a lot of scheduler/executor/proto types — powerful for advanced users but increases the "everything is public" surface.

**Reusability Assessment (this slice)**:
- Strong in state, shuffle, runtime, and parts of scheduler (clean traits + multiple backends + `validate_safe_id` security hook).
- Weaker in metrics (one giant concrete registry + string keys) and the large coordinator/runner structs (prior review god-object findings still apply — 1805 LOC and 1430 LOC respectively on entry to this session).
- DataFusion leakage is well-contained at public boundaries (recent `KrishivDataFrameOps` abstraction + "implementation detail" docs in sql crate) — a clear win from the stability sprint.

**Technical Debt Estimate for Graph Drift**: **Medium** (not correctness-breaking today, but increases long-term maintenance cost and risk of accidental cycles or leaky abstractions as the system grows toward R19 multi-region / cell-based designs).

---

## Early Remaining Technical Debt & Mitigation Priorities (Slice 1 Snapshot)

| ID | Area | Severity | One-Line Issue | Link to Prior Work | Suggested Small Mitigation |
|----|------|----------|----------------|--------------------|----------------------------|
| D-2026-01 | krishiv-metrics | Medium-High | Monolithic `KrishivMetrics` + stringly keys + retain() scans | New in this sprint + observability audit | Extract key types; split renderer; bounded cleanup |
| D-2026-02 | Async discipline | High | Remaining `block_in_place` / `block_on` in connectors + api/session | Standards.md + partial fixes in status | Systematic `spawn_blocking` audit + wrapper policy |
| D-2026-03 | Architecture docs | Medium | crate-map.md significantly out of date vs real Cargo + imports | crate-map.md + prior review | Update graph + add "intentional cross edges for unified execution" note |
| D-2026-04 | God modules (recurring) | High | coordinator.rs 1805 LOC, runner.rs 1430 LOC — no splitting since prior deep review | review_report.md (B-S, B-E sections) | One small extract per checkpoint (e.g., barrier dispatch or task launch logic) |
| D-2026-05 | Stringly state (metrics) | Medium | `"{job}:{stage}"` keys + manual split in renderer | Same pattern risk as old merge key regex issues (C-series fixes) | Typed key newtypes or nested maps |
| D-2026-06 | Reusability (metrics) | Medium | No abstraction over the registry; Prometheus is the only renderer | New surface | Small `MetricsExporter` trait behind the concrete impl |

(Full table will grow in later slices. All items above are incremental to the executed P0-P2 work.)

---

## Validation for This Slice (Phase 1, Slice 1)

```bash
ls -l docs/reviews/architecture-debt-review-2026-05.md
head -60 docs/reviews/architecture-debt-review-2026-05.md
cargo check --workspace 2>&1 | tail -3
cargo fmt -- --check
```

**Next Slice Suggestion** (per approved plan): Expand the debt doc with 2-3 more core crates (e.g., deeper scheduler + executor sampling using the new runner.rs changes visible in git) + any P0s discovered, then close the checkpoint in status.md.

---

**This document is intentionally partial** (first durable slice only). It will be extended in subsequent small units. All findings are grounded in direct file reads, greps, Cargo manifests, and the approved plan produced during plan mode exploration.

**References to prior artifacts are mandatory** — this is a follow-up, not a replacement.