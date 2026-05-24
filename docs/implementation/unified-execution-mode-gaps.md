# Unified Execution — Mode Parity Gap Analysis

Analysis before merge of PR #43 (2026-05-24). Covers Embedded, SingleNode, and Distributed for unified batch + streaming.

## Mode routing model (current)

| Session config | Runtime type | Data plane (collect) | Plan accept |
|----------------|--------------|----------------------|-------------|
| Embedded | `InProcessExecutionRuntime` | Session-scoped `InProcessCluster` | Local backend |
| SingleNode (no URL) | `InProcessExecutionRuntime` | Same | Local backend |
| SingleNode + `with_local_cluster` | `RemoteExecutionRuntime` + fallback | **Local cluster** | **Local cluster** (fixed) |
| Distributed + `with_coordinator` | `RemoteExecutionRuntime` + fallback | **Local cluster** | **Local cluster** (fixed) |

**Phase-0 semantics:** Distributed/SingleNode-with-URL sessions use a **client-embedded** coordinator+executor for all data-plane work. Flight URL is reserved for future true remote execution when local fallback is disabled.

---

## Fixed in this PR (post follow-up)

| ID | Issue | Fix |
|----|-------|-----|
| C1 | `RemoteExecutionRuntime::accept_plan` required Flight SQL → window/stream jobs failed without Flight server | `accept_plan` delegates to local cluster when fallback is set |
| H1 (partial) | `ensure_local_mode` blocked memory streams, read_parquet/delta/hudi, legacy stream ops in Distributed | Removed guards; reads route via `register_parquet` + `sql_async` |
| H2 (partial) | `read_parquet().collect()` bypassed coordinator | `read_parquet_async` registers table + `SELECT *` through runtime |
| M3 | `memory_stream` swallowed registration errors | Propagate `register_memory_stream` result |
| H5 (partial) | `RemoteExecutionRuntime::mode()` always returned Distributed | Tracks session `RuntimeMode` |

---

## Remaining gaps (document, do not block merge)

### CRITICAL — true remote execution (R2+)

**C2 — Distributed collect never hits remote cluster when fallback is set**

- `collect_batch_sql` / `collect_bounded_window` always prefer `local_fallback`.
- `Session.connect("http://remote:50051")` executes on client-embedded cluster, not remote workers.
- **Next:** `SessionBuilder::with_remote_execution(true)` or env `KRISHIV_REMOTE_EXEC=1` to skip fallback; gRPC job submit for batch/stream.

**C3 — Flight SQL cannot see client-registered tables**

- `execute_remote_sql` sends SQL only; Flight server uses fresh embedded session per request.
- **Next:** Catalog sync over Flight or route remote batch through coordinator `sql:` tasks.

**C4 — `sql_as` / policy path bypasses `ExecutionRuntime`**

- Policy executes locally via `PolicyEnforcingSqlEngine`; results wrapped in `from_batches`.
- **Next:** Policy check client-side, execute via `collect_batch_sql` on executor.

### HIGH

**H2 — `DataFrame.collect_async` fallback for logical-only frames**

- Frames without `sql_query` still call `SqlDataFrame::collect()` directly (local DataFusion).
- Affects explain-only and legacy construction paths.

**H4 — Continuous streaming remote execution**

- Push/drain always uses local fallback; no remote continuous job API.
- Python lacks `submit_stream_job` / `poll_stream_job` bindings.

**H6 — `krishiv local start` vs session cluster**

- CLI starts external coordinator/executor processes; session always creates its own `InProcessCluster`.
- External daemon not wired to session execution yet.

**H7 — Python `stream_exec` double SQL collect for SQL sources**

- `resolve_input_batches` runs `sql` + `collect` before window runtime (wasteful but correct).

### MEDIUM

**M1 — Parallel routing helpers**

- `accept_plan_with_backend` still exists for legacy paths; prefer `ExecutionRuntime::accept_plan` everywhere.

**M2 — `explain_async` still local-only**

- `ensure_local_mode` retained for explain (plan metadata; no data movement).

**M5 — Test coverage**

- No test against live remote Flight-only runtime (fallback disabled).
- No Python distributed window integration test.

---

## Per-mode capability matrix (after fixes)

| Capability | Embedded | SingleNode | SingleNode+URL | Distributed |
|------------|----------|------------|----------------|-------------|
| `Session.sql()` + collect | ✅ coordinator | ✅ | ✅ local cluster | ✅ local cluster |
| `read_parquet` + collect | ✅ coordinator | ✅ | ✅ | ✅ |
| `read_delta` / `read_hudi` | ✅ (sql query set) | ✅ | ✅ | ✅ |
| Window `collect` | ✅ | ✅ | ✅ | ✅ |
| `submit_stream_job` + poll | ✅ | ✅ | ✅ | ✅ |
| `memory_stream` + window | ✅ | ✅ | ✅ | ✅ |
| Stream `collect_bounded`/map/filter | ✅ | ✅ | ✅ | ✅ |
| `explain` | ✅ local | ✅ local | ❌ blocked | ❌ blocked |
| `sql_as` (policy) | ✅ local only | ✅ local only | ✅ local only | ✅ local only |
| True remote cluster execution | N/A | N/A | ❌ phase-0 | ❌ phase-0 |

---

## Validation

```bash
cargo +stable test -p krishiv-plan -p krishiv-exec -p krishiv-runtime -p krishiv-executor -p krishiv-api --lib
```
