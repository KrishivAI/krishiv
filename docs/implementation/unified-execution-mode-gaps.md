# Unified Execution — Mode Parity Gap Analysis

Analysis for PR #43 (2026-05-24). Covers Embedded, SingleNode, and Distributed for unified batch + streaming.

## Mode routing model (current)

| Session config | Runtime type | Data plane (collect) | Plan accept |
|----------------|--------------|----------------------|-------------|
| Embedded | `InProcessExecutionRuntime` | Session-scoped `InProcessCluster` | Local backend |
| SingleNode (no URL) | `InProcessExecutionRuntime` | Same | Local backend |
| SingleNode + `with_local_cluster` | `RemoteExecutionRuntime` + optional fallback | Local cluster (default) or Flight (remote) | Same |
| Distributed + `with_coordinator` | `RemoteExecutionRuntime` + optional fallback | Local cluster (default) or Flight (remote) | Same |

**Remote execution:** `SessionBuilder::with_remote_execution(true)` or `KRISHIV_REMOTE_EXEC=1` disables the in-process fallback and routes batch SQL, explain, bounded windows, and continuous streaming through Arrow Flight SQL.

---

## Fixed in this PR

| ID | Issue | Fix |
|----|-------|-----|
| C1 | `RemoteExecutionRuntime::accept_plan` required Flight SQL | `accept_plan` delegates to local cluster when fallback is set |
| C2 | Local fallback always wins | `with_remote_execution(true)` + `KRISHIV_REMOTE_EXEC` env |
| C3 | Flight SQL cannot see client tables | Flight comment protocol (`krishiv-register-parquet`) + shared `FlightExecutionHost` catalog |
| C4 | `sql_as` bypassed runtime | Policy authorize + `collect_batch_sql` + masking on results |
| H1 | `ensure_local_mode` blocked distributed reads/streams | Removed guards; reads route via runtime |
| H2 | `read_parquet` / logical-only collect bypass | Coordinator / runtime paths |
| H4 | Continuous streaming remote-only | Flight protocol register/push/drain + remote runtime methods |
| H4b | Python lacks stream job bindings | `submit_stream_job`, `push_stream_job_input`, `poll_stream_job` |
| H6 | `krishiv local start` had no Flight server | Spawns `krishiv-flight-server` on `:50051` |
| H7 | Python `stream_exec` double collect | Single `sql_async` + `collect_async` path |
| M1 | Duplicate `accept_plan_with_backend` | Removed; uses `ExecutionRuntime::accept_plan` |
| M2 | `explain_async` local-only | `ExecutionRuntime::explain_sql` (local + remote) |
| M5 | No remote-only test | `remote_execution_without_fallback_uses_flight_server` integration test |
| M3 | `memory_stream` swallowed errors | Propagate registration result |
| H5 | `RemoteExecutionRuntime::mode()` always Distributed | Tracks session `RuntimeMode` |
| M4 | Orphan runtimes in `DataFrame::new` / `Stream::new` | Process-wide shared embedded runtime |

---

## Per-mode capability matrix (after fixes)

| Capability | Embedded | SingleNode | SingleNode+URL | Distributed |
|------------|----------|------------|----------------|-------------|
| `Session.sql()` + collect | ✅ coordinator | ✅ | ✅ local or Flight | ✅ local or Flight |
| `read_parquet` + collect | ✅ | ✅ | ✅ | ✅ |
| `read_delta` / `read_hudi` | ✅ | ✅ | ✅ | ✅ |
| Window `collect` | ✅ | ✅ | ✅ local or Flight | ✅ local or Flight |
| `submit_stream_job` + poll | ✅ | ✅ | ✅ local or Flight | ✅ local or Flight |
| `memory_stream` + window | ✅ | ✅ | ✅ | ✅ |
| Stream `collect_bounded`/map/filter | ✅ | ✅ | ✅ | ✅ |
| `explain` | ✅ | ✅ | ✅ local or Flight | ✅ local or Flight |
| `sql_as` (policy) | ✅ via runtime | ✅ | ✅ | ✅ |
| True remote cluster execution | N/A | N/A | ✅ (remote flag) | ✅ (remote flag) |

---

## Validation

```bash
cargo +stable test -p krishiv-plan -p krishiv-exec -p krishiv-runtime -p krishiv-executor -p krishiv-api -p krishiv-flight-sql -p krishiv-sql-policy --lib
```
