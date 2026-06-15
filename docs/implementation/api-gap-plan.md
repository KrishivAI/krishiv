# API Gap Implementation Plan

Cross-surface plan to close Rust / SQL / Python parity gaps identified in the API
matrix review.

## Category 1 — Documentation drift

| Item | Action |
|------|--------|
| `grammar.rs` marks `$1` binding as planned | Mark supported (local + Flight) |
| `phase-4-user-apis.md` lists cache/persist as remaining | Mark implemented |
| `Session::sql_as` documented but missing | Implement on `Session` |
| Prepared statements listed as remaining | Update status to partial (local + Flight server) |

## Category 2 — SQL gaps

| Item | Action |
|------|--------|
| `CREATE/REFRESH/DROP LIVE TABLE` not routed | Intercept in `SqlEngine::sql()` |
| `DESCRIBE` / `SHOW COLUMNS` | New `introspection_sql` intercept |
| `EXPLAIN` as SQL statement | Route to `explain_sql` helpers |
| Grammar matrix entries | Add describe/explain/live-table features |

## Category 3 — Python parity

| Item | Action |
|------|--------|
| `LiveTable`, `read_kinesis`, `read_pulsar` exports | Add to `__init__.py` `__all__` |
| `read_stream` / `DataStreamReader` | PyO3 binding |
| `StreamingDataFrame` joins / dedup / side output | `PyStreamingDataFrame` wrapper |
| Typed catalog API | Bind `table_metadata`, `list_table_identifiers`, etc. |
| `sql_as` | Bind on `Session` |
| `operation_registry` per session | Share session registry |

## Category 4 — Rust API gaps

| Item | Action |
|------|--------|
| `Session::sql_as` | Auth + policy + `sql_async` |
| Auth/policy on `Session` | Store from `SessionBuilder` |
| `Session::operation_registry` | Shared `OperationRegistry` |
| `Session::live_table_registry` | Expose engine registry |

## Category 5 — Remote query progress / cancel

| Item | Action |
|------|--------|
| `OperationRegistry` progress | `update_progress` / `progress` |
| Flight `CancelOperation` / `GetOperationProgress` | New `KrishivFlightAction` variants |
| `FlightExecutionHost` registry | Shared across requests |

## Deferred (explicit follow-up) — completed 2026-06-15

- Distributed atomic sink writes (`DataFrameWriter` → staged parquet sink)
- JDBC/ODBC gateway (`krishiv-sql-gateway` crate)
- Flight SQL transactions (`BeginTransaction` / `EndTransaction` actions)
- Generic reader/writer `option()` compatibility mapping
- Python: `BlockingSession`, streaming joins, `register_function` for Rust UDFs
