# Phase 4 — Complete User-Facing APIs

Goal: make Krishiv practical to adopt from Rust and Python without exposing
DataFusion implementation types.

## Implemented in this change

- [x] Typed Rust `Expr` API with quoted column references, typed literals,
      predicates, arithmetic, aliases, ordering, null checks, and common
      aggregate functions.
- [x] Typed `DataFrame::select_exprs`, `filter_expr`, `with_column_expr`, and
      `group_by(...).agg(...)` APIs.
- [x] Engine-neutral grouped aggregation through `KrishivDataFrameOps` rather
      than leaking DataFusion expressions into `krishiv-api`.
- [x] Generic `Session::read().format(...).load(...)` and
      `DataFrame::write().format(...).save(...)` builders for Parquet, CSV, and
      JSON.
- [x] Shared session configuration `set/get/unset/configs` API and builder-time
      properties.
- [x] Logical, physical, and analyze explain modes with lightweight local query
      statistics.
- [x] Python DataFrame transformation parity for select, expression select,
      filter, limit, distinct, sort, drop, rename, computed columns, null fill,
      grouped aggregation, file writes, and explain modes.
- [x] Python CSV/JSON readers and session configuration parity.

## Deliberate compatibility rules

- `Expr::raw` is the explicit escape hatch for advanced SQL. Safe constructors
  quote identifiers and escape string literals.
- Generic reader/writer options are reserved but rejected until each option has
  format-specific semantics; silently ignoring options would be unsafe.
- Current file writers retain the documented local collect-and-write semantics.
  They are not presented as distributed atomic sink operators.
- Analyze statistics are currently local. Remote statistics require a versioned
  coordinator/Flight metrics response rather than client-side guesses.
- Flight SQL `BeginTransaction`/`EndTransaction` track an opaque transaction id
  for client bookkeeping only; every statement still executes autocommit
  regardless of an open transaction (no write buffering, no snapshot reads,
  no participation by the catalog or execution layer). `Commit` and
  `Rollback` are therefore both no-ops against already-applied statements —
  real atomicity/isolation requires staged execution and is tracked as
  remaining work, not implied by the actions existing.

## Remaining Phase 4 work

- [ ] Add distributed physical sink operators and write modes (`append`,
      `overwrite`, dynamic partition overwrite, error/ignore if exists).
- [ ] Add partitioned/bucketed writes and target output file sizing.
- [ ] Propagate supported reader/writer options into DataFusion and connector
      configuration using typed format-specific option structures.
- [ ] Add Python Arrow-batch scalar UDF execution with explicit memory/time
      limits; current Python UDF integration remains row/function oriented.
- [ ] Add aggregate, table, and async lookup UDF parity in Python.
- [ ] Add coordinator query progress, metrics streaming, and cancellation to
      `ExecutionRuntime`, Flight actions, Rust `Session`, and Python `Session`.
- [ ] Add JDBC/ODBC compatibility through a separately versioned SQL gateway;
      Arrow Flight SQL remains the native protocol. (`krishiv-sql-gateway` facade
      crate landed; wire-protocol drivers remain follow-up.)
- [ ] Add remote analyze metrics once coordinator transport is versioned.

## Recently completed (API gap closure)

- [x] Cache/persist/unpersist and temporary-view APIs (Rust + Python).
- [x] Local prepared-statement parameter binding (`$1`, `$2`, …).
- [x] `Session::sql_as` policy-enforced SQL entry point.
- [x] SQL `DESCRIBE` / `SHOW COLUMNS` / `EXPLAIN` intercepts.
- [x] `CREATE LIVE TABLE` routing through `SqlEngine::sql()`.
- [x] Distributed atomic parquet sink writes via `DataFrameWriter` (remote SQL-backed).
- [x] Flight SQL `BeginTransaction` / `EndTransaction` actions (bookkeeping
      only — see "Deliberate compatibility rules" for the atomicity caveat).
- [x] Generic reader/writer `option()` compatibility mapping.
- [x] `krishiv-sql-gateway` separately versioned JDBC/ODBC facade crate.
- [x] Python `BlockingSession`, streaming joins (`stream_table_join`, `temporal_join`,
      `stream_stream_join`), and `register_function` for Rust scalar UDFs.

## Exit criteria

The local Rust and Python API slice is implemented when focused SQL/API/Python
checks pass. Remote analyze metrics and full JDBC/ODBC wire-protocol drivers
remain explicit follow-up work rather than undocumented claims.

## Stable API continuation

Phase 4 added a preview API slice; it did not freeze that slice as the final
1.0 contract. The canonical API identity, sync/async rules, expression AST,
language parity, query lifecycle, structured streaming, and release gates are
defined in [`stable-public-api-plan.md`](stable-public-api-plan.md) and
[ADR-0002](../decisions/0002-public-api-shape-and-execution-semantics.md).
