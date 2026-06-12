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
      Arrow Flight SQL remains the native protocol.
- [ ] Add prepared statements and parameter binding across Flight SQL.
- [ ] Add cache/persist/unpersist and temporary-view APIs.
- [ ] Add remote analyze metrics once coordinator transport is versioned.

## Exit criteria

The local Rust and Python API slice is implemented when focused SQL/API/Python
checks pass. Distributed writes, remote progress/cancellation, JDBC/ODBC, and
prepared statements remain explicit follow-up work rather than undocumented
claims.
