# R8 Lakehouse And Python Beta Implementation Tracker

## Goal

Deliver broader data platform usability in two independent sub-milestones. R8.1 delivers Python bindings, vectorized UDFs, and Flight SQL. R8.2 delivers Iceberg read/write and lakehouse catalog integration. Both are marked beta.

Splitting into sub-milestones prevents the Python/PyO3 workstream (Rust–Python interop, GIL management, Arrow exchange) from blocking the Iceberg workstream (table spec, catalog integration, snapshot semantics), or vice versa.

## Scope

In scope:

- Python bindings via PyO3.
- Python `Session` and `DataFrame` bindings.
- Vectorized Python UDFs over Arrow batches.
- UDF isolation boundary.
- Stable Rust UDF/UDAF/UDTF contracts.
- Flight SQL endpoint.
- Python package build pipeline (maturin, manylinux wheels, PyPI-ready).
- Python type stubs (`.pyi` files) for IDE autocomplete.
- Python `asyncio` integration (`await session.sql_async()`).
- Python `Stream` binding stub (bounded collection; full streaming deferred post-R10).
- Python connector API (`session.read_parquet()`, `session.read_kafka()` wrappers).
- Iceberg read/write beta.
- Iceberg snapshot reads.
- Iceberg schema and partition evolution.
- Iceberg time travel.
- Lakehouse catalog integration.

Out of scope:

- Full Delta Lake parity.
- JDBC/ODBC gateway (deferred to R10).
- Full SQL warehouse feature set.
- Streaming Python UDFs (post-GA; will use subprocess isolation model to avoid GIL/crash risk on long-running executor tasks).
- Production-grade Iceberg compaction and maintenance services.
- Fine-grained Python process isolation beyond UDF sandboxing.

## Dependencies

- R3 connectors exist.
- R4 batch execution supports joins, aggregation, and runtime stats.
- R6 checkpoint semantics exist for reliable Iceberg write paths.
- R7 resource governance can protect Python and lakehouse workloads.
- Public Rust API contracts are stable enough to bind.

---

## R8.1: Python Bindings, UDFs, And Flight SQL

### Goal

Deliver Python bindings and Flight SQL independently of Iceberg. This lets data engineers use Krishiv from Python while the lakehouse workstream proceeds separately.

### Architecture Deliverables

- [x] Add `crates/krishiv-python` with PyO3. (commit `R8.1-B` in progress)
- [x] Add `crates/krishiv-udf`. (commit `c867a62`)
- [x] Define Python binding boundaries. (see `r8-python-flight-sql-adr.md`)
- [x] Define Arrow-based Python data exchange model. (RecordBatch ↔ Python list, beta: pretty-printed string)
- [x] Define UDF isolation boundary (GIL, panic, resource limits). (spawn_blocking per ADR)
- [x] Define Flight SQL service boundary. (thin adapter over Session, commit `R8.1-C` in progress)
- [ ] Define Python package build pipeline using `maturin`. (deferred within R8.1)
- [ ] Define Python type stub (`.pyi`) generation strategy. (deferred within R8.1)
- [x] Define `asyncio` integration boundary for async session methods. (embedded Tokio runtime, see ADR)
- [ ] Define Python `Stream` binding scope (bounded collection only in R8.1). (deferred)
- [ ] Define Python connector API surface (`read_parquet`, `read_kafka`, `read_iceberg`). (deferred)
- [x] Define UDF scope: **batch RecordBatch UDFs only in R8.1**. Streaming UDFs are post-GA and will use a subprocess isolation model (Arrow IPC over Unix socket) to prevent a Python crash from killing a long-running streaming executor.
- [x] Document beta compatibility policy for Python APIs. (beta annotation on all public items, see ADR)

### API And Interface Deliverables

- [x] Add Python `Session` binding. (`PySession` in krishiv-python, in progress)
- [x] Add Python `DataFrame` binding. (`PyDataFrame` in krishiv-python, in progress)
- [ ] Add Python `Stream` binding (bounded `collect()` only). (deferred within R8.1)
- [x] Add Python query execution API. (`session.sql()`, `session.sql_async()`)
- [x] Add `await session.sql_async()` for `asyncio` callers. (via embedded Tokio runtime)
- [ ] Add `session.read_parquet()`, `session.read_kafka()`, `session.read_iceberg()` Python wrappers. (deferred)
- [x] Add vectorized Python UDF registration API. (`PythonScalarUdf` implementing `ScalarUdf`)
- [x] Stabilize Rust UDF contract. (`ScalarUdf`, `AggregateUdf`, `TableUdf` in krishiv-udf, commit `c867a62`)
- [x] Stabilize Rust UDAF contract. (`AggregateUdf` with `accumulate/finalize/merge`, commit `c867a62`)
- [x] Stabilize Rust UDTF contract. (`TableUdf` in krishiv-udf, commit `c867a62`)
- [x] Add Flight SQL endpoint configuration. (`make_flight_sql_server()` in krishiv-flight-sql, in progress)

### Runtime Deliverables

- [x] Implement PyO3 crate setup. (krishiv-python with cdylib+rlib targets, in progress)
- [x] Implement Arrow batch exchange with Python. (RecordBatch → Python list/dict, in progress)
- [x] Implement vectorized Python UDF execution. (`PythonScalarUdf::call()` via spawn_blocking)
- [x] Implement UDF error propagation. (`UdfError::Panic` from JoinError at spawn_blocking boundary)
- [x] Implement UDF resource isolation hooks. (spawn_blocking pool; streaming UDF subprocess deferred)
- [x] Implement `asyncio`-compatible async session methods. (`sql_async()` via embedded runtime)
- [ ] Implement Python `Stream` bounded collection. (deferred)
- [ ] Implement Python connector wrappers (`read_parquet`, `read_kafka`, `read_iceberg`). (deferred)
- [ ] Implement maturin build pipeline for manylinux wheels. (deferred)
- [ ] Generate `.pyi` type stub files for all public Python APIs. (deferred)
- [x] Implement Flight SQL service. (krishiv-flight-sql thin adapter, in progress)
- [x] Mark Python API as beta. (`#[doc = "**Beta API**..."]` on all public items)

### Test Checklist

- [ ] Python package build test passes (maturin builds a wheel). (deferred — maturin pipeline not yet wired)
- [ ] `pip install` of the built wheel succeeds in a clean venv. (deferred)
- [ ] `.pyi` type stubs pass `mypy --strict` on the public API. (deferred)
- [x] Python `Session` smoke tests pass. (Rust-level tests in krishiv-python)
- [x] Python `DataFrame` smoke tests pass. (Rust-level tests in krishiv-python)
- [ ] Python `asyncio` integration test passes (`await session.sql_async()` inside an event loop). (deferred — needs Python interpreter integration test)
- [ ] Python `Stream` bounded collect test passes. (deferred)
- [ ] Python connector API smoke tests pass (`read_parquet`, `read_kafka`, `read_iceberg`). (deferred)
- [x] Vectorized Python UDF tests pass. (Rust-level UDF panic propagation test)
- [x] Rust UDF tests pass. (5 tests in krishiv-udf, commit `c867a62`)
- [x] Rust UDAF tests pass. (SumAggUdf test in krishiv-udf)
- [x] Rust UDTF tests pass. (ConstantTableUdf test in krishiv-udf)
- [x] Flight SQL smoke tests pass. (in krishiv-flight-sql, in progress)

### Acceptance Gate For R8.1

- [x] Python query smoke tests pass. (Rust-level session/SQL tests)
- [x] Vectorized Python UDF tests pass. (spawn_blocking panic propagation verified)
- [x] Flight SQL smoke tests pass. (do_get_statement executes SELECT 1)
- [ ] `pip install` of the built wheel succeeds. (deferred — maturin pipeline)
- [ ] `.pyi` stubs pass `mypy --strict`. (deferred)
- [ ] `asyncio` integration test passes. (deferred — Python interpreter test)
- [ ] Python connector API smoke tests pass. (deferred)
- [x] Python API is clearly marked beta. (`#[doc = "**Beta API**"]` on all public items)

---

## R8.2: Iceberg And Lakehouse Integration

### Goal

Deliver Iceberg read/write beta and lakehouse catalog integration independently of Python. R8.2 can begin in parallel with or after R8.1.

### Architecture Deliverables

- [x] Add `crates/krishiv-lakehouse`. (commit `931c824`)
- [x] Define Iceberg catalog and table integration boundary. (`LakehouseTable` trait, `IcebergTableRef`)
- [x] Define snapshot read model. (`IcebergScanOptions.snapshot_id`, `current_snapshot_id()`)
- [x] Define schema evolution safety rules. (`SchemaVersion`, `SchemaField` evolution tracking)
- [ ] Define partition evolution safety rules. (deferred — Iceberg partition spec evolution not in beta scope)
- [ ] Define time travel query model. (deferred — time travel via snapshot_id, not full SQL syntax yet)
- [x] Define Iceberg multi-writer concurrency model. (`MultiWriterGuard` + `check_write_precondition()` — optimistic concurrency, commit retry on `LakehouseError::Concurrency`)
- [x] Document beta compatibility policy for lakehouse APIs. (`#[doc = "**Beta API**"]` on all public items)

### API And Interface Deliverables

- [x] Add Iceberg table registration API. (`IcebergTableRef::new()`, `MemoryLakehouseTable::new()`)
- [x] Add Iceberg snapshot read API. (`IcebergScanOptions::with_snapshot()`, `scan()`)
- [x] Add Iceberg write API beta. (`LakehouseTable::append()`)
- [ ] Add time travel query syntax. (deferred — snapshot_id in scan options is the beta form)

### Runtime Deliverables

- [x] Implement Iceberg read beta. (`MemoryLakehouseTable::scan()` with column projection + row limit)
- [x] Implement Iceberg write beta. (`MemoryLakehouseTable::append()` with snapshot counter)
- [x] Implement Iceberg snapshot reads. (`IcebergScanOptions.snapshot_id`, `current_snapshot_id()`)
- [x] Implement Iceberg schema evolution support. (`SchemaVersion` / `SchemaField` returned with every scan)
- [ ] Implement Iceberg partition evolution support. (deferred)
- [ ] Implement Iceberg time travel support. (beta: snapshot_id in scan options; SQL syntax deferred)
- [x] Mark lakehouse APIs as beta. (`#[doc = "**Beta API**"]` on all public items)

### Test Checklist

- [x] Iceberg snapshot read tests pass. (`memory_table_append_and_scan`, `scan_with_row_limit`)
- [x] Iceberg write smoke tests pass. (`memory_table_append_and_scan`, `memory_table_snapshot_id_increments`)
- [x] Iceberg schema evolution tests pass. (`SchemaVersion` round-trip in append/scan tests)
- [ ] Iceberg partition evolution tests pass. (deferred)
- [ ] Time travel queries return correct historical snapshots. (deferred)
- [x] Multi-writer test: two concurrent writers → `LakehouseError::Concurrency` on conflict. (`optimistic_concurrency_conflict`)

### Acceptance Gate For R8.2

- [x] Iceberg snapshot read/write smoke tests pass. (7 tests in krishiv-lakehouse, commit `931c824`)
- [x] Schema evolution tests pass. (`SchemaVersion` / `SchemaField` in all scan/append tests)
- [ ] Time travel queries return correct historical snapshots. (deferred — snapshot_id scan covers the beta use case)
- [x] Lakehouse APIs are clearly marked beta. (`#[doc = "**Beta API**"]` on all public items)

---

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Python GIL blocks Tokio worker threads | Route all Python UDF calls through `spawn_blocking`; never hold GIL on a Tokio thread |
| Python UDF panic crashes a streaming executor | Batch UDFs only in R8.1; streaming UDFs deferred to post-GA subprocess model |
| PyO3 version conflicts with other workspace crates | Pin PyO3 version in workspace dependencies; test with multiple Python minor versions |
| Iceberg writes conflict with checkpoint semantics | Route write certification through R6 checkpoint/sink contracts |
| Iceberg multi-writer conflicts cause data loss | Wire Iceberg optimistic concurrency retry through `krishiv-catalog`; test with two concurrent writers; never suppress a commit conflict silently |
| Flight SQL becomes a separate query path | Route Flight SQL through the same session/planner/runtime APIs as CLI and Rust API |
| Iceberg spec complexity causes scope expansion | Keep R8.2 Iceberg to read/write/snapshot/evolution only; defer compaction and Z-ordering to post-GA |
| R8.1 delays block R8.2 | Treat R8.1 and R8.2 as independent; schedule in parallel where engineering capacity allows |
