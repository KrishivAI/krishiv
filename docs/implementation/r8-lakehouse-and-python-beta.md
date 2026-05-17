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

- [ ] Add `crates/krishiv-python` with PyO3.
- [ ] Add `crates/krishiv-udf`.
- [ ] Define Python binding boundaries.
- [ ] Define Arrow-based Python data exchange model.
- [ ] Define UDF isolation boundary (GIL, panic, resource limits).
- [ ] Define Flight SQL service boundary.
- [ ] Define Python package build pipeline using `maturin`.
- [ ] Define Python type stub (`.pyi`) generation strategy.
- [ ] Define `asyncio` integration boundary for async session methods.
- [ ] Define Python `Stream` binding scope (bounded collection only in R8.1).
- [ ] Define Python connector API surface (`read_parquet`, `read_kafka`, `read_iceberg`).
- [ ] Define UDF scope: **batch RecordBatch UDFs only in R8.1**. Streaming UDFs are post-GA and will use a subprocess isolation model (Arrow IPC over Unix socket) to prevent a Python crash from killing a long-running streaming executor.
- [ ] Document beta compatibility policy for Python APIs.

### API And Interface Deliverables

- [ ] Add Python `Session` binding.
- [ ] Add Python `DataFrame` binding.
- [ ] Add Python `Stream` binding (bounded `collect()` only).
- [ ] Add Python query execution API.
- [ ] Add `await session.sql_async()` for `asyncio` callers.
- [ ] Add `session.read_parquet()`, `session.read_kafka()`, `session.read_iceberg()` Python wrappers.
- [ ] Add vectorized Python UDF registration API.
- [ ] Stabilize Rust UDF contract.
- [ ] Stabilize Rust UDAF contract.
- [ ] Stabilize Rust UDTF contract.
- [ ] Add Flight SQL endpoint configuration.

### Runtime Deliverables

- [ ] Implement PyO3 crate setup.
- [ ] Implement Arrow batch exchange with Python.
- [ ] Implement vectorized Python UDF execution.
- [ ] Implement UDF error propagation.
- [ ] Implement UDF resource isolation hooks.
- [ ] Implement `asyncio`-compatible async session methods.
- [ ] Implement Python `Stream` bounded collection.
- [ ] Implement Python connector wrappers (`read_parquet`, `read_kafka`, `read_iceberg`).
- [ ] Implement maturin build pipeline for manylinux wheels.
- [ ] Generate `.pyi` type stub files for all public Python APIs.
- [ ] Implement Flight SQL service.
- [ ] Mark Python API as beta.

### Test Checklist

- [ ] Python package build test passes (maturin builds a wheel).
- [ ] `pip install` of the built wheel succeeds in a clean venv.
- [ ] `.pyi` type stubs pass `mypy --strict` on the public API.
- [ ] Python `Session` smoke tests pass.
- [ ] Python `DataFrame` smoke tests pass.
- [ ] Python `asyncio` integration test passes (`await session.sql_async()` inside an event loop).
- [ ] Python `Stream` bounded collect test passes.
- [ ] Python connector API smoke tests pass (`read_parquet`, `read_kafka`, `read_iceberg`).
- [ ] Vectorized Python UDF tests pass.
- [ ] Rust UDF tests pass.
- [ ] Rust UDAF tests pass.
- [ ] Rust UDTF tests pass.
- [ ] Flight SQL smoke tests pass.

### Acceptance Gate For R8.1

- [ ] Python query smoke tests pass.
- [ ] Vectorized Python UDF tests pass.
- [ ] Flight SQL smoke tests pass.
- [ ] `pip install` of the built wheel succeeds.
- [ ] `.pyi` stubs pass `mypy --strict`.
- [ ] `asyncio` integration test passes.
- [ ] Python connector API smoke tests pass.
- [ ] Python API is clearly marked beta.

---

## R8.2: Iceberg And Lakehouse Integration

### Goal

Deliver Iceberg read/write beta and lakehouse catalog integration independently of Python. R8.2 can begin in parallel with or after R8.1.

### Architecture Deliverables

- [ ] Add `crates/krishiv-lakehouse`.
- [ ] Define Iceberg catalog and table integration boundary.
- [ ] Define snapshot read model.
- [ ] Define schema evolution safety rules.
- [ ] Define partition evolution safety rules.
- [ ] Define time travel query model.
- [ ] Define Iceberg multi-writer concurrency model: concurrent Krishiv jobs writing to the same table use Iceberg optimistic concurrency control; the losing writer retries the snapshot commit with the updated table state. Document this behavior clearly — data files are not re-written, only the commit is retried.
- [ ] Document beta compatibility policy for lakehouse APIs.

### API And Interface Deliverables

- [ ] Add Iceberg table registration API.
- [ ] Add Iceberg snapshot read API.
- [ ] Add Iceberg write API beta.
- [ ] Add time travel query syntax.

### Runtime Deliverables

- [ ] Implement Iceberg read beta.
- [ ] Implement Iceberg write beta.
- [ ] Implement Iceberg snapshot reads.
- [ ] Implement Iceberg schema evolution support.
- [ ] Implement Iceberg partition evolution support.
- [ ] Implement Iceberg time travel support.
- [ ] Mark lakehouse APIs as beta.

### Test Checklist

- [ ] Iceberg snapshot read tests pass.
- [ ] Iceberg write smoke tests pass.
- [ ] Iceberg schema evolution tests pass.
- [ ] Iceberg partition evolution tests pass.
- [ ] Time travel queries return correct historical snapshots.
- [ ] Multi-writer test: two concurrent Krishiv jobs writing to the same Iceberg table produce a consistent final snapshot without data loss or duplication.

### Acceptance Gate For R8.2

- [ ] Iceberg snapshot read/write smoke tests pass.
- [ ] Schema evolution tests pass.
- [ ] Time travel queries return correct historical snapshots.
- [ ] Lakehouse APIs are clearly marked beta.

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
