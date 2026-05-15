# R1 Foundation Alpha Implementation Tracker

## Goal

Deliver the first Krishiv release: an embedded and single-node local hybrid engine with SQL over Parquet, basic DataFrame and Stream API skeletons, a local memory stream source, and `EXPLAIN`.

R1 proves that Krishiv can share one planning/runtime model across local batch and local streaming without introducing distributed Kubernetes complexity.

## Scope

In scope:

- Rust workspace and initial crate layout.
- Embedded runtime.
- Single-node runtime.
- SQL CLI.
- DataFrame and Stream API skeletons.
- DataFusion integration for local SQL over Parquet.
- Local memory stream source.
- Logical and physical plan display through `krishiv explain`.
- SQL compatibility baseline docs.
- SQL golden tests and embedded/single-node parity tests.

Out of scope:

- Kubernetes execution.
- Distributed scheduling.
- Durable shuffle service.
- RocksDB state backend.
- Checkpoints/savepoints.
- Exactly-once guarantees.
- Python bindings.
- Iceberg/Delta support.

## Milestone 1: Bootstrap Workspace And Stubs

Goal: create a compiling workspace with crate boundaries, API stubs, runtime stubs, CLI help, and explanatory docs before real engine logic lands.

- [x] Create root Cargo workspace.
- [x] Create initial R1 crates.
- [x] Add public `Session`, `SessionBuilder`, `DataFrame`, `Stream`, and `ExecutionMode` stubs.
- [x] Add bootstrap `QueryResult`, `RecordBatch`, and `StreamBatch` result shapes.
- [x] Add logical and physical plan wrapper stubs.
- [x] Add runtime traits and embedded/single-node backend stubs.
- [x] Add help-only CLI shell for `krishiv`, `krishiv sql`, `krishiv explain`, and `krishiv jobs`.
- [x] Add crate map and R1 bootstrap architecture docs.
- [x] Add R1 bootstrap file guide.
- [x] Add R1 SQL compatibility placeholder.
- [x] Add example/test directory placeholders.
- [x] Validate with `cargo check --workspace`.
- [x] Validate with `cargo test --workspace`.
- [x] Validate CLI help with `cargo run -p krishiv-cli -- --help`.
- [x] Validate explain help with `cargo run -p krishiv-cli -- explain --help`.

## Architecture Deliverables

- [x] Create root `Cargo.toml` workspace.
- [x] Create `crates/krishiv-api`.
- [x] Create `crates/krishiv-cli`.
- [x] Create `crates/krishiv-sql`.
- [x] Create `crates/krishiv-plan`.
- [x] Create `crates/krishiv-exec`.
- [x] Create `crates/krishiv-runtime`.
- [x] Add crate dependency rules in workspace metadata or docs.
- [x] Add examples directory for embedded and batch SQL examples.
- [x] Add tests directory for integration and golden tests.

## API Deliverables

- [x] Define `Session` skeleton in `krishiv-api`.
- [x] Define `DataFrame` skeleton in `krishiv-api`.
- [x] Define `Stream` skeleton in `krishiv-api`.
- [x] Define `ExecutionMode` with `Embedded`, `SingleNode`, and reserved `Distributed`.
- [x] Define basic query result type using bootstrap record batches.
- [x] Define minimal stream item/batch model using bootstrap record batches.
- [x] Expose a session builder with execution mode selection.
- [x] Add API docs for public R1 types.

## SQL And Planning Deliverables

- [ ] Integrate DataFusion session context in `krishiv-sql`.
- [ ] Support registering local Parquet paths as tables.
- [ ] Support simple `SELECT`, `WHERE`, projection, aggregate, and limit queries through DataFusion.
- [x] Add Krishiv logical plan wrapper in `krishiv-plan`.
- [x] Add physical plan wrapper in `krishiv-plan`.
- [ ] Implement `EXPLAIN` output for logical plan.
- [ ] Implement `EXPLAIN` output for physical plan.
- [x] Document R1 SQL subset in `docs/sql-compatibility/r1.md`.

## Runtime Deliverables

- [x] Define `ExecutionBackend` trait in `krishiv-runtime`.
- [x] Define embedded backend.
- [x] Define single-node backend.
- [ ] Route local SQL execution through the selected backend.
- [ ] Route local stream execution through the selected backend.
- [x] Add local job registry for `krishiv jobs`.
- [x] Add simple job states: pending, running, succeeded, failed.

## CLI Deliverables

- [ ] Implement `krishiv sql`.
- [ ] Implement `krishiv explain`.
- [ ] Implement `krishiv jobs`.
- [x] Add CLI help text and examples.
- [ ] Add basic error reporting for missing files, invalid SQL, and unsupported features.

## Streaming Deliverables

- [x] Add local memory stream source.
- [x] Support bounded memory stream for tests.
- [ ] Support simple unbounded memory stream abstraction for future streaming tests.
- [ ] Add basic map/filter-style stream API skeleton if practical.
- [x] Keep R1 streaming local-only.
- [ ] Document R1 streaming limitations.

## Test Checklist

- [x] Workspace builds.
- [x] Unit tests pass.
- [ ] SQL golden tests pass.
- [ ] Embedded SQL over Parquet test passes.
- [ ] Single-node SQL over Parquet test passes.
- [ ] Embedded and single-node produce the same result for supported SQL.
- [x] Memory stream source test passes.
- [ ] `krishiv explain` snapshot test passes.
- [x] CLI smoke tests pass.

## Acceptance Gate

R1 is complete when:

- [ ] A user can run a local SQL query over Parquet.
- [ ] A user can run a simple in-memory stream pipeline.
- [ ] `krishiv explain` shows logical and physical plans.
- [ ] `krishiv jobs` shows local job status.
- [ ] Embedded and single-node execution produce the same results for supported features.
- [ ] R1 SQL compatibility and known limitations are documented.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| R1 grows into a distributed runtime too early | Keep Kubernetes, scheduler, shuffle, and checkpoints out of R1 |
| DataFusion abstractions leak too deeply into public APIs | Wrap DataFusion internals behind Krishiv API and plan types |
| Streaming semantics become underspecified | Document local-only R1 stream semantics and defer checkpointed state |
| CLI behavior diverges from embedded API | Route both through the same `ExecutionBackend` abstraction |
| Tests become brittle before APIs stabilize | Use golden tests for behavior, not internal debug formatting unless intentional |
