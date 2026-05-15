# R1 Bootstrap Architecture

## Purpose

R1 bootstrap created the rails for Krishiv before the first real execution
logic landed. The follow-on R1 local execution slice now keeps those crate
boundaries and adds DataFusion-backed SQL, Arrow-backed results, local memory
streams, and CLI execution.

## Delivered Shape

```text
CLI shell
  -> sql
  -> explain
  -> jobs

Public API
  -> Session
  -> DataFrame
  -> Stream
  -> ExecutionMode

SQL seam
  -> DataFusion SessionContext
  -> Parquet table registration
  -> SQL collect/explain

Plan seam
  -> LogicalPlan
  -> PhysicalPlan
  -> PlanNode

Exec seam
  -> logical-to-physical placeholder lowering

Runtime seam
  -> ExecutionBackend
  -> EmbeddedBackend
  -> SingleNodeBackend
  -> LocalJobRegistry
```

## What Is Real

- The Cargo workspace is real.
- Crate boundaries are real.
- Public API type names are real enough to guide R1 work.
- CLI commands execute through `krishiv-api`.
- SQL execution over local Parquet is real through DataFusion.
- Query results use Arrow record batches.
- Bounded memory stream map/filter/collect behavior is real for local tests.
- Unit and golden tests validate the R1 local behavior.

## What Is Stubbed

- Physical operator execution.
- Persistent job history.
- Streaming operators beyond local bounded memory batches.
- Distributed runtime behavior.

Stubbed methods must return explicit unsupported errors or clearly documented placeholder output.

## R1 Streaming Limitations

- Streams are local-only.
- Bounded in-memory streams can be collected in tests and embedded examples.
- Unbounded memory streams exist as an API shape but cannot be collected in R1.
- There are no watermarks, timers, keyed state, checkpoints, or streaming SQL in R1.
- Durable stateful streaming starts in later releases.

## Bootstrap Acceptance Gate

The bootstrap slice is complete when:

- `cargo check --workspace` passes.
- `cargo test --workspace` passes.
- `cargo run -p krishiv-cli -- --help` prints top-level help.
- `cargo run -p krishiv-cli -- explain --help` prints explain help.
- Crate ownership is documented in `docs/architecture/crate-map.md`.
- R1 tracker and status ledger reflect the completed bootstrap work.

## R1 Local Execution Acceptance Gate

The local execution slice is complete when:

1. Arrow/DataFusion dependencies are introduced behind `krishiv-sql`.
2. Local Parquet paths can be registered as SQL tables.
3. Minimal SQL queries execute through DataFusion.
4. Results return Arrow-backed batches through the public API.
5. Embedded and single-node parity tests pass.
6. `krishiv sql`, `krishiv explain`, and `krishiv jobs` smoke tests pass.
