# R1 Bootstrap Architecture

## Purpose

R1 bootstrap creates the rails for Krishiv before the first real execution logic lands. It should compile, expose stable skeletons, and make crate ownership clear without pretending query execution is implemented.

## Delivered Shape

```text
CLI shell
  -> command help only

Public API
  -> Session
  -> DataFrame
  -> Stream
  -> ExecutionMode

SQL seam
  -> placeholder SQL plan

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
- CLI command names are real.
- Unit tests validate the bootstrap behavior.

## What Is Stubbed

- SQL execution.
- DataFusion integration.
- Arrow-backed record batches.
- Physical operator execution.
- Job execution.
- Streaming operators.
- Distributed runtime behavior.

Stubbed methods must return explicit unsupported errors or clearly documented placeholder output.

## Bootstrap Acceptance Gate

The bootstrap slice is complete when:

- `cargo check --workspace` passes.
- `cargo test --workspace` passes.
- `cargo run -p krishiv-cli -- --help` prints top-level help.
- `cargo run -p krishiv-cli -- explain --help` prints explain help.
- Crate ownership is documented in `docs/architecture/crate-map.md`.
- R1 tracker and status ledger reflect the completed bootstrap work.

## Follow-On Slice

After bootstrap, implement the first real R1 capability:

1. Introduce Arrow/DataFusion dependencies.
2. Register local Parquet paths.
3. Execute a minimal SQL query through DataFusion.
4. Return real Arrow-backed batches or a Krishiv wrapper around Arrow batches.
5. Preserve embedded/single-node parity tests.
