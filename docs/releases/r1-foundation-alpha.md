# R1 Foundation Alpha Release Notes

## Summary

R1 Foundation Alpha delivers Krishiv's first usable local engine surface:
embedded mode, single-node mode, local SQL over Parquet, Arrow-backed query
results, bounded memory streams, `EXPLAIN`, local job status, and CLI smoke
commands.

This release is an alpha foundation, not a production distributed runtime.
Kubernetes, shuffle, checkpoints, savepoints, exactly-once sinks, RocksDB state,
Python bindings, and lakehouse table support remain later-roadmap work.

## Included Features

- Rust workspace with R1 crate boundaries.
- Public `Session`, `DataFrame`, `Stream`, `ExecutionMode`, `QueryResult`, and
  `StreamBatch` APIs.
- DataFusion-backed local SQL planning and execution in `krishiv-sql`.
- Arrow `RecordBatch` query results exposed through `krishiv-api`.
- Local Parquet table registration and direct Parquet reads.
- Embedded and single-node execution modes with parity tests.
- Bounded local memory stream source with map/filter/collect support.
- Unbounded memory stream API shape with explicit unsupported collection in R1.
- CLI commands: `krishiv sql`, `krishiv explain`, and `krishiv jobs`.
- Golden tests for stable CLI SQL and explain output.

## Example Commands

```bash
cargo run -p krishiv -- sql --query "select 1 as value"
cargo run -p krishiv -- explain --query "select 1 as value"
cargo run -p krishiv -- jobs
cargo run -p krishiv-api --example local_sql_parquet
cargo run -p krishiv-api --example memory_stream
```

## Compatibility Contract

The R1 SQL compatibility contract is documented in
`docs/sql-compatibility/r1.md`. DataFusion may accept more SQL than Krishiv
documents, but only the documented subset is considered part of the R1
contract.

## Known Limitations

- Distributed mode is reserved for R2.
- `krishiv jobs` reports process-local job state only.
- Streaming is local-only and bounded, except for an unbounded API placeholder.
- There is no durable state, checkpointing, savepointing, or exactly-once
  certification in R1.
- Join behavior is not certified as part of the R1 contract.

## Validation

- `cargo fmt --all --check`
- `cargo check --workspace`
- `cargo test --workspace`
- `cargo run -p krishiv -- sql --query "select 1 as value"`
- `cargo run -p krishiv -- explain --query "select 1 as value"`
- `cargo run -p krishiv -- jobs`
