---
name: krishiv-engine
description: Build, test, document, or review the Krishiv Rust workspace.
---

# Krishiv Engine

Rust-native hybrid compute framework — batch SQL, streaming pipelines, lakehouse.

## Architecture Invariants

- One runtime model across embedded, single-node, distributed.
- One active coordinator per job; executors are replaceable workers.
- Arrow `RecordBatch` + DataFusion for columnar data and SQL.
- Typed IDs, typed errors, capability flags over string routing.
- No master/slave terminology. No active-active job scheduling.

## Crate Map

| Crate | Owns |
|-------|------|
| `krishiv` | Facade, CLI binary |
| `krishiv-common` | Shared utils, error types |
| `krishiv-api` | Session, DataFrame, public API |
| `krishiv-sql` | DataFusion integration, SQL execution |
| `krishiv-plan` | Logical/physical plans, expressions, UDFs |
| `krishiv-runtime` | Runtime routing (embedded/single/distributed) |
| `krishiv-dataflow` | Arrow operators, windows, joins, stateful ops |
| `krishiv-scheduler` | Coordinator, job/task lifecycle, metadata |
| `krishiv-executor` | Task runner, shuffle/checkpoint hooks |
| `krishiv-proto` | Typed IDs, gRPC wire contracts |
| `krishiv-shuffle` | Shuffle service (disk/object-store/Flight) |
| `krishiv-state` | RocksDB state, checkpoints, TTL |
| `krishiv-connectors` | Sources/sinks (Parquet/Kafka/S3/Iceberg/Delta) |
| `krishiv-operator` | Kubernetes CRD/operator |
| `krishiv-flight-sql` | Arrow Flight SQL server |
| `krishiv-python` | PyO3 bindings |

## Workflow

1. Read `docs/implementation/status.md` for current handoff.
2. Inspect the relevant crate before editing.
3. Keep changes scoped to the owning crate.
4. Add focused tests with behavior changes.
5. Run validation before responding.

## CI Quality Gates

```bash
# Fix first
cargo fmt
cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos --fix --allow-dirty -- -D warnings

# Then check
cargo fmt --check
cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings
```

### Common Fixes

- **Unused imports**: `cargo clippy --fix --allow-dirty`
- **Dead code (test-only)**: `#[cfg(test)]` not `#[allow(dead_code)]`
- **Complex type**: extract `type Alias = …;`
- **Duplicate definitions**: keep in one module, `pub(crate)` + `#[cfg(test)]` if needed
- **Unused `DEFAULT_*`**: wire as `.unwrap_or(DEFAULT_CONSTANT)`

### Connector / Python

- Sink types take `&RecordBatch` (borrow), no `flush()` method.
- Kafka sinks use `BaseRecord`, `ThreadedProducer::send(record)` — one arg, no timeout.
- `krishiv-python` excluded from workspace clippy — lint separately with `maturin develop`.

## Validation

```bash
cargo check -p <crate>          # narrow
cargo test -p <crate>           # narrow
cargo test --workspace          # wide
```

## Build Notes

GCC 15: prepend `CXXFLAGS="-include cstdint"` to any command linking rocksdb.

## Session Handoff

For substantial sessions, update `docs/implementation/status.md` with:
completed work, validation, blockers, and the next useful command.
