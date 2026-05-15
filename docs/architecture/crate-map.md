# Krishiv Crate Map

This map explains the R1 crate ownership boundaries. The bootstrap slice
created the rails, and the R1 local execution slice now runs SQL through
DataFusion while keeping public Krishiv APIs stable.

## Workspace Crates

| Crate | Owns | Must Not Own |
|---|---|---|
| `krishiv-api` | Public Rust `Session`, `DataFrame`, `Stream`, `ExecutionMode`, Arrow-backed query results, and local stream APIs | Kubernetes, RocksDB, connector-specific implementations, long-term DataFusion internals |
| `krishiv-cli` | `krishiv sql`, `krishiv explain`, `krishiv jobs`, command parsing, and user-facing output | Engine logic, SQL planning, runtime execution internals |
| `krishiv-sql` | DataFusion session integration, local SQL execution, Parquet registration, and SQL explain formatting | Public user session state, runtime scheduling |
| `krishiv-plan` | Krishiv logical/physical plan wrappers and DAG-level concepts | SQL parser details, physical operator execution |
| `krishiv-exec` | Physical operator descriptors and future Arrow execution operators | User-facing API, distributed scheduling |
| `krishiv-runtime` | Runtime traits, local backends, job/task status, execution backend boundary | SQL parsing, connector-specific guarantees, Kubernetes CRDs |

## Dependency Direction

Current R1 dependency direction:

```text
krishiv-cli

krishiv-api
  -> krishiv-plan
  -> krishiv-runtime
  -> krishiv-sql

krishiv-sql
  -> arrow
  -> datafusion
  -> krishiv-plan

krishiv-exec
  -> krishiv-plan

krishiv-runtime
  -> krishiv-plan

krishiv-plan
```

Future dependencies should preserve a simple rule: user-facing crates can depend on lower-level crates, but low-level crates should not depend on `krishiv-api` or `krishiv-cli`.

## R1 Bootstrap Notes

- `krishiv-api::RecordBatch` re-exports Arrow record batches.
- `krishiv-sql` owns the DataFusion integration and keeps DataFusion types out of the public API.
- `krishiv-exec::lower_to_physical` is not an optimizer.
- `krishiv-runtime::{EmbeddedBackend, SingleNodeBackend}` accept R1 physical-plan wrappers before local execution.
- `krishiv-cli` executes local SQL and explain commands through `krishiv-api`.

## Next Expected Slice

The next implementation slice should harden R1 local execution with broader
SQL golden coverage and example applications, then prepare R2 coordinator and
executor skeletons without changing R1 semantics.
