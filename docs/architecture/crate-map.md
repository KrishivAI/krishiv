# Krishiv Crate Map

This map explains the R1 bootstrap crate ownership boundaries. The current crates are intentionally thin stubs; their purpose is to make future implementation work land in the right place.

## Workspace Crates

| Crate | Owns | Must Not Own |
|---|---|---|
| `krishiv-api` | Public Rust `Session`, `DataFrame`, `Stream`, `ExecutionMode`, and result-shape APIs | Kubernetes, RocksDB, connector-specific implementations, long-term DataFusion internals |
| `krishiv-cli` | `krishiv` command shell and command routing | Engine logic, SQL planning, runtime execution internals |
| `krishiv-sql` | SQL planning seam and future DataFusion integration | Public user session state, runtime scheduling |
| `krishiv-plan` | Krishiv logical/physical plan wrappers and DAG-level concepts | SQL parser details, physical operator execution |
| `krishiv-exec` | Physical operator descriptors and future Arrow execution operators | User-facing API, distributed scheduling |
| `krishiv-runtime` | Runtime traits, local backends, job/task status, execution backend boundary | SQL parsing, connector-specific guarantees, Kubernetes CRDs |

## Dependency Direction

Current R1 bootstrap dependency direction:

```text
krishiv-cli

krishiv-api
  -> krishiv-plan
  -> krishiv-runtime
  -> krishiv-sql

krishiv-sql
  -> krishiv-plan

krishiv-exec
  -> krishiv-plan

krishiv-runtime
  -> krishiv-plan

krishiv-plan
```

Future dependencies should preserve a simple rule: user-facing crates can depend on lower-level crates, but low-level crates should not depend on `krishiv-api` or `krishiv-cli`.

## R1 Bootstrap Notes

- `krishiv-api::RecordBatch` is a temporary bootstrap stand-in for Arrow record batches.
- `krishiv-sql` does not use DataFusion yet.
- `krishiv-exec::lower_to_physical` is not an optimizer.
- `krishiv-runtime::{EmbeddedBackend, SingleNodeBackend}` only accept placeholder physical plans.
- `krishiv-cli` exposes help for command shapes but does not execute queries yet.

## Next Expected Slice

The next implementation slice should add real local SQL planning/execution by introducing Arrow/DataFusion dependencies behind `krishiv-sql` and `krishiv-api`, while keeping the public API shape stable.
