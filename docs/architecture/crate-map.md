# Krishiv Crate Map

This map explains the current crate ownership boundaries. R1 established the
local API, SQL, planning, execution, and runtime rails. R2 is adding the first
distributed control-plane, scheduler, Kubernetes, and status UI surfaces while
keeping public Krishiv APIs stable.

## Workspace Crates

| Crate | Owns | Must Not Own |
|---|---|---|
| `krishiv-api` | Public Rust `Session`, `DataFrame`, `Stream`, `ExecutionMode`, Arrow-backed query results, and local stream APIs | Kubernetes, RocksDB, connector-specific implementations, long-term DataFusion internals |
| `krishiv-cli` | `krishiv sql`, `krishiv explain`, `krishiv jobs`, command parsing, and user-facing output | Engine logic, SQL planning, runtime execution internals |
| `krishiv-sql` | DataFusion session integration, local SQL execution, Parquet registration, and SQL explain formatting | Public user session state, runtime scheduling |
| `krishiv-plan` | Krishiv logical/physical plan wrappers and DAG-level concepts | SQL parser details, physical operator execution |
| `krishiv-exec` | Physical operator descriptors and future Arrow execution operators | User-facing API, distributed scheduling |
| `krishiv-runtime` | Runtime traits, local backends, job/task status, execution backend boundary | SQL parsing, connector-specific guarantees, Kubernetes CRDs |
| `krishiv-proto` | R2 control-plane contracts: typed ids, lifecycle states, job/stage/task specs, executor heartbeats, and task updates | Runtime scheduling decisions, Kubernetes clients, transport servers |
| `krishiv-operator` | R2 `KrishivJob` resource models, resource validation, scheduler job conversion, and status reconciliation | Live Kubernetes watch loops, scheduling policy, durable metadata, SQL execution |
| `krishiv-scheduler` | R2 active coordinator skeleton, executor registry, static placement, Krishiv DAG-to-job conversion, task lifecycle updates, and job snapshots | SQL parsing, DataFusion execution, Kubernetes CRDs, durable metadata |
| `krishiv-ui` | R2 status HTTP API, health/readiness endpoints, and server-rendered Web UI over scheduler snapshots | Scheduling decisions, Kubernetes controllers, durable metadata, SQL execution |

## Dependency Direction

Current dependency direction:

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

krishiv-operator
  -> krishiv-proto
  -> krishiv-scheduler
  -> serde

krishiv-scheduler
  -> krishiv-plan
  -> krishiv-proto

krishiv-ui
  -> krishiv-proto
  -> krishiv-scheduler
  -> axum
  -> askama

krishiv-proto

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

The next implementation slice should attach the `krishiv-operator`
reconciliation behavior to a live Kubernetes watch/controller entrypoint while
keeping scheduling static and one coordinator active.
