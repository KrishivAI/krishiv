# Krishiv Crate Map

This map explains the current crate ownership boundaries. R1 established the
local API, SQL, planning, execution, and runtime rails. R2 added the first
distributed control-plane, scheduler, Kubernetes, and status UI surfaces. R3.1
is adding the executor process and versioned coordinator/executor contracts
while keeping public Krishiv APIs stable.

## Workspace Crates

| Crate | Owns | Must Not Own |
|---|---|---|
| `krishiv` | **User-facing façade**: re-exports all public API types, owns the `krishiv` binary and CLI dispatch (`src/cli.rs`), and is the single crate users add to `Cargo.toml` | Engine internals — all heavy logic stays in the crates below |
| `krishiv-api` | Core public Rust types: `Session`, `DataFrame`, `Stream`, `ExecutionMode`, Arrow-backed query results, and local stream APIs. Used directly by `krishiv-python`, `krishiv-flight-sql`, and `krishiv-bench`. | Kubernetes, RocksDB, connector-specific implementations, long-term DataFusion internals |
| `krishiv-sql` | DataFusion session integration, local SQL execution, Parquet registration, and SQL explain formatting | Public user session state, runtime scheduling |
| `krishiv-plan` | Krishiv logical/physical plan wrappers and DAG-level concepts | SQL parser details, physical operator execution |
| `krishiv-exec` | Physical operator descriptors and future Arrow execution operators | User-facing API, distributed scheduling |
| `krishiv-runtime` | Runtime traits, local backends, job/task status, execution backend boundary | SQL parsing, connector-specific guarantees, Kubernetes CRDs |
| `krishiv-proto` | R2/R3.1 control-plane contracts: typed ids, lifecycle states, job/stage/task specs, executor heartbeats, task assignments, task updates, transport versions, task attempts, executor lease generations, tonic-shaped service traits, generated protobuf service types, and domain/wire conversions (`ids`, `domain`, `wire` modules) | Runtime scheduling decisions, Kubernetes clients, concrete transport servers |
| `krishiv-flight` | Shared Arrow Flight SQL comment protocol (catalog register, continuous jobs, bounded windows) | Session state, coordinator scheduling |
| `krishiv-barrier` | Checkpoint barrier ack tracking and gRPC inject client | Coordinator job logic, SQL execution |
| `krishiv-executor` | R3.1 executor binary skeleton, executor startup config, construction of versioned registration/heartbeat requests, tonic-shaped coordinator service calls, networked gRPC client helpers, executor-side task assignment receiver, and minimal task runner skeleton | Scheduling policy, Kubernetes controllers, durable metadata, SQL planning |
| `krishiv-operator` | R2/R3.1 `KrishivJob` resource models, resource validation, scheduler job conversion, shared coordinator runtime, status reconciliation, live Kubernetes watch loop, status server wiring, coordinator/executor gRPC server wiring, and status subresource patching | Scheduling policy, durable metadata, SQL execution, HA leadership |
| `krishiv-scheduler` | R2/R3.1 active coordinator skeleton, shared coordinator handle, executor registry, static placement, Krishiv DAG-to-job conversion, tonic-shaped coordinator service adapter, networked coordinator/executor gRPC server, task assignment emission, task lifecycle updates, and job snapshots | SQL parsing, DataFusion execution, Kubernetes CRDs, durable metadata |
| `krishiv-ui` | R2 status HTTP API, health/readiness endpoints, and server-rendered Web UI over shared scheduler snapshots | Scheduling decisions, Kubernetes controllers, durable metadata, SQL execution |

## Dependency Direction

Current dependency direction:

```text
krishiv  (facade + binary)
  -> krishiv-api
  -> krishiv-checkpoint
  -> krishiv-connectors
  -> krishiv-lakehouse
  -> krishiv-metrics
  -> krishiv-proto
  -> krishiv-scheduler
  -> krishiv-udf

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

krishiv-executor
  -> krishiv-proto
  -> tonic

krishiv-runtime
  -> krishiv-plan

krishiv-operator
  -> krishiv-proto
  -> krishiv-scheduler
  -> krishiv-ui
  -> serde
  -> kube

krishiv-scheduler
  -> krishiv-plan
  -> krishiv-proto
  -> tonic

krishiv-ui
  -> krishiv-proto
  -> krishiv-scheduler
  -> axum
  -> askama

krishiv-proto

krishiv-plan
```

Future dependencies should preserve a simple rule: user-facing crates can depend on lower-level crates, but low-level crates should not depend on `krishiv-api` or `krishiv`.

## R1 Bootstrap Notes

- `krishiv-api::RecordBatch` re-exports Arrow record batches.
- `krishiv-sql` owns the DataFusion integration and keeps DataFusion types out of the public API.
- `krishiv-exec::lower_to_physical` is not an optimizer.
- `krishiv-runtime::{EmbeddedBackend, SingleNodeBackend}` accept R1 physical-plan wrappers before local execution.
- CLI dispatch lives in `krishiv/src/cli.rs` and is called by the `krishiv` binary.

## Next Expected Slice

The next implementation slice should add the first real local execution fragment
on top of the task runner skeleton: execute `SELECT 1` or an in-memory Arrow
batch, then return result metadata without putting bulk Arrow data into
control-plane Protobuf messages. Do not start R3.2 connector certification yet.
