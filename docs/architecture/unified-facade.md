# Unified Krishiv Facade

End users should depend on a single crate (`krishiv`) and a single binary (`krishiv`) for SQL, streaming, local clusters, and distributed daemons.

## Binary: one multitool

| Legacy binary | Unified command | Role |
|---------------|-----------------|------|
| `krishiv-coordinator` | `krishiv coordinator` | Active coordinator (gRPC + optional HTTP) |
| `krishiv-clusterd` | `krishiv clusterd` | Cluster control plane (CCP) |
| `krishiv-job-coordinator` | `krishiv job-coordinator` | Per-job coordinator (JCP) |
| `krishiv-executor` | `krishiv executor` | Data-plane worker |
| `krishiv-flight-server` | `krishiv flight-server` | Arrow Flight SQL |
| `krishiv-shuffle-svc` | `krishiv shuffle-svc` | Optional shuffle HTTP service |
| — | `krishiv` (root) | Client: `sql`, `submit`, `jobs`, `local`, `cluster`, … |

Legacy binary names remain as thin wrappers for systemd/K8s manifests; new installs should use `krishiv <subcommand>`.

Lifecycle helpers spawn daemons via the **same** executable:

- `krishiv local start` → `krishiv coordinator`, `krishiv flight-server`, `krishiv executor`
- `krishiv cluster start` → `krishiv clusterd`, `krishiv executor` (×N)

## Library: `krishiv` crate

### Already re-exported (application API)

- `Session`, `SessionBuilder`, `DataFrame`, `Stream`, window builders
- Connectors, lakehouse, UDFs
- `ExecutionMode` (embedded / single-node / distributed)

### New: `krishiv::distributed`

For operators and custom deployments that embed daemons:

- `Coordinator`, `ClusterControlPlane`, `JobCoordinator`
- `CoordinatorDaemonConfig`, `run_standalone_coordinator`, `run_clusterd_daemon`, `run_job_coordinator_daemon`
- `ExecutorConfig`, `ExecutorRuntime`, `ExecutorTaskRunner`, `GrpcCoordinatorService`
- Proto IDs: `JobId`, `ExecutorId`, `CoordinatorId`, …

## CLI gaps (client commands not yet mirrored)

| Session / API | CLI today | Suggested CLI |
|---------------|-----------|---------------|
| `execute_local` / `execute_remote` | `sql --mode` only | `krishiv sql --local` / `--remote` |
| `submit_stream_job` | — | `krishiv stream submit` |
| `push_stream_job_input` / `poll_stream_job` | — | `krishiv stream push` / `poll` |
| `sql_as` (auth) | — | `krishiv sql --api-key` |
| `read_delta_async` / `read_hudi_async` | — | `krishiv table read --format delta` |
| Remote coordinator gRPC on Session | `-c` / `KRISHIV_COORDINATOR` | ✓ (partial) |

## Out of scope for the main crate (by design)

| Component | Why separate |
|-----------|--------------|
| `krishiv-operator` | Kubernetes-only; cluster admins install once |
| `krishiv-ui` | Optional status UI |
| Python / JVM bindings | Separate release artifacts (roadmap) |

## Validation

```bash
cargo +stable build -p krishiv --bin krishiv
cargo +stable test -p krishiv -p krishiv-scheduler -p krishiv-executor --lib
./target/debug/krishiv help daemons
```
