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

## Client CLI (Session API mirrors)

| Session / API | CLI |
|---------------|-----|
| `sql` / `sql_async` | `krishiv sql --query …` |
| `execute_local` | `krishiv sql --local --query …` |
| `execute_remote` | `krishiv sql --remote -c <URL> --query …` |
| `sql_as` | `krishiv sql --api-key <KEY> --query …` (requires `KRISHIV_API_KEYS`) |
| `submit_stream_job` | `krishiv stream submit --job-id …` |
| `push_stream_job_input` | `krishiv stream push --job-id … --parquet <path>` |
| `poll_stream_job` | `krishiv stream poll --job-id …` |
| `read_parquet` / `read_delta_async` / `read_hudi_async` | `krishiv table read --path … --format parquet\|delta\|hudi` |
| Window kinds (tumbling / sliding / session) | `krishiv stream submit --window tumbling\|sliding\|session` + size/slide/gap flags |
| Remote coordinator | `-c` / `KRISHIV_COORDINATOR` on client commands |

Stream submit/push/poll share one in-process cluster per `krishiv` process (via `SessionBuilder::with_in_process_cluster`).

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
