# Execution modes, placement, and routing

`krishiv-runtime` and `krishiv-api` decide *where* work runs. This document
covers the three modes, the placement decision that is kept separate from
them, how a `Session` is built, how a request is routed, and the sync/async
seam every surface crosses.

## Mode versus placement

Two enums that are deliberately not one:

| | Type | Values | Meaning |
|---|---|---|---|
| **Mode** | `krishiv_api::ExecutionMode` (= `krishiv_runtime::RuntimeMode`) | `Embedded`, `SingleNode`, `Distributed` | what the user asked for |
| **Placement** | `krishiv_runtime::ExecutionPlacement` | `LocalInProcess`, `SingleNodeDaemon`, `RemoteClusterRequired` | where data-plane work is *allowed* to run |

| Mode | Placement | Control/data path | Required endpoint |
|---|---|---|---|
| `Embedded` | `LocalInProcess` | In-process coordinator + executor over channels; DataFusion in the caller's process | none |
| `SingleNode` | `SingleNodeDaemon` | Local daemon reached over Arrow Flight / gRPC | a local coordinator/Flight endpoint |
| `Distributed` | `RemoteClusterRequired` | Remote coordinator and executors over Flight / gRPC | an explicit coordinator URL |

**The fail-closed rule.** A `Distributed` session with remote execution
disabled is invalid, and a distributed session whose endpoint is missing or
loopback is rejected at build time (`SessionBuilder::build`) or at runtime
construction. The engine will not report a remote job while quietly executing
locally. `Session::check_routing` exposes the resolved decision.

A third enum, `DeploymentTarget` (`Embedded`, `SingleNode`, `BareMetal`,
`K8s`), describes the *deployment* the process lives in — it selects defaults
such as auth expectations and advertised addresses; it does not change
placement.

## Building a session

`krishiv_api::SessionBuilder`:

| Builder method | Effect |
|---|---|
| `with_execution_mode(mode)` | the mode above |
| `with_deployment_target(target)` | deployment defaults |
| `with_coordinator(url)` | Flight/gRPC coordinator for `Distributed` (also used by `--remote`) |
| `with_coordinator_http(url)` | HTTP control plane (jobs, IVM, continuous streams) |
| `with_local_cluster(url)` | the local daemon for `SingleNode` |
| `with_remote_execution(bool)` | must be `true` for `Distributed` |
| `with_in_process_cluster(...)` | an explicit in-process coordinator/executor pair for `Embedded` |
| `with_target_parallelism(n)` | DataFusion `target_partitions` for the session's engine |
| `with_shuffle_partitions(n)` | shuffle bucket count override |
| `with_config(key, value)` | a DataFusion/Krishiv config key |
| `with_iceberg_catalog(catalog, name)` | attach a catalog (feature `iceberg-catalog`) |
| `with_auth(provider)` / `with_policy(hook)` | authentication and SQL policy hooks |
| `with_state_ttl(...)` | default keyed-state TTL |

`SessionBuilder::from_env()` reads the deployment from the environment:

| Variable | Values | Default |
|---|---|---|
| `KRISHIV_MODE` | `embedded`, `single-node`, `distributed`, `k8s`, `bare-metal` | inferred: a loopback coordinator URL → `single-node`; a non-loopback URL → `distributed`; none → `embedded` |
| `KRISHIV_COORDINATOR_URL` (alias `KRISHIV_COORDINATOR`) | Flight/gRPC URL | — |
| `KRISHIV_COORDINATOR_HTTP` | HTTP control-plane URL | — |
| `KRISHIV_REMOTE_EXEC` | `1`/`true` | derived from mode |
| `KRISHIV_TARGET_PARALLELISM` | positive integer | `available_parallelism()` |
| `KRISHIV_SHUFFLE_PARTITIONS` | positive integer | derived (see `06`) |

A non-loopback coordinator URL is always treated as `Distributed`, never as
`SingleNode`, so a production cluster cannot be reached through the
single-node path by accident. The CLI (`krishiv sql`, `krishiv explain`)
builds its session explicitly from flags and honours
`KRISHIV_TARGET_PARALLELISM` through the same parser as `from_env`.

## The `ExecutionRuntime`

`krishiv_runtime::ExecutionRuntime` is the one routing object. It exposes a
mixed sync/async surface: the async methods (`collect_batch_sql_async`, …)
are canonical; the sync ones delegate to them across a single `block_on` seam
(`krishiv_common::async_util::block_on`, the only sanctioned bridge — see
`17-testing-and-quality.md`, "Async contract"). Remote backends drive Flight
and gRPC directly on the async path; the in-process backend offloads
DataFusion work to the blocking pool.

### Embedded: the in-process cluster

`krishiv_runtime::in_process_cluster` builds a session-scoped coordinator and
executor connected over in-memory channels (`in_process` transport,
ADR-12.4). Batch SQL runs through the session's own `SqlEngine`; streaming
plans run through `local_streaming`, which delegates to the same
`krishiv-dataflow` operator runtime the executor uses (ADR-12.5). Nothing
listens on a socket.

Two behaviours are only enabled when a process has declared itself a
one-shot CLI process — `krishiv_common::executor_capacity::declare_single_query_process()`,
called by the CLI's query commands and by nothing else (not `krishiv mcp`,
not the Python bindings, not a coordinator): the CTE materialisation and
grouping-set rewrite in `02-sql-engine.md`, the grace hash join where plans
are never encoded, and the degenerate-broadcast rescue in `spillable_join`.
The reason is that these either execute eagerly inside a lazy API or produce
plans the distributed encoder cannot serialise; a process that serves many
queries or plans stages for others must not take them.

### Single node: the local daemon

`krishiv local start` launches a coordinator (`krishiv clusterd`), an
executor, and a Flight SQL server on one host with automatically chosen
ports, and `krishiv local status|stop` manages them. A `SingleNode` session
talks to that daemon over Flight/gRPC; the daemon uses the
`single-node-durable` profile by default (`14-deployment-and-durability.md`).

### Distributed: the remote cluster

A `Distributed` session holds a `DistributedBackend` with an Arrow Flight SQL
client (`flight_client`) for SQL and table registration, a gRPC management
client for jobs/executors, and an HTTP client (`coordinator_http_client`) for
the control-plane APIs. Table registrations for `--parquet` tables are
forwarded to the coordinator; result batches come back over Flight. The
daemons themselves are described in `04` and `05`; deployment in `14`.

## Routing a query in the CLI

`krishiv sql` has three execution selectors, mutually exclusive:

| Flag | `QueryExecution` | Behaviour |
|---|---|---|
| (none) | `Default` | `Session::sql` — routed by the session's mode |
| `--local` | `Local` | `Session::execute_local` — always the in-process `SqlEngine`, regardless of mode; used for embedded benchmarking |
| `--remote` | `Remote` | `Session::execute_remote` — always the coordinator; requires a URL |

`--mode embedded|single-node|distributed` sets the session mode; `-c/--coordinator`
or `KRISHIV_COORDINATOR` supplies the endpoint. `krishiv explain --analyze`
executes locally and reports per-operator metrics; it refuses `--remote`
because a distributed plan's metrics live on the executors that ran it.

## What runs where

| Work | Embedded | Single-node | Distributed |
|---|---|---|---|
| Batch SQL | in-process DataFusion | daemon's executor(s) | staged, partition-parallel tasks on the executor pool |
| Streaming windows | `local_streaming` in-process | daemon executor run-loop | `stream:rloop:` tasks with keyed exchange between executors |
| IVM ticks | in-process `IncrementalFlow` | coordinator-resident or executor-resident per job shape | same; see `09` for which jobs reach an executor |
| Checkpoints | ephemeral local (`dev-local`) | local filesystem | object store |
| Metadata | memory | RocksDB | etcd |

## Related documents

- `04-scheduler-and-coordinator.md` — what the coordinator does with a routed job.
- `14-deployment-and-durability.md` — how each placement is deployed and made durable.
- `15-configuration.md` — the full environment-variable model.
- `../decisions/0002-public-api-shape-and-execution-semantics.md` — why Rust is
  async-first and where blocking is allowed.
