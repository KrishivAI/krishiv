# R2 Control Plane Skeleton

## Purpose

R2 starts Krishiv's distributed control plane without introducing Kubernetes
clients, CRDs, durable metadata, or network transports in the first slice. The
goal is to make coordinator and executor semantics explicit in Rust before
mapping them to Kubernetes resources.

## Crate Ownership

- `krishiv-proto` owns typed control-plane contracts:
  - coordinator, job, stage, task, and executor identifiers
  - coordinator, job, stage, task, and executor lifecycle states
  - job, stage, and task specs
  - executor registration and heartbeat messages
  - task assignment and task status update messages
- `krishiv-scheduler` owns in-process scheduling behavior:
  - active and standby coordinator skeletons
  - executor registry
  - static round-robin task placement
  - task launch and completion/failure reporting
  - job snapshots for future CLI or Web UI status

## Leadership Model

R2 keeps exactly one active coordinator. A standby coordinator can exist as a
type-level skeleton, but it rejects mutating operations. HA leader election,
leases, fencing tokens, and failover are intentionally deferred to R9.

## Executor Model

Executors are replaceable data-plane workers. In this slice, executors can
register, heartbeat, become healthy, and be marked lost. Heartbeat timeouts are
modeled with deterministic scheduler ticks in R2 so tests and future
controllers can drive timeout behavior without wall-clock coupling. Executors
do not own durable job truth.

## Scheduling Model

The R2 scheduler uses static round-robin placement over schedulable executors.
It does not autoscale, rebalance running tasks, use resource queues, or perform
adaptive placement.

## DAG Routing Model

R2 can convert Krishiv logical and physical DAG wrappers from `krishiv-plan`
into distributed scheduler jobs:

- Batch DAGs map to `JobKind::Batch`.
- Streaming DAGs map to `JobKind::Streaming` with R1-level local state
  semantics only.
- Each plan node becomes a static task in the first R2 stage.
- Empty plans are represented as one scheduler task so bootstrap physical plans
  can still flow through the distributed path.

## Retry Model

R2 implements conservative stage-level retry. A failed task retries the whole
stage up to the coordinator's configured `max_stage_retries`. Retried tasks keep
their static executor assignment and move back through the assigned/running
lifecycle. Once retry budget is exhausted, the stage and job become failed.

## CLI Surface

The first R2 CLI slice exposes the in-process scheduler skeleton without
claiming Kubernetes submission:

- `krishiv submit` builds a synthetic distributed job spec, registers one local
  executor, statically places tasks, and prints job, stage, task, and executor
  status.
- `krishiv jobs --distributed` prints the distributed status shape for jobs
  known to the current process.
- `krishiv jobs` without flags preserves the R1 process-local job behavior.

## Status API And Web UI

The R2 status UI is implemented in `krishiv-ui` as a Rust-native `axum` server
with server-rendered `askama` templates. It reads the same scheduler snapshot
types used by the CLI and exposes:

- `GET /healthz` for process liveness.
- `GET /readyz` for coordinator snapshot readiness.
- `GET /api/v1/jobs` for job summaries.
- `GET /api/v1/jobs/{job_id}` for stage/task detail.
- `GET /api/v1/executors` for executor summaries.
- `GET /ui` and `GET /ui/jobs/{job_id}` for the HTML status pages.

The standalone server can be run with `cargo run -p krishiv-ui -- --demo` to
seed one local coordinator, one executor, and one running demo job. In R2 this
is an in-process status surface only; persistence, authentication,
OpenTelemetry, and Kubernetes-backed job history are deferred.

## Kubernetes Surface

The first R2 Kubernetes slice adds static manifests under `k8s/`:

- `k8s/crds/krishivjobs.yaml` defines the `krishiv.io/v1alpha1` `KrishivJob`
  custom resource.
- `k8s/manifests/` defines the namespace, service account, RBAC, one
  coordinator deployment, coordinator service, replaceable executor deployment,
  and a sample batch `KrishivJob`.
- The coordinator deployment is intentionally `replicas: 1` to preserve the R2
  single-active-coordinator rule.
- Manifest tests validate the expected offline shape without requiring a
  Kubernetes cluster.

## Operator Reconciliation Model

The first operator slice is implemented in `krishiv-operator`. It does not run
a live Kubernetes watch loop yet; instead, it pins the reconciliation behavior
that the future controller will call:

- Typed Rust models mirror the R2 `krishiv.io/v1alpha1` `KrishivJob` shape.
- Resource validation rejects unsupported API versions, kinds, empty names,
  empty images, zero tasks, and zero parallelism.
- A namespaced `KrishivJob` maps to a URL-safe scheduler id in the form
  `<namespace>.<name>`.
- `spec.mode` maps to `JobKind::Batch` or `JobKind::Streaming`.
- `spec.tasks` becomes static `task-1..task-N` entries in `stage-1`.
- If no healthy executor is available, reconciliation returns an accepted
  status with a `Scheduled=False` / `NoExecutors` condition instead of marking
  the resource failed.
- Once a scheduler job exists, reconciliation refreshes `.status.phase`,
  `.status.stages`, `.status.tasks`, `.status.coordinator`, and
  `.status.conditions` from scheduler snapshots.

The next controller slice should connect this reconciler to Kubernetes watch
events and status subresource patches.

## Limitations

- No live Kubernetes watch/controller loop yet.
- No gRPC/protobuf wire transport yet.
- No durable metadata store.
- No persistent cross-process job history.
- No Kubernetes `kind` smoke test yet.
- No exactly-once semantics.
- No shuffle, checkpoint, or savepoint ownership.
