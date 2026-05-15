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
register, heartbeat, become healthy, and be marked lost. Executors do not own
durable job truth.

## Scheduling Model

The R2 scheduler uses static round-robin placement over schedulable executors.
It does not autoscale, rebalance running tasks, use resource queues, or perform
adaptive placement.

## Limitations

- No Kubernetes API integration yet.
- No `KrishivJob` CRD yet.
- No gRPC/protobuf wire transport yet.
- No durable metadata store.
- No stage-level retry implementation yet.
- No exactly-once semantics.
- No shuffle, checkpoint, or savepoint ownership.
