# Deployment and durability

A Krishiv deployment is a durability profile plus a placement. The profile
decides what survives a restart; the placement decides which processes exist
and how they find each other. This document covers both, the artefacts under
`deploy/`, high availability, and upgrades.

## Durability profiles (`krishiv_common::durability`)

`KRISHIV_DURABILITY_PROFILE` (aliases in parentheses):

| Component | `dev-local` (`dev`, `local`) | `single-node-durable` (`single-node`) | `distributed-durable` (`distributed`) |
|---|---|---|---|
| coordinator metadata | memory | local RocksDB | etcd (consensus) |
| shuffle | memory | local disk | tiered: local disk + object store |
| operator state | memory | local RocksDB | local RocksDB with checkpoint restore |
| checkpoints | ephemeral local | local filesystem | object store |
| survives restart | no | yes | yes |
| multi-node safe | no | no | yes |
| requires fencing | no | no | yes (etcd lease) |

An invalid value falls back to `dev-local` with a warning. The profile is
read by every crate that owns a durable component, so a single variable
changes them together; `krishiv doctor` and `krishiv capabilities` report the
resolved profile and what it implies.

Storage locators: `KRISHIV_METADATA_BACKEND` / `KRISHIV_METADATA_PATH` /
`KRISHIV_ETCD_ENDPOINTS` / `KRISHIV_ETCD_LEADER_KEY`; `KRISHIV_SHUFFLE_URI`
(or `KRISHIV_SHUFFLE_DIR`, `KRISHIV_SHUFFLE_ADDR`, `KRISHIV_SHUFFLE_FLIGHT_ADDR`);
`KRISHIV_STATE_BACKEND` / `KRISHIV_STATE_DIR` / `KRISHIV_STATE_DFS_ROOT`;
`KRISHIV_CHECKPOINT_STORAGE` / `KRISHIV_CHECKPOINT_DIR`.

## Placements

### Embedded

No processes. The library builds an in-process coordinator and executor per
session (`01`). Use for tests, notebooks, one-shot CLI queries, and
applications that embed the engine.

### Single node

`krishiv local start` launches `clusterd` (coordinator + HTTP + UI), one
executor, and a Flight SQL server with auto-selected ports;
`single-node-durable` by default. `deploy/systemd/krishiv-clusterd.service`
and `krishiv-executor@.service` run the same processes as services on a
host; `deploy/docker/Dockerfile.single-node` packages them.

### Bare metal / distributed

One or more `krishiv clusterd` coordinators (etcd-backed, leader-elected),
N `krishiv executor` processes, optionally `krishiv shuffle-svc` for external
shuffle, and Flight SQL servers. `just run-bare-metal` and
`scripts/run_bare_metal.sh` wire a cluster from environment variables;
`Dockerfile.distributed` / `Dockerfile.prod` build the `distributed` /
`prod` feature presets (`15`).

### Kubernetes

`deploy/k8s/`:

| Directory | Content |
|---|---|
| `crds/` | `KrishivJob`, `KrishivQueue`, `KrishivExecutorPool` definitions |
| `operator/` | the `krishiv-operator` deployment, RBAC, and reconciler configuration |
| `infra/` | etcd, MinIO/object store, Postgres (catalog), OTLP collector |
| `direct/` | coordinator `StatefulSet`, executor `Deployment`, Services, `NetworkPolicy`, Secrets by reference, PVCs |
| `helm/` | the chart form of the same |
| `jobs/` | example `KrishivJob` manifests |
| `ga-soak/`, `phase58/`, `cancel-cert/`, `bench/` | the soak, HA chaos, cancellation certification, and benchmark environments |
| `kustomization.yaml` | overlays composing the above |

The operator reconciles a `KrishivJob` into a coordinator submission,
tracks it (`Submitted`, `Observed`, `WaitingForExecutors`,
`ExecutorPodLaunchFailed`), writes status conditions, adds a finalizer so
deletion cancels the scheduler job, and scales executor pools (a
`ScaledObject` example is included for KEDA). Executor capacity is derived
from the pod's cgroup (`05`), so requests/limits are the only sizing input.

## High availability

- Coordinators are leader-elected through an etcd lease; every durable write
  carries the fencing token (`04`). Followers serve reads and answer
  `/leaderz` false so Services route to the leader.
- Executors re-register idempotently with a new lease generation; their
  in-flight tasks are re-queued and shuffle output regenerated within budget.
- Streaming jobs restore from the last sealed checkpoint with sink
  transactions committed or aborted deterministically (`07`).
- Shuffle survives executor loss on the tiered store and orphan reclamation
  waits both a count and a clock before deleting (`06`).

The HA chaos gate (`../engineering-log/ha-chaos-gate-log.md`) kills
coordinators and executors on a schedule against a running cluster and
asserts recovery; the GA soak
(`../engineering-log/ga-soak-report-2026-08-10.md`) ran the production
preset for days with the chaos CronJob active. Two wedges found there —
etcd I/O on the scheduling runtime and IVM snapshots over etcd's request
ceiling — are the reasons for the dedicated etcd runtime and the chunked,
compressed snapshot format.

## Upgrades and rollback

- Wire compatibility is versioned (`TypedTaskFragment.version`, IVM wire
  dialects with capability echo, checkpoint metadata versions, savepoint
  format v1). Roll executors forward before coordinators; an older
  coordinator never receives a payload it cannot parse (`05`).
- State schema changes ship with a registered migration (`07`); the
  key-group hash change is the one documented break that required fresh
  checkpoints.
- Take a savepoint before an upgrade; `krishiv restore` from it is the
  rollback path.
- `docs/RELEASE.md` is the release procedure; `docs/COMPATIBILITY.md` the
  supported version matrix (`18`).

## Related

- `01` (placement enum), `12` (auth per deployment), `15` (feature presets
  per image), `16` (cluster benchmark environments).
