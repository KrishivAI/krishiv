# Krishiv Distributed Deployment Targets

**Status:** Active — applies from R2 onwards.
**Owner:** Architecture team.

---

## Overview

Krishiv's distributed mode supports two deployment targets. The core runtime — coordinator binary, executor binaries, gRPC transport, task assignment, heartbeat, ShuffleStore, MetadataStore — is **identical** on both. The difference is only in how processes are started and managed.

| Target | Status | Process management | Job submission | K8s dependency |
|---|---|---|---|---|
| **Kubernetes** | Primary | Operator creates pods | `KrishivJob` CRD or `krishiv submit` | Yes |
| **Bare metal / VM** | Secondary | systemd / supervisord / shell | `krishiv` CLI → coordinator address | None |

---

## Kubernetes Deployment (Primary)

### How It Works

The Krishiv Kubernetes operator (cluster control plane) watches `KrishivJob` CRDs. When a job is submitted:
1. The CCP reconciles the job into the shared coordinator and executor pool.
2. When `spec.dedicatedCoordinator` is true, a per-job orchestration loop ([`JobCoordinator`](../../crates/krishiv-scheduler/src/job_coordinator.rs)) drives task launch for that job.
3. Executors (static pool Deployment) register with the CCP via gRPC.
4. Optional: a dedicated `krishiv-job-coordinator` process can share durable metadata on bare metal (see `krishiv-job-coordinator` binary).
5. On delete, the finalizer cancels the scheduler job; shuffle GC runs on the CCP tick loop.

### Setup

```bash
# Install CRDs and operator
kubectl apply -f k8s/crds/
kubectl apply -f k8s/operator/

# Submit a job
kubectl apply -f my-job.yaml
# or
krishiv submit --file my-job.yaml
```

### Kubernetes-Only Features

| Feature | Why K8s only |
|---|---|
| `KrishivJob` CRD | Kubernetes API extension |
| Operator lifecycle management | Requires Kubernetes API access |
| NetworkPolicy for gRPC isolation | Kubernetes networking primitive |
| IRSA / Workload Identity for S3 | AWS/GCP service account binding to K8s SA |
| Executor pod `terminationGracePeriodSeconds` | Kubernetes pod spec |
| Executor pod launch failure detection | Operator watches pod `Ready` condition |
| HA coordinator leader election (R9) | Kubernetes `Lease` API |

---

## Bare Metal / VM Deployment (Secondary)

### How It Works

The coordinator and executor are plain Rust binaries. Start them on any machine with TCP connectivity between them. No Kubernetes, no operator, no CRDs.

```bash
# Machine A — start coordinator
krishiv-coordinator \
  --listen 0.0.0.0:7070 \
  --data-dir /var/krishiv/meta \
  --shuffle-dir /var/krishiv/shuffle

# Machine B — start executor (points at coordinator)
krishiv-executor \
  --coordinator http://192.168.1.10:7070 \
  --data-dir /var/krishiv/shuffle \
  --parallelism 8

# Machine C — another executor
krishiv-executor \
  --coordinator http://192.168.1.10:7070 \
  --data-dir /var/krishiv/shuffle \
  --parallelism 8

# Any machine — submit and query
krishiv sql \
  --coordinator http://192.168.1.10:7070 \
  "SELECT count(*) FROM parquet.read('s3://bucket/data/')"

krishiv jobs --coordinator http://192.168.1.10:7070
krishiv cancel --coordinator http://192.168.1.10:7070 --job-id abc123
```

### Process Management Options

| Tool | How to use |
|---|---|
| `systemd` | Write a `.service` file for coordinator and executor; `Restart=always` for auto-restart |
| `supervisord` | Define programs in `supervisord.conf`; handles crash restart |
| `docker run` | Run coordinator and executor in containers on bare metal; use `--net=host` or explicit ports |
| Shell / manual | For development: start in separate terminals |

### Network Isolation (Without NetworkPolicy)

On bare metal, use OS-level firewall rules to restrict gRPC port access:

```bash
# Allow gRPC only from executor IP range
ufw allow from 192.168.1.0/24 to any port 7070
ufw deny 7070
```

Or use a private VLAN / VPC security group that limits port 7070 to internal hosts.

---

## Feature Availability Matrix

| Feature | Kubernetes | Bare metal / VM |
|---|---|---|
| Coordinator binary | ✅ | ✅ |
| Executor binary | ✅ | ✅ |
| gRPC task assignment / heartbeat / cancellation | ✅ | ✅ |
| Arrow Flight shuffle (local or object-store mode) | ✅ | ✅ |
| MetadataStore (KrishivJob CRD status backend) | ✅ | ⚠️ Use file/SQLite backend |
| `krishiv sql / jobs / cancel / explain` CLI | ✅ | ✅ |
| `krishiv submit` (CRD-based) | ✅ | ❌ Use binary flags directly |
| Savepoint / restore | ✅ | ✅ |
| Checkpoint (object-store backed) | ✅ | ✅ |
| Kafka / Parquet / S3 connectors | ✅ | ✅ |
| Kubernetes Operator | ✅ | ❌ |
| `KrishivJob` CRD | ✅ | ❌ |
| NetworkPolicy (gRPC isolation) | ✅ | ❌ (use firewall) |
| IRSA / Workload Identity | ✅ | ❌ (use env credentials) |
| Auto executor restart on crash | ✅ (K8s pod restart) | ⚠️ Use systemd `Restart=always` |
| Executor pod launch failure detection | ✅ (operator) | ❌ (manual monitoring) |
| HA coordinator (R9) | ✅ (K8s Lease) | ❌ Deferred — needs etcd |

---

## MetadataStore on Bare Metal

The coordinator's `MetadataStore` on Kubernetes is backed by the `KrishivJob` CRD status subresource (Kubernetes persists it to etcd). On bare metal, Kubernetes is not available. The bare metal backend options are:

| Option | Notes |
|---|---|
| **Embedded SQLite** (recommended for bare metal) | Single file, no external dependency, survives coordinator restart |
| **Write-ahead log on disk** | Fast writes, replay on restart |
| **External etcd** | Adds an operational dependency; used if HA is needed on bare metal |

The `MetadataStore` trait is the same regardless of backend. The backend is selected by configuration:

```toml
[metadata]
backend = "sqlite"              # for bare metal
path    = "/var/krishiv/meta.db"

# OR for Kubernetes (default when operator is present):
# backend = "kubernetes-crd"
```

---

## HA Coordinator on Bare Metal (Post-R9)

HA coordinator in R9 uses the Kubernetes `Lease` API for leader election. This is Kubernetes-only.

For bare metal HA (post-R9, not planned in the first 10 releases):
- Deploy an external **etcd** cluster.
- Implement `MetadataStore` and leader election backed by etcd leases.
- Coordinator candidates watch the etcd lease; only the holder is active.

This is explicitly deferred until benchmark data shows bare metal HA is a production requirement.

---

## When to Use Each Target

| Use case | Target |
|---|---|
| Local development | Bare metal (two processes on one machine) |
| CI integration tests | Bare metal (start/stop with test harness) |
| Small production (2–10 nodes, self-managed) | Bare metal with systemd |
| Production on cloud VMs (no K8s cluster available) | Bare metal with env-based credentials |
| Production on Kubernetes | Kubernetes (primary — full feature set) |
| Multi-tenant production | Kubernetes (NetworkPolicy, RBAC, resource quotas) |
| On-premise regulated environments (no K8s allowed) | Bare metal |
