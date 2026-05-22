# Krishiv Production Hardening Guide (R10)

Operational runbook for deploying Krishiv in a production environment. Covers security,
Kubernetes configuration, observability, upgrades, and known limitations as of R10.

See also: [`docs/architecture/stability-policy.md`](../architecture/stability-policy.md)

---

## 1. Pre-Production Checklist

Complete every item before routing production traffic.

### TLS

| Check | Requirement |
|---|---|
| Flight SQL (port 31337) | `tls: required` in service config. Never `optional` or `disabled`. |
| Certificate validity | Cert must not expire within the next 30 days at deploy time. Use cert-manager for automated rotation. |
| mTLS | Preferred over API keys for service-to-service auth. |

```
# Verify TLS config in KrishivFlightSqlService
grep -r "tls:" deploy/ | grep -v "required"   # should return nothing
```

### Authentication

- `AuthProvider` must be explicitly configured on `KrishivFlightSqlService`.
- `StaticApiKeyAuthProvider` is **dev only**. Use OIDC or mTLS for production.
- Verify the provider rejects unauthenticated connections before go-live.

### Policy Hooks

- At least one `PolicyHook` enforcing table-level access control must be wired.
- Passthrough / no-op hooks are **dev only**.
- Verify the hook is present: missing hook = all queries pass unchecked.

### Leader Election

| Parameter | Requirement |
|---|---|
| Client | `K8sLeaseElection` must use a real `kube::Client` via `with_kube_client()`. Simulation mode is forbidden in production. |
| Lease duration | 15s default. Must be ≥ 10s. Do not reduce below 10s. |

```bash
# Confirm simulation mode is NOT in use
grep -r "simulation_mode\|with_simulation" deploy/   # should return nothing
```

### Fencing Tokens

- `validate_fencing_token()` must be called at every checkpoint write site.
- **Post-R12 review (GAP-CP-03):** `commit_epoch` in `krishiv-scheduler` currently writes
  metadata without calling `validate_fencing_token`. Treat checkpoint exactly-once as
  **not production-certified** until GAP-CP-03 and GAP-CK-01 are closed. See
  [`r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md).
- Audit the codebase before GA: `rg validate_fencing_token` must show call sites on
  every write and restore path.
- Alert on `fencing_token_rejection_total` > 0 (see Section 3).

### Replicas

| Component | Minimum | Notes |
|---|---|---|
| Coordinator | 2 | Enable PodDisruptionBudget (minAvailable: 1). |
| Executor | 1 per active job | Autoscale via HPA. |

### Resource Quotas

- `QuotaPolicy` must be configured per namespace or tenant.
- No unbounded queues. Set `ThrottleCommand` thresholds to prevent OOM under load.

---

## 2. Kubernetes Configuration

### Namespace Strategy

- Use a dedicated namespace per tenant **or** per environment (dev / staging / prod).
- Use K8s RBAC to prevent tenants from accessing each other's Lease objects, Secrets, and PersistentVolumeClaims.

### NetworkPolicy

Restrict inter-pod traffic to the minimal required paths:

```yaml
# Allow only coordinator <-> executor and coordinator <-> Flight SQL gateway
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: krishiv-allow-internal
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/part-of: krishiv
  ingress:
    - from:
        - podSelector:
            matchLabels:
              krishiv.io/role: coordinator
        - podSelector:
            matchLabels:
              krishiv.io/role: executor
        - podSelector:
            matchLabels:
              krishiv.io/role: flight-sql-gateway
  policyTypes:
    - Ingress
```

Block all other ingress by default.

### Resource Limits

Arrow workloads are memory-intensive. Set limit = 2x the expected working set.

```yaml
resources:
  requests:
    cpu: "2"
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"   # 2× expected Arrow working set
```

Set on **all** Krishiv pods: coordinator, executor, Flight SQL gateway, operator.

### PodDisruptionBudget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: krishiv-coordinator-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      krishiv.io/role: coordinator
```

### Topology Spread

Spread executor pods across availability zones to tolerate zone failures:

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
        krishiv.io/role: executor
```

### Checkpoint Storage

- Use a PersistentVolumeClaim backed by a **durable** StorageClass (e.g. `gp3`, Ceph RBD).
- Never use `emptyDir` for checkpoint storage — data is lost on pod restart.
- Prefer NVMe-backed PVCs; checkpoint writes are synchronous (see Section 6).

```yaml
volumeClaimTemplates:
  - metadata:
      name: checkpoint-storage
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-nvme      # durable, not emptyDir
      resources:
        requests:
          storage: 100Gi
```

### Secrets Management

- Store API keys and TLS certificates in **K8s Secrets**, not ConfigMaps.
- Mount secrets as volumes, not environment variables, where possible.
- Enable encryption at rest for the K8s Secrets store.

---

## 3. Observability

### Tracing (OTLP)

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=https://collector.internal:4317
export OTEL_SERVICE_NAME=krishiv-coordinator
```

- Set `OTEL_EXPORTER_OTLP_ENDPOINT` to a production collector before starting any Krishiv process.
- Verify spans appear in the collector (search for `krishiv.*` service name).
- Use the `MetricsHandle::shutdown()` call on graceful shutdown to flush pending spans.

### Structured Logging

| Environment | `RUST_LOG` setting |
|---|---|
| Production (steady state) | `info` |
| Troubleshooting | `debug` |
| Never in steady state | `debug` or `trace` |

```bash
# Production
RUST_LOG=info ./krishiv-scheduler

# Scoped debug (single crate only)
RUST_LOG=krishiv_scheduler=debug,info ./krishiv-scheduler
```

### Metrics

- Expose the Prometheus scrape endpoint if wired, or use the stdout exporter with a log aggregator.
- Ensure `MetricsConfig` has `otlp_endpoint` set in production.
- Scrape interval: 15s recommended.

### Audit Logs

Verify these events appear in the log stream for every production deployment:

| Event | Log call |
|---|---|
| Job submit | `audit_log("job.submit", ...)` |
| Job cancel | `audit_log("job.cancel", ...)` |
| Query execution | `audit_log("query.execute", ...)` |

Missing audit log entries for any of these events is a production blocker.

### Alerting

| Alert | Condition | Severity | Action |
|---|---|---|---|
| Coordinator instability | `coordinator_failover_total` rising | Warning | Investigate lease renewal; check node health |
| Stale coordinator | `fencing_token_rejection_total` > 0 | Critical | Old coordinator is alive and writing; investigate immediately |
| Queue pressure | Job queue depth > threshold | Warning | Scale executors or throttle ingestion |
| Cert expiry | TLS cert expires in < 14 days | Warning | Rotate via cert-manager or manually |

---

## 4. Upgrade Procedure

### Pre-Upgrade

1. Take a savepoint of all running jobs:
   ```bash
   krishiv jobs list --status running --format id | xargs -I{} krishiv jobs savepoint {}
   ```
2. Verify savepoints succeeded before proceeding.
3. Back up checkpoint storage (snapshot the PVC or underlying object store).

### Rolling Upgrade (Minor Versions)

Supports in-place upgrades across one minor version gap.

1. Upgrade coordinator pods first (via rolling deployment update).
2. Wait for all coordinator pods to be `Ready`.
3. Upgrade executor pods.
4. Maximum supported version gap: **1 minor version** (e.g. 1.2 → 1.3).

```bash
kubectl set image deployment/krishiv-coordinator coordinator=krishivai/coordinator:v1.3.0
kubectl rollout status deployment/krishiv-coordinator
kubectl set image deployment/krishiv-executor executor=krishivai/executor:v1.3.0
kubectl rollout status deployment/krishiv-executor
```

### Major Version Upgrade

1. Stop all running jobs:
   ```bash
   krishiv jobs list --status running --format id | xargs -I{} krishiv jobs cancel {}
   ```
2. Drain executors and wait for idle.
3. Run migration tool on the new version binary:
   ```bash
   krishiv migrate --from v1.x --to v2.0
   ```
4. Deploy new version images.
5. Run `cargo test -p krishiv-upgrade-tests` on the new deployment before routing traffic.

### Rollback

1. Re-deploy previous version images:
   ```bash
   kubectl set image deployment/krishiv-coordinator coordinator=krishivai/coordinator:<prev-tag>
   kubectl set image deployment/krishiv-executor executor=krishivai/executor:<prev-tag>
   ```
2. Restore jobs from savepoints:
   ```bash
   krishiv jobs restore --from-savepoint <savepoint-id>
   ```

---

## 5. Security Hardening

### Simulation Mode

`K8sLeaseElection` simulation mode is for unit tests only. Production must use a real
`kube::Client`:

```rust
// Correct — production
let election = K8sLeaseElection::new(lease_name).with_kube_client(client);

// Wrong — simulation only, will not acquire real leases
let election = K8sLeaseElection::new(lease_name);  // simulation mode
```

### TLS Certificate Rotation

- Use cert-manager for automated rotation. Configure a `Certificate` resource with `renewBefore: 720h` (30 days).
- For manual rotation: replace the K8s Secret containing the cert, then perform a rolling restart of Flight SQL gateway pods.
- Never let a certificate expire in production.

### API Key Rotation

- Rotate API keys on a regular schedule (90 days or per your security policy).
- Keys are invalidated immediately when removed from `StaticApiKeyAuthProvider` config.
- Prefer OIDC or mTLS over static keys in production — static keys cannot be revoked per-session.

### Audit Log Retention

- Retain audit logs for **≥ 90 days** to meet common compliance frameworks (SOC 2, ISO 27001).
- Ship logs to durable storage (S3, GCS, or equivalent) with immutable write policy.

### RBAC

| Principal | Allowed Roles |
|---|---|
| Read-only service accounts | `Reader` only |
| Job submission services | `Writer` |
| Admin tooling | `Admin` |

Do not grant `Writer` or `Admin` to read-only consumers.

### Namespace Isolation

Use K8s RBAC to restrict each tenant to its own namespace resources:

```yaml
# Prevent tenant-a from reading tenant-b's Leases
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: krishiv-tenant-a-binding
  namespace: tenant-a
subjects:
  - kind: ServiceAccount
    name: krishiv-tenant-a
roleRef:
  kind: Role
  name: krishiv-tenant-role
  apiGroup: rbac.authorization.k8s.io
```

Ensure Lease objects, Secrets, and PersistentVolumeClaims are namespace-scoped and not cluster-scoped.

---

## 6. Performance Tuning

### Arrow Memory

Default Arrow memory pool is unbounded and will grow until OOM under sustained load.

```bash
export ARROW_MEMORY_POOL_SIZE_MB=8192   # 8 GB; set to ~80% of pod memory limit
```

Set per-pod in the Deployment spec, not globally.

### DataFusion Parallelism

```bash
export DATAFUSION_TARGET_PARTITIONS=7   # num_cpus - 1 for compute-heavy workloads
```

For I/O-bound workloads (e.g. large scan with small aggregation), num_cpus is acceptable.

### Shuffle Storage

`LocalShuffleStore` defaults to the OS temp directory. Use a fast NVMe path:

```bash
export KRISHIV_SHUFFLE_DIR=/mnt/nvme/krishiv-shuffle
```

Ensure the path has sufficient free space: plan for 3–5x the input partition size during peak shuffle.

### Checkpoint Storage

Checkpoint writes are **synchronous**. Network-attached storage (NFS, slow EBS) directly
impacts checkpoint latency and job throughput.

- Use NVMe-backed PVCs for checkpoint storage.
- Monitor checkpoint write latency; sustained p99 > 500ms indicates storage contention.

### Backpressure

Configure `ThrottleCommand` thresholds in `QuotaPolicy` to prevent OOM under traffic spikes:

```rust
QuotaPolicy::builder()
    .max_inflight_bytes(4 * 1024 * 1024 * 1024)  // 4 GB
    .throttle_command(ThrottleCommand::BackpressureSource)
    .build()
```

Set per-tenant, not globally, to prevent one tenant from starving others.

---

## 7. Known Limitations

### R10 GA scope boundaries

| Area | Limitation |
|---|---|
| Materialized views | Incremental refresh is not supported. Refresh-on-commit only. |
| CDC fan-out | Multi-table CDC fan-out is **beta**. Single-table pipelines are certified for production. |
| Schema evolution during live CDC | Column **additions** only. Column renames, drops, and type changes require pipeline restart. |
| Benchmark certification | TPC-DS/TPC-H at SF100 is aspirational. **SF10 is the certified performance gate.** |
| Multi-region | No global multi-region active-active. One active coordinator per job, one region. |
| Spark/Flink API compatibility | Not in scope for v1.0. |

### Post-R12 maturity gaps (do not deploy as production-ready)

Full register: [`docs/architecture/r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md).

| Area | Limitation | Gap ID | Target fix |
|---|---|---|---|
| Remote coordinator CLI | `RemoteCoordinatorClient` may return success without RPC | GAP-RT-04 | R12 carryover |
| Distributed `Session` | `DistributedBackend` logs URL only; local DataFusion still runs | GAP-RT-01 | R13 (Flight SQL) |
| Runtime backends | Embedded/SingleNode accept plans without coordinator loop | GAP-RT-01 | R13 S6.2–S6.3 |
| Policy | `Session::sql()` bypasses policy when hooks configured | GAP-RT-05 | R12 carryover |
| Checkpoint fencing | Write path may skip `validate_fencing_token` | GAP-CP-03 | R12 carryover |
| Coordinator restart | Binary may not recover jobs from metadata store | GAP-CP-04 | R12 carryover / R13 |
| Streaming HA | Window state in memory, not `StateBackend`; barriers deferred | GAP-ST-01, GAP-ST-03 | R16 |
| Shuffle | Compression codecs not on executor hot path; unstable partition hash | GAP-SH-01, GAP-SH-03 | R12 carryover |
| Kafka / CDC | Default stubs; duplicate rdkafka CDC type risks build failure | GAP-CN-01, GAP-CN-02 | R12 carryover / R13 |
| Connector matrix | LocalParquet “certified” ahead of CI suite | GAP-CN-03 | R14 |
| Executor process | Binary register/heartbeat only | GAP-CP-09 | R13 |
| HA leadership | No wired `LeaderElection` in coordinator process | GAP-CP-01 | R16 |

Workloads requiring rows in the right-hand column must wait for the target release or
use embedded/local batch SQL only.
