# R19 Multi-Region, Autoscaling & Cloud-Native Implementation Tracker

## Goal

Deliver global-scale production deployment capabilities so that an enterprise
running jobs across AWS, GCP, and Azure can manage them from a single Krishiv
control plane. R19 introduces multi-region coordinator federation with
configurable routing and failover, KEDA-based autoscaling driven by Kafka lag
and Prometheus metrics, spot/preemptible instance recovery using the incremental
checkpoints from R16, bare-metal HA via etcd leader election, cost-aware job
placement with budget constraints and optimization goals, and a serverless
execution mode for short-lived batch jobs on AWS Lambda and Google Cloud Run.
Pathway and Sail are single-region at this stage; R19 is a differentiation
window that must be complete before the enterprise packaging of R20.

## Scope

In scope:

- Multi-region federation currently remains scheduler-owned; introduce a
  dedicated crate only when the R19 surface is large enough to justify it.
- `ks.Session.connect(coordinators={region: url}, routing=..., failover=...)` Python API.
- `RoutingPolicy.nearest(latency_threshold_ms)` and `FailoverPolicy.automatic(rpo_seconds, rto_seconds)`.
- Global coordinator tier architecture per ADR-19.1: separate control plane (global job catalog) from data plane (regional task execution).
- KEDA external scaler gRPC server in `krishiv-scheduler` per ADR-19.2 (Option A).
- `AutoscalePolicy(min_executors, max_executors, scale_metric, scale_down_delay)` Python API.
- KEDA `ScaledObject` CRD generation in `krishiv-operator`.
- Spot/preemptible recovery: Kubernetes `SIGTERM` handler in `krishiv-executor` that triggers incremental checkpoint and graceful shutdown per ADR-19.3.
- `PlacementPolicy(node_preferences, checkpoint_on_preemption, max_preemption_fraction, recovery_timeout)`.
- `krishiv-etcd` crate: bare-metal HA coordinator via `etcd-client` for leader election and metadata storage.
- `krishiv-coordinator --ha-mode etcd --etcd-endpoints ...` CLI flag.
- `BudgetConstraint(max_hourly_cost_usd, cloud_provider, region)` and `OptimizationGoal` (COST, LATENCY, THROUGHPUT).
- Global job routing: batch_jobs to cheapest region, streaming_jobs to lowest-latency region.
- Serverless mode (Lambda/Cloud Run) for batch jobs under 14 minutes per ADR-19.4.

Out of scope:

- Full Raft consensus (openraft) for cross-region metadata — deferred pending ADR-19.1 resolution; Option C (global coordinator tier) does not require Raft in R19.
- CGROUP-level isolation for multi-tenant workloads (deferred to R20, where it is also explicitly out of scope).
- Serverless coordinator state in DynamoDB between Lambda invocations (Option C in ADR-19.4 — deferred beyond R20).
- GCP Cloud Spanner or Azure Cosmos DB as the global coordinator metadata store — single Postgres instance for R19.
- Autoscaling based on custom user-defined metrics (only Kafka lag and Prometheus metrics in R19).
- Cross-cloud shuffle data transfer cost optimisation.

## Dependencies

- R12 complete: `spawn_blocking` and no nested `block_on` patterns are established. Any new blocking coordinator calls must follow the same pattern.
- R13 complete: Python API layer is stable; `ks.Session.connect` already accepts a single coordinator URL. R19 extends this to a multi-coordinator map.
- R16 complete: RocksDB incremental checkpointing is the prerequisite for spot recovery. ADR-19.3 explicitly depends on R16's incremental checkpoint implementation. Spot recovery cannot be safely implemented without sub-second-delta snapshots.
- `krishiv-operator` crate exists with CRD reconciliation from R2/R9.
- `krishiv-scheduler` crate exposes a job and task model sufficient for cost-aware placement decisions.
- DataFusion session context exposes per-job resource metrics needed for cost attribution.

## Architectural Decisions Required

### ADR-19.1: Multi-Region Metadata Consistency Model

**Problem**

The multi-region coordinator federation must agree on which jobs exist, their
current state, and which region is authoritative for each job. The current
single-coordinator design has no concept of region-local vs. global state.
Before any federation code is written, the architecture of the global metadata
tier must be decided — the choice determines every data structure and API
in a future dedicated federation module or crate.

**Options**

- A. Strong consistency via Raft (openraft crate): all metadata writes go through
  a Raft leader; followers replicate synchronously. All reads from any region see
  the latest committed state. Latency for cross-region writes is bounded by the
  leader-to-follower RTT (typically 50–200ms across regions). Single-machine
  failure does not cause split-brain. Significant implementation complexity;
  openraft integration requires defining the state machine, log storage, and
  network transport.
- B. Region-local metadata with async replication (eventual consistency): each
  region's coordinator writes to its own metadata store and replicates changes
  asynchronously to peers. Fast local writes. Split-brain is possible during
  network partition — two regions may simultaneously believe they are authoritative
  for the same job, leading to duplicate task scheduling and data loss.
- C. Separate global control plane from regional data planes: a new global
  coordinator tier (single process, potentially Postgres-backed) owns the job
  catalog, job state, and region assignment. Regional coordinators own task
  execution within their region and report back to the global coordinator. The
  global coordinator is the single source of truth; regional coordinators are
  stateless task dispatchers. No Raft required for R19 — the global coordinator
  is a single writer with Postgres optimistic locking. HA for the global
  coordinator is handled by `krishiv-etcd` (Sprint 4) on bare-metal or by
  cloud-provider managed Postgres on cloud deployments.

**Recommendation**

Option C. The clean separation of global control plane and regional data plane
is architecturally correct and avoids the complexity of a distributed consensus
protocol in R19. The global coordinator is a single writer for the job catalog;
its HA story is simpler than full Raft and delivers the same correctness
guarantees for the R19 use cases. This ADR must be recorded as DECIDED before
Sprint 1 begins. Option A (Raft) remains the correct long-term path for a
fully-distributed control plane and should be scheduled as a future ADR.

**Risk if deferred**

Writing any multi-region code before this decision is made will produce
incompatible data models across sprints. Federation code written against Option
B (async replication) cannot be safely refactored to Option C without a full
rewrite of the metadata storage and locking layer.

---

### ADR-19.2: KEDA Integration Approach

**Problem**

KEDA (Kubernetes Event-Driven Autoscaling) scales Kubernetes Deployments based
on external metrics. Krishiv must expose its internal job metrics (source lag,
executor CPU, checkpoint age) to KEDA so that it can scale executor Deployments
up and down. KEDA offers three integration paths for custom metrics sources.

**Options**

- A. Implement a Krishiv KEDA external scaler: a gRPC server embedded in
  `krishiv-scheduler` that implements the KEDA `ExternalScaler` proto interface
  (`IsActive`, `GetMetricSpec`, `GetMetrics`). KEDA polls this server to get
  `currentMetricValue` and `desiredMetricValue` per job. Full control over which
  metrics are exposed and how scaling decisions are made. No dependency on
  Prometheus.
- B. Push job metrics to Prometheus (already partially implemented in
  `krishiv-metrics`) and use KEDA's built-in Prometheus scaler. No custom gRPC
  server required. Depends on a running Prometheus instance in the cluster.
  Less flexible — only metrics that are scraped by Prometheus are available for
  scaling decisions; internal Krishiv state (e.g., checkpoint lag) is harder to
  expose.
- C. Use KEDA's built-in Kafka scaler for Kafka-backed streaming jobs only.
  Zero custom code. Limited to Kafka consumer lag as the scaling metric; CPU
  and checkpoint-age-based scaling are not possible.

**Recommendation**

Option A (external scaler) for full control, with Option B as a fallback for
operators who already run Prometheus and prefer not to expose an additional gRPC
port. The external scaler gRPC server must expose: `currentMetricValue` (current
Kafka lag or CPU usage), `desiredMetricValue` (target lag or usage), and
`IsActive` (whether the job is running and has measurable lag). The gRPC server
listens on a port configurable via `--keda-scaler-port` on `krishiv-scheduler`.

**Risk if deferred**

Without KEDA integration, executor count must be managed manually or via
Kubernetes HPA (which does not support Kafka lag). This blocks streaming
workloads from scaling cost-efficiently and makes spot recovery more expensive
(too many executors when traffic is low).

---

### ADR-19.3: Spot Recovery Checkpoint Timing

**Problem**

Kubernetes sends `SIGTERM` to a pod before eviction. The `terminationGracePeriodSeconds`
is configurable (default 30s, commonly set to 60–120s for stateful workloads).
The executor must complete a checkpoint within this grace period. If the checkpoint
takes longer than the grace period, the job loses all state since the last
completed checkpoint epoch. Large RocksDB state (e.g., 10GB+ for session windows)
cannot be checkpointed from scratch within 60 seconds over a WAN link.

**Options**

- A. Checkpoint at `SIGTERM` reception: the executor starts a full checkpoint
  when it receives `SIGTERM` and hopes it completes within the grace period.
  Correct for small state sizes. For large state (> a few hundred MB), this
  races with the eviction deadline and produces inconsistent checkpoints if the
  pod is killed mid-write.
- B. Continuous incremental checkpointing with short intervals (using the R16
  RocksDB incremental SSTable upload mechanism). On `SIGTERM`, the executor only
  needs to write the delta since the last incremental checkpoint — typically
  seconds of data regardless of total state size. The `SIGTERM` handler flushes
  the current incremental checkpoint, marks the epoch complete in the coordinator,
  and exits cleanly.
- C. Checkpoint only job metadata on `SIGTERM` (not full state): save the
  current watermark, epoch number, and Kafka offsets to a fast key-value store
  (etcd or Redis). On restart, replay from the last committed Kafka offset.
  Correct for exactly-once with Kafka, but loses in-memory window state between
  the last full checkpoint and the eviction.

**Recommendation**

Option B. R16's RocksDB incremental checkpointing is the architectural
prerequisite that makes Option B viable. With incremental checkpoints running
every 30–60 seconds, the `SIGTERM` handler only needs to flush the current
delta (typically < 50MB) to object storage before the grace period expires.
This is the only option that is both correct (no state loss) and feasible at
large state sizes. If R16 incremental checkpointing is not complete when R19
begins, spot recovery must be scoped to small-state jobs only (< 500MB total
state) with a clear error if the state size exceeds the safe threshold.

**Risk if deferred**

Without spot recovery, spot/preemptible instances cannot be used for stateful
streaming jobs without accepting potential data loss on eviction. This makes
the cost-saving benefit of spot instances (Option A in cost-aware placement)
unavailable for the most common streaming workload type.

---

### ADR-19.4: Serverless Execution Mode Scope

**Problem**

AWS Lambda has a hard 15-minute execution limit and ephemeral storage. A Krishiv
coordinator managing a batch job longer than 14 minutes cannot run on Lambda.
The scope of serverless support must be defined before the coordinator execution
model is extended to handle serverless runtimes.

**Options**

- A. Support only short batch jobs (< 14 minutes) on Lambda/Cloud Run. The
  Krishiv session detects `runtime="aws_lambda"` and returns an error if the
  estimated job duration exceeds 14 minutes. Clear documentation on limitations.
  Simple to implement: the coordinator runs in Lambda's default execution
  environment with no special state management.
- B. Use Lambda for coordinator startup only; hand off to a long-running ECS
  Fargate task for the actual job execution. Lambda acts as a job trigger and
  status poller. Requires an ECS cluster to be available, which reduces the
  "serverless" simplicity claim.
- C. Implement a Lambda-aware execution mode where coordinator state (job status,
  task assignments, checkpoint epoch) is stored in DynamoDB between Lambda
  invocations. Lambda runs as a series of short-lived invocations that each
  advance the coordinator state machine. Correct for arbitrary-length jobs on
  Lambda. Significant infrastructure work; requires DynamoDB client, state
  machine serialisation, and Lambda invocation chaining.

**Recommendation**

Option A for R19. Scoped serverless support with clear documentation is the
correct approach for an initial implementation. The primary serverless use case
is ad-hoc batch queries (data exploration, one-off transformations) that fit
within 14 minutes. Option C is the full solution for long-running serverless
jobs and should be scheduled as a future ADR for a potential R21+ release.

**Risk if deferred**

Attempting Option C in R19 would consume most of the sprint capacity on
infrastructure work that is not the primary R19 goal (multi-region and
autoscaling). Option B adds ECS dependency that contradicts the "serverless"
pitch. Option A delivers genuine value for the target use case.

## Sprint 1 — Multi-Region Coordinator Federation

### S1.1: Dedicated federation crate — deferred

- [ ] If multi-region federation grows beyond the current scheduler HTTP path,
      create a dedicated crate with explicit ownership and feature gating
      instead of a placeholder workspace member.
- [ ] Define `FederationClient` trait with methods: `submit_job`, `cancel_job`, `job_status`, `list_jobs`, `route_task(job_id) -> RegionUrl`.
- [ ] Define `RegionMap: HashMap<String, Url>` and `RoutingPolicy` enum (`Nearest { latency_threshold_ms: u64 }`, `RoundRobin`, `DataLocality`).
- [ ] Define `FailoverPolicy` struct with `rpo_seconds` and `rto_seconds` fields.

**Validation:** `cargo build -p <future-federation-crate>`

### S1.2: Global coordinator tier — krishiv-scheduler

- [ ] Add a `GlobalCoordinator` mode to `krishiv-scheduler` (enabled by `--global-mode`) that stores the job catalog in a Postgres-backed `JobCatalogStore` instead of the in-memory default.
- [ ] Implement `JobCatalogStore` using `sqlx` with a `jobs` table schema: `(job_id UUID, region TEXT, status TEXT, submitted_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)`.
- [ ] Expose `assign_region(job_id, region)` and `get_authoritative_region(job_id)` methods.
- [ ] Add a unit test with a SQLite in-memory database (for CI without Postgres) covering CRUD operations on the job catalog.

**Validation**: `cargo test -p krishiv-scheduler`

### S1.3: Latency-aware routing — dedicated federation module/crate

- [ ] Implement `LatencyProber` that pings each coordinator endpoint via a lightweight `GET /health` HTTP call and records the round-trip latency with a 5-second exponential moving average.
- [ ] Implement `RoutingPolicy::Nearest`: route to the region with the lowest EMA latency that is below `latency_threshold_ms`; fall back to any healthy region if all are above threshold.
- [ ] Add a test using a mock HTTP server with configurable artificial delay asserting that the nearest region is selected correctly.

**Validation:** `cargo test -p <future-federation-crate-or-scheduler-module>`

### S1.4: Failover logic — dedicated federation module/crate

- [ ] Implement `FailoverManager` that monitors coordinator health via `LatencyProber`. If a region fails `health_check_interval * 3` consecutive checks, mark it `RegionStatus::Unavailable`.
- [ ] On region unavailability, `FederationClient::route_task` returns a different available region; in-flight jobs in the failed region are re-queued in the global coordinator.
- [ ] Add a test that marks a region unavailable and asserts all subsequent `route_task` calls return a different region within `rto_seconds`.

**Validation:** `cargo test -p <future-federation-crate-or-scheduler-module>`

### S1.5: Python federation API — krishiv (Python bindings)

- [ ] Extend `ks.Session.connect` to accept `coordinators: Dict[str, str]` (region → URL), `routing: RoutingPolicy`, `failover: FailoverPolicy`.
- [ ] Expose `ks.RoutingPolicy.nearest(latency_threshold_ms: int)` and `ks.FailoverPolicy.automatic(rpo_seconds: int, rto_seconds: int)`.
- [ ] Add `.pyi` stub entries.
- [ ] Add a Python integration test with two mock coordinator URLs.

**Validation**: `cargo test -p krishiv-python`

## Sprint 2 — KEDA Autoscaling

### S2.1: KEDA external scaler gRPC server — krishiv-scheduler

- [ ] Add `keda-external-scaler-proto` to `krishiv-proto` with the KEDA `ExternalScaler` service definition (clone from the KEDA proto repository, which is Apache 2.0 licensed).
- [ ] Implement `KedaExternalScalerService` in `krishiv-scheduler` implementing `IsActive`, `GetMetricSpec`, `StreamIsActive`, and `GetMetrics` gRPC methods.
- [ ] `GetMetrics` returns `currentMetricValue` as the current source lag (messages behind) for Kafka-backed jobs, or current CPU utilization for compute-bound jobs.
- [ ] `GetMetricSpec` returns `desiredMetricValue` from the job's `AutoscalePolicy.scale_metric` configuration.
- [ ] Bind the gRPC server on `--keda-scaler-port` (default 9090) in `krishiv-scheduler` alongside the existing control-plane port.

**Validation**: `cargo test -p krishiv-scheduler`

### S2.2: AutoscalePolicy and Python API — krishiv (Python bindings)

- [ ] Define `AutoscalePolicy` struct in `krishiv-api` with fields: `min_executors: u32`, `max_executors: u32`, `scale_metric: ScaleMetric`, `scale_down_delay: Duration`.
- [ ] Implement `ScaleMetric::SourceLag { target_lag: Duration }` and `ScaleMetric::CpuUtilization { target_fraction: f64 }`.
- [ ] Expose `ks.AutoscalePolicy(min_executors, max_executors, scale_metric, scale_down_delay)` and `ks.ScaleMetric.source_lag(target_lag)` in Python.
- [ ] Wire `AutoscalePolicy` into `session.submit_job(pipeline, autoscale=...)`.
- [ ] Add `.pyi` stub entries.

**Validation**: `cargo test -p krishiv-api && cargo test -p krishiv-python`

### S2.3: KEDA ScaledObject CRD generation — krishiv-operator

- [ ] Extend `krishiv-operator`'s reconciliation loop to generate a `ScaledObject` custom resource for any `KrishivJob` with `spec.autoscale` populated.
- [ ] The `ScaledObject` must reference the executor `Deployment` as the `scaleTargetRef` and point to the KEDA external scaler gRPC endpoint as an `externalScaler` trigger.
- [ ] Add a second trigger of type `prometheus` when `AutoscalePolicy.scale_metric` is `CpuUtilization` and a Prometheus URL is configured.
- [ ] Add a controller test that asserts a `ScaledObject` is created with the correct trigger configuration when a `KrishivJob` with autoscale is submitted.

**Validation**: `cargo test -p krishiv-operator`

### S2.4: Scale-down delay enforcement — krishiv-scheduler

- [ ] Track the timestamp of the last scale-up event per job in `krishiv-scheduler`'s in-memory state.
- [ ] When KEDA requests `GetMetrics` and the metric value has dropped below the target, return the target value unchanged (preventing scale-down) if less than `scale_down_delay` has elapsed since the last scale-up.
- [ ] Add a test that simulates a lag spike followed by recovery and asserts the scaler does not report a reduced metric within the delay window.

**Validation**: `cargo test -p krishiv-scheduler`

## Sprint 3 — Spot/Preemptible Recovery

### S3.1: SIGTERM handler in executor — krishiv-executor

- [ ] Implement `SpotEvictionHandler` in `krishiv-executor` that registers a `tokio::signal::unix::signal(SignalKind::terminate())` listener.
- [ ] On `SIGTERM`, call `CheckpointCoordinator::trigger_incremental_checkpoint(epoch)` on the current job epoch and wait for completion with a configurable timeout (`--spot-checkpoint-timeout`, default 25s).
- [ ] If the checkpoint completes within the timeout, mark the epoch committed in the coordinator and call `executor_shutdown_graceful()`. If it times out, log a warning, write a partial checkpoint manifest, and exit.
- [ ] Add a test that sends `SIGTERM` to a mock executor process and asserts the checkpoint trigger is called.

**Validation**: `cargo test -p krishiv-executor`

### S3.2: PlacementPolicy and node affinity — krishiv-operator, krishiv-api

- [ ] Define `PlacementPolicy` in `krishiv-api` with fields: `node_preferences: Vec<NodePreference>` (Spot, OnDemand), `checkpoint_on_preemption: bool`, `max_preemption_fraction: f64`, `recovery_timeout: Duration`.
- [ ] In `krishiv-operator`, translate `node_preferences` to Kubernetes `nodeAffinity` with `requiredDuringSchedulingIgnoredDuringExecution` for OnDemand fallback and `preferredDuringSchedulingIgnoredDuringExecution` for Spot preference.
- [ ] Annotate executor pods with `node.kubernetes.io/lifecycle: spot` or `on-demand` tolerations accordingly.
- [ ] Enforce `max_preemption_fraction` by capping the number of spot executor pods at `floor(total_executors * max_preemption_fraction)` in the reconciliation loop.

**Validation**: `cargo test -p krishiv-operator`

### S3.3: Spot executor recovery in scheduler — krishiv-scheduler

- [ ] When an executor pod transitions to `Failed` or `Unknown` (detected via the K8s watch stream in `krishiv-operator`), publish a `TaskEvicted { executor_id, tasks: Vec<TaskId> }` event to `krishiv-scheduler`.
- [ ] In the scheduler, re-queue all evicted tasks as `Pending` and increment their `retry_count`.
- [ ] If `retry_count` exceeds `PlacementPolicy.max_retries` (default 3), fail the task and propagate the error to the job.
- [ ] Add a test that simulates executor eviction and asserts all affected tasks are re-queued within `recovery_timeout`.

**Validation**: `cargo test -p krishiv-scheduler && cargo test -p krishiv-operator`

### S3.4: Python placement policy API

- [ ] Expose `ks.PlacementPolicy(node_preferences, checkpoint_on_preemption, max_preemption_fraction, recovery_timeout)`.
- [ ] Expose `ks.NodePreference.SPOT` and `ks.NodePreference.ON_DEMAND`.
- [ ] Wire `PlacementPolicy` into `session.submit_job(pipeline, placement=...)`.
- [ ] Add `.pyi` stub entries.

**Validation**: `cargo test -p krishiv-python`

## Sprint 4 — Bare-Metal HA (etcd)

### S4.1: krishiv-etcd crate — new crate

- [ ] Create `crates/krishiv-etcd/Cargo.toml` with dependencies: `etcd-client = "0.13"`, `tokio`, `tracing`, `serde_json`.
- [ ] Implement `EtcdLeaderElection: LeaderElection` (the async trait from R12's ADR-12.2 redesign) using etcd leases and `PUT` with TTL for leader election.
- [ ] Implement `try_acquire` via an etcd `txn` (compare-and-swap on the election key).
- [ ] Implement `renew` via etcd `lease_keep_alive`.
- [ ] Implement `release` via etcd lease revocation.
- [ ] Add unit tests with an etcd test container (gated behind `#[cfg(feature = "etcd-integration")]`).

**Validation**: `cargo test -p krishiv-etcd`

### S4.2: EtcdMetadataStore — krishiv-etcd

- [ ] Implement `EtcdMetadataStore` for storing coordinator metadata (job state, epoch numbers, checkpoint manifest) as etcd key-value pairs under a configurable prefix.
- [ ] Implement `watch_changes(prefix) -> impl Stream<Item = MetadataEvent>` using etcd watch API so that a standby coordinator can react to leader changes.
- [ ] Add a test that writes metadata, starts a watch, updates the metadata, and asserts the watch stream receives the event.

**Validation**: `cargo test -p krishiv-etcd`

### S4.3: HA mode in krishiv-coordinator — krishiv-scheduler

- [ ] Add `--ha-mode etcd` and `--etcd-endpoints <url,...>` CLI flags to the coordinator binary.
- [ ] When `--ha-mode etcd` is set, replace the default in-process `LocalLeaderElection` with `EtcdLeaderElection` from `krishiv-etcd`.
- [ ] Replace the in-memory `JobStore` with `EtcdMetadataStore` so that job state survives coordinator process restart.
- [ ] Add a failover test: start two coordinator processes with the same etcd cluster, kill the leader, assert the standby wins the election and resumes job scheduling within 10 seconds.

**Validation**: `cargo test -p krishiv-scheduler`

### S4.4: Bare-metal HA documentation

- [ ] Add a subsection to `docs/deployment-targets.md` documenting the `--ha-mode etcd` configuration.
- [ ] Document the recommended etcd cluster size (3 nodes) and `terminationGracePeriodSeconds` for coordinators.
- [ ] Document the Postgres-backed `JobCatalogStore` as the global coordinator's metadata store (complement to etcd for bare-metal deployments that do not use K8s).

**Validation**: manual review; no compilation gate.

## Sprint 5 — Cost-Aware Placement & Serverless Mode

### S5.1: Cloud pricing client — krishiv-scheduler

- [ ] Implement `CloudPricingClient` trait with `async fn instance_hourly_cost(instance_type: &str, cloud: CloudProvider, region: &str) -> PricingResult<f64>`.
- [ ] Implement `AwsPricingClient` using the AWS Pricing API (`aws-sdk-pricing` crate) with a 1-hour TTL cache.
- [ ] Implement `GcpPricingClient` and `AzurePricingClient` as stubs returning configurable static prices (full API integration deferred; static prices cover 90% of R19 use cases).
- [ ] Add a unit test with a mock pricing client asserting the cheapest region is selected from a three-region comparison.

**Validation**: `cargo test -p krishiv-scheduler`

### S5.2: Cost-aware job placement — krishiv-scheduler

- [ ] Extend `TaskPlacementEngine` to accept `BudgetConstraint` and `OptimizationGoal`.
- [ ] For `OptimizationGoal::Cost`: query `CloudPricingClient` for all available executor instance types in the target region; select the cheapest that meets the job's CPU/memory requirement; cap total hourly cost at `max_hourly_cost_usd`.
- [ ] For `OptimizationGoal::Latency`: prefer regions with the lowest `LatencyProber` EMA (reusing Sprint 1 infrastructure).
- [ ] For `OptimizationGoal::Throughput`: prefer regions with the most available executor capacity.
- [ ] Add unit tests for each optimization goal using a mock pricing client and mock latency prober.

**Validation**: `cargo test -p krishiv-scheduler`

### S5.3: Global job routing — dedicated federation module/crate

- [ ] Implement `JobRoutingPolicy` with `batch_jobs: JobRoutingStrategy` and `streaming_jobs: JobRoutingStrategy`.
- [ ] `JobRoutingStrategy::CheapestRegion` calls `CloudPricingClient` to find the cheapest eligible region for the job's resource profile.
- [ ] `JobRoutingStrategy::LowestLatency` uses `LatencyProber` EMA.
- [ ] `JobRoutingStrategy::DataLocality` inspects the job's input table locations (S3 bucket region tags, Iceberg catalog metadata) and prefers the region where the data resides.
- [ ] Add tests for each routing strategy asserting the correct region is selected given mocked pricing and latency data.

**Validation:** `cargo test -p <future-federation-crate-or-scheduler-module>`

### S5.4: Serverless execution mode — krishiv-api, krishiv (Python bindings)

- [ ] Implement `ServerlessRuntime::AwsLambda` and `ServerlessRuntime::GoogleCloudRun` variants in `krishiv-api`.
- [ ] In `ks.Session.serverless(runtime="aws_lambda"|"google_cloud_run")`, build a coordinator session that runs in-process (no external coordinator) and uses `spawn_blocking` for all I/O.
- [ ] Add a duration guard: if the estimated job duration (from query planning statistics) exceeds 840 seconds (14 minutes), return `ServerlessError::JobTooLong` with a message explaining the Lambda limit.
- [ ] Add `.pyi` stub entries.
- [ ] Add a unit test that submits a mock 5-minute job in serverless mode and asserts it completes, and a 20-minute job that returns `ServerlessError::JobTooLong`.

**Validation**: `cargo test -p krishiv-api && cargo test -p krishiv-python`

### S5.5: Python cost and routing API

- [ ] Expose `ks.BudgetConstraint(max_hourly_cost_usd, cloud_provider, region)`.
- [ ] Expose `ks.OptimizationGoal.COST`, `.LATENCY`, `.THROUGHPUT`.
- [ ] Expose `ks.JobRoutingPolicy(batch_jobs, streaming_jobs, data_locality)`.
- [ ] Wire all three into `session.submit_job(pipeline, placement=..., routing=...)`.
- [ ] Add `.pyi` stub entries.

**Validation**: `cargo test -p krishiv-python`

## Acceptance Gate

R19 is complete when:

- [ ] Multi-region failover test: start two coordinator regions (two local processes with distinct ports); kill the active region's coordinator; verify the standby region takes over within `rto_seconds` (configured to 60s in the test).
- [ ] KEDA external scaler: a `GetMetrics` gRPC call to `KedaExternalScalerService` returns a non-zero `currentMetricValue` for a running streaming job with backlog.
- [ ] KEDA autoscaling integration: submit a job with `AutoscalePolicy`; assert that a `ScaledObject` CRD is created in the test Kubernetes cluster with the correct `scaleTargetRef` and trigger configuration.
- [ ] Spot recovery test: send `SIGTERM` to a mock executor mid-window; verify the incremental checkpoint trigger is called; verify the job resumes on a new executor without data loss (confirmed by comparing output row counts before and after eviction).
- [ ] Bare-metal HA test: start two coordinator processes pointing at the same etcd test container; kill the leader; assert the standby wins election and resumes job scheduling within 10 seconds.
- [ ] Cost-aware placement test: submit identical jobs with `OptimizationGoal.COST` and `OptimizationGoal.LATENCY`; assert different executor instance types are selected by the placement engine (verified by inspecting `TaskPlacementEngine` output, not requiring a real cloud API).
- [ ] Serverless mode: a Python script calling `ks.Session.serverless(runtime="aws_lambda")` and running a `SELECT count(*) FROM parquet.\`s3://...\`` query completes without a running coordinator process.
- [ ] `cargo test --workspace` passes with zero failures.
- [ ] `cargo clippy --workspace -- -D warnings` passes.
- [ ] The ADR-19.1 decision is recorded as DECIDED in `docs/architecture/architectural-decisions-r12-r20.md` before Sprint 2 begins.
