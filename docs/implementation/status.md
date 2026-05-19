# Krishiv Implementation Status

## Current Phase

**R7 COMPLETE.** R7.1 (resource governance foundation) and R7.2 (backpressure and adaptive governance) both fully implemented on branch `claude/plan-r7-implementation-lt3n3`. Commits: `0618c61` (R7.1), `b5570bb` (R7.2). Zero failures across full workspace (`cargo test --workspace`), zero clippy warnings.

## Active Task

**R7.2 — Backpressure and Adaptive Governance** — all groups complete:

### Group A: OperatorQueue with barrier-bypass (krishiv-exec)
- `OperatorMessage { Data(RecordBatch), Barrier { epoch } }`
- Bounded `data_tx/data_rx` (backpressure) + unbounded `barrier_tx/barrier_rx` (never blocked)
- `recv()` drains `barrier_rx` before each data item — guarantees barrier-before-data ordering
- `OperatorQueueMetrics { len, capacity, pending_barriers }`, `operator_queue(capacity)` factory
- 3 tests: data flows through, barrier arrives before queued data, metrics reflect capacity

### Group D: SpaceSaving hot-key detection (krishiv-exec)
- `HeavyHittersTracker` — fixed K-counter SpaceSaving algorithm (Metwally et al. 2005)
- O(K) memory regardless of key cardinality; guaranteed tracking for keys > 1/K fraction
- Eviction rule: replace min-count entry, new count = min_count + 1 (guaranteed overestimate)
- `HotKeyReport { key, estimated_count, max_error, heat_score }`, `hot_keys(threshold)`, `reset()`
- 5 tests: single key, eviction replaces min, heat score calculation, hot-key filter, reset

### Group C: Source throttling (krishiv-exec)
- `ThrottleCommand { source_id, rows_per_second: Option<u64> }` (None = clear throttle)
- `RateLimiter` — token-bucket with `try_consume(n, now_ms) -> Option<wait_ms>`
- Tokens replenish at `rows_per_second` per second; initial bucket is full
- 4 tests: initially full, depleted returns wait, refills over time, set_rate clamps tokens

### Group G: Slow-sink detection (krishiv-exec)
- `SinkLatencyTracker` — `record_write(ms)`, `avg_latency_ms()`, `max_latency_ms()`, `is_slow(threshold_ms)`
- 3 tests: avg zero when empty, records avg and max, is_slow detection

### Group F/H: Adaptive decision log (krishiv-scheduler)
- `AdaptiveDecisionKind { HotKeySplit, Repartition, SourceThrottle, SlowSinkDetected }`
- `AdaptiveDecisionLog { timestamp_ms, kind, affected_job_id, details, applied }`
- `AdaptiveOverrideConfig { disable_hot_key_splitting, disable_adaptive_repartition, disable_source_throttling }`
- `ThrottleDecision { source_id, rows_per_second }` returned from `executor_heartbeat()`
- `Coordinator.adaptive_decision_log(&job_id)` — accessor for per-job decision history
- `Coordinator.with_adaptive_override(cfg)` — builder
- `process_hot_key_reports()` — appends `AdaptiveDecisionLog` entries, respects override flag
- 5 new tests: empty log, hot-key reports appended, suppressed by override, multiple reports, override defaults

### Control-plane wiring (krishiv-proto)
- `HeartbeatHotKeyReport { key, estimated_count, max_error, heat_score, job_id, source_id }`
- `HeartbeatThrottleCommand { source_id, rows_per_second }`
- `ExecutorHeartbeat.hot_key_reports: Vec<HeartbeatHotKeyReport>` — ingest path (Group D)
- `ExecutorHeartbeatRequest.hot_key_reports: Vec<HeartbeatHotKeyReport>` — wire path
- `ExecutorHeartbeatResponse.throttle_commands: Vec<HeartbeatThrottleCommand>` — egress path (Group C)
- `CoordinatorExecutorTonicService` propagates both fields in heartbeat handler

### Architecture documentation
- `docs/architecture/backpressure-protocol.md` — scope boundary (intra-stage only for R7.2), barrier-bypass rule, credit model, SpaceSaving choice (Risk 4), adaptive repartition scope (batch-only, never mid-stage), manual override config

### Test counts (post-R7.2)
- `krishiv-exec`: 52 tests (19 new R7.2)
- `krishiv-scheduler`: 90 tests (5 new R7.2)
- Full workspace: all test result lines `ok`, 0 failures

## R7.1 — Resource Governance Foundation (complete, commit 0618c61)

### Group A: JobSpec resource fields (krishiv-proto)
- `priority: u8` (default 128), `namespace_id: Option<String>`, `cpu_limit_nanos: Option<u64>`, `memory_limit_bytes: Option<u64>`
- Builders: `with_priority()`, `with_namespace()`, `with_cpu_limit_nanos()`, `with_memory_limit_bytes()`

### Group B: Scheduler quota types (krishiv-scheduler)
- `ResourceUsage { cpu_nanos, memory_peak_bytes, task_count }`
- `NamespaceQuotaSnapshot { namespace_id, cpu_nanos_reserved, memory_bytes_reserved, active_job_count }`
- `QueueManager` trait: `admit(&self, spec, quota: &NamespaceQuotaSnapshot) -> SubmitOutcome`
- `QuotaPolicy`, `QuotaQueueManager`, `ConfigFileQueueManager` (JSON format)
- `Coordinator::namespace_quota_snapshot()`, `submit_job()` calls `admit()`
- `JobRecord.resource_usage` accumulated from `TaskRuntimeStats`
- Two-phase borrow pattern in `apply_task_update()` for lock-safety
- 11 new scheduler tests

### Group C: CrdQueueManager (krishiv-operator)
- `KrishivQueue` CRD type with `KrishivQueueSpec`, `KrishivQueueStatus`
- `CrdQueueManager` implementing `QueueManager` — K8s isolation preserved
- 4 new operator tests

### Group D: KrishivQueue CRD (k8s/crds/krishivqueues.yaml)
- Full CRD definition with openAPIV3Schema, status subresource, additionalPrinterColumns

### Group E: UI resource fields (krishiv-ui)
- `ResourceUsageView`, `NamespaceQuotaView`, `QueuesResponse`
- `GET /api/v1/queues` route
- `JobSummaryView` gained `priority`, `namespace_id`, `resource_usage`

## Next Steps

1. **R8.1**: Multi-tenancy and security — authn/authz, job isolation, role-based admission
2. **R8.2**: Observability — distributed tracing, fine-grained metrics, SLO dashboards
3. **R9**: End-to-end credit propagation across shuffle boundaries (cross-stage flow control)

## Known Blockers

- R2 `kind` smoke validation is deferred because local Podman image build hit a TLS certificate trust issue while pulling the Rust base image.

## Last Validation

- `cargo test --workspace`: 0 failures across all crates (90 scheduler, 52 exec, full workspace green)
- `cargo clippy --workspace -- -D warnings`: 0 warnings, 0 errors
- Branch: `claude/plan-r7-implementation-lt3n3`
- Commits: `0618c61` (R7.1), `b5570bb` (R7.2)

## Architectural Inputs To Preserve

- Distributed mode has two targets: Kubernetes is primary, and bare metal / VM is secondary. Core runtime crates must remain deploy-target neutral; Kubernetes API access belongs in `krishiv-operator`, Kubernetes packaging under `k8s/`, and narrowly scoped CLI paths.
- Control-plane traffic stays on tonic gRPC + Protobuf for registration, heartbeat, task assignment, task status, cancellation, and deregistration.
- Bulk Arrow data must not be added to control-plane Protobuf messages. R4 uses Arrow IPC for shuffle writes and Arrow Flight for shuffle reads/query result transfer.
- R4 shuffle defaults to local executor disk with optional object-store durability. Do not assume S3/object storage is required for distributed execution.
- Pre-R9 coordinator/executor gRPC has no mTLS or application-level auth. Task specs must not contain credentials or secret values; shared Kubernetes deployments require namespace isolation, NetworkPolicy, and component-specific service accounts.
- R7.2 backpressure is intra-stage only. Cross-stage throttling uses the coarser `ThrottleCommand` control-plane signal. Full end-to-end credit propagation across shuffle boundaries is deferred to R9.
- Adaptive repartitioning is batch-only, between stages only (never mid-stage). Streaming hot-key splitting follows the savepoint model (per R6 rescaling model).
- `QueueManager.admit()` is stateless policy — coordinator owns live reservation state via `NamespaceQuotaSnapshot`.
- `CrdQueueManager` lives in `krishiv-operator` (not `krishiv-scheduler`) to preserve the Kubernetes isolation rule — no `kube` crate in `krishiv-scheduler`.
