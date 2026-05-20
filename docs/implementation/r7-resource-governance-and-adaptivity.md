# R7 Resource Governance And Adaptivity Implementation Tracker

## Goal

Deliver multi-tenant production control in two sub-milestones. R7.1 delivers the resource manager, queues, admission control, quotas, and cost metrics — the governance foundation. R7.2 delivers backpressure, adaptivity, and hot-key handling.

## Status: COMPLETE

Both R7.1 and R7.2 are fully implemented on branch `claude/plan-r7-implementation-lt3n3`.
- R7.1 commit: `0618c61`
- R7.2 commit: `b5570bb`
- All tests pass (`cargo test --workspace`, zero failures)
- Clippy clean (`cargo clippy --workspace -- -D warnings`, zero warnings)

---

## R7.1: Resource Management Foundation — COMPLETE

### Architecture Decisions

**Decision 1 (QueueManager design)**: Option C — stateless policy. `QueueManager.admit()` receives a `NamespaceQuotaSnapshot` (live reservation totals computed by coordinator) and returns `SubmitOutcome`. Coordinator owns reservation state; `QueueManager` is pure policy. No interior mutability needed.

**Decision 2 (Kubernetes isolation)**: `CrdQueueManager` lives in `krishiv-operator` (not `krishiv-scheduler`) so no `kube` crate enters the core scheduler. The `QueueManager` trait stays in `krishiv-scheduler` and is injected via `with_queue_manager()`.

**Decision 3 (ConfigFileQueueManager format)**: JSON format, no new crate dependency. Already has `serde_json` available.

**Decision 4 (Reservation model)**: Reservation-based admission from `JobSpec` fields at submission time, not post-hoc from `TaskRuntimeStats`. Avoids race conditions between admission and execution.

### Architecture Deliverables

- [x] `QueueManager` trait — `admit(&self, spec, quota: &NamespaceQuotaSnapshot) -> SubmitOutcome`
- [x] `SubmitOutcome { Accepted, Queued { position } }` returned by `submit_job`
- [x] `ResourceUsage { cpu_nanos, memory_peak_bytes, task_count }` accumulated from `TaskRuntimeStats`
- [x] `NamespaceQuotaSnapshot` computed by coordinator from active job reservations
- [x] `QuotaPolicy`, `QuotaQueueManager` with per-namespace policies
- [x] `ConfigFileQueueManager` reading JSON quota config (no new crate dependency)
- [x] `CrdQueueManager` in `krishiv-operator` (Kubernetes isolation rule enforced)
- [x] `KrishivQueue` CRD with openAPIV3Schema, status subresource, additionalPrinterColumns
- [x] `k8s/crds/krishivqueues.yaml` added, referenced from `k8s/manifests/kustomization.yaml`

### API And Interface Deliverables

- [x] `priority: u8` (default 128) added to `JobSpec` in `krishiv-proto`
- [x] `namespace_id: Option<String>` added to `JobSpec`
- [x] `cpu_limit_nanos: Option<u64>` added to `JobSpec`
- [x] `memory_limit_bytes: Option<u64>` added to `JobSpec`
- [x] Builders: `with_priority()`, `with_namespace()`, `with_cpu_limit_nanos()`, `with_memory_limit_bytes()`
- [x] `ResourceUsageView`, `NamespaceQuotaView`, `QueuesResponse` in `krishiv-ui`
- [x] `GET /api/v1/queues` route added
- [x] `JobSummaryView` extended with `priority`, `namespace_id`, `resource_usage`

### Runtime Deliverables

- [x] `Coordinator::namespace_quota_snapshot(namespace_id)` walks active non-terminal jobs summing reservations
- [x] `Coordinator::submit_job()` calls `queue_manager.admit(&spec, &quota)` before placement
- [x] `apply_task_update()` accumulates `cpu_nanos` and `memory_bytes` from `TaskRuntimeStats` into `JobRecord.resource_usage`
- [x] Two-phase borrow pattern in `apply_task_update()` to avoid single-lock contention (Risk 2 mitigation)
- [x] `QueueManager.on_job_complete()` called after job reaches terminal state
- [x] `PersistedJobRecord`/`PersistedJobSpec` use `#[serde(default)]` for backward compatibility

### Tests

- 11 new scheduler tests: quota limits, namespace policies, ConfigFile loading, namespace_quota_snapshot accumulation, coordinator queuing behavior, ResourceUsage accumulation, JobSpec round-trip
- 4 new operator tests: CrdQueueManager behavior
- 2 new UI tests: queues endpoint

### Acceptance Gate

- [x] Jobs above CPU quota return `Queued`
- [x] Jobs above memory quota return `Queued`
- [x] Jobs above concurrent job limit return `Queued`
- [x] Namespace-specific policies override default policy
- [x] `ConfigFileQueueManager` loads policies from JSON
- [x] `namespace_quota_snapshot` correctly accumulates active reservations
- [x] Cost metrics visible per job in status API (`ResourceUsageView`)
- [x] Queue state visible through `GET /api/v1/queues`

---

## R7.2: Backpressure And Adaptivity — COMPLETE

### Architecture Decisions

**Decision A (Backpressure scope)**: Intra-stage only for R7.2. Cross-stage throttling uses the coarser `ThrottleCommand` control-plane signal. Full end-to-end credit propagation across shuffle boundaries deferred to R9.

**Decision B (Barrier-bypass, Risk 3 mitigation)**: Barriers travel on an unbounded channel that bypasses credit-gating. `OperatorQueueSender` has two channels: bounded `data_tx` (backpressure) and unbounded `barrier_tx` (never blocked). `recv()` drains `barrier_rx` before each data item.

**Decision C (SpaceSaving algorithm, Risk 4 mitigation)**: Plain HashMap forbidden for frequency tracking (unbounded memory). `HeavyHittersTracker` uses SpaceSaving (Metwally et al. 2005) with fixed K counters. O(K) memory guaranteed.

**Decision D (Adaptive repartitioning scope)**: Batch jobs only; between stages only (never mid-stage). Streaming hot-key splitting uses savepoint model (per R6 rescaling model).

**Decision E (QueueManager stateless, Risk 2 mitigation)**: Already enforced in R7.1. `executor_heartbeat()` now returns `Vec<ThrottleDecision>` (currently empty; coordinator hot-key processing plugs in).

### Architecture Deliverables

- [x] `OperatorMessage { Data(RecordBatch), Barrier { epoch } }` in `krishiv-exec`
- [x] `OperatorQueueSender` (bounded data + unbounded barrier channels)
- [x] `OperatorQueueReceiver.recv()` — drains `barrier_rx` before each data item
- [x] `OperatorQueueMetrics`, `operator_queue(capacity)` factory
- [x] `HeavyHittersTracker` (SpaceSaving top-K, O(K) memory)
- [x] `HotKeyReport { key, estimated_count, max_error, heat_score }`
- [x] `ThrottleCommand { source_id, rows_per_second: Option<u64> }`
- [x] `RateLimiter` — token-bucket `try_consume(n, now_ms) -> Option<wait_ms>`
- [x] `SinkLatencyTracker { record_write, avg_latency_ms, max_latency_ms, is_slow }`
- [x] `AdaptiveDecisionKind { HotKeySplit, Repartition, SourceThrottle, SlowSinkDetected }`
- [x] `AdaptiveDecisionLog { timestamp_ms, kind, affected_job_id, details, applied }`
- [x] `AdaptiveOverrideConfig { disable_hot_key_splitting, disable_adaptive_repartition, disable_source_throttling }`
- [x] `ThrottleDecision` returned from `Coordinator::executor_heartbeat()`
- [x] `docs/architecture/backpressure-protocol.md` documenting all scope and interaction rules

### API And Interface Deliverables

- [x] `HeartbeatHotKeyReport` in `krishiv-proto` (executor → coordinator)
- [x] `HeartbeatThrottleCommand` in `krishiv-proto` (coordinator → executor)
- [x] `ExecutorHeartbeat.hot_key_reports: Vec<HeartbeatHotKeyReport>` (ingest)
- [x] `ExecutorHeartbeatRequest.hot_key_reports` (wire level)
- [x] `ExecutorHeartbeatResponse.throttle_commands` (wire level)
- [x] `CoordinatorExecutorTonicService` propagates both fields
- [x] `Coordinator.with_adaptive_override(cfg)` builder
- [x] `Coordinator.adaptive_decision_log(&job_id)` accessor

### Runtime Deliverables

- [x] Bounded `OperatorQueue` with barrier-bypass (intra-stage backpressure)
- [x] `HeavyHittersTracker` — SpaceSaving hot-key detection, O(K) memory
- [x] `RateLimiter` — token-bucket source throttling
- [x] `SinkLatencyTracker` — slow-sink detection
- [x] `process_hot_key_reports()` — coordinator records adaptive decisions per heartbeat
- [x] Override flag `disable_hot_key_splitting` suppresses decision (logged with `applied=false`)

### Tests

- 19 new exec tests (operator queue barrier bypass, SpaceSaving eviction, rate limiter, sink latency, adaptive decision types)
- 5 new scheduler tests (decision log empty, hot-key reports appended, override suppresses, multiple reports, override defaults)

### Acceptance Gate

- [x] Barrier arrives before queued data in `OperatorQueue` (barrier-bypass rule verified)
- [x] `HeavyHittersTracker` evicts min-count entry and applies SpaceSaving overestimate
- [x] `RateLimiter` returns wait time when bucket depleted; refills proportionally to elapsed time
- [x] `SinkLatencyTracker.is_slow()` correctly classifies slow vs fast sinks
- [x] Hot-key reports from executor heartbeat are appended to `adaptive_decision_log`
- [x] `disable_hot_key_splitting` override causes `applied=false` in log
- [x] All R7.1 tests continue to pass (regression-clean)

---

## Risks And Mitigations

| Risk | Status |
|---|---|
| R7.1 or R7.2 independently takes too long | Resolved: both complete |
| Adaptive behavior destabilizes jobs | Mitigated: conservative defaults; `AdaptiveOverrideConfig` allows full disable; all decisions logged |
| Quota enforcement breaks existing tests | Resolved: all existing tests continue to pass |
| Hot-key splitting causes state redistribution issues | Mitigated: R7.2 hot-key splitting is stateless; state-aware splitting deferred to R9 |
| Backpressure spreads through pipelines | Mitigated: intra-stage only for R7.2; cross-stage via coarser `ThrottleCommand` |
| Cost metrics are inaccurate | Mitigated: `ResourceUsage` accumulated from actual `TaskRuntimeStats`; deterministic tests validate accumulation |
| SpaceSaving eviction uses unbounded HashMap (Risk 4) | Resolved: fixed K-counter structure, O(K) memory guaranteed |
| Barrier/backpressure deadlock (Risk 3) | Resolved: barriers on unbounded channel, never subject to credit-gating |
| Single-lock contention in `apply_task_update` (Risk 2) | Resolved: two-phase borrow pattern extracts owned values before calling `on_job_complete` |
