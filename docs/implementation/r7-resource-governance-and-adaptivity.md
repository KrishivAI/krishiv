# R7 Resource Governance And Adaptivity Implementation Tracker

## Goal

Deliver multi-tenant production control in two sub-milestones. R7.1 delivers the resource manager, queues, admission control, quotas, and cost metrics — the governance foundation. R7.2 delivers backpressure, adaptivity, and hot-key handling.

Splitting into sub-milestones reduces the risk of R7 stalling R8–R10. R7.1 can ship and be validated independently before R7.2 begins.

## Scope

In scope:

- Resource manager service.
- Job queues and priorities.
- Admission control.
- CPU and memory quotas.
- Namespace isolation.
- Runtime cost metrics.
- Bounded operator queues.
- Credit-based backpressure.
- Source throttling.
- Hot-key detection and splitting.
- Adaptive repartitioning.
- Manual override and explainable decisions.

Out of scope:

- Global multi-region resource pooling.
- GPU quota and scheduling.
- Fine-grained billing integration.
- Automatic cost-based autoscaling across cloud providers.
- Full backpressure re-architecture beyond credit-based flow.

## Dependencies

- R2 coordinator/executor model exists.
- R4 runtime statistics model exists.
- R6 checkpoint semantics do not interfere with throttled jobs.
- Job/stage/task status API can expose queue and admission state.

---

## R7.1: Resource Management Foundation

### Goal

Deliver job queues, priorities, admission control, quotas, namespace isolation, and cost metrics. Prove that the coordinator can enforce resource policy before work is submitted to executors.

### Pre-R7.1 Architectural Decisions (implemented during R6 closeout)

These decisions were made and partially implemented before R7.1 began, to avoid mid-release breakage:

**`QueueManager` trait stabilized in `krishiv-scheduler`.**

The trait is implemented inside `krishiv-scheduler` (same crate as `MetadataStore` and `LeaderElection`) to keep the deployment unit cohesive. The interface:

```rust
pub trait QueueManager: Send + Sync + fmt::Debug {
    fn admit(&self, spec: &JobSpec) -> SubmitOutcome;
}
pub enum SubmitOutcome { Accepted, Queued { position: usize } }
```

`InMemoryQueueManager` (always admits) is the default — wired into `Coordinator::active_with_config` and `standby_with_config`. Replaced via `Coordinator::with_queue_manager(qm)` builder. R7.1 will add `CrdQueueManager` (Kubernetes) and `ConfigFileQueueManager` (process mode) without touching any existing call sites.

**`submit_job` now returns `SchedulerResult<SubmitOutcome>`** (was `SchedulerResult<()>`).

All existing callers that use `?` or `.unwrap()` and discard the value compile unchanged. R7.1 adds quota enforcement by returning `Queued` before placement, with no API changes needed.

**Checkpoint interval timer wired.**

`CheckpointCoordinator::try_tick(elapsed_ms)` and `CoordinatorConfig::tick_period_ms` (default 1 000 ms) added. `Coordinator::advance_heartbeat_clock` now drives per-job checkpoint interval timers automatically. R7 throttling can adjust tick frequency or call `try_tick` directly.

**Per-job resource attribution model** (decision, not yet implemented): `TaskRuntimeStats` in `krishiv-proto` carries `output_rows` and `cpu_nanos` per task. R7.1 quota enforcement will accumulate these into a per-job `ResourceUsage` aggregate stored on `JobRecord`. No schema changes required — the data is already being sent by executors.

### Architecture Deliverables

- [x] Define `QueueManager` trait — `QueueManager` + `InMemoryQueueManager` in `krishiv-scheduler`; `Coordinator::with_queue_manager` builder wired.
- [x] Define `SubmitOutcome` — `Accepted` | `Queued { position }` returned by `submit_job`.
- [ ] Add resource manager service.
- [ ] Define `CrdQueueManager` (Kubernetes mode) — wraps `KrishivQueue` CRD status.
- [ ] Define `ConfigFileQueueManager` (process mode) — reads queue config from a TOML/YAML file.
- [ ] Define `KrishivQueue` CRD (used by Kubernetes mode).
- [ ] Define queue and priority model.
- [ ] Define admission control policy model.
- [ ] Define CPU and memory quota model — accumulate `TaskRuntimeStats` into per-job `ResourceUsage` on `JobRecord`.
- [ ] Define namespace isolation model.
- [ ] Define cost metric model.
- [ ] Document resource manager API and operator guide.

### API And Interface Deliverables

- [ ] Add job queue configuration.
- [ ] Add job priority field to `JobSpec`.
- [ ] Add admission control configuration.
- [ ] Add quota configuration.
- [ ] Add namespace isolation configuration.
- [ ] Add cost metrics to the status API and Web UI.

### Runtime Deliverables

- [ ] Implement resource manager service.
- [ ] Implement job queues.
- [ ] Implement job priorities.
- [ ] Implement admission control via `QueueManager` (replace `InMemoryQueueManager` with quota-aware implementation).
- [ ] Implement CPU and memory quota enforcement.
- [ ] Implement namespace isolation enforcement.
- [ ] Add runtime cost metrics.
- [ ] Add quota/admission tests.

### Acceptance Gate For R7.1

- [ ] Jobs above quota are rejected or queued.
- [ ] Admission control rejects jobs when resources are unavailable.
- [ ] Cost metrics are visible per job in the status API.
- [ ] Queue and priority ordering is visible through the CLI and Web UI.

---

## R7.2: Backpressure And Adaptivity

### Goal

Deliver credit-based backpressure, bounded operator queues, source throttling, hot-key detection and splitting, and adaptive repartitioning. R7.2 begins after R7.1 acceptance gate passes.

### Architecture Deliverables

- [ ] Define bounded operator queue model.
- [ ] Define credit-based flow control protocol.
- [ ] Define source throttling hooks.
- [ ] Define slow-sink detection model.
- [ ] Define hot-key detection and splitting model.
- [ ] Define adaptive repartitioning model.
- [ ] Define manual override and explainable-decision log model.

### API And Interface Deliverables

- [ ] Add operator queue configuration.
- [ ] Add backpressure visibility to the status API.
- [ ] Add source throttling configuration.
- [ ] Add hot-key detection output.
- [ ] Add manual override for adaptive decisions.
- [ ] Add explainable adaptive-decision logs.

### Runtime Deliverables

- [ ] Implement bounded operator queues.
- [ ] Implement credit-based flow control.
- [ ] Implement source throttling.
- [ ] Detect slow sinks.
- [ ] Detect hot keys.
- [ ] Implement hot-key splitting.
- [ ] Implement adaptive repartitioning.
- [ ] Add backpressure stress tests.
- [ ] Add hot-key simulation tests.

### Acceptance Gate For R7.2

- [ ] Overloaded jobs are throttled without destabilizing other jobs.
- [ ] Hot-key tests show load reduction after splitting.
- [ ] Adaptive decisions are visible to operators.
- [ ] Manual override disables adaptive behavior correctly.

---

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| R7.1 or R7.2 independently takes too long | Keep each sub-milestone independently shippable; do not gate R8 on R7.2 if R7.1 is complete |
| Adaptive behavior destabilizes jobs | Conservative defaults; manual override required; explainable decisions logged |
| Quota enforcement breaks existing tests | Run R1–R6 parity tests after every R7.1 change |
| Hot-key splitting causes state redistribution issues | Defer state-aware hot-key splitting to R9; keep R7.2 splitting stateless |
| Backpressure spreads through pipelines | Add credit-based flow before source throttling; measure separately |
| Cost metrics are inaccurate | Validate stats in deterministic tests before using them for admission decisions |
