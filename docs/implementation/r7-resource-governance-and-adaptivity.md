# R7 Resource Governance And Adaptivity Implementation Tracker

## Goal

Deliver multi-tenant production controls and adaptive runtime behavior: resource manager, queues, priorities, quotas, admission control, namespace isolation, cost metrics, backpressure, source throttling, hot-key splitting, and adaptive repartitioning.

R7 makes Krishiv safer to operate with many teams and mixed workloads.

## Scope

In scope:

- Resource manager service.
- `KrishivQueue` CRD.
- Queues and priorities.
- Admission control.
- CPU and memory quotas.
- Namespace isolation model.
- Runtime cost metrics.
- Bounded operator queues.
- Credit-based flow control.
- Source throttling.
- Slow-sink detection.
- Hot-key detection and splitting.
- Adaptive repartitioning.
- Manual override for adaptive behavior.
- Explainable adaptive-decision logs.

Out of scope:

- Full cluster autoscaler integration.
- Cloud-provider cost optimization.
- ML-based scheduling.
- Cross-cell scheduling.
- Hard multi-tenant security enforcement beyond documented isolation model.

## Dependencies

- R2 scheduler and executor model exists.
- R4 runtime stats and partitioning exist.
- R5 stateful streaming exists.
- R6 checkpoint semantics can protect adaptive changes for stateful jobs.
- Kubernetes CRD patterns are established.

## Architecture Deliverables

- [ ] Add resource manager service.
- [ ] Define scheduler/resource-manager boundary.
- [ ] Define queue and priority model.
- [ ] Define admission control model.
- [ ] Define quota model.
- [ ] Define namespace isolation model.
- [ ] Define backpressure signal model.
- [ ] Define adaptive decision model.
- [ ] Document manual override behavior.

## API And Interface Deliverables

- [ ] Define `KrishivQueue` CRD.
- [ ] Add job queue selection.
- [ ] Add job priority selection.
- [ ] Add quota configuration.
- [ ] Add CLI/status visibility for queued jobs.
- [ ] Add CLI/status visibility for throttled jobs.
- [ ] Add adaptive decision logs.
- [ ] Add cost metric output for jobs and operators.

## Runtime Deliverables

- [ ] Implement job queues.
- [ ] Implement job priorities.
- [ ] Implement admission control.
- [ ] Implement CPU quota checks.
- [ ] Implement memory quota checks.
- [ ] Implement namespace isolation hooks.
- [ ] Add runtime cost metrics.
- [ ] Implement bounded operator queues.
- [ ] Implement credit-based flow control.
- [ ] Implement source throttling.
- [ ] Detect slow sinks.
- [ ] Detect hot keys.
- [ ] Implement hot-key splitting.
- [ ] Implement adaptive repartitioning.
- [ ] Add manual override for adaptive behavior.

## Test Checklist

- [ ] Queue ordering tests pass.
- [ ] Priority scheduling tests pass.
- [ ] Admission control tests pass.
- [ ] CPU quota tests pass.
- [ ] Memory quota tests pass.
- [ ] Namespace isolation tests pass.
- [ ] Backpressure stress tests pass.
- [ ] Source throttling tests pass.
- [ ] Slow-sink tests pass.
- [ ] Hot-key tests pass.
- [ ] Adaptive repartition tests pass.
- [ ] Cost metric validation tests pass.

## Acceptance Gate

R7 is complete when:

- [ ] Overloaded jobs are throttled without destabilizing other jobs.
- [ ] Jobs above quota are rejected or queued.
- [ ] Hot-key tests show load reduction after splitting.
- [ ] Adaptive decisions are visible to operators.
- [ ] Manual override can disable adaptive behavior for a job.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Adaptive behavior destabilizes jobs | Use conservative defaults and manual override |
| Quotas reject valid workloads | Add explainable admission decisions and queue visibility |
| Backpressure causes deadlocks | Use bounded queues and targeted stress tests |
| Hot-key splitting breaks state semantics | Protect stateful repartition changes with checkpoint-aware transitions |
| Cost metrics are misleading | Label early metrics as runtime estimates and validate against deterministic tests |
