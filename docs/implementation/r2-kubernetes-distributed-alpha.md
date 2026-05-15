# R2 Kubernetes Distributed Alpha Implementation Tracker

## Goal

Deliver the first Kubernetes-capable distributed Krishiv runtime with a single active coordinator, replaceable executors, static task scheduling, basic job/task status, and distributed DAG submission for batch and early streaming jobs.

R2 proves that Krishiv's R1 local execution model can be lifted into a distributed control plane without changing the public API semantics.

## Scope

In scope:

- Coordinator service skeleton.
- Executor service skeleton.
- Control-plane RPC contracts.
- Static task placement.
- Heartbeats and task lifecycle.
- Stage-level retry.
- `KrishivJob` CRD.
- Basic Kubernetes manifests.
- Basic Web UI or status endpoint.
- Distributed batch DAG submission.
- Distributed streaming DAG submission with R1-level stream semantics.

Out of scope:

- HA coordinators.
- Per-job leader election.
- Dynamic autoscaling.
- Durable shuffle service.
- Checkpoints/savepoints.
- Exactly-once guarantees.
- Resource queues and quotas.

## Dependencies

- R1 workspace, API, plan, runtime, and CLI crates exist.
- R1 embedded/single-node behavior is validated.
- R1 logical and physical plan wrappers can be serialized or converted into distributed tasks.

## Architecture Deliverables

- [ ] Add `crates/krishiv-scheduler`.
- [ ] Add `crates/krishiv-proto`.
- [ ] Define coordinator service lifecycle.
- [ ] Define executor service lifecycle.
- [ ] Define task, stage, executor, and job identifiers.
- [ ] Define distributed job state model.
- [ ] Define distributed task state model.
- [ ] Keep one active coordinator for R2.
- [ ] Document R2 control-plane limitations.

## API And Interface Deliverables

- [ ] Add `krishiv submit` CLI skeleton.
- [ ] Add distributed job status output to `krishiv jobs`.
- [ ] Define `KrishivJob` CRD.
- [ ] Add minimal Kubernetes manifests under `k8s/`.
- [ ] Define coordinator/executor RPC messages in `krishiv-proto`.
- [ ] Add a status endpoint or basic Web UI for jobs, stages, tasks, and executors.

## Runtime Deliverables

- [ ] Implement executor registration.
- [ ] Implement executor heartbeat.
- [ ] Implement static task placement.
- [ ] Implement task launch.
- [ ] Implement task completion reporting.
- [ ] Implement task failure reporting.
- [ ] Implement stage-level retry.
- [ ] Route distributed batch DAG execution through the scheduler.
- [ ] Route distributed streaming DAG execution through the scheduler with local-only state semantics.
- [ ] Preserve R1 embedded/single-node behavior.

## Test Checklist

- [ ] Coordinator unit tests pass.
- [ ] Executor unit tests pass.
- [ ] Task lifecycle tests pass.
- [ ] Heartbeat timeout tests pass.
- [ ] Stage retry tests pass.
- [ ] `KrishivJob` manifest validation tests pass.
- [ ] Kubernetes `kind` smoke test submits one batch job.
- [ ] Kubernetes `kind` smoke test submits one early streaming job.

## Acceptance Gate

R2 is complete when:

- [ ] A simple distributed batch job can be submitted on Kubernetes.
- [ ] A simple distributed streaming job can be submitted on Kubernetes.
- [ ] Job, stage, task, and executor status are visible through CLI or Web UI.
- [ ] Failed tasks are retried at stage level.
- [ ] Embedded and single-node R1 tests still pass unchanged.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Scheduler instability | Use static placement and one active coordinator only |
| Distributed runtime changes local semantics | Keep R1 parity tests in the R2 validation suite |
| Kubernetes API scope grows too early | Start with `KrishivJob` only; defer cluster and queue CRDs |
| Task retry causes duplicate side effects | Keep R2 sinks limited to non-exactly-once semantics and document limitations |
| Control-plane messages churn | Keep `krishiv-proto` small and version internal messages conservatively |
