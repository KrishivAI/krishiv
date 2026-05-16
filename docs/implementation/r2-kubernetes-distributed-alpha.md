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

- [x] Add `crates/krishiv-scheduler`.
- [x] Add `crates/krishiv-proto`.
- [x] Define coordinator service lifecycle.
- [x] Define executor service lifecycle.
- [x] Define task, stage, executor, and job identifiers.
- [x] Define distributed job state model.
- [x] Define distributed task state model.
- [x] Keep one active coordinator for R2.
- [x] Document R2 control-plane limitations.

## API And Interface Deliverables

- [x] Add `krishiv submit` CLI skeleton.
- [x] Add distributed job status output to `krishiv jobs`.
- [x] Define `KrishivJob` CRD.
- [x] Add minimal Kubernetes manifests under `k8s/`.
- [x] Define coordinator/executor RPC messages in `krishiv-proto`.
- [x] Add a status endpoint or basic Web UI for jobs, stages, tasks, and executors.

## Runtime Deliverables

- [x] Implement executor registration.
- [x] Implement executor heartbeat.
- [x] Implement static task placement.
- [x] Implement task launch.
- [x] Implement task completion reporting.
- [x] Implement task failure reporting.
- [x] Implement stage-level retry.
- [x] Route distributed batch DAG execution through the scheduler.
- [x] Route distributed streaming DAG execution through the scheduler with local-only state semantics.
- [x] Preserve R1 embedded/single-node behavior.

## Test Checklist

- [x] Coordinator unit tests pass.
- [x] Executor unit tests pass.
- [x] Task lifecycle tests pass.
- [x] Lost-executor marking tests pass.
- [x] CLI submit/status tests pass.
- [x] `KrishivJob` manifest validation tests pass.
- [x] Heartbeat timeout tests pass.
- [x] Stage retry tests pass.
- [ ] Kubernetes `kind` smoke test submits one batch job.
- [ ] Kubernetes `kind` smoke test submits one early streaming job.

## Acceptance Gate

R2 is complete when:

- [ ] A simple distributed batch job can be submitted on Kubernetes.
- [ ] A simple distributed streaming job can be submitted on Kubernetes.
- [x] Job, stage, task, and executor status are visible through CLI or Web UI.
- [x] Failed tasks are retried at stage level.
- [x] Embedded and single-node R1 tests still pass unchanged.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Scheduler instability | Use static placement and one active coordinator only |
| Distributed runtime changes local semantics | Keep R1 parity tests in the R2 validation suite |
| Kubernetes API scope grows too early | Start with `KrishivJob` only; defer cluster and queue CRDs |
| Task retry causes duplicate side effects | Keep R2 sinks limited to non-exactly-once semantics and document limitations |
| Control-plane messages churn | Keep `krishiv-proto` small and version internal messages conservatively |
