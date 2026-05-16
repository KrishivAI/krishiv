# R9 Governance And Operations Implementation Tracker

## Goal

Deliver enterprise operations: OpenTelemetry metrics/traces/logs, OpenLineage-compatible events, audit logs, RBAC/TLS, policy hooks, row/column masking hooks, HA coordinators, per-job leader election, leases, fencing tokens, replay bundles, plan diffing, and Helm packaging.

R9 hardens Krishiv for production operation and failover-sensitive control-plane behavior.

## Scope

In scope:

- OpenTelemetry metrics, traces, and structured logs.
- OpenLineage-compatible job/run/dataset events.
- Audit logging.
- RBAC integration.
- TLS configuration.
- Policy hooks.
- Row and column masking hooks.
- HA coordinator deployment.
- Per-job leader election.
- Durable leases.
- Fencing tokens.
- Replay bundles.
- Plan diffing.
- Helm chart.

Out of scope:

- Full policy engine implementation.
- Complete data governance product.
- Cross-region active-active jobs.
- Managed service control plane.
- Fine-grained billing system.

## Dependencies

- R2 coordinator/executor model exists.
- R3 task attempts, executor leases, coordinator restart recovery, and durable job event log exist.
- R6 checkpoint ownership and epoch metadata exist.
- R6 checkpoint/savepoint metadata is versioned and recovery-tested.
- R7 resource manager and queues exist.
- R8 client surfaces exist for Python and Flight SQL.
- Runtime services emit enough metadata for observability.

## Architecture Deliverables

- [ ] Add `crates/krishiv-metrics`.
- [ ] Add `crates/krishiv-governance`.
- [ ] Define telemetry signal model.
- [ ] Define lineage event model.
- [ ] Define audit event model.
- [ ] Define policy hook interface.
- [ ] Define row/column masking hook boundaries.
- [ ] Define HA coordinator deployment model.
- [ ] Define per-job leader election model.
- [ ] Define durable lease model building on R3 executor leases and R6 checkpoint ownership.
- [ ] Define fencing token model.
- [ ] Define stale-coordinator rejection path for every failover-sensitive write.
- [ ] Define replay bundle contents.
- [ ] Define Helm chart structure.

## API And Interface Deliverables

- [ ] Add metrics configuration.
- [ ] Add tracing configuration.
- [ ] Add structured log configuration.
- [ ] Add lineage emission configuration.
- [ ] Add audit log configuration.
- [ ] Add RBAC configuration.
- [ ] Add TLS configuration.
- [ ] Add policy hook registration interface.
- [ ] Add plan diff CLI or API.
- [ ] Add replay bundle generation CLI.
- [ ] Add Helm values file.

## Runtime Deliverables

- [ ] Emit OpenTelemetry metrics.
- [ ] Emit OpenTelemetry traces.
- [ ] Emit structured logs.
- [ ] Emit OpenLineage-compatible job events.
- [ ] Emit OpenLineage-compatible run events.
- [ ] Emit OpenLineage-compatible dataset events.
- [ ] Emit audit logs for query execution.
- [ ] Emit audit logs for job submit/cancel.
- [ ] Emit audit logs for savepoint/restore.
- [ ] Emit audit logs for admin actions.
- [ ] Implement RBAC integration.
- [ ] Implement TLS configuration.
- [ ] Implement policy hooks.
- [ ] Implement row masking hooks.
- [ ] Implement column masking hooks.
- [ ] Implement HA coordinator deployment.
- [ ] Implement per-job leader election.
- [ ] Implement durable leases.
- [ ] Implement fencing tokens.
- [ ] Implement stale-coordinator rejection for checkpoint, savepoint, task assignment, and sink commit ownership.
- [ ] Implement replay bundle generation.
- [ ] Implement plan diffing.
- [ ] Add Helm chart.

## Test Checklist

- [ ] Metrics tests pass.
- [ ] Trace propagation tests pass.
- [ ] Structured log tests pass.
- [ ] OpenLineage event validation tests pass.
- [ ] Audit event tests pass.
- [ ] RBAC tests pass.
- [ ] TLS configuration tests pass.
- [ ] Policy hook tests pass.
- [ ] Masking hook tests pass.
- [ ] Kubernetes `kind` e2e tests pass.
- [ ] Per-job leader failover tests pass.
- [ ] Stale coordinator write rejection tests pass.
- [ ] Fencing-token tests pass.
- [ ] Replay bundle tests pass.
- [ ] Plan diff tests pass.
- [ ] Helm template tests pass.

## Acceptance Gate

R9 is complete when:

- [ ] Coordinator failover does not allow duplicate checkpoint ownership.
- [ ] Fencing tokens prevent stale coordinators from committing.
- [ ] R3 task-attempt and executor-lease invariants still hold after per-job coordinator failover.
- [ ] R6 checkpoint/savepoint recovery invariants still hold after per-job coordinator failover.
- [ ] OpenTelemetry signals are emitted for supported jobs.
- [ ] Audit and lineage events are emitted for supported actions.
- [ ] Helm chart can deploy the supported R9 cluster shape.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Control-plane correctness fails under failover | Use leases, fencing tokens, and durable ownership metadata |
| HA introduces new recovery semantics too late | Build on R3 task attempts/executor leases/event log and R6 versioned checkpoint metadata |
| Observability is bolted on inconsistently | Make metrics/traces/logs mandatory for runtime services |
| Policy hooks become a full governance product | Keep R9 hooks pluggable and defer policy engine depth |
| Audit logs miss sensitive actions | Define audit event taxonomy before implementation |
| Helm chart diverges from manifests | Generate or test both paths through the same expected resources |
