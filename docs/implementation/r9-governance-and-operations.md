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

- [x] Add `crates/krishiv-metrics`. (OTel init, tracing bridge, stdout exporter — commit R9.1-A)
- [x] Add `crates/krishiv-governance`. (RBAC, audit log, OpenLineage, policy hooks — commit R9.1-B)
- [x] Define telemetry signal model. (`tracing` facade + OTel bridge; OTLP HTTP/proto for production)
- [x] Define lineage event model. (`RunEvent`, `RunRef`, `JobRef`, `LineageDataset` — OpenLineage spec)
- [x] Define audit event model. (`AuditAction` enum; `tracing` target `"krishiv::audit"`)
- [x] Define policy hook interface. (`PolicyHook` trait: `check_table_access`, `column_masking_rule`)
- [x] Define row/column masking hook boundaries. (`MaskingRule`: Redact, Nullify, Hash)
- [x] Define HA coordinator deployment model. (K8s Lease-backed `K8sLeaseElection` in operator)
- [x] Define per-job leader election model. (extended `LeaderElection` trait in scheduler)
- [x] Define durable lease model. (`K8sLeaseElection` with monotonic fencing token per acquire)
- [x] Define fencing token model. (`LeaderElection::fencing_token()` + `validate_fencing_token()` in checkpoint)
- [x] Define stale-coordinator rejection path. (`validate_fencing_token()` at `write_epoch_metadata` boundary)
- [x] Define replay bundle contents. (`ReplayBundle` struct + `generate_replay_bundle()`)
- [x] Define Helm chart structure. (`k8s/helm/krishiv/` with coordinator, executor, RBAC, service templates)

## API And Interface Deliverables

- [x] `krishiv_metrics::init(MetricsConfig)` + `MetricsHandle` with auto-shutdown on drop.
- [x] `krishiv_metrics::current_traceparent()` — W3C traceparent for the active span.
- [x] `krishiv_governance::audit_log(principal, action)` — structured audit event via tracing.
- [x] `krishiv_governance::OpenLineageEmitter` trait — NoOp, Logging, and HTTP implementations.
- [x] `krishiv_governance::new_run_event()` — build RunEvent with UUID run_id.
- [x] `krishiv_governance::PolicyHook` trait — NoOp and RoleBased implementations.
- [x] `krishiv_governance::AuthProvider` trait — `StaticApiKeyAuthProvider`.
- [x] `krishiv_scheduler::TlsConfig` — cert/key/CA PEM for opt-in mTLS.
- [x] `krishiv_scheduler::extract_auth_context()` — bearer token from `Authorization` gRPC header.
- [x] `LeaderElection::try_acquire/renew/release/fencing_token` — default methods, backward-compatible.
- [x] `K8sLeaseElection::new(lease_name, namespace, holder_identity)` in `krishiv-operator`.
- [x] `validate_fencing_token(metadata, current_token)` in `krishiv-checkpoint`.
- [x] `generate_replay_bundle(storage, job_id, epoch)` in `krishiv-checkpoint`.
- [x] `diff_plans(before, after) -> PlanDiff` in `krishiv-plan`.
- [x] Helm chart values file with coordinator/executor/operator/TLS/RBAC/observability knobs.

## Runtime Deliverables

- [x] Emit OpenTelemetry traces. (OTel tracer provider; stdout exporter; OTLP configured via `MetricsConfig`)
- [x] Emit structured logs. (JSON formatter via `tracing-subscriber` fmt layer)
- [x] Emit OpenLineage-compatible events. (`LoggingEmitter` emits to `"krishiv::lineage"` tracing target)
- [x] Emit audit logs for query execution. (`AuditAction::QueryExecuted`)
- [x] Emit audit logs for job submit/cancel. (`AuditAction::JobSubmitted`, `JobCancelled`)
- [x] Emit audit logs for savepoint/restore. (`AuditAction::SavepointCreated`, `SavepointRestored`)
- [x] Emit audit logs for admin actions. (`AuditAction::AdminAction`)
- [x] Implement RBAC integration. (`StaticApiKeyAuthProvider` + `extract_auth_context()` bearer token)
- [x] Implement TLS configuration. (`TlsConfig` struct in scheduler)
- [x] Implement policy hooks. (`PolicyHook` trait + `RoleBasedPolicyHook`)
- [x] Implement row/column masking hooks. (`MaskingRule` enum; enforcement at operator boundary deferred to R10)
- [x] Implement per-job leader election. (`LeaderElection` trait extended; `K8sLeaseElection` in operator)
- [x] Implement durable leases. (`K8sLeaseElection` with monotonic fencing token)
- [x] Implement fencing tokens. (`validate_fencing_token()` + `CheckpointError::StaleFencingToken`)
- [x] Implement stale-coordinator rejection for checkpoint writes. (`validate_fencing_token` guards write path)
- [x] Implement replay bundle generation. (`generate_replay_bundle()` + `ReplayBundle` struct)
- [x] Implement plan diffing. (`diff_plans()` + `PlanDiff` struct in krishiv-plan)
- [x] Add Helm chart. (`k8s/helm/krishiv/`: Chart.yaml, values.yaml, 5 templates)
- [ ] Implement HA coordinator deployment (live K8s Lease API calls deferred to R10 — R9 uses simulated lease).
- [ ] Enforce policy hooks at DataFusion scan layer (deferred to R10).
- [ ] Kubernetes `kind` e2e test (deferred — requires kind cluster in CI).

## Test Checklist

- [x] `krishiv-metrics`: 5 tests pass.
- [x] `krishiv-governance`: 10 tests pass (RBAC, policy hooks, audit, OpenLineage).
- [x] `krishiv-checkpoint`: fencing token tests pass (current accepted, stale rejected, display).
- [x] `krishiv-checkpoint`: replay bundle tests pass (roundtrip, missing epoch error).
- [x] `krishiv-plan`: plan diff tests pass (identical, added, removed, changed).
- [x] `krishiv-operator`: K8sLeaseElection tests pass (7 unit tests).
- [x] `krishiv-operator`: `failover_stale_coordinator_checkpoint_rejected` integration test passes.
- [ ] OTLP integration test (deferred — requires live collector).
- [ ] `kind` cluster e2e failover test (deferred).

## Acceptance Gate

- [x] Fencing tokens prevent stale coordinators from committing. (`failover_stale_coordinator_checkpoint_rejected` test)
- [x] OpenTelemetry signals are emitted for supported jobs. (stdout exporter verified in metrics tests)
- [x] Audit and lineage events are emitted for supported actions. (tracing target verified in governance tests)
- [x] Helm chart renders all required resources for the R9 cluster shape.
- [ ] Coordinator failover does not allow duplicate checkpoint ownership in a live cluster (deferred — kind e2e).

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Control-plane correctness fails under failover | Use leases, fencing tokens, and durable ownership metadata |
| HA introduces new recovery semantics too late | Build on R3 task attempts/executor leases/event log and R6 versioned checkpoint metadata |
| Observability is bolted on inconsistently | Make metrics/traces/logs mandatory for runtime services |
| Policy hooks become a full governance product | Keep R9 hooks pluggable and defer policy engine depth |
| Audit logs miss sensitive actions | Define audit event taxonomy before implementation |
| Helm chart diverges from manifests | Generate or test both paths through the same expected resources |
