# Krishiv Implementation Status

## Current Phase

R3.1 Distributed Execution Foundation.

## Active Task

The R3.1 networked coordinator/executor gRPC path is complete for registration,
heartbeat, and task-status updates. The next active task is scheduler-side
attempt/lease validation: store executor lease generations, reject stale
heartbeats/status updates, and make duplicate task status updates idempotent.

## Completed

- Created `docs/architecture/krishiv-roadmap.md`.
- Created `AGENTS.md`.
- Created `docs/engineering/standards.md`.
- Created `docs/implementation/r1-foundation-alpha.md`.
- Created repo-local `codex/skills/krishiv-engine/SKILL.md`.
- Installed the `krishiv-engine` skill globally under `/Users/gopal/.agents/skills/krishiv-engine`.
- Added Codex rate-limit and resumability workflow documentation.
- Synced the updated `krishiv-engine` resume protocol into the global skill install.
- Added `docs/implementation/README.md` as the implementation tracker index.
- Added implementation trackers for R2 through R10.
- Synced the updated tracker-index guidance into the global `krishiv-engine` skill install.
- Created the root Rust workspace.
- Created R1 bootstrap crates: `krishiv-api`, `krishiv-cli`, `krishiv-sql`, `krishiv-plan`, `krishiv-exec`, and `krishiv-runtime`.
- Added public API stubs for `Session`, `SessionBuilder`, `DataFrame`, `Stream`, `ExecutionMode`, `QueryResult`, and `StreamBatch`.
- Added plan, runtime, SQL, execution, and CLI stubs.
- Added R1 bootstrap architecture docs, crate map, SQL compatibility placeholder, and example/test placeholders.
- Added `docs/architecture/file-guide.md` to explain each bootstrap file.
- Added `.gitignore` for local build artifacts.
- Added Arrow/DataFusion dependencies behind `krishiv-sql`.
- Implemented DataFusion-backed local SQL execution and `EXPLAIN`.
- Implemented local Parquet registration and direct Parquet reads.
- Replaced bootstrap result placeholders with Arrow `RecordBatch` results.
- Implemented bounded and unbounded local memory stream API shapes with bounded map/filter/collect support.
- Routed embedded and single-node local execution through the runtime backend seam.
- Implemented `krishiv sql`, `krishiv explain`, and `krishiv jobs`.
- Added embedded/single-node SQL-over-Parquet parity coverage.
- Added R1 CLI golden tests for `sql` and `explain`.
- Updated R1 SQL compatibility, crate map, file guide, and tracker docs.
- Created R1 checkpoint commit `dd19774`.
- Added runnable Cargo examples for embedded SQL over Parquet and bounded memory streams.
- Added batch SQL README commands for `krishiv sql`, `krishiv explain`, and Parquet registration.
- Added broader R1 CLI contract tests for projection, filter, aggregate, limit, invalid SQL, and missing Parquet files.
- Added Parquet aggregate golden output.
- Added R1 Foundation Alpha release notes.
- Updated R1 tracker with the R1.1 hardening checklist.
- Added `crates/krishiv-proto` for R2 control-plane contracts.
- Added typed coordinator, job, stage, task, and executor identifiers.
- Added coordinator, job, stage, task, and executor lifecycle states.
- Added R2 job/stage/task specs, executor heartbeat, task assignment, and task status update contracts.
- Added `crates/krishiv-scheduler` for the R2 in-process active coordinator skeleton.
- Added executor registry, heartbeat handling, lost-executor marking, static task placement, task launch, task completion/failure updates, and job snapshots.
- Documented the R2 control-plane skeleton and limitations.
- Updated R2 tracker, crate map, and file guide.
- Added `krishiv submit` CLI skeleton backed by the R2 scheduler/proto model.
- Added `krishiv jobs --distributed` status output while preserving R1 `krishiv jobs`.
- Added CLI output for distributed job, stage, task, and executor status in the submit path.
- Added scheduler detail snapshots for job/stage/task status consumers.
- Added CLI tests for submit, distributed jobs, and submit validation.
- Updated R2 control-plane docs and examples with the new CLI surface.
- Added the first `krishiv.io/v1alpha1` `KrishivJob` CRD.
- Added minimal static Kubernetes manifests under `k8s/` for namespace, service account, RBAC, coordinator service, operator-owned coordinator runtime, executors, and a sample job.
- Added offline manifest validation tests for the CRD, kustomization, coordinator service, operator runtime, executor, RBAC, and sample job.
- Updated R2 tracker, control-plane docs, file guide, and Kubernetes README.
- Added coordinator configuration for stage retry and deterministic heartbeat timeout ticks.
- Implemented stage-level retry before terminal job failure.
- Implemented heartbeat timeout handling that marks stale executors lost.
- Added scheduler tests for stage retry and heartbeat timeout behavior.
- Updated R2 tracker and control-plane docs for retry and timeout semantics.
- Added conversion from Krishiv logical/physical plans into R2 scheduler job specs.
- Added coordinator APIs to submit logical and physical DAGs through the scheduler.
- Routed batch DAGs as `JobKind::Batch` and streaming DAGs as `JobKind::Streaming` with R1-level local state semantics.
- Added scheduler tests for batch logical DAG routing, streaming physical DAG routing, and empty-plan routing.
- Updated R2 tracker, crate map, file guide, and control-plane docs for DAG routing.
- Added `crates/krishiv-ui` as a Rust-native R2 status API and server-rendered Web UI using `axum` and `askama`.
- Added `/healthz`, `/readyz`, `/api/v1/jobs`, `/api/v1/jobs/{job_id}`, `/api/v1/executors`, `/ui`, and `/ui/jobs/{job_id}` routes.
- Added deterministic UI demo state with one local coordinator, executor, and running job.
- Added UI route tests for health, job listing, job detail, missing job, and HTML rendering.
- Updated R2 tracker, crate map, file guide, and control-plane docs for the status API/Web UI.
- Added `crates/krishiv-operator` for typed `KrishivJob` resource models, validation, scheduler job conversion, and status reconciliation.
- Added `KrishivJobReconciler` to submit resources into the active R2 coordinator or return an accepted `NoExecutors` status when placement cannot happen yet.
- Added `KrishivJob/status` models for phase, coordinator, observed generation, stage count, task counters, and conditions.
- Added operator tests for batch/streaming resource conversion, invalid resources, waiting for executors, submit/observe reconciliation, running task counters, and succeeded status.
- Updated R2 tracker, crate map, file guide, Kubernetes README, and control-plane docs for the operator reconciliation model.
- Added live Kubernetes watch/controller support to `krishiv-operator` using `kube` dynamic objects and watcher events.
- Added `KrishivJob/status` merge patching through the Kubernetes status subresource.
- Added the `krishiv-operator` binary entrypoint with namespace/all-namespace watching, selectors, coordinator id, and optional R2 bootstrap executor flags.
- Added `k8s/manifests/operator-deployment.yaml` and included it in the R2 kustomization.
- Added tests for dynamic object conversion, explicit API resource plural, status patch shape, operator CLI parsing, and operator manifest validation.
- Added sample early streaming `KrishivJob` manifest and included it in the R2 kustomization.
- Added Docker image build support for the R2 binaries with `Dockerfile` and `.dockerignore`.
- Added opt-in `kind` smoke tests for batch and early streaming `KrishivJob` status reconciliation, gated by `KRISHIV_KIND_E2E=1`.
- Applied roadmap review: added executor binary, gRPC transport, `MetadataStore` trait, and typed plan node items to R3; added minimal `FencingToken` to R6; split R7 into R7.1/R7.2 sub-milestones; split R8 into R8.1/R8.2 sub-milestones; added numeric benchmark target requirement to R10; updated `docs/architecture/krishiv-roadmap.md` and all affected tracker files.
- Applied reliability pull-forward review: added R3.1 task attempts, idempotent task updates, executor leases, coordinator restart recovery, durable job event log, Kubernetes finalizer cleanup, and basic stability metrics; added R4 shuffle orphan cleanup; added R5 checkpoint-barrier/watermark protocol design; added R6 versioned checkpoint/savepoint metadata and coordinator restart recovery; added R9 stale-coordinator rejection; added R10 metadata schema upgrade tests.
- Added a shared R2 coordinator handle so the live operator reconciler and status API read/write the same scheduler state.
- Added optional scheduler-backed status serving to `krishiv-operator` with `--status-addr`.
- Updated Kubernetes manifests so the single operator replica owns the active R2 coordinator runtime and the `krishiv-coordinator` service exposes that runtime's HTTP status surface.
- Added `docs/architecture/stage-local-execution.md` for the R3.1 stage-local coordinator/executor execution contract, including task attempts, executor leases, `MetadataStore` recovery, event-log expectations, failure handling, status metrics, and R4-R6 handoff boundaries.
- Updated the R3.1 and R4 trackers, roadmap, and file guide to mark the Stage-Local Execution Model as written while keeping review/approval and runtime acceptance gates open.
- Added `crates/krishiv-executor` with an executor startup config, minimal runtime facade, CLI skeleton, and construction of versioned registration/heartbeat requests.
- Added R3.1 versioned coordinator/executor transport contracts to `krishiv-proto`: `TransportVersion`, `AttemptId`, `LeaseGeneration`, registration, heartbeat, task assignment, task status, input partition, plan fragment, output contract, and response disposition types.
- Updated the runtime image build to include `krishiv-executor`.
- Updated R3.1 roadmap/tracker/docs to mark the executor crate and transport contracts complete while leaving real gRPC, scheduler idempotency, and lease-expiry behavior open.
- Added tonic as the R3.1 coordinator/executor service-boundary dependency.
- Added tonic-shaped coordinator/executor service traits to `krishiv-proto`.
- Added `CoordinatorExecutorTonicService` in `krishiv-scheduler` to apply executor registration, heartbeat, and task-status requests to the shared active coordinator.
- Added executor runtime helpers to call coordinator registration and heartbeat through the tonic-shaped service boundary.
- Updated the executor Kubernetes deployment to run `krishiv-executor` directly before the networked gRPC path landed.
- Added generated protobuf/tonic contracts for the R3.1 `CoordinatorExecutor` service in `krishiv-proto`.
- Added domain-to-wire conversion helpers for registration, heartbeat, and task-status messages.
- Added a scheduler-backed networked coordinator/executor gRPC server in `krishiv-scheduler`.
- Added executor gRPC client helpers and CLI modes for one-shot registration and long-running heartbeat loops.
- Wired the operator binary to optionally serve the coordinator/executor gRPC endpoint alongside the status API.
- Updated Kubernetes manifests so the coordinator service exposes gRPC on port 9090 and executor pods connect with `krishiv-executor --connect`.
- Added networked registration, heartbeat, and task-status smoke coverage.

## In Progress

- None.

## Next Steps

1. Teach the scheduler to store and validate `AttemptId` and `LeaseGeneration`, including stale-attempt and stale-lease rejection.
2. Add duplicate task-status idempotency rules and tests.
3. Add task assignment RPC from coordinator to executor.
4. R3.2 (connectors) cannot start until R3.1 acceptance gate passes — enforce this sequencing strictly.

## Known Blockers

- R2 `kind` smoke validation is deferred because local Podman image build hit a TLS certificate trust issue while pulling the Rust base image.

## Last Validation

- `python3 /Users/gopal/.codex/skills/.system/skill-creator/scripts/quick_validate.py codex/skills/krishiv-engine` passed.
- `python3 /Users/gopal/.codex/skills/.system/skill-creator/scripts/quick_validate.py /Users/gopal/.agents/skills/krishiv-engine` passed.
- `find docs/implementation -maxdepth 1 -type f -print | sort` shows R1-R10 trackers, README, and status files.
- `wc -l docs/implementation/*.md` completed successfully.
- `python3 /Users/gopal/.codex/skills/.system/skill-creator/scripts/quick_validate.py /Users/gopal/.agents/skills/krishiv-engine` passed after tracker-index sync.
- `cargo fmt --all --check` passed.
- `cargo check --workspace` passed.
- `cargo test --workspace` passed.
- `cargo run -p krishiv-cli -- sql --query "select 1 as value"` passed.
- `cargo run -p krishiv-cli -- explain --query "select 1 as value"` passed.
- `cargo run -p krishiv-cli -- jobs` passed.
- `cargo run -p krishiv-cli -- submit --job-id job-demo --name demo --tasks 2 --launch` passed.
- `cargo run -p krishiv-cli -- jobs --distributed` passed.
- `cargo test -p krishiv-scheduler` passed, including offline R2 manifest validation tests.
- `cargo test -p krishiv-scheduler --test r2_k8s_manifests` passed.
- `cargo test -p krishiv-ui` passed.
- `cargo test -p krishiv-operator` passed.
- `cargo test -p krishiv-operator --test r2_kind_smoke` passed with the default skip path.
- `cargo check -p krishiv-operator` passed.
- `cargo test -p krishiv-cli` passed.
- `cargo run -p krishiv-operator -- --help` passed and listed `--status-addr`.
- `cargo run -p krishiv-ui -- --help` passed.
- `cargo run -p krishiv-ui -- --demo --addr 127.0.0.1:18080` started the demo status server.
- `curl http://127.0.0.1:18080/healthz` returned `ok`.
- `curl http://127.0.0.1:18080/api/v1/jobs` returned the demo `job-demo` status.
- `curl http://127.0.0.1:18080/ui` rendered the R2 status HTML page.
- `cargo run -p krishiv-api --example local_sql_parquet` passed.
- `cargo run -p krishiv-api --example memory_stream` passed.
- `cargo run -p krishiv-cli -- --help` passed.
- `cargo run -p krishiv-cli -- explain --help` passed.
- `find . -path './target' -prune -o -type f -print | sort` confirmed the bootstrap file inventory.
- Placeholder scan across repo docs and crates returned no actionable markers.
- `git diff --check` passed after the R3.1 Stage-Local Execution Model document update.
- Placeholder scan across the updated R3.1/R4 roadmap and tracker docs returned no actionable markers.
- `cargo fmt --all --check` passed after adding `krishiv-executor` and R3.1 transport contracts.
- `cargo check --workspace` passed.
- `cargo test -p krishiv-proto -p krishiv-executor` passed.
- `cargo run -p krishiv-executor -- --help` passed.
- `cargo run -p krishiv-executor -- --executor-id exec-demo --host demo-pod --slots 2 --coordinator http://coordinator:8080` passed and printed registration/heartbeat contract summaries.
- `git diff --check` passed after the R3.1 executor/transport contract slice.
- Placeholder scan across the R3.1 executor/transport contract files and updated docs returned no actionable markers.
- `cargo check -p krishiv-proto -p krishiv-scheduler -p krishiv-executor` passed after adding tonic-shaped services.
- `cargo fmt --all --check` passed after adding tonic-shaped services.
- `cargo check --workspace` passed after adding tonic-shaped services.
- `cargo test -p krishiv-proto -p krishiv-scheduler -p krishiv-executor` passed.
- `cargo test -p krishiv-scheduler --test r2_k8s_manifests` passed after updating the executor manifest command.
- `cargo run -p krishiv-executor -- --executor-id exec-demo --host demo-pod --slots 2 --coordinator http://coordinator:8080` passed after adding the tonic-shaped service helpers.
- `git diff --check` passed after the tonic-shaped service boundary slice.
- Placeholder scan across the tonic-shaped service boundary files and updated docs returned no actionable markers.
- `cargo check -p krishiv-proto -p krishiv-scheduler -p krishiv-executor -p krishiv-operator` passed after adding the networked gRPC server/client path.
- `cargo test -p krishiv-scheduler grpc_service_registers_and_heartbeats_over_network` passed; the test skips only when the local sandbox denies loopback sockets.
- `cargo test -p krishiv-scheduler grpc_service_registers_and_heartbeats_over_network -- --nocapture` passed with elevated loopback-socket permission and exercised the real networked gRPC path.
- `cargo fmt --all --check` passed after formatting the gRPC transport slice.
- `cargo check --workspace` passed after adding protobuf generation and tonic transport dependencies.
- `cargo test -p krishiv-proto -p krishiv-scheduler -p krishiv-executor -p krishiv-operator` passed.
- `cargo test -p krishiv-executor` passed after the executor heartbeat-loop polish.
- `cargo check --workspace` passed again after final code/doc updates.
- `cargo run -p krishiv-executor -- --help` passed and listed `--register-once`, `--connect`, and `--heartbeat-interval-secs`.
- `cargo run -p krishiv-operator -- --help` passed and listed `--executor-grpc-addr`.
- `cargo run -p krishiv-executor -- --executor-id exec-demo --host demo-pod --slots 2 --coordinator http://coordinator:9090` passed and printed dry-run registration/heartbeat summaries.
- `git diff --check` passed after the networked gRPC transport slice.
- Stale network-placeholder scan across `crates`, `docs`, and `k8s` returned no matches.

## Resume Instructions

For a new Codex session:

1. Read `AGENTS.md`.
2. Read this file.
3. Read `docs/implementation/r3-connector-contracts.md`.
4. Continue R3.1 by adding scheduler-side attempt and lease validation, then duplicate-status idempotency.
