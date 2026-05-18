# Krishiv Implementation Status

## Current Phase

R4 Shuffle and Batch AQE — Tier 1 + Tier 2 + Tier 3 complete.

## Active Task

R4 full implementation complete across all three tiers (122→270 tests, 0 failures):

**Tier 1 (Foundation)**
- Proto extensions: `StageSpec.upstream_stage_ids`, `output_partition_count`, `ShufflePartitionOutput`, `TaskRuntimeStats`, `InputPartitionDescriptor::ShuffleFlight`
- Arrow IPC shuffle server (`krishiv-shuffle::flight`): TCP `<job/stage/partition>\n` → 4-byte length + IPC bytes; `FlightShuffleClient::fetch`
- Stage N+1 wait in coordinator: checks `upstream_stage_ids` before launching
- Shuffle GC: `Coordinator::take_gc_ready_jobs()` + coordinator binary `--shuffle-dir` GC loop
- Executor shuffle read path: `read_shuffle_flight_partitions` → registers as DataFusion tables
- Executor shuffle write path: `shuffle-write:hash:<key>:<N>` fragment via `HashPartitioner` + `LocalDiskShuffleStore`

**Tier 2 (TPC-H gate correctness)**
- Pre-aggregation: `pre-agg:sql:` fragments auto-route through existing SQL path
- Runtime statistics: `SqlDataFrame::collect_with_stats` reads DataFusion `output_rows`/`elapsed_compute`; executor wires into `TaskOutputMetadata`
- Distributed joins: two-stage (shuffle + local DataFusion JOIN) works with current `ShuffleFlight` + SQL execution path

**Tier 3 (AQE / observability)**
- Health/metrics HTTP: `--http-addr` on coordinator + executor; `/healthz`, `/readyz`, `/metrics` (Prometheus)
- EXPLAIN annotations: `describe_plan` shows `[broadcast-eligible]` and `[est-rows: N]`
- `SmallFilePlanner`: greedy file grouper for scan parallelism (4 tests)
- `ObjectStoreShuffleStore`: `ShuffleStore` impl backed by `Arc<dyn ObjectStore>` (3 tests)

Architecture docs: `shuffle-retry-lineage.md` (Option B retry policy), `shuffle-recovery-expectations.md` (per-failure-point recovery matrix)

## Completed

- Hardened R3 practical remaining slices: exact shuffle lease registration/rejection before commit, operator pod-launch failure executor fencing/requeue, typed task I/O descriptors, JSON metadata schema envelopes, streaming-execution-model roadmap reconciliation, R1/R2 roadmap reconciliation, and R3 tracker reconciliation.
- Created `docs/architecture/krishiv-roadmap.md`.
- Created `AGENTS.md`.
- Created `docs/engineering/standards.md`.
- Created `docs/implementation/r1-foundation-alpha.md`.
- Created repo-local `codex/skills/krishiv-engine/SKILL.md`.
- Installed the `krishiv-engine` skill globally under `/Users/gopal/.agents/skills/krishiv-engine`.
- Added Codex rate-limit and resumability workflow documentation.
- Expanded the agent workflow so Codex and Claude Code share rate-limit, resume, and cross-agent handoff protocols.
- Added a Claude Code project-skill shim under `.claude/skills/krishiv-engine/SKILL.md` so Claude can use the existing canonical Krishiv skill through `/krishiv-engine`.
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
- Added executor lease generation storage to scheduler executor records.
- Added stale executor lease rejection for heartbeats and task-status updates.
- Added lease generation bumping when executors are marked lost or time out.
- Added same-id executor re-registration after loss with the next valid lease generation.
- Added stale task-attempt rejection and duplicate terminal task-status idempotency.
- Mapped stale lease, stale attempt, duplicate status, and unknown executor outcomes to transport dispositions.
- Added the `ExecutorTask.AssignTask` gRPC service and wire conversions for task assignments.
- Added scheduler assignment emission with job/stage/task ids, attempt id, executor lease generation, input partitions, plan fragment, and output contract.
- Added an executor-side assignment inbox and networked task receiver service.
- Added a minimal executor task runner skeleton that consumes one assignment, reports `Running`, validates placeholder fragment metadata, and reports terminal status.
- Reviewed the pending deployment, shuffle, data-plane transport, and security architecture docs and folded their constraints into the active R3.1 handoff.
- Aligned R4 shuffle docs around local executor disk as the default durability mode and object-store durability as opt-in.
- Added the first narrow R3.1 executor SQL fragment execution path for `sql: SELECT 1`-style assignments, returning lightweight output metadata without Arrow payloads in control-plane Protobuf.
- Added lifecycle coverage that sends a task assignment to an executor inbox over gRPC, executes the local SQL fragment, and reports status back to the scheduler-backed coordinator over gRPC.
- Added bootstrap R3.1 `local-parquet:<table>:<path>` input partition registration for executor-local `sql:` fragments without starting R3.2 connector certification.
- Added executor tests for local Parquet partition descriptor validation, scheduler-backed Parquet scan execution, and networked assignment/status Parquet scan execution with row/batch/column output metadata.
- Added `EventLogEvent` enum (JobSubmitted, StagePlanned, TaskAssigned, TaskStarted, TaskSucceeded, TaskFailed, ExecutorLost, JobCancelled) to `krishiv-scheduler`.
- Added `MetadataStore` trait + `InMemoryMetadataStore` to `krishiv-scheduler`.
- Added `LeaderElection` trait + `SingleNodeElection` (no-op) to `krishiv-scheduler`.
- Added `JobSubmitter` trait to `krishiv-scheduler`.
- Added `Coordinator::recover_from_store` for restart recovery from a `MetadataStore`.
- Added `ExecutorRuntime::deregister_with_grpc_endpoint` (best-effort `Draining` heartbeat) to `krishiv-executor`.
- Added SIGTERM handler to executor `heartbeat_loop`: on signal, sends deregistration heartbeat and exits cleanly.
- Added `terminationGracePeriodSeconds: 30` to `k8s/manifests/executor-deployment.yaml`.
- Added `deletion_timestamp` field to `ObjectMeta` in `krishiv-operator`.
- Added `has_finalizer` and `is_being_deleted` helpers to `ObjectMeta`.
- Added `FinalizerAdded` and `FinalizerRemoved` variants to `ReconcileAction`.
- Wired finalizer lifecycle logic at the top of `KrishivJobReconciler::reconcile`.
- Added `Coordinator::with_store` builder; wired `MetadataStore` write-through into `submit_job` and `apply_task_update` (Slice 1).
- `advance_heartbeat_clock` now resets Running tasks on lost executors to `Assigned` for automatic reassignment (Slice 3).
- Added `Coordinator::push_cancel_job` async method that sends `CancelTask` gRPC to all executors owning running tasks (Slice 4).
- Added `NodeOp` typed operator enum, `PlanSchema`/`SchemaField`/`FieldType` types, and optional `op`/`output_schema` fields to `PlanNode` in `krishiv-plan` (Slice 5).
- Added `memory_used_bytes`, `memory_limit_bytes`, `active_task_count` to `ExecutorHeartbeatRequest` and `ExecutorHeartbeat`; stored as `ExecutorHealthSnapshot` per `ExecutorRecord`; memory-aware placement skips over-threshold executors (Slice 6).
- Added `operator_restart_does_not_duplicate_scheduler_jobs` test confirming idempotent reconciliation (Slice 7).
- Added dedicated `DeregisterExecutor` RPC: `DeregisterExecutorRequest`/`DeregisterExecutorResponse` in `krishiv-proto`, `Coordinator::deregister_executor`, gRPC service handler in `krishiv-scheduler`, wire helpers, and `grpc_deregister_transitions_executor_to_removed` test.
- Wired `cancel_job` into `KrishivJobReconciler` delete path before stripping finalizer; added `reconcile_delete_calls_cancel_job_before_removing_finalizer` test.
- Added `task_timeout_secs` to `TaskSpec` and `ExecutorTaskAssignment`; wired through proto (`uint64 task_timeout_secs = 11`); executor enforces with `tokio::time::timeout` reporting `TaskFailed` on expiry.
- Added `last_failure_reason` to `TaskRecord` and `TaskSnapshot`; propagated to `TaskView` in status API.
- Added `lease_generation`, `memory_used_bytes`, `memory_limit_bytes`, `active_task_count` to `ExecutorView` in status API.
- Added `k8s/manifests/network-policy.yaml` restricting coordinator gRPC (port 9090) to `krishiv-system` namespace; added to kustomization; validated by `network_policy_restricts_coordinator_grpc_to_krishiv_namespace` test.
- Wired `ExecutorRuntime::deregister_with_grpc_endpoint` to call the real `DeregisterExecutor` gRPC RPC; SIGTERM handler in `heartbeat_loop` calls this path; added `deregister_via_grpc_endpoint_transitions_executor_to_removed` test.
- Added `CancelTask` running-task handler: `cancel_task` now marks tasks in a `cancelled_tasks` set; runner checks after `Running` status and sends `TaskCancelled` instead of executing; added `task_runner_reports_cancelled_when_inbox_cancel_received` test.
- Wired live `StabilityMetrics` to `/metrics` endpoint in `krishiv-ui`; replaces hardcoded zeros with running task count, retry count, failed assignments, and max heartbeat age in Prometheus text format.
- Added `StabilityMetrics::empty()` constructor for lock-unavailable fallback.
- Added standalone `krishiv-coordinator` binary to `krishiv-scheduler` (`--coordinator-id`, `--grpc-addr`, `--help`); starts gRPC server for bare-metal / VM deployments without Kubernetes; 5 CLI tests pass.

## In Progress

- None (R4 all tiers committed to branch `claude/analyze-r3-plan-r4-0QYyr`).

## Next Steps

1. R4 TPC-H SF10 correctness gate: write a multi-stage end-to-end test that runs a TPC-H Q1/Q3 style query through the full shuffle pipeline.
2. R4 AQE coalesce: wire `CoalesceRule` results into coordinator stage scheduling to merge small shuffle output partitions.
3. R5 prep: checkpoint-barrier/watermark protocol design review.
4. Live external Kafka broker integration (opt-in, blocked on Kafka client selection).

## Known Blockers

- R2 `kind` smoke validation is deferred because local Podman image build hit a TLS certificate trust issue while pulling the Rust base image.

## Architectural Inputs To Preserve

- Distributed mode has two targets: Kubernetes is primary, and bare metal / VM is secondary. Core runtime crates must remain deploy-target neutral; Kubernetes API access belongs in `krishiv-operator`, Kubernetes packaging under `k8s/`, and narrowly scoped CLI paths.
- Control-plane traffic stays on tonic gRPC + Protobuf for registration, heartbeat, task assignment, task status, cancellation, and deregistration.
- Bulk Arrow data must not be added to control-plane Protobuf messages. R4 uses Arrow IPC for shuffle writes and Arrow Flight for shuffle reads/query result transfer.
- R4 shuffle defaults to local executor disk with optional object-store durability. Do not assume S3/object storage is required for distributed execution.
- Pre-R9 coordinator/executor gRPC has no mTLS or application-level auth. Task specs must not contain credentials or secret values; shared Kubernetes deployments require namespace isolation, NetworkPolicy, and component-specific service accounts.

## Last Validation

- `cargo fmt --all` applied and `cargo fmt --check` passed (branch `claude/analyze-recommend-slices-Ai5GY`).
- `cargo check --workspace` passed.
- `cargo test -p krishiv-catalog -p krishiv-connectors` passed — 28 tests, 0 failures (S3: 3, Kafka: 7, Parquet: 4, lib: 5, DataFusion bridge: 3, CertificationSuite new: 2).
- `cargo fmt --all --check` passed (branch `claude/analyze-codebase-recommendations-5vvXH`).
- `cargo check --workspace` passed.
- `cargo test --workspace` passed — 0 failures across all crates.
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
- `cargo test -p krishiv-executor` passed after the first narrow executor SQL fragment execution path landed.
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
- `cargo fmt --all --check` passed after adding bootstrap local Parquet partition execution.
- `cargo check -p krishiv-executor` passed after adding bootstrap local Parquet partition execution.
- `cargo test -p krishiv-executor` passed after adding descriptor validation plus scheduler-backed and networked local Parquet scan coverage.
- `cargo check --workspace` passed after the R3.1 local Parquet executor slice.
- `git diff --check` passed after the R3.1 local Parquet executor slice.
- `cargo check --workspace` passed again after final code/doc updates.
- `cargo run -p krishiv-executor -- --help` passed and listed `--register-once`, `--connect`, and `--heartbeat-interval-secs`.
- `cargo run -p krishiv-operator -- --help` passed and listed `--executor-grpc-addr`.
- `cargo run -p krishiv-executor -- --executor-id exec-demo --host demo-pod --slots 2 --coordinator http://coordinator:9090` passed and printed dry-run registration/heartbeat summaries.
- `git diff --check` passed after the networked gRPC transport slice.
- Stale network-placeholder scan across `crates`, `docs`, and `k8s` returned no matches.
- `cargo test -p krishiv-scheduler --lib` passed after adding scheduler-side lease and attempt validation.
- `cargo fmt --all --check` passed after the scheduler-side lease/attempt validation slice.
- `cargo check --workspace` passed after the scheduler-side lease/attempt validation slice.
- `cargo test -p krishiv-proto -p krishiv-scheduler -p krishiv-executor -p krishiv-cli -p krishiv-ui -p krishiv-operator` passed after the scheduler-side lease/attempt validation slice.
- `git diff --check` passed after the scheduler-side lease/attempt validation slice.
- `cargo fmt --all --check` passed after the task-assignment RPC/receiver slice.
- `cargo check -p krishiv-proto -p krishiv-scheduler -p krishiv-executor` passed after the task-assignment RPC/receiver slice.
- `cargo test -p krishiv-proto -p krishiv-scheduler -p krishiv-executor` passed, including the executor assignment gRPC loopback test.
- `cargo check --workspace` passed after the task-assignment RPC/receiver slice.
- `git diff --check` passed after the task-assignment RPC/receiver slice.
- `git diff --check` passed after reconciling pending architecture/security roadmap docs.
- Search for stale S3-default shuffle and old Kubernetes-isolation wording returned no matches in `docs/architecture`, `docs/implementation`, or `docs/security`.
- `cargo check -p krishiv-executor` passed after the minimal task runner skeleton.
- `cargo fmt --all --check` passed after the minimal task runner skeleton.
- `cargo check --workspace` passed after the minimal task runner skeleton.
- `cargo test -p krishiv-proto -p krishiv-scheduler -p krishiv-executor` passed after the minimal task runner skeleton.
- `git diff --check` passed after the minimal task runner skeleton.
- `git diff --check` passed after the shared Codex/Claude Code agent workflow update.
- `python3 - <<'PY' ...` verified both agent interface YAML files include display, default prompt, resume prompt, rate-limit strategy, and supported-agent metadata.
- `rg -n "Codex|Claude Code|rate-limit|resume|status.md" ...` confirmed the shared workflow, skill, Claude entrypoint, interface configs, and status handoff all reference the rate-limit/resume paths.
- `git diff --check` passed after adding the Claude Code project-skill shim and correcting Claude skill invocation docs.
- `test -f .claude/skills/krishiv-engine/SKILL.md && rg -n "/krishiv-engine|codex/skills/krishiv-engine/SKILL.md|Claude Code" .claude/skills/krishiv-engine/SKILL.md CLAUDE.md docs/engineering/codex-workflow.md codex/skills/krishiv-engine/SKILL.md codex/skills/krishiv-engine/agents/claude.yaml` confirmed Claude Code project-skill discovery and canonical-skill references.
- `cargo test -p krishiv-scheduler --lib` passed (32 tests, including `in_memory_metadata_store_round_trips`, `single_node_election_is_always_leader`, `coordinator_recovers_jobs_from_store`).
- `cargo test -p krishiv-operator --lib` passed (17 tests, including `reconcile_adds_finalizer_on_first_observe`, `reconcile_removes_finalizer_on_deletion`).
- `cargo test -p krishiv-executor --lib` passed (7 tests).
- `cargo fmt --all --check` passed after R3.1 remaining slices.
- `cargo fmt --all` applied; `cargo check --workspace` passed after R3.1 deregister/cancel/timeout/NetworkPolicy/UI-status slices.
- `cargo test --workspace` passed — 0 failures across all crates (all test result lines `ok`).
- `cargo run -p krishiv-scheduler --bin krishiv-coordinator -- --help` passed and listed `--coordinator-id` and `--grpc-addr`.
- `cargo fmt --all` applied; `cargo check --workspace` passed after A–D slices.
- `cargo test --workspace` passed — 0 failures (executor: 10 tests; ui: 8 tests; scheduler: 42 tests).
- R3.2 Slices 1–3: `crates/krishiv-connectors` (connector traits + Parquet reader/writer, 9 tests) and `crates/krishiv-catalog` (catalog types + InMemoryCatalog, 4 tests) created.
- `cargo fmt --all` applied; `cargo check --workspace` passed; `cargo test -p krishiv-catalog -p krishiv-connectors` passed — 13 tests, 0 failures.
- R3.2 Slices 4–7: S3 connector (`s3.rs`), Kafka stubs (`kafka.rs`), DataFusion catalog bridge (`datafusion_bridge` module in `krishiv-catalog`), and expanded `CertificationSuite` (`run_bounded_exhaustion_test`, `run_idempotent_sink_test`) implemented.
- `object_store = "0.12"`, `bytes = "1"`, `async-trait = "0.1"` added to workspace dependencies.
- `cargo fmt --all` applied; `cargo check --workspace` passed; `cargo test -p krishiv-catalog -p krishiv-connectors` passed — 28 tests, 0 failures.
- **R3.2 Slice A**: At-least-once sink contract (`AtLeastOnceSinkContract` doc struct), `ParquetOffset` implementing `Offset` with encode/decode, `CertificationSuite::run_offset_round_trip_test`, and 3 new tests added to `krishiv-connectors`. `ParquetSource::current_offset()` returns typed `ParquetOffset`.
- **R3.2 Slice B**: CDC design document written at `docs/rfcs/cdc-design.md` covering log-based/poll-based capture, `_cdc_op/_cdc_ts_ms/_cdc_lsn/_cdc_table` column model, offset model for PostgreSQL/MySQL/poll-based, Krishiv integration points, and R3 limitations.
- **R3.2 Slice C**: `SchemaRegistry` trait and `InMemorySchemaRegistry` backed by `BTreeMap` added to `krishiv-catalog`; 3 new tests pass.
- **R3.2 Slice D**: `ConnectorCapabilityFlags` struct added to `krishiv-proto`; `TaskSpec` extended with `source_capabilities`/`sink_capabilities` builder methods; `TaskSnapshot` in `krishiv-scheduler` propagates fields; `ConnectorCapabilityView` added to `krishiv-ui` `TaskView`.
- **R4 Bootstrap Slice E**: `register_record_batches()` added to `SqlEngine` in `krishiv-sql`; `krishiv-connectors` dep added to `krishiv-executor`; `CONNECTOR_PARQUET_PARTITION_PREFIX` + `read_connector_parquet_partitions()` wired into `execute_stage_fragment()`; new test `executor_runs_parquet_task_via_connector_source` passes.
- **R4 Bootstrap Slice F**: `ShuffleStore` trait, `InMemoryShuffleStore`, and `LocalDiskShuffleStore` added to `krishiv-shuffle` with lease-token zombie-executor rejection; `parquet` and `bytes` deps added to `krishiv-shuffle/Cargo.toml`; 8 new tokio async tests pass.

- **R3 closure slices (previous session)**: Added `PostWriteOffsetCommitProtocol` and `OffsetCommitter` to enforce write → flush → offset commit ordering; added deterministic in-memory Kafka-compatible source and commit log; added executor Kafka → Parquet pipeline support using `ParquetSink`; added real-runner tests for the pipeline and connector-Parquet path; added assignment lease-generation → shuffle stale-token rejection proof.
- **R3 hardening slices (previous session)**: Added object-store Parquet source/sink execution descriptors on the real executor runner, `JsonFileMetadataStore` for durable local metadata/event-log recovery, and operator-side executor pod launch failure detection/status reporting.
- **R3 practical remaining slices (previous session)**: Hardened `ShuffleStore` with registered exact lease-token validation before commit, extended the zombie-executor proof so stale writes cannot win before fresh output commits, and made operator pod-launch failure handling mark associated executors lost/requeue running tasks.
- **R1–R3 architecture remediation (this session)**: Added typed task input/output descriptors and wire round trips while keeping legacy string compatibility; migrated executor connector/object/Kafka tests to typed descriptors; added JSON metadata `schema_version`/`store_kind` envelope validation; reconciled R1/R2 roadmap checklist state; documented the R1–R3 architecture review and reconciled the already-approved streaming execution model in the roadmap.

## Last Validation (R4 full implementation, branch `claude/analyze-r3-plan-r4-0QYyr`)

- `cargo check --workspace` passed (clean, no warnings on non-dead-code items).
- `cargo test --workspace` passed — 270 tests, 0 failures across all crates and doc tests.
- Commits: `f2d8b6d` (Tier 1), `9add4f3` (Tier 2), `98f9c59` (Tier 3).
- Branch pushed to remote: `origin/claude/analyze-r3-plan-r4-0QYyr`.

## Resume Instructions

For a new Codex session:

1. Read `AGENTS.md`.
2. Read this file.
3. Read `docs/implementation/r4-shuffle-and-batch-aqe.md`.
4. R4 Tiers 1–3 are complete. Continue with TPC-H SF10 correctness gate or R5 checkpoint-barrier protocol design.

For a new Claude Code session:

Start with `/krishiv-engine resume`, then:

1. Read `AGENTS.md`.
2. Read `CLAUDE.md`.
3. Read this file.
4. Continue from the Next Steps list above; do not rely on Codex-only or Claude-only chat history.
