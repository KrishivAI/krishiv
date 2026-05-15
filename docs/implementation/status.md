# Krishiv Implementation Status

## Current Phase

R2 Kubernetes Distributed Alpha.

## Active Task

R2 distributed DAG routing slice is complete. The next active task is adding a
status endpoint/basic Web UI, or starting controller work needed before
Kubernetes `kind` smoke tests can become meaningful.

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
- Added minimal static Kubernetes manifests under `k8s/` for namespace, service account, RBAC, one coordinator, coordinator service, executors, and a sample job.
- Added offline manifest validation tests for the CRD, kustomization, coordinator, executor, RBAC, and sample job.
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

## In Progress

- None.

## Next Steps

1. Add status endpoint or basic Web UI for jobs, stages, tasks, and executors.
2. Start controller/operator work needed to reconcile `KrishivJob` resources.
3. Add Kubernetes `kind` smoke tests after the controller path exists.
4. Keep scheduling static and maintain exactly one active coordinator in R2.

## Known Blockers

- None known.

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
- `cargo test -p krishiv-cli` passed.
- `cargo run -p krishiv-api --example local_sql_parquet` passed.
- `cargo run -p krishiv-api --example memory_stream` passed.
- `cargo run -p krishiv-cli -- --help` passed.
- `cargo run -p krishiv-cli -- explain --help` passed.
- `find . -path './target' -prune -o -type f -print | sort` confirmed the bootstrap file inventory.
- Placeholder scan across repo docs and crates returned no actionable markers.

## Resume Instructions

For a new Codex session:

1. Read `AGENTS.md`.
2. Read this file.
3. Read `docs/implementation/r2-kubernetes-distributed-alpha.md`.
4. Continue R2 with distributed DAG routing or status endpoint/Web UI work, unless the user asks for Kubernetes controller work.
