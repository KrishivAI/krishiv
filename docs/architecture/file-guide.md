# R1 Bootstrap File Guide

This guide explains the files introduced by the R1 bootstrap slice. It is meant for humans and Codex sessions resuming implementation work.

## Workspace Root

| File | Purpose |
|---|---|
| `Cargo.toml` | Defines the Rust workspace, initial crate members, shared package metadata, and shared lint policy. |
| `Cargo.lock` | Locks the current dependency graph, including Arrow/DataFusion for R1 local SQL. |
| `.gitignore` | Ignores local build output such as `target/`. |
| `AGENTS.md` | Repo-wide Codex instructions, architecture invariants, and resumability workflow. |

## R1 Crates

| File | Purpose |
|---|---|
| `crates/krishiv-api/Cargo.toml` | Defines the public API crate, local dependencies, and Arrow/Tokio integration. |
| `crates/krishiv-api/src/lib.rs` | Exposes `Session`, `SessionBuilder`, `DataFrame`, `Stream`, `ExecutionMode`, Arrow-backed `QueryResult`, and `StreamBatch`. |
| `crates/krishiv-api/examples/local_sql_parquet.rs` | Runnable embedded example that writes a small Parquet file, registers it, and runs SQL. |
| `crates/krishiv-api/examples/memory_stream.rs` | Runnable embedded example for bounded local memory stream collection. |
| `crates/krishiv-cli/Cargo.toml` | Defines the CLI crate and `krishiv` binary target. |
| `crates/krishiv-cli/src/lib.rs` | Owns help text, command parsing, `sql`, `explain`, and `jobs` dispatch. |
| `crates/krishiv-cli/src/main.rs` | Thin binary entrypoint that forwards arguments to `krishiv-cli` dispatch. |
| `crates/krishiv-cli/tests/r1_cli_golden.rs` | Validates stable R1 CLI output against golden fixtures. |
| `crates/krishiv-cli/tests/r1_cli_contract.rs` | Validates R1 CLI Parquet query behavior and user-facing error paths. |
| `crates/krishiv-sql/Cargo.toml` | Defines the SQL seam crate and Arrow/DataFusion dependencies. |
| `crates/krishiv-sql/src/lib.rs` | Owns DataFusion session integration, Parquet registration, SQL collect, and explain formatting. |
| `crates/krishiv-plan/Cargo.toml` | Defines the plan crate. |
| `crates/krishiv-plan/src/lib.rs` | Owns `ExecutionKind`, `PlanNode`, `LogicalPlan`, and `PhysicalPlan`. |
| `crates/krishiv-exec/Cargo.toml` | Defines the physical execution crate. |
| `crates/krishiv-exec/src/lib.rs` | Defines physical operator descriptors and placeholder logical-to-physical lowering. |
| `crates/krishiv-runtime/Cargo.toml` | Defines the runtime crate. |
| `crates/krishiv-runtime/src/lib.rs` | Owns runtime traits, local backend acceptance, job/task status, and local job registry. |

## Architecture And Engineering Docs

| File | Purpose |
|---|---|
| `docs/architecture/krishiv-roadmap.md` | Canonical 10-release roadmap and high-level architecture. |
| `docs/architecture/crate-map.md` | Explains crate ownership and dependency direction. |
| `docs/architecture/r1-bootstrap.md` | Explains what the bootstrap and local execution slices deliver, what remains stubbed, and streaming limitations. |
| `docs/architecture/file-guide.md` | This file; explains each R1 file. |
| `docs/engineering/standards.md` | Rust, async, testing, error handling, and documentation standards. |
| `docs/engineering/codex-workflow.md` | Rate-limit and session-resumability workflow for Codex. |
| `docs/releases/r1-foundation-alpha.md` | R1 alpha release notes, features, limitations, example commands, and validation commands. |
| `docs/sql-compatibility/r1.md` | R1 SQL compatibility baseline, supported surfaces, and known limitations. |

## Implementation Trackers

| File | Purpose |
|---|---|
| `docs/implementation/README.md` | Index of release implementation trackers. |
| `docs/implementation/status.md` | Current status ledger for resumable Codex sessions. |
| `docs/implementation/r1-foundation-alpha.md` | Active R1 implementation tracker. |
| `docs/implementation/r2-kubernetes-distributed-alpha.md` | R2 tracker for first Kubernetes distributed runtime. |
| `docs/implementation/r3-connector-contracts.md` | R3 tracker for connector contracts and initial connectors. |
| `docs/implementation/r4-shuffle-and-batch-aqe.md` | R4 tracker for shuffle and batch AQE. |
| `docs/implementation/r5-stateful-streaming-core.md` | R5 tracker for stateful streaming. |
| `docs/implementation/r6-checkpoints-and-savepoints.md` | R6 tracker for checkpoints, savepoints, and certified exactly-once paths. |
| `docs/implementation/r7-resource-governance-and-adaptivity.md` | R7 tracker for resource governance and adaptivity. |
| `docs/implementation/r8-lakehouse-and-python-beta.md` | R8 tracker for lakehouse and Python beta work. |
| `docs/implementation/r9-governance-and-operations.md` | R9 tracker for governance, observability, and HA operations. |
| `docs/implementation/r10-ga-platform-release.md` | R10 tracker for GA readiness. |

## Examples And Tests

| File | Purpose |
|---|---|
| `examples/embedded/README.md` | Documents how to run R1 embedded Cargo examples. |
| `examples/batch-sql/README.md` | Documents R1 local SQL and explain CLI commands. |
| `tests/integration/README.md` | Placeholder for future cross-crate integration tests. |
| `tests/golden/README.md` | Describes SQL/plan/CLI golden test fixtures. |
| `tests/golden/r1-sql-literal.txt` | Golden output for a minimal `krishiv sql` query. |
| `tests/golden/r1-explain-literal.txt` | Golden output for a minimal `krishiv explain` query. |
| `tests/golden/r1-sql-parquet-aggregate.txt` | Golden output for a Parquet-backed projection/filter/aggregate/limit query. |

## Codex Skill Source

| File | Purpose |
|---|---|
| `codex/skills/krishiv-engine/SKILL.md` | Repo-local source for the Krishiv implementation skill. |
| `codex/skills/krishiv-engine/agents/openai.yaml` | UI metadata for the repo-local skill source. |
