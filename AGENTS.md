# Krishiv Codex Instructions

These instructions apply to all work in this repository.

## Project Intent

Krishiv is a Rust-native hybrid compute framework for batch SQL, stateful streaming, and lakehouse pipelines. Keep implementation decisions aligned with [docs/architecture/krishiv-roadmap.md](docs/architecture/krishiv-roadmap.md).

Primary defaults:

- Use Rust and Tokio for runtime implementation.
- Use Apache Arrow as the internal columnar memory format.
- Use DataFusion as the SQL, expression, logical planning, and local execution foundation.
- Support embedded and single-node modes from R1, and Kubernetes distributed mode from R2.
- Keep embedded, single-node, and distributed behavior semantically aligned for supported features.
- Prioritize native Krishiv SQL/DataFrame/Stream APIs before Spark/Flink API compatibility.

## Architecture Invariants

- Do not build separate engines for batch and streaming. Model both as DAG execution modes in one runtime.
- Do not use classic master/slave terminology for the long-term architecture.
- Do not implement full active-active multi-master scheduling for the same job.
- Use active-active API servers with exactly one active `JobCoordinator` per job.
- Treat executors as replaceable data-plane workers.
- Keep shuffle and state behind independent service/backend abstractions.
- Use durable checkpoint epochs, leases, and fencing tokens for failover-sensitive work.
- Document exactly-once only for certified source/sink/checkpoint combinations.

## Implementation Workflow

- Before starting implementation, read [docs/implementation/status.md](docs/implementation/status.md) if it exists.
- Before implementing a roadmap item, identify its target release phase in [docs/architecture/krishiv-roadmap.md](docs/architecture/krishiv-roadmap.md).
- Use [docs/implementation/README.md](docs/implementation/README.md) as the release tracker index.
- For R1 work, also update [docs/implementation/r1-foundation-alpha.md](docs/implementation/r1-foundation-alpha.md).
- Keep crate boundaries close to the proposed repo architecture.
- Add tests with every feature. Prefer focused unit tests first, then integration tests for cross-crate behavior.
- Update docs/checklists when implementation changes scope, behavior, or acceptance criteria.
- Avoid unrelated refactors while implementing roadmap items.
- Before ending a substantial session, update [docs/implementation/status.md](docs/implementation/status.md) with completed work, next steps, blockers, and validation results.

## Rate Limit And Session Resumability

- Work in small durable units: one feature, one test slice, and one checklist update at a time.
- Prefer repo files as memory over chat history. Keep current state in `docs/implementation/status.md`.
- Record the next useful command or task before stopping.
- If a session resumes after interruption, read `AGENTS.md`, `docs/implementation/status.md`, and only the smallest relevant roadmap/tracker docs.
- Keep long design context in docs, not in prompts.
- Commit or checkpoint coherent chunks when git is available.

## Rust Standards

- Follow [docs/engineering/standards.md](docs/engineering/standards.md).
- Prefer explicit error types at public crate boundaries.
- Avoid panics in library code except for impossible internal invariants.
- Keep async boundaries clear; do not hide blocking work inside async tasks.
- Prefer structured data models over stringly typed state.

## Codex Skill Source

The repo-local source for the Krishiv implementation skill is at [codex/skills/krishiv-engine/SKILL.md](codex/skills/krishiv-engine/SKILL.md). Use it as the task workflow guide when implementing roadmap features.
