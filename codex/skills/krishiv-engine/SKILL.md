---
name: krishiv-engine
description: Build, test, document, or review the Krishiv Rust workspace. Use when work touches Krishiv runtime modes, SQL/DataFusion, scheduler, executor, shuffle, state, checkpointing, connectors, lakehouse, governance, metrics, Python bindings, Kubernetes manifests, or repo-agent workflow.
---

# Krishiv Engine

## Purpose

Use this skill to keep Krishiv work aligned with the current codebase and the
small documentation surface.

## Required Context

Before changing Krishiv code or docs, read:

- `AGENTS.md`
- `docs/README.md`
- `docs/implementation/status.md`

Then inspect the relevant crate or file directly. Do not rely on old roadmap or
review documents; the docs have been intentionally collapsed to avoid drift.

## Workflow

1. Identify the crate or interface that owns the requested behavior.
2. Read nearby code and tests before editing.
3. Make the smallest coherent change that satisfies the request.
4. Add or update focused tests for behavior changes.
5. Run the narrowest useful validation command.
6. For substantial sessions, update `docs/implementation/status.md` with a
   concise handoff.

## Architecture Rules

- Keep one runtime model across embedded, single-node, and distributed modes.
- Keep one active coordinator owner per job; do not add active-active job
  scheduling.
- Treat executors as replaceable workers.
- Keep shuffle, state, checkpoint, metadata, and connector behavior behind
  explicit crate APIs.
- Use Arrow and DataFusion for columnar data and SQL/local planning.
- Keep exactly-once guarantees scoped to certified source/sink/checkpoint
  combinations.
- Prefer typed IDs, typed fragments, typed errors, and capability flags over
  stringly public contracts.

## Validation Defaults

Prefer narrow checks while iterating:

```bash
cargo check -p <crate>
cargo test -p <crate>
cargo test -p krishiv-scheduler --lib
cargo test -p krishiv-executor --lib
cargo test -p krishiv-runtime
```

Use workspace-wide checks when the change crosses crate boundaries:

```bash
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt --check
```
