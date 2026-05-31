# Krishiv - AI Agent Instructions

These instructions apply to all work in this repository and are shared by Codex
and Claude Code.

## Project Intent

Krishiv is a Rust-native hybrid compute framework for batch SQL, streaming
pipelines, and lakehouse-oriented data work.

Use the current codebase as the source of truth. The minimal docs are:

- `docs/README.md` - architecture, crate map, runtime modes, commands, and rules.
- `docs/implementation/status.md` - short session handoff note only.

## Core Defaults

- Use Rust 2024 and Tokio.
- Use Apache Arrow `RecordBatch` as the internal columnar data model.
- Use DataFusion for SQL parsing, planning, expressions, and local execution.
- Keep one runtime model across embedded, single-node, and distributed modes.
- Keep scheduler/executor/control-plane behavior behind crate APIs.
- Keep exactly-once claims tied to specific certified source/sink/checkpoint
  combinations.

## Architecture Invariants

- Do not build separate engines for batch and streaming.
- Do not use classic master/slave terminology.
- Do not implement active-active scheduling for the same job.
- Use active-active API surfaces only when job ownership remains fenced to one
  active coordinator per job.
- Treat executors as replaceable data-plane workers.
- Keep shuffle, state, checkpoint, metadata, and connector behavior behind
  explicit abstractions.
- Prefer typed IDs, typed fragments, typed errors, and capability flags over
  stringly routed public contracts.

## Workflow

1. Read `docs/README.md` and `docs/implementation/status.md`.
2. Inspect the relevant crate before planning edits.
3. Keep changes scoped to the crate that owns the behavior.
4. Add or update focused tests with behavior changes.
5. Run the narrowest useful validation command before final response.
6. For substantial sessions, update `docs/implementation/status.md` with:
   completed work, validation, blockers, and the next useful command.

## Rust Standards

- Prefer explicit error types at public crate boundaries.
- Avoid panics in library code except for impossible internal invariants.
- Keep async boundaries clear; do not hide blocking filesystem/database work
  inside async tasks.
- Avoid unrelated refactors.
- Preserve user changes in a dirty worktree; never revert work you did not make
  unless explicitly asked.

## Skill Source

The repo-local Krishiv skill lives at:

- `codex/skills/krishiv-engine/SKILL.md`

Agent interface configs live alongside it:

- `codex/skills/krishiv-engine/agents/openai.yaml`
- `codex/skills/krishiv-engine/agents/claude.yaml`
