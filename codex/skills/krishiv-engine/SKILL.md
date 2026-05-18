---
name: krishiv-engine
description: Build and evolve the Krishiv Rust-native hybrid compute framework. Use when Codex is asked to implement, refactor, test, document, or review Krishiv roadmap work, especially tasks involving Arrow/DataFusion, embedded or single-node execution, Kubernetes control plane design, scheduler, shuffle, state, checkpoints, connectors, lakehouse support, governance, or release-phase checklists.
---

# Krishiv Engine

## Overview

Use this skill to keep Krishiv implementation work aligned with the roadmap, architecture invariants, crate boundaries, and release checklists.

## Required Context

Before changing code for Krishiv, read the smallest relevant set of project docs:

- `AGENTS.md` for repo-wide Codex instructions.
- `docs/implementation/status.md` for current phase, active task, next steps, blockers, and last validation.
- `docs/architecture/krishiv-roadmap.md` for architecture, release phases, risks, and acceptance gates.
- `docs/implementation/README.md` for the release tracker index.
- `docs/implementation/r1-foundation-alpha.md` for R1 tasks.
- `docs/engineering/standards.md` for Rust, async, testing, and crate-boundary standards.
- `docs/engineering/codex-workflow.md` when Codex or Claude Code is resuming after interruption, rate limits, or context loss.

## Agent Interface Support

- Codex uses `AGENTS.md`, this skill, and `codex/skills/krishiv-engine/agents/openai.yaml`.
- Claude Code uses `CLAUDE.md`, the project skill shim at `.claude/skills/krishiv-engine/SKILL.md`, this canonical skill, and `codex/skills/krishiv-engine/agents/claude.yaml`.
- Both agents must use `docs/implementation/status.md` as the durable handoff file for rate-limit recovery, context resets, and cross-agent session resumes.
- Keep resume prompts and rate-limit guidance semantically aligned across the two interface config files and the `.claude/skills/krishiv-engine/SKILL.md` shim.

## Workflow

1. Identify the target release phase from the user request or roadmap.
2. Read `docs/implementation/status.md` and recover the active task before planning edits.
3. Open the matching release tracker from `docs/implementation/README.md`.
4. Inspect the current repo before deciding where to edit.
5. Keep the implementation inside the release scope unless the user explicitly expands scope.
6. Preserve the core architecture invariants:
   - One shared batch/stream planning and runtime model.
   - Embedded and single-node behavior parity from R1.
   - Kubernetes distributed mode from R2.
   - Active-active API servers, but exactly one active `JobCoordinator` per job.
   - No full active-active multi-master scheduling for the same job.
   - Exactly-once only for certified source/sink/checkpoint combinations.
7. Update tests and docs/checklists when behavior changes.
8. Update `docs/implementation/status.md` before ending a substantial session.
9. Run the narrowest useful validation command before final response.

## Implementation Defaults

- Use Rust + Tokio.
- Use Apache Arrow record batches as the primary in-memory data model.
- Use DataFusion for SQL parsing, expression evaluation, planning, and local execution unless there is a documented reason not to.
- Prefer small public traits at crate boundaries.
- Keep connector guarantees explicit with capability flags.
- Avoid exposing DataFusion internals directly through long-term Krishiv public APIs.
- Avoid adding Spark/Flink API compatibility unless the user asks for that roadmap item.
- Use the repo as durable memory: roadmap for intent, implementation trackers for checklists, and `status.md` for handoff.

## Release Discipline

- For R1, do not implement Kubernetes, durable shuffle, RocksDB state, checkpoints, savepoints, Python bindings, or lakehouse support.
- For R2, keep scheduling static and use one active coordinator.
- For R3 and later, every connector must declare capabilities and pass connector certification before being documented as supported.
- For R6 and later, checkpoint epoch ownership must prevent duplicate sink commits.
- For R9 and later, leader election must use leases and fencing tokens.
