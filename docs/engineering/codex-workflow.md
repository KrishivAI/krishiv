# Codex Workflow For Krishiv

## Purpose

This workflow makes Krishiv implementation resilient to rate limits, context resets, interrupted turns, and new Codex sessions. The repository should carry the project memory; chat history should be optional.

## Core Rule

Work in small durable units:

```text
one task = one small feature + relevant tests + docs/checklist update
```

Examples of good work units:

- Create the Cargo workspace and empty R1 crates.
- Add `Session` and `ExecutionMode` skeletons with unit tests.
- Add `krishiv explain` CLI help and a snapshot test.
- Add local memory stream source with one parity test.

Avoid giant work units such as "implement all R1" or "build the whole runtime".

## Start-Of-Session Protocol

At the beginning of a substantial session:

1. Read `AGENTS.md`.
2. Read `docs/implementation/status.md`.
3. Read the smallest relevant implementation tracker, such as `docs/implementation/r1-foundation-alpha.md`.
4. Inspect the current repo files before editing.
5. Identify the target release phase and one concrete task.
6. Confirm the validation command that will prove the task.

## During-Session Protocol

While working:

- Keep edits scoped to the active task.
- Update the relevant checklist when task status changes.
- Add or update tests with feature work.
- Prefer small, reviewable changes over broad rewrites.
- Record blockers in `docs/implementation/status.md` instead of relying on memory.

## End-Of-Session Protocol

Before ending a substantial session:

1. Update `docs/implementation/status.md`.
2. Update the relevant release tracker checklist.
3. Record validation commands and results.
4. Record known blockers or incomplete work.
5. Record the next recommended task.

## Rate Limit Strategy

- Keep implementation tasks narrow enough to complete in one session.
- Avoid repeatedly loading large files when a focused section is enough.
- Use `rg` and targeted reads to recover context quickly.
- Keep architecture decisions in `docs/architecture/`.
- Keep active implementation state in `docs/implementation/status.md`.
- Keep coding standards in `docs/engineering/standards.md`.

## Resume Prompt

A future Codex session can resume with:

```text
Use $krishiv-engine. Read AGENTS.md and docs/implementation/status.md, then continue the next recommended task.
```

## Git Checkpoint Guidance

When git is initialized, prefer commits at durable boundaries:

- planning/setup docs complete
- workspace skeleton complete
- public API skeleton complete
- CLI command complete
- test slice complete

Each commit should leave `docs/implementation/status.md` accurate.
