# AI Agent Workflow For Krishiv

## Purpose

This workflow makes Krishiv implementation resilient to rate limits, context resets, interrupted turns, and new agent sessions. It applies to both Codex and Claude Code. The repository should carry the project memory; chat history should be optional.

## Supported Agent Interfaces

Krishiv keeps a shared workflow plus interface-specific entrypoints:

| Agent | Entry Point | Shared Workflow Source | Interface Config |
|---|---|---|---|
| Codex | `AGENTS.md` and `Use $krishiv-engine ...` prompts | `codex/skills/krishiv-engine/SKILL.md` | `codex/skills/krishiv-engine/agents/openai.yaml` |
| Claude Code | `CLAUDE.md` and `/krishiv-engine ...` prompts | `.claude/skills/krishiv-engine/SKILL.md` shim to `codex/skills/krishiv-engine/SKILL.md` | `codex/skills/krishiv-engine/agents/claude.yaml` |

Both agents must treat `docs/implementation/status.md` as the durable handoff file and must update it before stopping a substantial session.

### Claude Code Skill Discovery

Claude Code discovers project skills from `.claude/skills/<skill-name>/SKILL.md`. Krishiv therefore includes `.claude/skills/krishiv-engine/SKILL.md` as a thin shim that points Claude Code to the canonical shared skill in `codex/skills/krishiv-engine/SKILL.md`. Use `/krishiv-engine` in Claude Code, and use `$krishiv-engine` in Codex.

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

At the beginning of a substantial Codex or Claude Code session:

1. Read `AGENTS.md`.
2. If using Claude Code, also read `CLAUDE.md`.
3. Read `docs/implementation/status.md`.
4. Read the smallest relevant implementation tracker, such as `docs/implementation/r3-connector-contracts.md` for current R3.1 work.
5. Inspect the current repo files before editing.
6. Identify the target release phase and one concrete task.
7. Confirm the validation command that will prove the task.

## During-Session Protocol

While working:

- Keep edits scoped to the active task.
- Update the relevant checklist when task status changes.
- Add or update tests with feature work.
- Prefer small, reviewable changes over broad rewrites.
- Record blockers in `docs/implementation/status.md` instead of relying on memory.
- When rate-limit pressure is likely, stop after the next coherent checkpoint instead of starting a broad refactor.

## End-Of-Session Protocol

Before ending a substantial session:

1. Update `docs/implementation/status.md`.
2. Update the relevant release tracker checklist.
3. Record validation commands and results.
4. Record known blockers or incomplete work.
5. Record the next recommended task.
6. Commit at a durable boundary when git is available.

## Rate Limit Strategy

Use the same repository-backed strategy for Codex and Claude Code:

- Keep implementation tasks narrow enough to complete in one session.
- Avoid repeatedly loading large files when a focused section is enough.
- Use `rg` and targeted reads to recover context quickly.
- Keep architecture decisions in `docs/architecture/`.
- Keep active implementation state in `docs/implementation/status.md`.
- Keep coding standards in `docs/engineering/standards.md`.
- Prefer a checklist update plus a small commit over leaving unstated chat-only progress.
- If an agent reports an impending or active rate limit, write the last completed step, current partial step, blocker, validation status, and next command into `docs/implementation/status.md` before stopping.

## Resume Protocols

### Codex

Use this prompt to resume a Codex session after a rate limit, context reset, or interruption:

```text
Use $krishiv-engine. Read AGENTS.md and docs/implementation/status.md, then continue the next recommended task. Keep the work to one durable checkpoint and update status.md before stopping.
```

Codex sessions must recover from repository state first: `AGENTS.md`, `docs/implementation/status.md`, the relevant tracker, then targeted source files.

### Claude Code

Use this prompt to resume Claude Code after a rate limit, context reset, or interruption:

```text
/krishiv-engine resume. Read AGENTS.md, CLAUDE.md, and docs/implementation/status.md, then continue the next recommended task. Keep the work to one durable checkpoint and update status.md before stopping.
```

Claude Code users can pair the prompt with Claude Code's built-in session-resume flows, then rely on the repository handoff if the previous transcript is unavailable or too costly to reload.

## Cross-Agent Handoff

When switching between Codex and Claude Code:

1. Prefer a clean git state or a small committed checkpoint.
2. Ensure `docs/implementation/status.md` names the current phase, active task, completed work, validation, blockers, and next task.
3. Keep the next task phrased so either agent can start from repo files alone.
4. Do not depend on Codex-only or Claude-only chat history.

## Git Checkpoint Guidance

When git is initialized, prefer commits at durable boundaries:

- planning/setup docs complete
- workspace skeleton complete
- public API skeleton complete
- CLI command complete
- test slice complete
- agent workflow or handoff update complete

Each commit should leave `docs/implementation/status.md` accurate.
