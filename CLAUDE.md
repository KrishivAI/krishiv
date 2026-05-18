# Krishiv – Claude Code Instructions

See [AGENTS.md](AGENTS.md) for project-wide rules shared with all AI assistants.  
See [codex/skills/krishiv-engine/SKILL.md](codex/skills/krishiv-engine/SKILL.md) for the structured task workflow.

## Common Commands

```bash
# Build
cargo build --workspace

# Test (all crates)
cargo test --workspace

# Test (single crate)
cargo test -p krishiv-scheduler

# Lint
cargo clippy --workspace -- -D warnings

# Format check
cargo fmt --check

# Format apply
cargo fmt --all

# Quick type-check without codegen
cargo check --workspace
```

## Session Start Protocol

1. Read `AGENTS.md` and this file.
2. Read `docs/implementation/status.md` to recover the active task, blockers, and last validation.
3. Read the smallest relevant release tracker from `docs/implementation/README.md`.
4. Inspect the repo before editing; do not assume structure from memory.
5. Identify one concrete task and the validation command that proves it done.

## Skill Usage

Claude Code can use project skills that live under `.claude/skills/<skill-name>/SKILL.md`. This repository provides a Claude Code discovery shim at [`.claude/skills/krishiv-engine/SKILL.md`](.claude/skills/krishiv-engine/SKILL.md).

Invoke it in Claude Code with:

```
/krishiv-engine implement the next Krishiv roadmap task
```

The Claude Code skill shim points to the canonical shared skill source at [`codex/skills/krishiv-engine/SKILL.md`](codex/skills/krishiv-engine/SKILL.md), which remains the source of truth for both Codex and Claude Code. If Claude Code cannot auto-load project skills in a given environment, ask it to read `.claude/skills/krishiv-engine/SKILL.md` and then follow the canonical skill file.

## Crate Map

| Crate | Responsibility |
|-------|---------------|
| `krishiv-api` | Public Rust APIs: `Session`, `DataFrame`, `Stream` |
| `krishiv-cli` | CLI binary: `sql`, `explain`, `jobs`, `submit` |
| `krishiv-sql` | DataFusion integration and SQL compatibility |
| `krishiv-plan` | Logical/physical plan DAG structures |
| `krishiv-exec` | Arrow physical operator descriptors |
| `krishiv-runtime` | Embedded, single-node, and distributed runtime traits |
| `krishiv-proto` | Protobuf/gRPC contracts for the control plane |
| `krishiv-scheduler` | Active coordinator, job/task management, gRPC server |
| `krishiv-executor` | Executor process and task runner |
| `krishiv-operator` | Kubernetes operator and CRD reconciliation |
| `krishiv-ui` | Status HTTP API and web UI |

Dependency direction is strictly one-way. See [`docs/architecture/crate-map.md`](docs/architecture/crate-map.md).

## Key Docs

| Document | Purpose |
|----------|---------|
| [`docs/architecture/krishiv-roadmap.md`](docs/architecture/krishiv-roadmap.md) | 10-release implementation plan |
| [`docs/implementation/status.md`](docs/implementation/status.md) | Current phase, active task, blockers |
| [`docs/implementation/README.md`](docs/implementation/README.md) | Release tracker index |
| [`docs/engineering/standards.md`](docs/engineering/standards.md) | Rust, async, testing, crate-boundary standards |
| [`docs/engineering/codex-workflow.md`](docs/engineering/codex-workflow.md) | Shared Codex/Claude Code session workflow, rate-limit strategy, and resume protocol |

## Rate Limits And Session Resume

Claude Code should use the shared agent workflow in [`docs/engineering/codex-workflow.md`](docs/engineering/codex-workflow.md) for rate-limit-safe work units and cross-agent handoffs.

When a Claude Code session is interrupted or rate-limited:

1. Resume the previous Claude Code session when available.
2. Re-read `AGENTS.md`, this file, and `docs/implementation/status.md` before editing.
3. Continue only the next durable checkpoint recorded in `status.md`.
4. Update `status.md` with completed work, partial work, blockers, validation, and the next command before stopping again.

Safe resume prompt:

```text
/krishiv-engine resume. Read AGENTS.md, CLAUDE.md, and docs/implementation/status.md, then continue the next recommended task. Keep the work to one durable checkpoint and update status.md before stopping.
```

## End-Of-Session Checklist

Before ending a substantial session:
1. Update `docs/implementation/status.md` with completed work, next steps, and blockers.
2. Update the relevant release tracker checklist.
3. Record validation commands and results.
4. Commit at a durable boundary (feature + tests + checklist update).
