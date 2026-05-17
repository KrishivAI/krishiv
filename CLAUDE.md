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

Invoke the Krishiv Engine skill in Claude Code with:

```
Use $krishiv-engine to implement the next Krishiv roadmap task.
```

The skill source is at [`codex/skills/krishiv-engine/SKILL.md`](codex/skills/krishiv-engine/SKILL.md).

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
| [`docs/engineering/codex-workflow.md`](docs/engineering/codex-workflow.md) | Session workflow and rate-limit strategy |

## End-Of-Session Checklist

Before ending a substantial session:
1. Update `docs/implementation/status.md` with completed work, next steps, and blockers.
2. Update the relevant release tracker checklist.
3. Record validation commands and results.
4. Commit at a durable boundary (feature + tests + checklist update).
