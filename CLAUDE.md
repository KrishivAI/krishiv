# Krishiv - Claude Code Instructions

See `AGENTS.md` for the shared project rules. See
`codex/skills/krishiv-engine/SKILL.md` for the shared Krishiv workflow.

## Common Commands

```bash
cargo check --workspace
cargo test --workspace
cargo test -p krishiv-scheduler --lib
cargo test -p krishiv-executor --lib
cargo test -p krishiv-runtime
cargo clippy --workspace --all-targets
cargo fmt --check
```

## Session Start

1. Read `AGENTS.md`.
2. Read `docs/README.md`.
3. Read `docs/implementation/status.md`.
4. Inspect the relevant crate before editing.
5. Pick one concrete task and one validation command.

## Skill Usage

Claude Code can use the project skill shim at
`.claude/skills/krishiv-engine/SKILL.md`:

```text
/krishiv-engine implement the requested Krishiv task
```

The shim points to `codex/skills/krishiv-engine/SKILL.md`, which is the
canonical skill source for Codex and Claude Code.

## End Of Session

For substantial work, update `docs/implementation/status.md` with:

- completed work
- validation run
- blockers, if any
- next useful command or task
