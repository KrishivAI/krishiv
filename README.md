![Krishiv logo](docs/assets/krishiv-logo.svg)

# Krishiv

Krishiv is a Rust-native hybrid compute framework for batch SQL, streaming
pipelines, and lakehouse-oriented data work.

The codebase is organized around one runtime model across embedded, single-node,
and distributed execution. Apache Arrow is the columnar data model, and
DataFusion is the SQL and local execution foundation.

## Current Docs

The documentation is intentionally small:

- `docs/README.md` - current architecture, crate map, commands, and engineering rules.
- `docs/implementation/status.md` - short session handoff note.
- `AGENTS.md` - shared AI-agent workflow.
- `CLAUDE.md` - Claude Code entry point.

Older release roadmaps, review reports, and planning trackers were removed to
avoid drift from the current code.

## Quick Start

```bash
cargo check --workspace
cargo run -p krishiv -- sql --query "select 1 as value"
cargo run -p krishiv -- explain --query "select 1 as value"
cargo run -p krishiv -- jobs
```

Run focused tests while iterating:

```bash
cargo test -p krishiv-runtime
cargo test -p krishiv-scheduler --lib
cargo test -p krishiv-executor --lib
```

## Workspace

Primary crates:

- `krishiv` - CLI and user-facing facade.
- `krishiv-api` - Session, DataFrame, Stream API.
- `krishiv-sql` - DataFusion integration and SQL helpers.
- `krishiv-plan` - logical/physical plans and task fragments.
- `krishiv-runtime` - embedded, single-node, and distributed runtime routing.
- `krishiv-scheduler` - coordinator, metadata, leadership, task lifecycle.
- `krishiv-executor` - executor process and task runner.
- `krishiv-dataflow` - Arrow operator runtime.
- `krishiv-shuffle`, `krishiv-state` - data-plane services (checkpoint merged into state).
- `krishiv-connectors` - I/O, sinks, and lakehouse helpers (Iceberg/Delta/Hudi via `lakehouse` feature); vector sinks live in `krishiv-ai::vector_sinks`.
- `krishiv-operator`, `krishiv-ui`, `krishiv-flight-sql`, `krishiv-python` - deployment and interface surfaces.

See `docs/README.md` for the full crate map.

## Contributing

1. Read `AGENTS.md` and `docs/README.md`.
2. Inspect the relevant crate before editing.
3. Keep changes scoped and add focused tests.
4. Run the narrowest useful validation command.
5. For substantial work, update `docs/implementation/status.md`.
