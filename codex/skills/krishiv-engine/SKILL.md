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

## CI Quality Gates

Every change must pass both gates before committing. Run in this order:

```bash
# 1. Formatting
cargo fmt --check

# 2. Linting (all warnings treated as errors)
cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings
```

Auto-fix commands (run before the check commands):

```bash
cargo fmt
cargo clippy --workspace --exclude krishiv-python \
    --exclude krishiv-chaos --fix --allow-dirty -- -D warnings
```

### Common CI Failure Patterns and Fixes

**Unused imports** — run `cargo clippy --fix --allow-dirty` to auto-remove. Never add `#[allow(unused_imports)]`.

**Dead code — test-only constants/functions** — annotate with `#[cfg(test)]` instead of `#[allow(dead_code)]`. Also annotate any types that become import-only outside tests: `#[cfg(test)] use some_crate::SomeType;`

**Dead code — intentional public API placeholder** — use `#[allow(dead_code)]` only on `pub` items not yet wired up. Add a one-line comment explaining future use.

**Very complex type** (`clippy::type_complexity`) — extract a `type Alias = …;` for inner types used in struct fields or function signatures.

**Duplicate definitions across split files** — when a file is split, shared helpers must live in exactly one module. Use `pub(crate)` with `#[cfg(test)]` when callers are only in test blocks. Remove all duplicate definitions immediately.

**`DEFAULT_*` constants defined but never used** — wire them as `.unwrap_or(DEFAULT_CONSTANT)` in the env-var reader function.

### Connector / Python Crate Specifics

- Concrete sink types (`CassandraSink`, `ElasticsearchSink`, `HBaseSink`) take `&RecordBatch`, NOT `RecordBatch`. Call `write_batch(&batch)` (borrow).
- Concrete sinks have no `flush()` method; do not call it.
- New Kafka transactional sinks use `BaseRecord` (not `FutureRecord`); `ThreadedProducer::send(record)` takes one arg, no timeout.
- Feature-gated Python sinks must return a friendly `RuntimeError` message naming the missing Cargo feature and the `maturin develop --features` command.

`krishiv-python` is excluded from the workspace clippy run. Lint it separately with `maturin develop` or `cargo check -p krishiv-python`.

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
```
