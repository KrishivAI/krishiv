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

## CI Quality Gates

Every change must pass the two CI gates before committing. Run them in this
order:

```bash
# 1. Formatting (rustfmt)
cargo fmt --check

# 2. Linting (clippy, all warnings treated as errors)
cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings
```

Auto-fix commands (use before the check commands):

```bash
cargo fmt                                                        # auto-format
cargo clippy --workspace --exclude krishiv-python \
    --exclude krishiv-chaos --fix --allow-dirty -- -D warnings  # auto-fix imports
```

### Common CI Failure Patterns and Fixes

**Unused imports** (most common after file splits or refactors)
- Run `cargo clippy --fix --allow-dirty` to auto-remove them.
- Do not add `#[allow(unused_imports)]`; just remove the import.

**Dead code — constants / functions only used in tests**
- Annotate with `#[cfg(test)]` instead of `#[allow(dead_code)]`.
- Also annotate any types they use that would then be import-only in non-test
  builds: `#[cfg(test)] use some_crate::SomeType;`

**Dead code — intentional public API placeholder**
- Use `#[allow(dead_code)]` only on `pub` items that form intentional API
  surface not yet wired up. Add a one-line comment explaining the future use.

**Very complex type** (clippy::type_complexity)
- Extract a `type Alias = …;` for the inner type used in struct fields,
  function signatures, or closures. Example:
  ```rust
  type BoundParamCache = Arc<Mutex<HashMap<String, LruCache<String, Vec<RecordBatch>>>>>;
  ```

**Duplicate definitions across split files**
- When a file is split, shared helpers (`key_group_range_for_task`, constants
  like `MAX_KEY_GROUPS`) must live in exactly one module.
- If callers are in the same module, keep the definition private.
- If callers are in sibling modules (or only in `#[cfg(test)]` blocks), use
  `pub(crate)` with `#[cfg(test)]` on the definition.
- Remove all duplicate definitions immediately; do not leave stale copies.

**`DEFAULT_*` constants defined but never used**
- Wire them as the `.unwrap_or(DEFAULT_CONSTANT)` fallback in the env-var
  reader function so the constant is actually used.

### Connector / Python Crate Specifics

- Concrete sink types (`CassandraSink`, `ElasticsearchSink`, `HBaseSink`) take
  `&RecordBatch`, NOT `RecordBatch`. Call `write_batch(&batch)` (borrow).
- Concrete sinks have no `flush()` method; do not call it.
- New Kafka transactional sinks use `BaseRecord` (not `FutureRecord`);
  `ThreadedProducer::send(record)` takes one arg, no timeout.
- Feature-gated Python sinks must return a friendly `RuntimeError` message
  naming the missing Cargo feature and the `maturin develop --features` command.

### `krishiv-python` Excluded from Clippy

`krishiv-python` is excluded from the workspace clippy run. Lint it
separately with `maturin develop` or `cargo check -p krishiv-python`.

## Build Notes (GCC 15)

- librocksdb-sys 0.16 (used by rocksdb 0.22) fails to compile with GCC 15
  (`uint64_t` not declared). Prepend `CXXFLAGS="-include cstdint"` to any
  `cargo build` / `just build-*` command that links rocksdb:
  ```bash
  CXXFLAGS="-include cstdint" just build-single-node
  CXXFLAGS="-include cstdint" cargo build -p krishiv --no-default-features --features single-node
  ```

## Python Examples

```bash
# Embedded mode (no cluster needed):
PYTHONPATH=crates/krishiv-python/python:$PYTHONPATH python3 examples/single-node/batch_example.py
PYTHONPATH=crates/krishiv-python/python:$PYTHONPATH python3 examples/single-node/streaming_example.py

# Cluster mode (after `krishiv local start`):
export KRISHIV_COORDINATOR=http://127.0.0.1:50051
PYTHONPATH=crates/krishiv-python/python:$PYTHONPATH python3 -c "
import krishiv as ks
session = ks.Session.connect('http://127.0.0.1:50051')
print(session.sql('SELECT 42 as answer').collect().pretty())
"
```

## Skill Files

- `skills/krishiv-engine/SKILL.md` — canonical skill (references this file)
- `skills/release/SKILL.md` — release orchestration skill
- `codex/skills/krishiv-engine/SKILL.md` — Codex shim → points to `skills/`
- `.claude/skills/krishiv-engine/SKILL.md` — Claude Code shim → points to `skills/`
