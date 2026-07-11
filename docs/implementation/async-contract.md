# Async / Threading Contract (Phase 51)

Policed by lint, not convention, since 2026-07-10:

- `clippy::await_holding_lock` + `clippy::await_holding_refcell_ref` are
  **deny** workspace-wide (`Cargo.toml [workspace.lints.clippy]`) — the GAP-4
  class (std lock held across `.await`) cannot re-enter the tree.
- `krishiv_common::async_util::block_on` is in clippy `disallowed-methods`
  (`clippy.toml`). Only deliberate sync-surface boundary modules carry a
  file-level `#![allow(clippy::disallowed_methods)]`; everything else must
  stay async end-to-end.

## Which crates are async-native

| Tier | Crates | Rule |
|---|---|---|
| Async-native core | krishiv-scheduler, krishiv-executor, krishiv-shuffle, krishiv-flight-sql, krishiv-state, krishiv-connectors (I/O paths), krishiv-ui, krishiv-mcp | `async fn` end-to-end; never call `block_on`. Filesystem I/O on hot paths goes through `tokio::fs` or `spawn_blocking`. |
| Sync public surfaces (allow-listed bridges) | krishiv-api blocking wrappers (`dataframe.rs`, `session.rs`, `io.rs`, `catalog.rs`), `krishiv` CLI command modules, krishiv-python (pyo3 is sync), krishiv-runtime `ExecutionBackend` (sync trait, `block_on` at the I/O boundary), `etcd_metadata.rs` (sync `MetadataStore` trait over an async client), CDC/delta lakehouse adapters | May call `async_util::block_on`. Each file carries the module-level allow with a justification comment. |
| Program entries | `main.rs` files, bench harnesses | Own their runtime; `Runtime::block_on` at the entry point is normal and not restricted. |

## Why `async_util::block_on` and never `Handle::block_on`

`async_util::block_on` is re-entrancy-safe: called from inside a Tokio runtime
it routes the future to a dedicated fallback runtime
(`KRISHIV_FALLBACK_RUNTIME_THREADS`, default 2) instead of panicking or
deadlocking the worker. Raw `Handle::block_on` from an async context panics.
That safety is also why bridge calls are *tolerable* on cold paths (startup,
CLI): the cost is a thread handoff, not a stalled executor.

## `spawn_blocking` is mandatory for

- Filesystem walks / large file reads inside async request handlers.
- RocksDB compaction-triggering operations on async paths.
- Any CPU-bound loop over ~10ms inside an async handler.

## Adding a new bridge site

1. Justify why the surface must be sync (FFI, public blocking API, CLI).
2. Add the file-level `#![allow(clippy::disallowed_methods)]` with the
   standard two-line justification comment.
3. Never bridge on a per-request hot path — bridge once at the surface.
