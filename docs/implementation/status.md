# Krishiv Implementation Status

This file is intentionally short. The codebase is the source of truth; use this
as a session handoff note, not as a release-plan archive.

## Current State

- Workspace is a Rust 2024 Cargo workspace with 30 active member crates.
- `krishiv-common` is the shared utility crate currently used by runtime and
  other crates.
- Removed crates still visible in git history include `krishiv-async-util`,
  `krishiv-sql-policy`, and `krishiv-upgrade-tests`.
- Runtime modes are embedded, single-node, and distributed via
  `krishiv-runtime`.
- Runtime placement is explicit via `ExecutionPlacement`: embedded and
  single-node may run `LocalInProcess`, single-node may use a `SingleNodeDaemon`,
  and distributed requires `RemoteClusterRequired`.
- Distributed local fallback is rejected during session/runtime construction
  rather than deferred until query execution.
- Distributed support uses scheduler, executor, proto, Flight SQL, and optional
  Kubernetes operator/manifests.
- The documentation set has been collapsed to `docs/README.md` plus this
  handoff file to avoid stale release-roadmap drift.

## Last Validation

Typed task fragment slice:

- Executor batch and streaming fragment execution now unwrap
  `krishiv_plan::TypedTaskFragment` at the boundary and execute the typed
  envelope body; legacy fragment strings still pass through unchanged.
- This closes a real mismatch where the scheduler could emit typed
  `krishiv-fragment:{...}` descriptions while executor fragment handlers still
  matched on legacy prefixes like `sql:` and `stream:`.
- Validation:
  - `cargo test -p krishiv-executor --lib executor_runs_typed_batch_fragment_body`
  - `cargo test -p krishiv-executor --lib executor_runs_typed_streaming_fragment_body`

Checkpoint async I/O slice:

- Added async primitives to `CheckpointStorage` while keeping sync wrappers for
  compatibility.
- Added async checkpoint helper functions for metadata, snapshots, manifests,
  epoch validation, and latest-epoch discovery.
- Added `CheckpointCoordinator::{receive_ack_async, commit_epoch_async,
  recover_from_storage_async}` and wired scheduler gRPC checkpoint acks through
  `Coordinator::handle_checkpoint_ack_async` instead of `block_in_place`.
- Remaining bottleneck: the gRPC path still holds the coordinator write guard
  across checkpoint commit I/O. The next slice should split commit preparation
  from durable storage writes so unrelated coordinator operations are not
  serialized behind object-store latency.
- Validation:
  - `cargo test -p krishiv-checkpoint --lib object_store_checkpoint_roundtrip`
  - `cargo test -p krishiv-scheduler --lib async_receive_ack_commits_epoch_without_blocking_wrapper`
  - `cargo test -p krishiv-scheduler --lib checkpoint_coordinator_initiates_and_collects_acks`

Execution placement slice:

- Added `ExecutionPlacement` to `krishiv-runtime` and wired `SessionBuilder`
  placement selection through `krishiv-api`.
- Removed implicit distributed local fallback; distributed mode now requires
  `RemoteClusterRequired` plus an explicit Flight coordinator URL.
- Single-node supports either `LocalInProcess` or `SingleNodeDaemon` placement.
- Updated the Flight SQL custom action listing to include `execute_plan`, which
  was already handled server-side and is required by typed remote plan submit.
- Validation:
  - `cargo test -p krishiv-runtime --lib execution_runtime::tests`
  - `cargo test -p krishiv-api distributed_session_rejects_disabled_remote_execution`
  - `cargo test -p krishiv-api embedded_read_parquet_collects_locally`
  - `cargo test -p krishiv-api remote_execution_without_fallback_uses_flight_server`
  - `cargo test -p krishiv-api with_coordinator`

## Next Useful Commands

```bash
cargo check --workspace
cargo test -p krishiv-api --lib
```
