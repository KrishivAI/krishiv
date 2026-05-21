# R11 Stability, Correctness, and CLI Completeness Implementation Tracker

## Goal

Eliminate every confirmed bug and stub found in the R1–R10 crate-by-crate audit
(2026-05-21). R11 makes no new architectural promises — it delivers correctness
on the existing promise set: no silent panics on lock poisoning, no split-brain
from fencing-token bypasses, no stubbed CLI commands that claim to succeed
without doing anything, and a CDC pipeline that runs a real event loop.

## Scope

In scope:

- Lock-poisoning hardening across scheduler, API, and catalog crates.
- Fencing-token correctness fix (reject both stale and future tokens).
- `executor_channels` double-connect race elimination.
- Real implementations of four previously-stubbed CLI commands: `savepoint`,
  `restore`, `checkpoints list`, `state inspect`.
- `CdcToLakehousePipeline` real event loop behind a `CdcEventSource` trait.
- `ShuffleMetadata` maximum-partition cap (OOM guard).
- K8s operator lease-state TTL eviction (memory-leak guard).

Out of scope:

- New distributed features.
- New API surface beyond what the stubs implied.
- AQE coalescing improvements (deferred to R12).
- Full gRPC barrier transport (deferred to R12).
- Incremental materialized view maintenance (deferred to R12).
- Multi-table CDC fan-out with schema evolution (deferred to R12).
- Remote-coordinator CLI mode (`--coordinator URL`) — local in-process mode
  only for R11; remote gRPC CLI planned for R12.

## Dependencies

- R10 acceptance gate is complete.
- `cargo test --workspace` passes clean before R11 work begins.

## Audit Findings Addressed

### Critical (crash / split-brain)

| ID  | Crate                | Finding                                                                    |
|-----|----------------------|----------------------------------------------------------------------------|
| C1  | krishiv-scheduler    | `.lock().unwrap()` on job store — lock poisoning crashes coordinator       |
| C2  | krishiv-scheduler    | `.lock().unwrap()` on executor-channel cache — same poison risk            |
| C3  | krishiv-checkpoint   | `fencing_token < current` allows future-generation tokens (split-brain)    |
| C4  | krishiv-api          | `jobs()` swallows lock poisoning with `unwrap_or_default()`                |
| C5  | krishiv-catalog      | `.expect()` in DataFusion async schema provider — panics on poison         |

### High (correctness / reliability)

| ID  | Crate                | Finding                                                                    |
|-----|----------------------|----------------------------------------------------------------------------|
| H1  | krishiv-scheduler    | Double-connect race: two concurrent callers both miss channel cache        |
| H2  | krishiv-connectors   | `CdcToLakehousePipeline::run()` returns `Ok(())` silently (no Kafka loop)  |

### Medium (resource management)

| ID  | Crate                | Finding                                                                    |
|-----|----------------------|----------------------------------------------------------------------------|
| M1  | krishiv-shuffle      | `ShuffleMetadata` partition map has no size cap (OOM on large partition counts) |
| M2  | krishiv-operator     | K8s lease state accumulates indefinitely (long-running memory leak)        |

### Stubs → Real implementations

| ID  | Crate                | Finding                                                                    |
|-----|----------------------|----------------------------------------------------------------------------|
| ST1 | krishiv-cli          | `krishiv savepoint` prints "not yet implemented"                           |
| ST2 | krishiv-cli          | `krishiv restore` prints "full restore not yet implemented"                |
| ST3 | krishiv-cli          | `krishiv checkpoints list` always returns empty + "not yet implemented"    |
| ST4 | krishiv-cli          | `krishiv state inspect` prints "not yet implemented"                       |

## Architecture Deliverables

- [x] Add `CdcEventSource` trait to `krishiv-connectors` for testable CDC polling.
- [x] Document CLI local-mode limitation (remote coordinator CLI deferred to R12).
- [x] Add `max_partitions` cap parameter to `ShuffleMetadata`.
- [x] Add lease-entry TTL eviction to `K8sLeaseElection` state.

## Sprint 1 — Critical Lock Safety and Fencing

### S1.1: Scheduler lock poisoning (C1)

- [x] Replace `store.lock().unwrap()` at `lib.rs:1811` with `unwrap_or_else(|p| p.into_inner())`.
- [x] Replace `store.lock().unwrap()` at `lib.rs:2200` with `unwrap_or_else(|p| p.into_inner())`.

### S1.2: Executor-channel cache race + poisoning (C2, H1)

- [x] Change `executor_channels` field type from `Arc<std::sync::Mutex<…>>` to
  `Arc<tokio::sync::Mutex<…>>`.
- [x] In `get_or_connect_channel`, hold the `tokio::sync::Mutex` lock across
  the `connect().await` call so concurrent callers serialise on the first
  connection rather than both attempting it.

### S1.3: Fencing token validation (C3)

- [x] Change condition in `validate_fencing_token` from
  `metadata.fencing_token < current_token` to
  `metadata.fencing_token != current_token`.
  A future-generation token is just as invalid as a stale one.

### S1.4: API `jobs()` silent swallow (C4)

- [x] Replace `self.jobs.lock()…unwrap_or_default()` with
  `unwrap_or_else(|p| p.into_inner().snapshot())` so poisoning surfaces real
  data from the recovered guard rather than an empty list.

### S1.5: Catalog lock panics in async context (C5)

- [x] Replace `.expect("catalog read lock poisoned")` at `lib.rs:572, 581, 597`
  with `.unwrap_or_else(|p| p.into_inner())` — recovers the guard data
  instead of unwinding the executor task.

## Sprint 2 — CDC Real Event Loop

### S2.1: `CdcEventSource` trait

- [x] Add `pub trait CdcEventSource: Send` with `fn poll_events(&mut self, max: usize) -> Result<Vec<String>, String>`.
- [x] Add `InMemoryCdcEventSource` for tests and local development.

### S2.2: `run_with_source` real loop

- [x] Implement `CdcToLakehousePipeline::run_with_source` with a real
  poll → parse → batch → write loop driven by any `CdcEventSource`.
- [x] Update `run()` to return a clear `Err` directing callers to
  `run_with_source` (no more silent `Ok(())`).
- [x] Add tests covering the full loop path with `InMemoryCdcEventSource`.

## Sprint 3 — CLI Feature Completeness

### S3.1: `krishiv checkpoints list`

- [x] Add `krishiv-checkpoint` as a dependency of `krishiv-cli`.
- [x] Implement `run_checkpoints_list` using `LocalFsCheckpointStorage` +
  `list_valid_epochs` + `read_epoch_metadata` to produce a real epoch table.
- [x] Default storage path: `./krishiv-checkpoints`; override with `--storage-path`.

### S3.2: `krishiv restore`

- [x] Implement `run_restore` using `LocalFsCheckpointStorage` +
  `read_epoch_metadata` (or `latest_valid_epoch` when `--latest` is passed).
- [x] Print a structured restore plan: epoch, fencing token, source offsets,
  operator snapshot count. Actual restore requires a live coordinator (print
  advisory in that case).

### S3.3: `krishiv savepoint`

- [x] Implement `run_savepoint` using an in-process `Coordinator` +
  `trigger_checkpoint_for_job` + `CheckpointCoordinator::initiate_savepoint`.
- [x] For local mode (no running distributed job), return a clear error:
  "no streaming job `<ID>` found in local coordinator — use a remote
  coordinator endpoint (--coordinator, planned for R12)".

### S3.4: `krishiv state inspect`

- [x] Implement `run_state_inspect` using `LocalFsCheckpointStorage` to load
  the latest epoch's operator snapshot manifest and report namespace/key-count
  metadata per operator.
- [x] Return "no checkpoints found" (not "not yet implemented") when storage
  is empty.

## Sprint 4 — Resource Management Guards

### S4.1: `ShuffleMetadata` partition cap (M1)

- [x] Add `max_partitions: usize` field to `ShuffleMetadata` (default 65536).
- [x] `mark_pending` returns `Err(ShuffleError::TooManyPartitions)` when cap
  is exceeded.

### S4.2: K8s operator lease TTL eviction (M2)

- [x] Add `last_renewed_at: Option<std::time::Instant>` to `K8sLeaseState`.
- [x] `is_leader()` auto-evicts stale `true` state: if `last_renewed_at` is older than `lease_duration_s`, sets `is_leader = false` and returns `false`.
- [x] `try_acquire` and `renew` stamp `last_renewed_at = Some(Instant::now())` on success.
- [x] All `.unwrap()` calls on `state.lock()` changed to `unwrap_or_else(|p| p.into_inner())`.

## Test Checklist

- [x] `cargo clippy --workspace -- -D warnings` passes.
- [x] `cargo test -p krishiv-checkpoint` — fencing-token tests updated.
- [x] `cargo test -p krishiv-scheduler` — lock-safety paths covered.
- [x] `cargo test -p krishiv-api` — `jobs()` poison-recovery test.
- [x] `cargo test -p krishiv-catalog` — bridge lock-recovery test.
- [x] `cargo test -p krishiv-connectors` — CDC loop tests with `InMemoryCdcEventSource`.
- [x] `cargo test -p krishiv-cli` — all four previously-stubbed commands have tests.
- [x] `cargo test -p krishiv-shuffle` — partition-cap boundary test.
- [x] `cargo test -p krishiv-operator` — lease TTL eviction test.
- [x] `cargo test --workspace` — full suite passes.

## Acceptance Gate

R11 is complete when:

- [x] No `unwrap()` or `expect()` on `Mutex` lock results in non-test production code paths (outside of `unwrap_or_else` recovery pattern).
- [x] `validate_fencing_token` rejects both `<` and `>` mismatches.
- [x] All four CLI commands (`savepoint`, `restore`, `checkpoints list`, `state inspect`) return real output or a structured error — never "not yet implemented".
- [x] `CdcToLakehousePipeline::run_with_source` runs a real event loop verified by test.
- [x] `cargo test --workspace` passes with zero failures.
- [x] `cargo clippy --workspace -- -D warnings` passes.

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Recovering from a poisoned lock may expose partial write | `unwrap_or_else(|p| p.into_inner())` gives the data at the point of panic; callers already have warn-on-failure semantics for store writes so partial data is acceptable |
| Holding `tokio::sync::Mutex` across connect serialises all gRPC channel creation | Connections are rare (one per executor, not per RPC); serialisation cost is negligible compared to TCP handshake latency |
| CLI local-mode savepoint always fails (no running job) | Expected and documented; remote coordinator mode planned for R12 |
| ShuffleMetadata cap may reject valid large jobs | Default cap of 65536 partitions covers all known workloads; configurable |
