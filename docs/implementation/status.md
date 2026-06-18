# Krishiv Implementation Status

## 2026-06-18 — Tier 1 implementation: scheduler, state/shuffle, connectors, delta, dataflow

Completed the bulk of Tier 1 correctness fixes from the cross-crate audit:

### Tier 1A — Scheduler fixes (#1–#6, #71)

- **#1/#2**: Fixed lock-order deadlock in `grpc.rs` checkpoint_ack and restore_job — `checkpoint_inner`
  guard now dropped before acquiring `coordinator.write()`. Restructured `restore_job` to acquire
  `coordinator` first, then `checkpoint_inner` (matching the documented 4-level lock order).
- **#2 (barrier path)**: Deferred `on_checkpoint_epoch_committed` (FS I/O) to outside the coordinator
  write lock in `drive_barrier_dispatches`. Added `apply_barrier_acks_deferred` method.
- **#3 (stall detection)**: Added `last_progress_ms` to `TaskRecord`, refreshed on output metadata and
  progress messages. `collect_stall_cancel_work` now compares against `last_progress_ms` (falling back
  to `assigned_at_ms`), so long-running tasks with progress are not killed.
- **#4 (StaleEpoch)**: Both sync and async paths now return `Accepted` for `Ok(false)` (ack recorded
  but quorum pending), matching the sharded path. Barrier fanout no longer logs N-1 spurious rejects.
- **#5 (circuit breaker)**: Replaced `tokio::spawn` with synchronous
  `clear_assignments_for_bad_executor_and_count_sync` under the coordinator write lock.
- **#6 (leadership)**: Added `lease_duration_s()` to `LeaderElection` trait; `run_leader_loop` uses
  `lease_duration / 3` as the renew interval (down from hardcoded 5s).
- **#71 (NTP)**: (partial — stall detection uses `last_progress_ms` as a programmatic hedge).
- **4 regression tests** added: non-quorum ack returns Accepted, deferred ack returns post-commit,
  circuit breaker clears sync, stall detection respects progress.
- **Scheduler tests: 314/314 passing.**

### Tier 1B — State/Checkpoint/Shuffle (#7, #8, #10, #11, #12)

- **#7 (TTL load_snapshot)**: Changed crash semantics from clear-then-insert (empty on crash) to
  insert-then-delete-orphans (superset on crash — never empty).
- **#8 (SavepointCoordinator delete)**: Added `with_storage(Arc<dyn CheckpointStorage>)` constructor;
  `delete_savepoint` now also removes the durable `savepoints/{epoch}/` copy via `io::delete_savepoint`.
- **#10 (tiered fallback)**: `TieredShuffleStore` now falls back to remote on local
  `ContentHashMismatch`, not just clean misses. Added `is_corruption_error` helper.
- **#11 (MemoryBudget)**: `SpillableShuffleBackend` now checks `try_reserve` return value; removed
  the broken `read_partition` budget release (releases on cloning reads undercounted memory).
- **#12 (blocking FS)**: `resolve_lease_token_async` added — filesystem operations (lease read/persist)
  offloaded to `spawn_blocking`. `TieredShuffleStore::write_partition` changed from `tokio::try_join!`
  to a `select!` loop that awaits local write to completion even if remote fails.
- **Shuffle tests: 132/132 passing. State tests: 301/301 passing.**

### Tier 1C — Connectors EOS (#13, #14)

- **#13 (Kafka txn sink)**: Added `with_timeout` constructor, `transactional_id` helper, epoch
  monotonicity validation, and one-outstanding-handle enforcement (rejects second `prepare` while
  transaction is open). Configurable `transaction.timeout.ms`.
- **#14 (Pulsar ack)**: Messages are now acked after appending to the batch in `next_batch`.

### Tier 1D — IVM/Delta (#27, #30, #40)

- **#27 (Trace consolidation)**: Changed `cascade_merge`/`consolidate`/`snapshot` in trace.rs to pass
  `&[]` (all columns) to `consolidate_batch` instead of `&self.key_col_names` — fixes silent row loss
  on foreign-key join sides with multiple rows per key.
- **#30 (Join cross term)**: Added `ΔA⋈ΔB` cross term to `IncrementalJoinOp::apply` — same-tick
  inserts on both sides now produce output.
- **#40 (DefaultHasher)**: Replaced `DefaultHasher` with `XxHash64::with_seed(0)` in `io.rs` for
  deterministic partition assignment across restarts.
- **Delta tests: 58/58 passing. IVM tests: 3/3 passing.**

### Tier 1E — Dataflow (#37)

- **#37 (barrier channel)**: Changed barrier channel from bounded (`mpsc::channel(64)`) to unbounded
  (`mpsc::unbounded_channel`) — matches the module-level doc contract and prevents checkpoint-protocol
  deadlock when 64+ barriers queue up.

### CI gate
- `cargo fmt --check` — clean.
- `cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings` — clean.

### Still pending
- Tier 1C: #15 (Parquet sink), #16 (Iceberg snap_counter), #17 (two_phase abort_after),
  #18 (CDC ordering), #19 (Kinesis)
- Tier 1D: #25, #26, #28, #29, #31, #32, #34
- Tier 1E: #35, #36, #38, #39, #41
- Tier 1 gate: `cargo test --workspace --exclude krishiv-python --exclude krishiv-chaos`
- Tiers 2-4

### Next useful command
```bash
cargo test -p krishiv-scheduler --lib
```
