# Architecture Improvement Tracker

Issues identified via grounded codebase analysis (2026-06-12).
All items now implemented unless marked otherwise.

---

## #1 — Event-driven coordinator task dispatch ✅ DONE

**Problem:** `spawn_orchestration_loops` task-launch loop (`coordinator/mod.rs:573`) polled on a
fixed 500 ms tick. A 4-stage query paid up to 2 s idle latency between task completions.

**Root cause:** `coordinator.notify: Arc<Notify>` already fired `notify_waiters()` on every state
change (job submit, task completion, executor registration) but the task-launch loop never
listened to it.

**Fix:** `coordinator/mod.rs` — grab the `Arc<Notify>` once before the loop, then add
`notify.notified()` as a third arm of the `tokio::select!`. The 500 ms tick stays as a fallback
sweep so a missed notification never permanently stalls the queue.

---

## #2 — Tiered shuffle as distributed-durable default ✅ DONE

**Problem:** `DurabilityProfile::DistributedDurable` mapped to `ShuffleDurability::ObjectStore`
(pure S3). Shuffle reads always round-tripped through S3 even when the writing executor was
one network hop away.

**Fix:**
- `krishiv-common/src/durability.rs` — change distributed-durable spec to `ShuffleDurability::Tiered`.
- `krishiv-shuffle/src/storage_uri.rs` — add `open_tiered_shuffle_backend(local_dir, s3_uri)` that
  builds `TieredShuffleStore(LocalDiskShuffleStore, ObjectStoreShuffleStore)`.
- `krishiv-executor/src/cli.rs` — when both `--shuffle-dir` and an `s3://` URI are set, auto-build
  the tiered backend instead of pure object-store.

The tiered store already existed and was tested; it just wasn't wired as the default.

---

## #3 — `/tmp` defaults replaced with `/var/lib/krishiv` ✅ DONE

**Problem:** `single-node-durable` auto-selected `/tmp/krishiv-shuffle`, `/tmp/krishiv-state`, and
`file:///tmp/krishiv-checkpoints`. On most Linux installs `/tmp` is `tmpfs` or cleared by
`systemd-tmpfiles`, so the profile's `restart_durable: true` claim held for process restart but
not host reboot.

**Fix:**
- `krishiv-executor/src/cli.rs` — `apply_shuffle_defaults` and `apply_state_default` now default to
  `/var/lib/krishiv/shuffle` and `/var/lib/krishiv/state`.
- Default checkpoint URI changed from `file:///tmp/krishiv-checkpoints` to
  `file:///var/lib/krishiv/checkpoints`.

Matches the systemd unit files which already used `/var/lib/krishiv`.

---

## #4 — Concurrent shuffle reads ✅ DONE

**Problem:** `read_shuffle_flight_partitions` (`executor/src/fragment/common.rs`) fetched
partitions sequentially in a `for` loop, materializing all batches into `Vec<RecordBatch>` before
registration.

**Fix:** Use `FuturesUnordered` (from `futures` crate, now added to executor deps) to drive all
partition fetches concurrently. Results are merged into the same `BTreeMap<table_name, batches>`
structure. The first error still aborts the task.

---

## #5 — etcd per-record keys + fix persist mechanism ✅ DONE

**Problem (scalability):** `EtcdMetadataStore::persist()` re-encoded ALL jobs + executors into a
single JSON blob on every `save_job` / `save_executor` call. This is O(total_jobs) per write and
bounded by the 1.5 MiB etcd single-key limit.

**Problem (runtime):** `persist` used `spawn_blocking` + a freshly constructed
`tokio::runtime::Builder::new_current_thread()` per call — two levels of nested runtimes plus
a wasted thread pool startup on the hot path.

**Fix:** `krishiv-scheduler/src/etcd_metadata.rs` fully rewritten:
- Per-record keys: `/krishiv/jobs/<job_id>` and `/krishiv/executors/<executor_id>`.
- `save_job` / `save_executor` write only the changed record — O(1) regardless of cluster size.
- `remove_executor` deletes the key directly.
- Startup loads all records via `get(prefix, GetOptions::new().with_prefix())`.
- `put_key` / `delete_key` use `block_in_place(|| Handle::current().block_on(...))` — parks the
  current worker so the etcd client's internally-spawned gRPC streams run on other workers.
- `append_event` no longer persists to etcd (events are audit-only; no behavioral change).

---

## #6 — Async-first traits (MetadataStore, CheckpointStorage) ⚠️ DEFERRED

**Problem:** Both traits are sync, forcing `block_in_place` bridges at every call site. This parks
a worker thread per call and requires multi-thread runtime.

**Status:** Deferred. A full trait flip to async is invasive (all 4 impls × all call sites) and
risks breaking the scheduler's write-lock semantics. The immediate `block_in_place` fix in #5 is
the correct short-term mitigation. The async-trait flip should be done as a dedicated session
with focused migration of `InMemoryMetadataStore`, `RocksDbMetadataStore`, and
`EtcdMetadataStore` in parallel.

**Next step command:**
```bash
cargo check -p krishiv-scheduler  # baseline before the async trait flip
```

---

## #7 — Naming and doc drift ✅ DONE

**Problems fixed:**
- `docs/README.md` and `docs/architecture.md` listed `SingleNode + LocalInProcess` as a valid
  mode pair; the code (`execution_runtime.rs:613-616`) rejects it.
- `docs/architecture.md` durability profile table listed "Redb" for state (actual impl: Fjall LSM,
  named `LocalRedb` only historically).
- `docs/architecture.md` distributed-durable shuffle listed as `Object store` (now `Tiered`).

**RocksDB feature gate:** `krishiv-state` compiles both `FjallStateBackend` and `RocksDbStateBackend`
unconditionally. Feature-gating RocksDB would cut binary size but requires updating all
downstream dep declarations. Left for a focused cleanup session.

---

## #8 — Bare-metal systemd deploy mismatch ✅ DONE

**Problem:** `deploy/systemd/krishiv-clusterd.service` used `--metadata-backend json` with no
`KRISHIV_DURABILITY_PROFILE`, making it effectively a dev-local deployment (no restart durability,
no fencing) despite being intended as a production bare-metal artifact.

**Fix:** Both unit files updated:
- `KRISHIV_DURABILITY_PROFILE=single-node-durable` set in environment.
- `--metadata-backend redb --metadata-path /var/lib/krishiv/metadata.db` for durable metadata.
- `krishiv-executor@.service`: explicit `--shuffle-dir /var/lib/krishiv/shuffle`,
  `--state-dir /var/lib/krishiv/state-%i`, `--checkpoint-uri file:///var/lib/krishiv/checkpoints`.
- Per-instance state dirs (`state-%i`) prevent multiple executors on the same host from sharing
  a state directory.

---

## Validation commands

```bash
cargo check -p krishiv-common
cargo check -p krishiv-shuffle
cargo check -p krishiv-executor
cargo check -p krishiv-scheduler
cargo test -p krishiv-common --lib
cargo test -p krishiv-shuffle --lib
cargo test -p krishiv-executor --lib
cargo test -p krishiv-scheduler --lib
```
