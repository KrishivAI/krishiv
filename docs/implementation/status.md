# Krishiv Implementation Status

## Production Stabilization Sprint A–C + Final Slice (2026-06-05)

Completed end-to-end wiring and production guards on branch `cursor/production-stabilization-dd55`:

### Sprint A — Profile-aware fragments & auth
- `validate_job_fragments` wired into scheduler `validate_job()` via `resolve_durability_profile()`.
- Executor hot paths use `task_body_for_profile` / `decode_for_profile` (batch, streaming, execution model).
- `set_allow_anonymous()` returns `Err` when `KRISHIV_PRODUCTION=1`; operator/coordinator call sites updated.
- Executor CLI rejects `memory://` checkpoint URIs for durable profiles (`validate_durable_startup`).
- Removed public `BarrierSimulator` export; production path is `BarrierInjector` + `TaskRunner::handle_initiate_checkpoint`.
- EO certification tests use `TransactionalKafkaSink::new_for_profile(DevLocal, ...)`.

### Sprint B/C — Runtime & API gating
- Remote Flight SQL-comment fallback disabled outside dev-local (`allows_remote_sql_comment_fallback`).
- Alpha APIs gated: `unbounded_memory_stream`, sliding/session windows, multi-source watermark (`allows_alpha_api`).
- `krishiv-plan` exports `validate_job_fragments`, `task_body_for_profile`; added `krishiv-proto` dependency.

### Final slice — workspace quality
- Fixed `block_on` for single-worker multi-thread Tokio runtimes (uses `block_in_place`).
- Fixed `temporal_join` schema assembly and zero-lookback eviction; repaired test batch helpers.
- Flight SQL `run_blocking` uses thread offload on current-thread runtimes.
- Stabilized flaky redb/metrics tests under parallel `--workspace` runs.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test --workspace --lib --no-fail-fast --exclude krishiv-python
cargo +nightly clippy --workspace --all-targets
```

Blockers: `krishiv-python` tests require system `libpython3.12` (excluded from workspace lib run).

Remaining follow-ups: coordinator sharding migration, broker-backed Kafka transactions, persistent catalog/lakehouse paths, UDF sandbox, object-store shuffle lease persistence.

Next useful commands:
```bash
cargo +nightly test -p krishiv-scheduler -p krishiv-executor -p krishiv-runtime -p krishiv-api --lib
KRISHIV_PRODUCTION=1 KRISHIV_DURABILITY_PROFILE=single-node-durable cargo +nightly run -p krishiv-executor -- --help
```

---

## Production Stabilization Waves 0–3 (2026-06-05)

Implemented cross-cutting production hardening across Waves 0–3 (merged via PR #56):

### Wave 0 — Security & data loss
- Added `krishiv-common::production` guards (`KRISHIV_PRODUCTION`, profile fail-closed helpers).
- Coordinator HTTP: bearer auth middleware for durable/production profiles; startup validation when HTTP enabled without tokens.
- `NonBlockingStoreHandle`: fail-closed writes (sync fallback instead of drop) wired from durability profile.
- Executor window fragments: pass `state_dir/<job_id>` into `execute_bounded_window`.
- Flight SQL: auth on handshake, prepared statements, DoAction; production requires `KRISHIV_API_KEYS`.
- UI: production fail-closed when token file unreadable.

### Wave 1 — Correctness & durability
- Typed task fragments: `TypedTaskFragment::decode_for_profile` rejects legacy strings in durable profiles.
- Object-store checkpoint writes: staging key + commit pattern.
- Kafka SQL: manual commit (no auto-commit) in durable/production profiles.
- `TransactionalKafkaSink::new_for_profile` rejects durable profiles.
- `S3Sink`: 1024-batch pending cap.
- `memory://` checkpoint URIs blocked in production mode.

### Wave 2 — Feature completion
- Remote streaming `accept_plan`: registers continuous stream via Flight instead of hard error.
- CEP operator: records `last_barrier_epoch` on barrier.
- SQL: non-SQL UDTF DDL rejected in production mode.
- `FjallStateBackend::open_for_profile` factory.

### Wave 3 — Operability
- Operator HTTP router uses `CoordinatorDaemonConfig::http_sidecar(DistributedDurable)` with auth.
- Re-exported `DurabilityProfile` from `krishiv-common` and `krishiv-scheduler`.

---
