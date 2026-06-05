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

## Production Stabilization Waves 0–3 (prior slice)

See git history on the same branch for Wave 0–3 details (production guards, HTTP/Flight auth, fail-closed metadata, checkpoint staging, etc.).
