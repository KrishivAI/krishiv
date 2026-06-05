# Krishiv Implementation Status

## Production Stabilization F1–F15 (2026-06-05)

Implemented full F1–F15 stabilization on branch `cursor/f1-f15-stabilization-dd55`:

### F1 — Coordinator auth & restore fencing
- `validate_runtime_security_config` now requires bearer tokens for `single-node-durable` and rejects `--insecure` gRPC on all durable profiles.
- Token file read failures fail startup via `validate_coordinator_bearer_token_sources`.
- Queued jobs rejected in durable/production profiles (fail-closed admission).
- gRPC `restore_job` passes live leader fencing token; durable restores fail without token validation.

### F2 — HTTP client auth
- All `coordinator_http_client` requests attach `Authorization: Bearer` from `KRISHIV_COORDINATOR_BEARER_TOKEN`.

### F3 — Executor gRPC & state
- Barrier gRPC wired with `ExecutorTaskAuthConfig`; durable profiles require task bearer token when task/barrier servers enabled.
- Checkpoint RPC state uses `FjallStateBackend::open_for_profile`; in-memory shuffle omitted outside dev-local.

### F4 — Kafka pipeline
- Durable profiles use `RdkafkaKafkaSource` with `KAFKA_BOOTSTRAP_SERVERS`; simulation connectors dev-only.
- Source throttle token-bucket enforced via `try_consume` (not log-only).

### F5 — Flight SQL routing
- Typed `ContinuousRegister` / `ContinuousPush` / `ContinuousDrain` proxy through coordinator HTTP when configured (matches `BoundedWindow`).

### F6–F8 — Durability guards
- `memory://` checkpoint URIs gated by `allows_memory_checkpoint_uri(profile)`.
- `flight_client::execute_remote_plan` SQL-comment fallback profile-gated.

### F9–F15 — API/SQL/operability
- `SessionBuilder::from_env` rejects embedded mode under durable profiles.
- `SqlEngine::with_in_memory_catalog` rejected in durable/production profiles.
- UDF sandbox production guard (`KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS` escape hatch).
- K8s lease simulation forbidden in production.
- Checkpoint storage commit failures increment `inc_checkpoint_failed` metrics.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test -p krishiv-scheduler -p krishiv-runtime -p krishiv-executor -p krishiv-flight-sql -p krishiv-api -p krishiv-udf -p krishiv-checkpoint --lib --no-fail-fast
```

Next useful command:
```bash
cargo +nightly test --workspace --lib --no-fail-fast --exclude krishiv-python
```

---

## Production Stabilization Sprint A–C + Final Slice (2026-06-05)

Completed end-to-end wiring and production guards on branch `cursor/production-stabilization-dd55` (merged via PR #57):

### Sprint A — Profile-aware fragments & auth
- `validate_job_fragments` wired into scheduler `validate_job()` via `resolve_durability_profile()`.
- Executor hot paths use `task_body_for_profile` / `decode_for_profile`.
- `set_allow_anonymous()` returns `Err` when `KRISHIV_PRODUCTION=1`.
- Executor CLI rejects `memory://` checkpoint URIs for durable profiles.

### Sprint B/C — Runtime & API gating
- Remote Flight SQL-comment fallback disabled outside dev-local.
- Alpha APIs gated for durable/production profiles.

### Final slice — workspace quality
- Fixed `block_on` for single-worker Tokio runtimes.
- Stabilized flaky redb/metrics tests under parallel runs.

---

## Production Stabilization Waves 0–3 (2026-06-05)

Merged via PR #56 — cross-cutting production hardening across security, durability, feature completion, and operability crates.
