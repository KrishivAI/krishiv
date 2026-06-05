# Krishiv Implementation Status

## Full Stabilization Waves 1–4 (2026-06-05)

Implemented Waves 1–4 on branch `cursor/full-stabilization-dd55` (continues PR #59):

### Wave 1 — Shuffle leases & wiring
- Durable shuffle lease sidecars (`.lease` / object-store sidecars) with monotonic validation and restart tests.
- `open_shuffle_backend_from_uri` for `file://`, `s3://`, `memory://`.
- Executor `--shuffle-uri` / `KRISHIV_SHUFFLE_URI` wired for distributed-durable object-store shuffle.
- Profile-aware UDF guards in `krishiv-udf`, `krishiv-sql` (`sync_scalar_udfs` / `sync_aggregate_udfs`), `krishiv-api` session registration, and CREATE FUNCTION stubs.

### Wave 2 — CEP partial state
- `CepOperator::persist_to_state` / `restore_from_state` plus JSON snapshot helpers for checkpoint metadata.

### Wave 3–4 — Observability & profile guards
- `GET /api/v1/jobs/{job_id}/diagnose` returns structured `ObservabilityReport`.
- `inc_checkpoint_committed` metrics on checkpoint quorum (sync) and finalize (async).
- Window operator watermark persistence across tumbling/sliding/session restore paths.
- Flight SQL, UI, and K8s lease simulation guards use durability-profile helpers (not production-only).

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test --workspace --lib --no-fail-fast --exclude krishiv-python
```

---

## Full Stabilization Wave 0 (2026-06-05)

Implemented Wave 0 P0 fixes on branch `cursor/full-stabilization-dd55`:

### Security & metadata durability
- JCP federation HTTP submit/poll attach coordinator bearer tokens.
- Non-terminal task metadata saves are synchronous under durable profiles.
- `SingleNodeLeader` bumps fencing token only on fresh leadership acquisition.
- Operator controller opens `RedbMetadataStore` from `KRISHIV_METADATA_PATH` with fail-closed writes.
- Metadata store `flush()` waits for in-flight background writes.

### Barriers & checkpoints
- Barrier gRPC auth matches task gRPC (token configured ⇒ required).
- Barrier stream acks deferred until checkpoint completion via `SharedBarrierAckRegistry`.
- Continuous executor gRPC stubs return `Rejected` instead of fake `Accepted`.

### Distributed execution
- `ExecutePlan` routes through coordinator HTTP in proxy mode; streaming uses typed plan nodes.
- `streaming_spec_from_plan` derives window specs from `PhysicalPlan` nodes (no hardcoded test tumbling).
- Flight client attaches bearer auth from `KRISHIV_FLIGHT_API_KEY` / `KRISHIV_API_KEY` / `KRISHIV_API_KEYS`.
- Continuous/bounded Flight fallbacks profile-gated like batch SQL fallback.

### Kafka & state
- SQL `register_kafka_source` respects manual commit under durable profiles.
- Kafka table loop calls `commit_current_offset` when auto-commit is disabled.
- `FjallStateBackend::ephemeral()` forbidden under durable profiles.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test --workspace --lib --no-fail-fast --exclude krishiv-python
```

Next useful task: Wave 1–2 shuffle lease persistence and CEP durable partial state.

---

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
