# Krishiv Implementation Status

## Production Stabilization Waves 0–3 (2026-06-05)

Implemented cross-cutting production hardening across Waves 0–3:

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

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test -p krishiv-common -p krishiv-plan -p krishiv-scheduler -p krishiv-runtime -p krishiv-connectors -p krishiv-checkpoint --lib
```

Blockers: none for this slice. Remaining follow-ups (not in this PR): coordinator sharding migration completion, broker-backed Kafka transactions, persistent catalog, UDF sandbox, object-store shuffle lease persistence.

Next useful commands:
```bash
cargo +nightly test --workspace --lib --no-fail-fast
KRISHIV_PRODUCTION=1 KRISHIV_API_KEYS='devkey=svc:admin' cargo +nightly run -p krishiv-flight-sql --bin krishiv-flight-server
```

---
