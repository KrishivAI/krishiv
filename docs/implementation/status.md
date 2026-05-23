# Krishiv Implementation Status

## Current Phase

**Gap mitigation — PR #36 `cursor/gap-mitigation-7aa2` (merge in progress)**  
**R18 COMPLETE on `main` (2026-05-23)** — Storage format unification & time travel.

- Gap plan: [`docs/engineering/gap-mitigation-plan.md`](../engineering/gap-mitigation-plan.md)
- R18 tracker: [`r18-storage-format-unification.md`](r18-storage-format-unification.md)

## Gap mitigation (PR #36)

### Optional follow-ups landed on branch

| ID | Item |
|----|------|
| P2-7 | Nexmark Q1/Q2/Q5/Q8 benches via `SqlEngine` + in-memory Arrow tables |
| P1-10 | `IcebergFsTable` — Parquet layers + `metadata.json`, restart durable |
| P2-11 | `spark_compat` / `spark_compat_date` downcasts → `DataFusionError` |
| P2-12 | Typed `AggKey` + `AggFunction::Avg` (Float64 output) |
| P2-13 | `upgrade_compat` typed `CheckpointMetadata` deserialize + validate |
| P3-7 | Processing-time timer O(1) cancel via identity index |
| P3-8 | `SharedStateMigrationRegistry` poison → `StateError::LockPoisoned` |
| P3-9 | OpenAI `call_one` native async (no `spawn_blocking`) |
| P3-10 | LLM rate-limiter map poison recovery + error log |
| P3-11 | `TokenAwareChunker` binary search; `tiktoken` feature for `tiktoken-rs` |
| P3-12 | Memo keys `{content_hash}:{chunk_index}`; per-chunk RAG skip |
| P3-13 | `MemoEntry.created_at_ms` + TTL eviction on `get` |
| P3-26 | `FederationClient` `async_trait` methods |

### Still deferred (infra / large)

- Full Iceberg catalog + `object_store` remote backend (beyond FS Parquet table)
- ONNX / `krishiv-ai` integration tests in CI without libstdc++
- Full workspace `cargo test --workspace`

### Validation (gap branch)

```bash
cargo check --workspace
cargo test -p krishiv-exec -p krishiv-state -p krishiv-federation -p krishiv-lakehouse -p krishiv-upgrade-tests -p krishiv-ai --lib
cargo test -p krishiv-upgrade-tests
cargo test -p krishiv-sql spark_compat
```

Optional bench: `cargo bench -p krishiv-bench --bench nexmark`

## R18 Implementation (on main, 2026-05-23)

### Slices delivered

| Sprint | Deliverable |
|--------|-------------|
| **S1 Delta** | `krishiv-lakehouse`: `local_delta` + `DeltaTableHandle`, `write_delta`, `merge_delta`; `SqlEngine::read_delta`; Python `read_delta` |
| **S2 Hudi** | `HudiSnapshotReader` (snapshot + incremental), `write_hudi_cow_fixture`, `SqlEngine::read_hudi`, Python `read_hudi` |
| **S3 Catalog + SR** | `krishiv-catalog` Iceberg REST (`GenericRestCatalog`, `GlueRestCatalog`, `NessieCatalog`); new `krishiv-schema-registry` (Confluent Avro/JSON/Protobuf); Python catalog + `schema_registry_confluent` |
| **S4 Time travel** | `preprocess_as_of_sql` (`VERSION` / `TIMESTAMP` / `FOR SYSTEM_TIME AS OF`); `apply_as_of_refs` for delta tables |
| **S5 MERGE** | `execute_merge_sql` dispatches Delta (`merge_delta`) and in-memory Iceberg tables; `Session.sql` routes `MERGE INTO` |
| **S6 Partition evolution** | `PartitionSpecResolver` + `IcebergCatalogClient` partition field REST APIs |

### Notes

- **Delta write path**: Native `_delta_log` + Parquet writer in `local_delta.rs` (avoids `deltalake-rs` / workspace Arrow 58 mismatch). Idempotent merge-on-key in `merge_delta`.
- **Proto / coordinator**: unchanged in R18 (LLM quota from R17).
- **Acceptance gate**: Large-scale (1M row) and live Nessie CI tests remain environment-dependent; unit/integration tests cover local Delta/Hudi/SQL AS OF/MERGE/catalog mock.

### Validation (R18)

```bash
cargo test -p krishiv-lakehouse --lib
cargo test -p krishiv-catalog --lib
cargo test -p krishiv-schema-registry --lib
cargo test -p krishiv-sql --lib
cargo check -p krishiv-api -p krishiv-python
```

### R18 next tasks

- Optional: wire `deltalake-rs` when Arrow versions align; Nessie/Glue live CI; Python `write_delta` / `alter_table` partition helpers.

## Documentation Update (2026-05-23)

- Added a root `README.md` focused on end-user onboarding with quick start commands for CLI and API examples.
- Linked architecture roadmap and implementation trackers for release-aware adoption.
- Documented execution-mode selection guidance for Embedded/SingleNode/Distributed starts.

## Next command

Merge PR #36 after CI green.
