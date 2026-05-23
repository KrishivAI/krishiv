# Krishiv Implementation Status

## Current Phase

**R18 COMPLETE (2026-05-23)** — Storage format unification & time travel.

Release tracker: [`r18-storage-format-unification.md`](r18-storage-format-unification.md)

Branch: `cursor/r18-storage-unification-7aa2`

## R18 Implementation (2026-05-23)

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

### Validation

```
cargo test -p krishiv-lakehouse --lib
cargo test -p krishiv-catalog --lib
cargo test -p krishiv-schema-registry --lib
cargo test -p krishiv-sql --lib
cargo check -p krishiv-api -p krishiv-python
```

### Next Task

- Optional: wire `deltalake-rs` when Arrow versions align; Nessie/Glue live CI; Python `write_delta` / `alter_table` partition helpers.
