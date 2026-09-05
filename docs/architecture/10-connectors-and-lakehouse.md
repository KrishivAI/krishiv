# Connectors and lakehouse

`krishiv-connectors` is every way data enters or leaves the engine: sources,
sinks, exactly-once sinks, offsets, capability declarations, the driver
registry, CDC, data quality, vector sinks, and the lakehouse formats. This
document is the contract view; the certified matrix of what each connector
supports is generated (`../reference/certification-matrix.md`,
`../reference/connector-reachability-matrix.md`) and must not be hand-edited.

## Contracts

| Trait | Purpose |
|---|---|
| `Source` | `capabilities()`, `source_schema()` (known-ahead schema so views can register before data), `read_batch()` (`None` = exhausted, bounded only), `current_offset()`, `encoded_checkpoint_offset()`, `reset()` (rewindable sources must override; a no-op default logs a warning) |
| `CheckpointSource: Source` | typed `Offset`; `checkpoint_offset()` / `restore_offset()` must round-trip exactly — restoring an offset makes the next read return what it returned originally |
| `Sink` | `write_batch()`, `flush()` |
| `TwoPhaseCommitSink` | `prepare(epoch, batch) → Handle`, `commit`, `abort` (idempotent), `finalize_prepared(path, commit)` + `prepare_path_of` for crash recovery (`07`) |
| `TransactionalSinkParticipant` | the streaming form: `stage`, `pre_commit(epoch)`, `commit_through(epoch)`, `abort_after(epoch)` |
| `Offset` / `OffsetCommitter` | serialisable cursor; commit is separate from read so write → flush → commit ordering is explicit (`PostWriteOffsetCommitProtocol`) |
| `DynSource` / `DynSink` | object-safe adapters (blanket impls) for `Box<dyn …>` |

`ConnectorCapabilities` flags: `bounded` xor `unbounded`, `rewindable`,
`transactional`, `idempotent`, `supports_checkpoint`,
`supports_two_phase_commit`, `resumable_flush` (a Parquet sink finalises its
footer on flush and cannot continue; a CSV or Elasticsearch sink can — the
streaming path checks this before flushing per cycle). `DeliveryGuarantee`
(`best-effort`, `at-least-once`, `effectively-once`, `exactly-once`) is a
*capability*; the job's guarantee is the weakest across source, sink,
checkpoint storage, and profile. `ConnectorMaturity` is `experimental`,
`preview`, or `certified` (covered by the failure/recovery suite).

## Registry

`registry/` maps `ConnectorKind` (parsed from the string `kind` in a
`ConnectorConfig`) and `ConnectorRole` (`Source`, `Sink`, `TwoPhaseSink`,
`VectorSink`) to drivers: `SourceDriver`, `SinkDriver`, `TwoPhaseSinkDriver`,
`VectorSinkDriver`, each with `descriptor()`, `validate(config)`, `open(config)`.
`SourceDriver::estimated_row_count` lets a Parquet source answer from footers
without opening. `default_registry()` is what SQL `CREATE EXTERNAL TABLE …
OPTIONS (kind = …)`, `Session::register_connector_*`, Python, and MCP use.

`ConnectorRole::VectorSink` is present even without the `vector-sinks`
feature — the role is a tag, the driver is gated — so downstream `match`es do
not change shape with the feature (`15`, "Feature graph").

## Connector inventory

| Kind | Role | Feature | Notes |
|---|---|---|---|
| `parquet`, `parquet-directory` (Hive partition discovery), `csv` | source/sink | base | always built |
| `avro` | source | `avro` | |
| `s3`, `s3-prefix` | source | `cloud` (object stores: AWS, GCS, Azure) | |
| `two-phase-parquet` | 2PC sink | base | local Parquet with atomic rename; the certified R6 sink |
| `kafka` | source/sink | `kafka` (librdkafka) | offsets per partition; schema registry with `schema-registry` |
| `kafka-transactional` | 2PC sink | `kafka` | broker transactions per epoch |
| `kinesis`, `pulsar` | source | `kinesis`, `pulsar-source` | |
| `jdbc`, `jdbc-sink` | source/sink | `jdbc` (sqlx: Postgres, MySQL) | |
| `elasticsearch`, `cassandra`, `hbase` | sink | own features | |
| `iceberg`, `delta`, `hudi` | table I/O via `lakehouse` module | `lakehouse` (+ `iceberg`) | not opened through batch drivers |
| `memory-vector`, `lancedb`, `weaviate`, `pinecone`, `qdrant`, `pgvector` | vector sink | `vector-sinks` (+ `qdrant`/`pgvector`) | embeddings with metadata; ANN retrieval (`AnnTopKPrefilter` in `02`) |
| `vortex` | format | `vortex` | columnar format reader |

## Lakehouse

`lakehouse/` implements table formats natively in Rust:

- **Iceberg** — `IcebergNativeTwoPhaseCommit` over iceberg-rust's
  `Transaction` API: `prepare` writes Parquet under `{root}/data/`, `commit`
  `fast_append`s manifests and metadata atomically; `version-hint.text`
  tracks the current metadata for the file/object-store catalog, or an
  injected persistent catalog (REST, Postgres, Glue, Hive, Nessie, Polaris —
  `02-sql-engine.md`, "Catalogs") tracks it itself. `KrishivStorage` is the
  object-store `Storage` bridge (S3/MinIO). `IcebergStreamingSink` (G7) is
  the checkpoint-aligned participant with `Append` (one snapshot per epoch)
  and `Upsert` (copy-on-write by key columns and an op column; merge-on-read
  equality deletes arrive with iceberg-rust 0.10). `dml` covers
  `INSERT`/`UPDATE`/`DELETE`/`MERGE` on Iceberg tables; `maintenance` covers
  expire-snapshots, rewrite, orphan cleanup; `partitioned_write` implements
  `PARTITIONED BY` transforms with partition-aware fan-out;
  `DistributedIcebergCommitCoordinator` merges many executors' staged files
  into one commit; Kafka offsets are recorded in the snapshot summary
  (`KAFKA_OFFSETS_SUMMARY_KEY`) so source position and commit are one
  transaction. `AsOfSpec` gives snapshot / timestamp time-travel.
- **Delta Lake** — `DeltaObjectStoreReader`, `write_delta`, `merge_delta`,
  `LocalDeltaTwoPhaseCommitSink`, `read_table_at_timestamp`, `vacuum_table`.
- **Hudi** — copy-on-write reader/writer (`HudiCowWriter`,
  `HudiSnapshotReader`, `HudiQueryType`), `HudiTwoPhaseCommitSink`,
  `vacuum_hudi_table`.
- `PartitionSpecResolver` / `PartitionSpecVersion` for evolving partition
  specs; `SchemaVersion` and `LakehouseError::SchemaConflict` for evolution;
  `LakehouseError::Concurrency` for optimistic-concurrency conflicts.

The heavyweight Iceberg tree is opt-in and unreachable from an embedded build
by construction (`15`).

## CDC

`cdc/`: Debezium 2.x envelopes (`parse_debezium_envelope`: `c`/`u`/`d`/`r` →
`CdcOp::{Insert, Update, Delete, SnapshotRead}` with `before`/`after`
unpacked to columns and source LSN/ts) → `CdcEvent` → `build_batch_from_events`
→ `CdcToLakehousePipeline` into Iceberg, with schema evolution state and a
schema-registry format option. `RdkafkaCdcEventSource` reads from Kafka;
`CdcOffsetTracker` (feature `state`) persists consumed offsets. The
incremental engine consumes the same events as `ChangelogBatch`es (`09`).

## Data quality

`quality.rs`: `DataQualityRule::{NotNull, Range, Regex}` compiled once per
config (`CompiledDataQualityConfig`) and evaluated per batch
(`check_batch_compiled`); `QualityAction` routes failing rows to a dead-letter
sink or fails the batch. Two-phase sinks apply the rules before staging.

## Safety properties the layer enforces

- Ids and paths are validated before they touch a filesystem or object store.
- A rewindable source that does not implement `reset()` logs in every build.
- A checkpoint-capable source that does not expose encoded offsets returns
  `Unsupported`, not `None` — the mismatch is loud.
- A 2PC sink without durable prepare state cannot be used under a durable
  profile.
- Every `#[ignore = "requires …"]` integration test (live Postgres, MinIO,
  Kafka) runs in CI's external-service job (`17`).

## Related

- `07` (exactly-once), `08`/`09` (which engine consumes what), `02`
  (SQL DDL and catalogs), `15` (feature flags per connector).
