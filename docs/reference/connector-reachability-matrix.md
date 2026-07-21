# Krishiv connector reachability matrix

_Generated from `krishiv-connectors/src/reachability.rs` — do not edit by hand._

Which connector-kind drivers are dispatchable from each of Krishiv's four connector entry points: the registry-backed SQL `CREATE SOURCE`/`CREATE SINK` DDL (`sql_ddl`), the ad-hoc SQL job source/sink provider (`sql_job`), distributed batch/streaming jobs (`distributed_job`), and the Python sink surface (`python_sink`). `yes`/`no` states whether that surface dispatches to the kind's driver today, not whether the driver itself works. See the module doc on `reachability.rs` for exactly what each surface checks and why Python source reachability is not a column here.

| Kind | Role | Maturity | sql_ddl | sql_job | distributed_job | python_sink | Notes |
|---|---|---|---|---|---|---|---|
| `parquet` | source | preview | yes | yes | yes | n/a |  |
| `parquet-directory` | source | preview | yes | yes | yes | n/a |  |
| `csv` | source | preview | yes | yes | yes | n/a |  |
| `avro` | source | preview | yes | no | yes | n/a | not wired into the ad-hoc SQL job source allowlist |
| `s3` | source | preview | yes | yes | yes | n/a |  |
| `s3-prefix` | source | preview | yes | yes | yes | n/a |  |
| `kafka` | source | preview | yes | no | yes | n/a | not wired into the ad-hoc SQL job source allowlist |
| `iceberg` | source | preview | yes | no | yes | n/a | not wired into the ad-hoc SQL job source allowlist |
| `delta` | source | experimental | yes | no | yes | n/a | not wired into the ad-hoc SQL job source allowlist |
| `hudi` | source | experimental | yes | no | yes | n/a | not wired into the ad-hoc SQL job source allowlist |
| `kinesis` | source | preview | yes | no | yes | n/a | not wired into the ad-hoc SQL job source allowlist |
| `pulsar` | source | preview | yes | no | yes | n/a | not wired into the ad-hoc SQL job source allowlist |
| `jdbc` | source | preview | yes | no | yes | n/a | not wired into the ad-hoc SQL job source allowlist |
| `parquet` | sink | preview | yes | yes | yes | yes |  |
| `csv` | sink | preview | yes | yes | no | no | not an OutputContractDescriptor variant; no Python CSV sink pyclass |
| `avro` | sink | preview | yes | no | no | no | not wired into the ad-hoc SQL job sink allowlist; no OutputContractDescriptor variant |
| `s3` | sink | preview | yes | yes | yes | no | distributed reach is ObjectParquetSink: Parquet format written to an object-store path, not a generic S3 sink of arbitrary format |
| `kafka` | sink | preview | yes | no | yes | yes | not wired into the ad-hoc SQL job sink allowlist; distributed reach is the checkpoint-aligned two-phase-commit KafkaSink (Phase 55) |
| `iceberg` | sink | preview | yes | no | yes | yes | distributed reach is the checkpoint-aligned two-phase-commit IcebergSink (G7) |
| `delta` | sink | experimental | yes | no | no | no | no OutputContractDescriptor variant; no Python pyclass |
| `hudi` | sink | experimental | yes | no | no | no | no OutputContractDescriptor variant; no Python pyclass |
| `elasticsearch` | sink | preview | yes | no | no | yes | has both a registry driver and a Python pyclass, but no OutputContractDescriptor variant |
| `cassandra` | sink | preview | yes | no | no | yes | has both a registry driver and a Python pyclass, but no OutputContractDescriptor variant |
| `hbase` | sink | preview | yes | no | no | yes | has both a registry driver and a Python pyclass, but no OutputContractDescriptor variant |
| `jdbc-sink` | sink | preview | yes | no | no | no | no OutputContractDescriptor variant; no Python pyclass, despite jdbc being source-reachable everywhere else |
| `two-phase-parquet` | two-phase-sink | preview | no | no | no | no | registered in default_registry() as a TwoPhaseSink driver, but none of these four surfaces dispatch to the TwoPhaseSink role at all |
| `kafka-transactional` | sink | preview | no | no | no | no | ConnectorKind::KafkaTransactional exists and parses, but has NO driver registered in default_registry() today under any role — dormant/parked, a different gap class from the other rows above (those have a registered driver some surfaces just don't reach; this one is unreachable everywhere because nothing registers it) |
| `memory-vector` | vector-sink | experimental | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `qdrant` | vector-sink | preview | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `pgvector` | vector-sink | preview | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `lancedb` | vector-sink | experimental | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `weaviate` | vector-sink | experimental | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `pinecone` | vector-sink | experimental | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |

## Roles no surface in this matrix reaches

`two-phase-sink` (`two-phase-parquet`) and `vector-sink` (`memory-vector`/`qdrant`/`pgvector`/`lancedb`/`weaviate`/`pinecone`) each have registered drivers in `default_registry()`, but none of `sql_ddl` (only checks `Source`/`Sink` roles), `sql_job`, `distributed_job`, or `python_sink` dispatch to either role. Vector writes happen through a separate embedding-pipeline path not covered by this matrix.

