# Krishiv connector reachability matrix

_Generated from `krishiv-connectors/src/reachability.rs` — do not edit by hand._

Which connector-kind drivers are dispatchable from each of Krishiv's four connector entry points: the registry-backed SQL `CREATE SOURCE`/`CREATE SINK` DDL (`sql_ddl`), the ad-hoc SQL job source/sink provider (`sql_job`), distributed batch/streaming jobs (`distributed_job`), and the Python sink surface (`python_sink`). `yes`/`no` states whether that surface dispatches to the kind's driver today, not whether the driver itself works. See the module doc on `reachability.rs` for exactly what each surface checks and why Python source reachability is not a column here.

| Kind | Role | Maturity | sql_ddl | sql_job | distributed_job | python_sink | Notes |
|---|---|---|---|---|---|---|---|
| `parquet` | source | preview | yes | yes | yes | n/a |  |
| `parquet-directory` | source | preview | yes | yes | yes | n/a |  |
| `csv` | source | preview | yes | yes | yes | n/a |  |
| `avro` | source | preview | yes | yes | yes | n/a |  |
| `s3` | source | preview | yes | yes | yes | n/a |  |
| `s3-prefix` | source | preview | yes | yes | yes | n/a |  |
| `kafka` | source | preview | yes | yes | yes | n/a |  |
| `iceberg` | source | preview | yes | yes | yes | n/a |  |
| `delta` | source | experimental | yes | yes | yes | n/a |  |
| `hudi` | source | experimental | yes | yes | yes | n/a |  |
| `kinesis` | source | preview | yes | yes | yes | n/a |  |
| `pulsar` | source | preview | yes | yes | yes | n/a |  |
| `jdbc` | source | preview | yes | yes | yes | n/a |  |
| `parquet` | sink | preview | yes | yes | yes | yes |  |
| `csv` | sink | preview | yes | yes | yes | yes | distributed reach is the batch registry-sink export (at-least-once, flushed before task success); no checkpoint-aligned streaming sink for this kind |
| `avro` | sink | preview | yes | yes | yes | yes | distributed reach is the batch registry-sink export (at-least-once, flushed before task success); no checkpoint-aligned streaming sink for this kind |
| `s3` | sink | preview | yes | yes | yes | yes | distributed reach is ObjectParquetSink (Parquet format written to an object-store path, with the staged-commit protocol) plus the generic batch registry-sink export; not a checkpoint-aligned streaming sink |
| `kafka` | sink | preview | yes | yes | yes | yes | distributed reach is the checkpoint-aligned two-phase-commit KafkaSink (Phase 55): exactly-once for read_committed consumers |
| `iceberg` | sink | preview | yes | yes | yes | yes | distributed reach is the checkpoint-aligned two-phase-commit IcebergSink (G7) |
| `delta` | sink | experimental | yes | yes | yes | yes | distributed reach is the batch registry-sink export (at-least-once, flushed before task success); no checkpoint-aligned streaming sink for this kind |
| `hudi` | sink | experimental | yes | yes | yes | yes | distributed reach is the batch registry-sink export (at-least-once, flushed before task success); no checkpoint-aligned streaming sink for this kind |
| `elasticsearch` | sink | preview | yes | yes | yes | yes | distributed reach is the batch registry-sink export (at-least-once, flushed before task success); no checkpoint-aligned streaming sink for this kind |
| `cassandra` | sink | preview | yes | yes | yes | yes | distributed reach is the batch registry-sink export (at-least-once, flushed before task success); no checkpoint-aligned streaming sink for this kind |
| `hbase` | sink | preview | yes | yes | yes | yes | distributed reach is the batch registry-sink export (at-least-once, flushed before task success); no checkpoint-aligned streaming sink for this kind |
| `jdbc-sink` | sink | preview | yes | yes | yes | yes | distributed reach is the batch registry-sink export (at-least-once, flushed before task success); no checkpoint-aligned streaming sink for this kind |
| `two-phase-parquet` | two-phase-sink | preview | no | no | no | no | registered in default_registry() as a TwoPhaseSink driver, but none of these four surfaces dispatch to the TwoPhaseSink role at all |
| `kafka-transactional` | two-phase-sink | preview | no | no | no | no | was dormant (a kind that parsed but had zero driver registration under any role); #197 registered KafkaTransactionalSinkDriver, so it is now a real TwoPhaseSink-role driver backed by RdkafkaTransactionalSink. Still unreachable from these four surfaces for the same reason as two-phase-parquet: none of them dispatch to the TwoPhaseSink role. The engine's exactly-once Kafka output ships through the streaming KafkaSink output contract instead, which drives the same underlying sink |
| `memory-vector` | vector-sink | experimental | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `qdrant` | vector-sink | preview | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `pgvector` | vector-sink | preview | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `lancedb` | vector-sink | experimental | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `weaviate` | vector-sink | experimental | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |
| `pinecone` | vector-sink | experimental | no | no | no | no | VectorSink role — see "Roles no surface in this matrix reaches" below |

## Roles no surface in this matrix reaches

`two-phase-sink` (`two-phase-parquet`, `kafka-transactional`) and `vector-sink` (`memory-vector`/`qdrant`/`pgvector`/`lancedb`/`weaviate`/`pinecone`) each have registered drivers in `default_registry()`, but none of `sql_ddl` (only checks `Source`/`Sink` roles), `sql_job`, `distributed_job`, or `python_sink` dispatch to either role. Vector writes happen through a separate embedding-pipeline path not covered by this matrix.

