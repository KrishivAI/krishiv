# Exactly-Once Certification Matrix (R16)

Krishiv certifies exactly-once delivery for the following source→sink pairs when checkpoint barriers, fencing tokens, and connector-specific commit protocols are enabled.

| Source | Sink | Mechanism | Conditions |
|--------|------|-----------|------------|
| Kafka | Iceberg | Offset checkpoint + Iceberg snapshot commit (R14) | gRPC barriers; `read_committed` source |
| Kafka | Kafka | Kafka transactions (`TransactionalKafkaSink`) | Txn id `{job_id}/{partition_id}/{epoch}`; zombie fencing on recovery |
| Kafka | Parquet/S3 | Two-phase staging (`TwoPhaseParquetSink`) | Staged `_staging/{epoch}/` then atomic rename to `data/` |
| S3/Parquet | Iceberg | File-list/byte offset checkpoint + snapshot commit | Source offset in checkpoint metadata |
| S3/Parquet | Kafka | Offset checkpoint + `TransactionalKafkaSink` | Same as Kafka→Kafka sink rules |

## Validation

```bash
cargo test -p krishiv-connectors --test exactly_once_certification
cargo test -p krishiv-connectors -- kafka_exactly_once
cargo test -p krishiv-connectors -- s3_2pc_exactly_once
```
