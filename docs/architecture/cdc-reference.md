# CDC-to-Lakehouse Reference Architecture

## Overview

This document defines the reference architecture for Change Data Capture (CDC) pipelines from transactional databases into Iceberg lakehouse tables. It covers the source protocol, Krishiv's CDC source component, the sink contract, exactly-once semantics, and the certified scope.

---

## Source: Debezium CDC over Kafka

Debezium publishes row-level change events for INSERT, UPDATE, and DELETE operations as JSON messages to Kafka topics. Each event carries a **Debezium envelope** with:

- `op`: operation type (`c` = create/insert, `u` = update, `d` = delete, `r` = snapshot read)
- `before`: row state before the change (null for inserts)
- `after`: row state after the change (null for deletes)
- `source`: source metadata including `lsn` (PostgreSQL log sequence number), `ts_ms`, `db`, `table`, `server_id`

Krishiv's CDC pipeline consumes this envelope. The `source.lsn` and `source.ts_ms` fields are used as the idempotency key per Kafka partition.

**Certified source databases**: PostgreSQL (via Debezium PostgreSQL connector 2.x) and MySQL (via Debezium MySQL connector 2.x). Other databases are experimental.

---

## Krishiv CDC Source: `DebeziumKafkaSource`

`DebeziumKafkaSource` is implemented in `krishiv-connectors`. It is a `Source` impl that:

1. Reads from a configured Kafka topic using the connector's Kafka consumer.
2. Deserializes the Debezium JSON envelope (with or without a schema registry; both modes supported).
3. Emits `CdcEvent` records into the Krishiv operator graph.

### `CdcEvent` Type

```rust
pub struct CdcEvent {
    pub op: CdcOp,           // Insert | Update | Delete | SnapshotRead
    pub before: Option<RecordBatch>,
    pub after: Option<RecordBatch>,
    pub source_lsn: Option<u64>,
    pub source_ts_ms: Option<i64>,
    pub partition_id: u32,   // Kafka partition
    pub offset: i64,         // Kafka offset
}
```

`CdcOp` is `#[non_exhaustive]` to allow future operations without breaking stable consumers.

---

## Sink: Iceberg via `krishiv-lakehouse`

Each CDC source table maps to one Iceberg table. The Iceberg table schema must match or be a superset of the source table schema. Schema evolution (adding columns) is beta; column removal and type changes require a pipeline restart with a new table.

The `IcebergSink` in `krishiv-lakehouse` applies upsert semantics using the configured primary key columns:

- **Insert / SnapshotRead**: append row to Iceberg table.
- **Update**: merge-on-read using `source_lsn` as the sequence key; newer LSN wins.
- **Delete**: write a delete marker using Iceberg positional or equality delete file.

---

## Exactly-Once Delivery Guarantee

The Kafka source is at-least-once: offsets are committed after the downstream Iceberg write succeeds, but a failure between write and offset commit results in replay.

Exactly-once (idempotent) semantics are achieved by combining:

1. **At-least-once Kafka source** (may replay events after failure).
2. **Idempotent Iceberg merge-on-read**: each row is keyed by `(primary_key_columns, source_lsn)`. Re-processing an event with the same LSN produces the same Iceberg state — no duplicate rows are appended.

This is **idempotent-exactly-once**: re-processed records result in the same final table state, not duplicate rows. It does not provide transactional cross-table exactly-once; each table is independently idempotent.

---

## Pipeline Template: `CdcToLakehousePipeline`

`CdcToLakehousePipeline` in `krishiv-connectors` is the configurable template. Configuration fields:

```toml
[cdc_pipeline]
source_topic        = "dbserver1.public.orders"
kafka_brokers       = ["kafka:9092"]
schema_registry_url = "http://schema-registry:8081"  # optional; omit for JSON-only mode
target_catalog      = "iceberg_catalog"
target_table        = "warehouse.orders"
primary_key_columns = ["order_id"]
source_db_type      = "postgresql"   # or "mysql"
debezium_version    = "2.x"
```

The pipeline wires `DebeziumKafkaSource` → `CdcEventRouter` → `IcebergSink`. The router applies upsert semantics per `CdcOp`.

---

## Certified Scope

| Dimension | Certified | Beta | Not Supported |
|---|---|---|---|
| Debezium format | 2.x JSON | — | 1.x (removed) |
| Source database | PostgreSQL, MySQL | MongoDB | Oracle, SQL Server |
| Table cardinality | Single-table pipeline | Multi-table fan-out | Cross-database |
| Schema evolution | Add nullable columns | Add non-null columns | Drop/rename columns |
| Delivery guarantee | Idempotent-exactly-once | — | Transactional cross-table |

**Multi-table fan-out** (one Kafka topic → multiple Iceberg tables, as used with Debezium's `route` SMT) is beta: functional but not in the certification suite.

**Schema evolution** during live pipeline operation is beta. Column additions that are nullable are handled by the `IcebergSink` without pipeline restart; all other schema changes require a coordinated pipeline restart.

---

## Operational Notes

- **Offset management**: Kafka offsets are committed by `DebeziumKafkaSource` after the Iceberg snapshot is committed. Never commit offsets before the Iceberg write is durable.
- **Snapshot reads** (`op = r`): treated as inserts. If a snapshot replay overlaps with live CDC, idempotency via LSN keying prevents duplicates.
- **LSN gaps**: if Debezium skips an LSN (e.g., filtered by Debezium transforms), Krishiv does not detect the gap. The LSN dedup key is best-effort; it does not substitute for application-level audit.
- **Kafka consumer group**: each pipeline instance uses a dedicated consumer group. Do not share consumer groups across pipeline instances targeting different Iceberg tables.
