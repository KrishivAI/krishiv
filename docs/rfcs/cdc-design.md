# RFC: Change Data Capture Design

## Status

Draft — R3 scope: design only. Implementation deferred to R5+.

## Summary

This document defines Krishiv's approach to Change Data Capture (CDC):
capturing row-level inserts, updates, and deletes from source systems and
propagating them as a structured stream of change events.

## Motivation

Batch SQL pipelines process snapshots. CDC enables low-latency replication
of transactional databases into the lakehouse without full-table scans,
reducing both latency and source load.

## Capture Mechanisms

### Log-based CDC (preferred)

Reads the database write-ahead log (WAL) or binlog directly.

- **PostgreSQL**: logical replication via `pgoutput` or `wal2json` plugin.
- **MySQL**: binlog consumer using the binary log event protocol.
- **Advantages**: low source load, sub-second latency, captures deletes.
- **Disadvantages**: requires replication permissions; schema changes need careful handling.

### Poll-based CDC (fallback)

Periodically queries rows where an `updated_at` watermark column exceeds the last checkpoint.

- No special database permissions required.
- Cannot capture deletes unless a `deleted_at` soft-delete column is present.
- Higher source load; latency bounded by poll interval.

## Change Event Model

Each CDC event is a `RecordBatch` row with these reserved columns:

| Column | Type | Description |
|--------|------|-------------|
| `_cdc_op` | `Utf8` | Operation: `INSERT`, `UPDATE`, `DELETE` |
| `_cdc_ts_ms` | `Int64` | Wall-clock timestamp of the change (epoch milliseconds) |
| `_cdc_lsn` | `Utf8` | Log sequence number or poll cursor (source-specific string) |
| `_cdc_table` | `Utf8` | Fully-qualified source table name |

Payload columns (the actual row fields) follow the source table schema.
For `UPDATE`, both before- and after-images are included as separate
`_before_*`-prefixed columns when the source supports them.

## Offset Model

CDC sources implement `Offset` using the source's native cursor:

- PostgreSQL: `PgLsn` (64-bit log sequence number).
- MySQL: `(binlog_file, binlog_pos)` pair.
- Poll-based: `(table, last_updated_at_us)` pair.

On executor reassignment, the new executor seeks to the committed offset,
replaying events from that point. Combined with idempotent sink writes,
this provides at-least-once delivery end-to-end.

## Krishiv Integration Points

1. **Source**: a future `CdcSource` will implement the `Source` trait with
   `capabilities().with_unbounded().with_rewindable()`.
2. **Offset**: each capture backend provides its own `Offset` implementor
   registered with a `SchemaRegistry` entry for the source table.
3. **Schema evolution**: column additions are propagated via the
   `SchemaRegistry`; renames and type changes require a pipeline restart.
4. **Sink**: CDC events are typically written to Parquet (via `ParquetSink`)
   or to a streaming topic (via `KafkaSink`). Both declare at-least-once
   semantics; Parquet additionally declares idempotent.

## Known Limitations (R3 scope)

- No CDC implementation in R3; design only.
- Schema evolution handling deferred to R8 (Iceberg/Delta support).
- Exactly-once CDC deferred to R9 (governance and operations).
- `_before_*` column support deferred to R5 (stateful streaming core).

## References

- [Debezium architecture](https://debezium.io/documentation/reference/architecture.html)
- [PostgreSQL logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
- Krishiv R5 tracker: `docs/implementation/r5-stateful-streaming-core.md`
