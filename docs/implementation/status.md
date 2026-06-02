# Krishiv Implementation Status

## Current Session (Completed)

### Gap/Bug Fixes — Native Kafka Connector

All 10 gaps identified post-implementation have been resolved:

| # | Fix | File |
|---|-----|------|
| 1 | Removed dead `spawn_blocking` watermark RPC on every poll timeout | `connectors/src/kafka.rs` |
| 2 | `project_batch` cast failure now emits tracing warning + null fill (no silent drop) | `sql/src/kafka_table.rs` |
| 3 | DDL-created Kafka tables (`CREATE EXTERNAL TABLE … STORED AS KAFKA`) now tracked in `streaming_sources` | `sql/src/kafka_table.rs`, `sql/src/lib.rs` |
| 4 | Demo binary now uses `register_kafka_source` end-to-end | `examples/rust/src/bin/kafka_streaming_sql.rs` |
| 5 | Unique group IDs per binary run (timestamp suffix) — repeat runs no longer re-read history | `examples/rust/src/bin/kafka_streaming_sql.rs` |
| 6 | Spawned polling task checks `tx.is_closed()` before each poll for proactive cancellation | `sql/src/kafka_table.rs` |
| 7 | Deleted empty `streaming_test.rs` placeholder | deleted |
| 8 | Added `SqlEngine::sql_to_kafka` write-back path | `sql/src/lib.rs` |
| 9 | `auto_commit_interval_ms: Option<u64>` added to `KafkaConfig`; streaming SQL path uses 1 s auto-commit | `connectors/src/kafka.rs`, `sql/src/kafka_table.rs`, `sql/src/lib.rs` |
| 10 | `RdkafkaKafkaSource` now uses `HashMap<i32, i64>` for per-partition offsets + `all_current_offsets()` | `connectors/src/kafka.rs` |

### Architecture after fixes
- `KafkaTableFactory` holds a shared `Arc<RwLock<HashSet<String>>>` created in `SqlEngine::new`
  so DDL and programmatic paths both update `streaming_sources`.
- `KafkaConfig.with_auto_commit(ms)` builder enables at-least-once for streaming SQL paths.
- `SqlEngine::sql_to_kafka(sql, bootstrap, topic) -> SqlResult<u64>` writes batch SQL results
  to any Kafka topic as JSON rows.
- Multi-partition topics tracked correctly via `HashMap<partition, offset>`.

## Validation
```
cargo check -p krishiv-sql -p krishiv-connectors   # clean
BOOTSTRAP=localhost:9092 cargo run --bin kafka_streaming_sql  # 10/10 scenarios + sql_to_kafka pass
```

## Next Steps
- True streaming windows (DataFusion streaming execution for unbounded GROUP BY).
- Wire `all_current_offsets()` into `krishiv-checkpoint` for exactly-once SQL streaming.
- Avro/Protobuf deserialization via `krishiv-schema-registry`.
