# Implementation Status

**Phase:** R14 — Incremental Computation & CDC Lakehouse  
**Last updated:** 2026-05-23  
**Branch:** `cursor/implement-r14-7aa2`

## Completed (this session)

- **S1 Live tables:** `CREATE|REFRESH|DROP LIVE TABLE` parsing and planning (`krishiv-sql`), `CreateLiveTableExec` / `RefreshLiveTableExec` (`krishiv-exec`), `ks.Session.live_table()` (`krishiv-python`).
- **S1 Delta store:** `DeltaStore`, `MemoryDeltaStore`, `RedbDeltaStore`, `KafkaDeltaStore` (`krishiv-lakehouse`).
- **S2 Memoization:** `MemoCache` with LRU (`krishiv-exec`), `memo_cache_info` / content-hash hooks (`krishiv-python`).
- **S3 CDC fan-out:** `SchemaNormalizeOperator`, `CdcRouter` multi-table routing (`krishiv-exec`, `krishiv-connectors`).
- **S3 Change feed:** `LiveTable.change_feed()` and Python tests.
- **S4 Exactly-once:** `MemoryIcebergTwoPhaseCommit`, transactional producer metadata (`krishiv-lakehouse`, `krishiv-connectors`), checkpoint `iceberg_snapshot_id` / `kafka_offsets`, `BarrierMetadata` proto, coordinator `set_barrier_alignment`, exactly-once integration test (`exactly-once-integration` feature).

## Validation

```bash
cargo test -p krishiv-exec -p krishiv-sql -p krishiv-connectors -p krishiv-lakehouse -p krishiv-checkpoint -p krishiv-scheduler --lib
cargo test -p krishiv-lakehouse --features exactly-once-integration
cd crates/krishiv-python && PYTHONPATH=python pytest python/tests/test_live_table.py python/tests/test_memo.py python/tests/test_change_feed.py
```

## Next steps

- Wire `@ks.transform(memo=True)` Python decorator (Rust `memo_transform_call` is exported; decorator wrapper in pure Python).
- Full workspace `cargo clippy --workspace -- -D warnings` and `cargo test --workspace`.
- Broker-backed `RdkafkaDeltaStore` / live Kafka transactional producer integration tests (feature `kafka` on lakehouse/connectors).
