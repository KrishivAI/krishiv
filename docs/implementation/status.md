# Implementation Status

**Phase:** R14 — Incremental Computation & CDC Lakehouse (in progress)  
**Last updated:** 2026-05-23  
**Branch:** `cursor/implement-r14-7aa2`

**R13 COMPLETE (2026-05-23)** — including deferred GAP-RT-01, native asyncio, and connector integration tests.

Release tracker: [`r13-python-streaming-api.md`](r13-python-streaming-api.md) · [`r14-incremental-cdc-lakehouse.md`](r14-incremental-cdc-lakehouse.md)

## R14 — Completed (this branch)

- **S1 Live tables:** `CREATE|REFRESH|DROP LIVE TABLE` parsing and planning (`krishiv-sql`), `CreateLiveTableExec` / `RefreshLiveTableExec` (`krishiv-exec`), `ks.Session.live_table()` (`krishiv-python`).
- **S1 Delta store:** `DeltaStore`, `MemoryDeltaStore`, `RedbDeltaStore`, `KafkaDeltaStore` (`krishiv-lakehouse`).
- **S2 Memoization:** `MemoCache` with LRU (`krishiv-exec`), `memo_cache_info` / content-hash hooks (`krishiv-python`).
- **S3 CDC fan-out:** `SchemaNormalizeOperator`, `CdcRouter` multi-table routing (`krishiv-exec`, `krishiv-connectors`).
- **S3 Change feed:** `LiveTable.change_feed()` and Python tests.
- **S4 Exactly-once:** `MemoryIcebergTwoPhaseCommit`, transactional producer metadata (`krishiv-lakehouse`, `krishiv-connectors`), checkpoint `iceberg_snapshot_id` / `kafka_offsets`, `BarrierMetadata` proto, coordinator `set_barrier_alignment`, exactly-once integration test (`exactly-once-integration` feature).

### R14 validation

```bash
cargo test -p krishiv-exec -p krishiv-sql -p krishiv-connectors -p krishiv-lakehouse -p krishiv-checkpoint -p krishiv-scheduler --lib
cargo test -p krishiv-lakehouse --features exactly-once-integration
cd crates/krishiv-python && PYTHONPATH=python pytest python/tests/test_live_table.py python/tests/test_memo.py python/tests/test_change_feed.py
```

### R14 next steps

- Wire `@ks.transform(memo=True)` Python decorator (Rust `memo_transform_call` is exported; decorator wrapper in pure Python).
- Full workspace `cargo clippy --workspace -- -D warnings` and `cargo test --workspace`.
- Broker-backed `RdkafkaDeltaStore` / live Kafka transactional producer integration tests (feature `kafka` on lakehouse/connectors).
- Close R14 acceptance gate items in [`r14-incremental-cdc-lakehouse.md`](r14-incremental-cdc-lakehouse.md).

## R13 Closure (merged from main)

| Item | Implementation |
|------|----------------|
| **GAP-RT-01** | `DistributedBackend` submits plans via Arrow Flight SQL (`krishiv-runtime/src/flight_client.rs`) |
| **Native asyncio** | `WindowedStream.__anext__` uses `pyo3-async-runtimes` + `future_into_py` (no `asyncio.to_thread`) |
| **Kafka / Iceberg tests** | `python/tests/test_connectors_integration.py` (local handles + optional live env-gated tests); `read_iceberg()` added |
| **Flight integration** | `distributed_backend_submits_plan_over_flight_sql` spawns in-process Flight SQL server |

### R13 validation

```
cargo test -p krishiv-runtime --lib
cargo test -p krishiv-python --lib
pytest crates/krishiv-python/python/tests/
```

Optional live connector smoke (CI or laptop):

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export ICEBERG_CATALOG_URI=http://localhost:8181
pytest crates/krishiv-python/python/tests/test_connectors_integration.py -m integration
```

### Merge note (2026-05-23)

Merged `origin/main` into `cursor/implement-r14-7aa2`; resolved Python crate conflicts (R14 live table/memo + R13 `read_iceberg`, `KeyedStream`, `pyo3-async-runtimes`).
