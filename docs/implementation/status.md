# Krishiv Implementation Status

## Current Phase

**R16 IN PROGRESS (2026-05-23).**

Release tracker: [`r16-advanced-streaming-exactly-once.md`](r16-advanced-streaming-exactly-once.md)

## R16 — Advanced Stateful Streaming & Exactly-Once

### Completed (this session)

- **S1** — `barrier.proto`, `BarrierInjector` / `SharedBarrierInjector`, `BarrierAligner`, `CheckpointBarrierTracker`, executor `BarrierService` gRPC server + `checkpoint_barrier_integration` test.
- **S2** — `krishiv-cep` crate (`Pattern`, `SequentialPatternMatcher`), `CepOperator` in `krishiv-exec`.
- **S3** — `TemporalJoinSpec` / `IntervalJoinSpec` / `SideOutput` in `krishiv-plan`; temporal join, interval join, side output routing, watermark E2E helper in `krishiv-exec`.
- **S4** — Key groups (`NUM_KEY_GROUPS=32768`), `StateMigrationRegistry`, `IncrementalCheckpointWriter`, `KeyGroupRescaler` in `krishiv-checkpoint`; `StateBackend::key_group_range` / `schema_version`.
- **S5** — `TransactionalKafkaSink`, `TwoPhaseParquetSink`, `exactly_once_certification` integration tests; `docs/reference/exactly-once-matrix.md`.
- **S6** — Incremental checkpoint manifest diffing; watermark E2E test; streaming fragment barrier flush on `OperatorMessage::Barrier`.

### API

- `KeyedStream::join_temporal`, `join_interval`, `cep_pattern`; `WindowedStream::with_side_output`.

### Validation (2026-05-23)

```
cargo test -p krishiv-cep -p krishiv-exec -p krishiv-state -p krishiv-checkpoint \
  -p krishiv-connectors -p krishiv-executor -p krishiv-scheduler  → pass
cargo test --workspace --lib → pass
cargo clippy -p krishiv-cep -p krishiv-exec -p krishiv-state -p krishiv-checkpoint \
  -p krishiv-connectors -p krishiv-executor -p krishiv-scheduler -p krishiv-api -p krishiv-plan -- -D warnings → pass
```

### Remaining / follow-up

- Python `@ks.state_migration` decorator (`krishiv-python`).
- CLI `krishiv stream jobs --show-watermarks` (coordinator task watermark report).
- Wire `BarrierService` on coordinator serve path alongside executor registration (client in `barrier_client.rs`).
- RocksDB SST incremental upload (current incremental writer diffs snapshot SHA-256 blobs for `RedbStateBackend`).
- Full workspace `cargo clippy -- -D warnings` blocked by pre-existing `krishiv-lakehouse` lints.

### Next command

```bash
cargo test -p krishiv-executor --test checkpoint_barrier_integration
cargo test -p krishiv-connectors --test exactly_once_certification
```
