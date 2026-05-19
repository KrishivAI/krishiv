# Krishiv Exactly-Once Certification

**Status:** Decision — approved for R6. Certified triple is fixed; all other combinations are at-least-once until individually certified in a future release.
**Owner:** Architecture team.
**Linked releases:** R6 (exactly-once certification, durable checkpoints, two-phase commit sinks).
**Linked docs:**
- `docs/architecture/checkpoint-protocol.md` — aligned barrier model, fencing invariant, operator/coordinator protocol.
- `docs/architecture/checkpoint-storage.md` — checkpoint key schema, metadata format, integrity manifest, `TwoPhaseCommitSink` API.
- `docs/architecture/rescaling-model.md` — rescaling and state schema evolution decisions; live rescaling deferred post-R6.

---

## 1. Certified Exactly-Once Triple (R6)

Krishiv certifies exactly-once delivery in R6 for **one specific combination** of source, state backend, and sink:

| Component | Certified Implementation |
|-----------|--------------------------|
| **Source** | Kafka (single partition, transactional consumer group) |
| **State** | In-memory state backend (`InMemoryStateBackend`) |
| **Sink** | S3/Parquet (`TwoPhaseCommitSink` with object-level atomic writes) |

**Every other source, state, or sink combination is at-least-once in R6.** Substituting any component outside this triple — including using multiple Kafka partitions with cross-partition ordering, a RocksDB state backend, or a non-two-phase-commit sink — downgrades delivery to at-least-once regardless of checkpoint configuration.

---

## 2. All Other Combinations Are At-Least-Once (R6)

The table below documents the delivery guarantee for R6 combinations that fall outside the certified triple.

| Source | State Backend | Sink | R6 Guarantee |
|--------|---------------|------|--------------|
| Kafka (single partition) | InMemoryStateBackend | S3/Parquet (`TwoPhaseCommitSink`) | **Exactly-once** (certified) |
| Kafka (multiple partitions) | Any | Any | At-least-once |
| Kafka (single partition) | RocksDbStateBackend | Any | At-least-once |
| Kafka (single partition) | InMemoryStateBackend | Non-2PC sink | At-least-once |
| File/object store source | Any | Any | At-least-once |
| Any custom connector source | Any | Any | At-least-once |
| Any source | Any | Kafka sink | At-least-once |
| Any source | Any | Custom connector sink | At-least-once |

At-least-once means: after recovery from a failure, a record may be reprocessed and appear more than once in sink output. Checkpoints still provide fault tolerance and bounded replay; they do not eliminate duplicates when the sink or source combination is not certified.

---

## 3. What "Certified Exactly-Once" Means

A source/state/sink combination is certified exactly-once when all of the following conditions are verified end-to-end:

### 3.1 All Three R6 Chaos Tests Pass

The certified triple must survive all three chaos failure scenarios (see §7) without producing duplicate or missing records in S3/Parquet output. Passing fewer than all three tests does not constitute certification.

### 3.2 Two-Phase Commit Sink Protocol

The sink must implement `TwoPhaseCommitSink` (defined in `crates/krishiv-connectors`). The protocol is:

1. **`prepare(epoch, batch)`** — stage the output batch under a key scoped to the checkpoint epoch. No output is visible to downstream consumers.
2. **`commit(handle)`** — make the staged output durable and visible. For S3/Parquet this is an atomic rename from `_staging/{epoch}/part.parquet` to the output prefix.
3. **`abort(handle)`** — discard the staged output without making it visible.

The checkpoint coordinator calls `commit` only after every operator in the job has acknowledged the barrier for epoch E. It calls `abort` if the checkpoint is abandoned. A sink that does not implement this protocol cannot prevent duplicate output on coordinator or executor restart.

### 3.3 Fencing Token Guards

Every checkpoint epoch carry is protected by a monotonically increasing fencing token (`u64` coordinator lease generation). The fencing token guards against split-brain: if a stale coordinator restarts and attempts to commit a superseded epoch, the metadata store rejects the write because the fencing token is stale. A new coordinator with a higher fencing token takes ownership. This prevents a stale commit from reaching the sink after a newer epoch has already been committed.

See `docs/architecture/checkpoint-protocol.md` §Fencing Invariant for the formal rule.

### 3.4 Manifest-Validated Checkpoints

Each checkpoint epoch is considered complete only when `manifest.sha256` is present and valid alongside `metadata.json`. The manifest lists the SHA-256 hash of every file in the epoch directory. On restore:

- The restore path reads `manifest.sha256`, recomputes hashes for all listed files, and rejects any epoch where a hash does not match.
- A checkpoint whose manifest is missing or invalid is treated as corrupt and is not used for restore.
- The restore path falls back to the most recent prior epoch whose manifest validates successfully.

This ensures that a partially written checkpoint — interrupted mid-write by a process kill or network failure — can never silently corrupt recovered state.

See `docs/architecture/checkpoint-storage.md` §5 for the manifest file format.

---

## 4. Conditions Required For Exactly-Once To Hold

All of the following conditions must be satisfied simultaneously. If any condition is violated, the delivery guarantee degrades to at-least-once.

| Condition | Where enforced |
|-----------|----------------|
| **Aligned barrier:** every operator receives the barrier for epoch E on all input channels before snapshotting state | `CheckpointCoordinator` and operator barrier protocol (`checkpoint-protocol.md` §Aligned Checkpoints) |
| **All tasks acknowledge:** the coordinator waits for `CheckpointAck(epoch=E)` from every operator instance in the job before committing | `CheckpointCoordinator::receive_ack()` state machine (`r6-checkpoints-and-savepoints.md`) |
| **Manifest written atomically:** `manifest.sha256` is written last, after all `state.bin` files and `metadata.json` are durable | `write_manifest` called only after all acks received (`checkpoint-storage.md` §2 write order invariant) |
| **Fencing token monotonic:** the coordinator's fencing token increases on every restart; the metadata store rejects writes with a stale token | `handle_checkpoint_ack` rejects stale tokens; conditional write in metadata store (`checkpoint-protocol.md` §Fencing Invariant) |
| **`TwoPhaseCommitSink` used:** the sink implements `prepare`/`commit`/`abort` and the coordinator calls `commit` only after all acks | `TwoPhaseCommitSink` trait in `crates/krishiv-connectors`; `S3ParquetSink` is the certified R6 implementation (`checkpoint-storage.md` §7) |
| **Single-partition Kafka source:** source offsets are per-partition and captured atomically in `CheckpointMetadata.source_offsets`; cross-partition ordering is not guaranteed in R6 | `SourceOffsetRecord` in `CheckpointMetadata`; multi-partition exactly-once deferred post-R6 |
| **InMemoryStateBackend:** state snapshot and restore round-trip is fully implemented and tested; RocksDB snapshot returns `SnapshotUnsupported` in R6 | `InMemoryStateBackend::snapshot()` / `load_snapshot()`; `RocksDbStateBackend` not certified (`checkpoint-storage.md` §4) |

---

## 5. What Is Deferred Post-R6

The following capabilities are explicitly out of scope for R6 exactly-once certification. They are listed here to prevent premature certification claims.

| Deferred capability | Notes |
|---------------------|-------|
| **Kafka transaction support** | Certifying Kafka as an exactly-once sink (using Kafka transactions) requires a transactional producer implementation. Deferred to a post-R6 release. |
| **RocksDB state backend** | `RocksDbStateBackend::snapshot()` returns `StateError::SnapshotUnsupported` in R6. Certifying RocksDB for exactly-once requires implementing and testing snapshot/restore under chaos conditions. |
| **Live rescaling (changing parallelism)** | Changing the partition count of a stateful stage while the job is running is not supported. The only supported path is savepoint + stop + resubmit with the same `task_count`. Live rescaling and coordinator-side state repartition are post-R6. See `docs/architecture/rescaling-model.md` §2. |
| **Multi-partition Kafka exactly-once** | Exactly-once across multiple Kafka partitions requires cross-partition offset coordination that is not implemented in R6. |
| **CDC-to-lakehouse exactly-once pipelines** | End-to-end exactly-once for change-data-capture sources writing to lakehouse sinks involves transactional guarantees that span multiple systems. Not in R6 scope. |
| **Failed checkpoint cleanup** | Orphaned staging directories from aborted checkpoints are not automatically deleted in R6; `abort` discards the staged sink output but filesystem cleanup is manual. |

---

## 6. R6 Chaos Tests That Validate The Exactly-Once Claim

The following chaos tests are the mandatory validation gate for the certified triple. All three must pass without duplicate or missing records in S3/Parquet output.

| Test | Description | What it validates |
|------|-------------|-------------------|
| `chaos_1_coordinator_kill_mid_checkpoint_no_duplicate_commit` | Kill the checkpoint coordinator while epoch E is in flight; restart; verify no epoch is committed twice and S3/Parquet output contains no duplicates. | Fencing token prevents stale coordinator from committing a superseded epoch after restart. |
| `chaos_2_executor_kill_mid_checkpoint_abort_is_clean` | Kill one executor mid-checkpoint while it is writing its state snapshot; restart; verify the aborted epoch does not reach the sink and output contains no duplicates. | Incomplete checkpoint (missing ack from killed executor) causes coordinator to abort; `TwoPhaseCommitSink::abort` discards staged output. |
| `chaos_3_sink_kill_mid_write_abort_discards_staged_output` | Kill the S3/Parquet sink writer mid-write during `prepare`; restart; verify the staged output is discarded and the subsequent epoch commits cleanly without duplicate records. | `TwoPhaseCommitSink::abort` correctly discards a partially staged epoch; next valid epoch commits to a clean output key. |

Additionally, `chaos_4_corrupt_checkpoint_fallback_to_prior_valid_epoch` validates that a corrupt checkpoint file is detected via manifest validation and that restore falls back to the prior valid epoch without panic or incorrect output. This test validates fault tolerance rather than the exactly-once guarantee directly, but must also pass for R6 acceptance.

The rolling-upgrade test `chaos_e6` validates that a streaming job survives a coordinator binary upgrade via the savepoint → upgrade → restore cycle without duplicate output.

---

## 7. Relationship To Other Architecture Documents

| Document | Relationship |
|----------|-------------|
| `docs/architecture/checkpoint-protocol.md` | Defines aligned barrier semantics, operator/coordinator barrier protocol, watermark ordering invariants, and the fencing invariant that prevents split-brain commits. |
| `docs/architecture/checkpoint-storage.md` | Defines the checkpoint key schema, metadata JSON format, integrity manifest, `TwoPhaseCommitSink` trait and protocol, and the S3/Parquet certified sink implementation. |
| `docs/architecture/rescaling-model.md` | Defines why live rescaling is deferred (state ownership per task, no coordinator-side repartition in R6) and the savepoint+repartition path that is the only supported rescaling operation. |
