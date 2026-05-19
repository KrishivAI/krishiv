# Checkpoint Storage Architecture

**Status:** Approved for R6 implementation.
**Linked releases:** R6 (durable checkpoints, savepoints, exactly-once).
**Linked docs:** `docs/architecture/checkpoint-protocol.md` (barrier model and fencing invariants).

---

## 1. Purpose

This document defines the on-disk layout, metadata format, snapshot serialization, integrity checking, and two-phase commit sink API that back Krishiv's R6 checkpoint and savepoint system.

Decisions here translate directly to the `crates/krishiv-checkpoint` crate and the `TwoPhaseCommitSink` trait in `crates/krishiv-connectors`.

---

## 2. Checkpoint Storage Key Schema

All checkpoint data is written under a prefix rooted at the configured checkpoint base directory (local filesystem for tests; S3-compatible object store for production).

```
{base_dir}/
  {job_id}/
    checkpoints/
      {epoch:020}/           ← zero-padded to 20 digits for lexicographic ordering
        metadata.json        ← versioned checkpoint metadata envelope
        manifest.sha256      ← SHA-256 integrity manifest (written last)
        {op_id}/
          {task_id}/
            state.bin        ← serialized operator state snapshot
```

**Epoch zero-padding**: epochs are zero-padded to 20 decimal digits (`{epoch:020}`). This ensures that `ls`/`readdir` ordering matches numeric ordering, simplifying "most recent valid epoch" scans.

**Write order invariant**: `state.bin` files are written before `metadata.json`, and `manifest.sha256` is written last. A checkpoint epoch is considered complete only when `manifest.sha256` is present and valid. Partially written epochs (missing manifest) are treated as corrupt and skipped during restore.

---

## 3. Checkpoint Metadata Format

`metadata.json` uses a versioned JSON envelope. The `version` field is `1` for all R6 checkpoints. Future releases increment this field when the schema changes; the restore path validates the version and rejects unknown versions.

```json
{
  "version": 1,
  "epoch": 42,
  "job_id": "job-abc123",
  "fencing_token": 7,
  "timestamp_ms": 1716000000000,
  "source_offsets": [
    { "partition_id": 0, "offset": 1234 }
  ],
  "operator_snapshots": [
    {
      "operator_id": "op-window-0",
      "task_id": "task-0",
      "snapshot_path": "job-abc123/checkpoints/00000000000000000042/op-window-0/task-0/state.bin"
    }
  ]
}
```

Fields:
- `version`: format version. Must be `1` for R6.
- `epoch`: monotonically increasing checkpoint epoch counter per job.
- `job_id`: identifies the streaming job this checkpoint belongs to.
- `fencing_token`: coordinator fencing token at the time this checkpoint was committed. Restore paths must reject checkpoints whose fencing token predates the current coordinator generation (see `docs/architecture/checkpoint-protocol.md` §Fencing Invariant).
- `timestamp_ms`: wall-clock time of checkpoint commit (informational; not used for ordering).
- `source_offsets`: one record per source partition capturing the last processed offset at the barrier boundary.
- `operator_snapshots`: one record per operator instance pointing to the `state.bin` for that instance. The `snapshot_path` is relative to the checkpoint base directory.

---

## 4. Operator State Snapshot Format

`state.bin` uses a simple length-prefixed binary encoding. No external serialization dependency is required. The format is self-describing enough for round-trip correctness; schema evolution beyond key/value binary blobs is deferred to post-R6.

```
[4-byte LE version = 1]
[8-byte LE entry_count]
for each entry:
  [8-byte LE operator_id byte length][operator_id bytes (UTF-8)]
  [8-byte LE state_name byte length][state_name bytes (UTF-8)]
  [8-byte LE key byte length][key bytes (arbitrary)]
  [8-byte LE value byte length][value bytes (arbitrary)]
```

This format maps directly to the `(operator_id, state_name, key) → value` storage model in `InMemoryStateBackend`. The `snapshot()` and `load_snapshot()` methods on the `StateBackend` trait produce and consume this format.

**`RocksDbStateBackend` snapshot**: not implemented in R6.0. The certified R6 path uses `InMemoryStateBackend`. RocksDB snapshot/restore is a post-R6 deliverable.

---

## 5. Integrity Manifest

`manifest.sha256` lists the SHA-256 hash of every file in the epoch directory, one line per file:

```
sha256:<64-char lowercase hex>  metadata.json
sha256:<64-char lowercase hex>  op-window-0/task-0/state.bin
```

The manifest itself is not hashed. The restore path reads `manifest.sha256`, computes the SHA-256 of each listed file, and compares against the manifest entries. Any mismatch causes the epoch to be classified as corrupt.

**Corrupt checkpoint policy**: if the most recent epoch fails manifest validation, the restore path falls back to the most recent prior epoch whose manifest validates successfully. If no valid epoch exists, restore fails with an explicit error rather than starting from scratch silently.

---

## 6. `CheckpointStorage` Trait

The `CheckpointStorage` trait in `crates/krishiv-checkpoint` abstracts over the underlying storage (local filesystem for tests, object store for production). All methods are synchronous; callers in async contexts must use `spawn_blocking`.

```rust
pub trait CheckpointStorage: Send + Sync {
    fn write_bytes(&self, path: &str, data: &[u8]) -> CheckpointResult<()>;
    fn read_bytes(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>>;
    fn list_dir(&self, prefix: &str) -> CheckpointResult<Vec<String>>;
    fn delete_prefix(&self, prefix: &str) -> CheckpointResult<()>;
}
```

Higher-level helpers (`write_epoch_metadata`, `read_epoch_metadata`, `write_operator_snapshot`, `write_manifest`, `validate_epoch`, `list_valid_epochs`, `delete_epoch`) are free functions that call these four primitives. This keeps the trait minimal and the higher-level logic testable against any `CheckpointStorage` implementation.

`LocalFsCheckpointStorage` implements `CheckpointStorage` using `std::fs` with the same atomic-write pattern as `RocksDbStateBackend` (write to `.tmp`, then rename).

---

## 7. Two-Phase Commit Sink API

The `TwoPhaseCommitSink` trait in `crates/krishiv-connectors` is the contract for sinks that participate in checkpoint commit.

```rust
pub trait TwoPhaseCommitSink: Send {
    type Handle: Send;
    fn prepare(&mut self, epoch: u64, batch: &RecordBatch) -> ConnectorResult<Self::Handle>;
    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()>;
    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()>;
}
```

Protocol:
1. `prepare(epoch, batch)` — buffer the batch under a staging key scoped to `epoch`. Returns a `Handle` that identifies this staged write. Multiple `prepare` calls for the same epoch are valid (one per RecordBatch).
2. `commit(handle)` — make the staged output durable and visible. For S3/Parquet: atomic rename from `_staging/{epoch}/part.parquet` to `{output_prefix}/epoch={epoch}/part.parquet`.
3. `abort(handle)` — discard the staged output without making it visible. For S3/Parquet: delete the staging key.

`commit` and `abort` are mutually exclusive for a given handle. The checkpoint coordinator calls `commit` on all sink handles after all operator barrier acknowledgments arrive, or `abort` on all handles if the checkpoint is abandoned.

**Certified R6 sink**: `S3/Parquet` — object-level atomic rename (or put-if-absent on stores that support it). `InMemoryTwoPhaseCommitSink` is provided for deterministic unit tests and the R6 chaos test harness.

---

## 8. Rolling Upgrade Protocol

The supported R6 upgrade path for coordinator and executor binaries:

1. Trigger a savepoint on the running streaming job.
2. Wait for the savepoint to be committed and marked valid in the metadata store.
3. Upgrade the coordinator binary and restart the coordinator process.
4. The new coordinator reads the savepoint and restores the streaming job.
5. Roll the executor binary upgrade via pod replacement (Kubernetes rolling update or equivalent).

This is the only certified upgrade path from R6 onwards. In-place binary replacement without a savepoint is not supported and may cause duplicate output.

---

## 9. Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Partial epoch leaves corrupt checkpoint visible | Write manifest last; restore only considers epochs with a valid manifest |
| Stale coordinator commits superseded epoch | Fencing token checked on metadata write; stale writes rejected |
| SHA-256 manifest check adds restore latency | Manifests are small; file data is already read for restore, so hash is incremental |
| State snapshot format becomes incompatible | Version byte in snapshot header; load rejects unknown versions immediately |
| RocksDB snapshot deferred — operators using it can't checkpoint | RocksDB snapshot is post-R6.0; R6 certified path uses InMemoryStateBackend only |
