# Krishiv Rescaling And State Schema Evolution

**Status:** Decision — approved for R6 (documentation and baseline enforcement only).
**Linked releases:** R6 (baseline), post-R6 (live rescaling tooling).
**Linked docs:** `docs/architecture/checkpoint-storage.md`, `docs/architecture/checkpoint-protocol.md`.

---

## 1. Purpose

This document records the R6 architectural decisions for two related topics:

- **State rescaling**: changing the parallelism (partition count) of a stateful streaming stage.
- **State schema evolution**: changing the binary format of a state snapshot across releases.

Both topics affect restore safety. Neither is fully implemented in R6; this document defines
the boundaries of what R6 enforces, what it defers, and what operators must not do.

---

## 2. Rescaling Model

### 2.1 Definition

Rescaling means changing the number of parallel task instances for a stateful operator
(e.g., changing a keyed window stage from 4 tasks to 8 tasks). Because each task owns a
disjoint subset of the key space, rescaling requires redistributing state across the new
partition layout before execution can resume.

### 2.2 R6 Supported Path: Savepoint + Explicit Repartition

Live rescaling (changing parallelism without stopping the job) is **not supported in R6**.

The only supported rescaling path from R6 onwards is:

1. Trigger a savepoint on the running job (`krishiv savepoint --job <id>`).
2. Wait for the savepoint to commit and be marked valid.
3. Stop the job.
4. Resubmit the job with the new `task_count`, specifying the savepoint epoch as the
   restore point (`krishiv restore --job <id> --from <epoch>`).
5. The coordinator reads the savepoint metadata, repartitions the operator state shards
   across the new task layout, and restarts execution.

Step 5 (coordinator-side repartition during restore) is a **post-R6 runtime deliverable**.
In R6, submitting a restore with a different `task_count` than the savepoint's recorded
task count returns an explicit error rather than silently producing incorrect state.

### 2.3 Partition Count in Checkpoint Metadata

Each `OperatorSnapshotRef` in `CheckpointMetadata.operator_snapshots` records the
`task_id` of the task that produced the snapshot. The restore path uses the set of
`task_id` values to determine the original parallelism. If the submitting job spec
requests a different parallelism, the restore path compares the counts and rejects
the restore in R6.

```json
{
  "operator_snapshots": [
    { "operator_id": "op-window-0", "task_id": "task-0", "snapshot_path": "..." },
    { "operator_id": "op-window-0", "task_id": "task-1", "snapshot_path": "..." }
  ]
}
```

Two entries for `op-window-0` means the original parallelism was 2. Restoring to
parallelism 4 is rejected in R6.

### 2.4 Post-R6 Repartition Strategy

When live repartition support is added post-R6, the expected strategy is:
- Key-range repartitioning: each key maps to `hash(key) % new_parallelism`. State
  shards are split or merged accordingly during the restore phase.
- This requires a coordinator-side repartition pass that reads all shards from the
  savepoint, redistributes key-value pairs, and writes new shards before resuming tasks.

This is explicitly out of scope for R6 and must not be partially implemented.

---

## 3. State Schema Evolution Baseline

### 3.1 Current Format

Operator state snapshots (`state.bin`) use a length-prefixed binary format with a
4-byte little-endian version header:

```
[4-byte LE version = 1]
[8-byte LE entry_count]
for each entry:
  [8-byte LE operator_id length][operator_id bytes]
  [8-byte LE state_name length][state_name bytes]
  [8-byte LE key length][key bytes]
  [8-byte LE value length][value bytes]
```

The version byte is read first in `InMemoryStateBackend::load_snapshot`. Any version
other than `1` is immediately rejected with `StateError::SnapshotCorrupt` before any
state is modified. This is the R6 schema evolution baseline — it is already implemented.

Similarly, `CheckpointMetadata.version` is validated in `CheckpointMetadata::validate()`
and unknown versions are rejected with `CheckpointError::IncompatibleVersion` before
any restore action proceeds.

### 3.2 R6 Policy: Reject Unknown Versions Immediately

The R6 policy is strict rejection:

- Unknown `state.bin` version → `StateError::SnapshotCorrupt` (restore aborted).
- Unknown `metadata.json` version → `CheckpointError::IncompatibleVersion` (restore aborted).
- In both cases, the error is surfaced to the operator before any state mutation occurs.
- The fallback policy (fall back to prior valid epoch) applies only to storage-level
  corruption (SHA-256 mismatch, missing manifest). Version mismatch is a hard error —
  there is no fallback epoch that would resolve a version mismatch.

### 3.3 Version Increment Policy

When a format change is made in a future release:

- Increment the version constant before merging the change.
- Add a migration path in `load_snapshot` for `version == old_version` → migrate → produce
  `version == new_version` state before returning.
- Add a test that reads a golden `state.bin` at the old version and verifies it loads
  correctly under the new code.
- Never remove a version migration path from `load_snapshot` within the same major release
  family (R6.x).

Full migration tooling (CLI command to upgrade savepoints across incompatible versions)
is a post-R6 deliverable and must not be partially implemented in R6.

### 3.4 RocksDB State Backend

`RocksDbStateBackend::snapshot()` returns `StateError::SnapshotUnsupported` in R6. The
certified R6 path uses `InMemoryStateBackend` only. Schema evolution for RocksDB state
is deferred to the release that certifies RocksDB for exactly-once.

---

## 4. Acceptance Gate (R6)

| Condition | How verified |
|---|---|
| Restore with mismatched parallelism returns an explicit error | Unit test: submit restore with wrong `task_count` |
| Unknown `state.bin` version is rejected before state mutation | Existing test: `in_memory_load_snapshot_rejects_unknown_version` (or equivalent) |
| Unknown `metadata.json` version is rejected before restore | Existing test: `metadata_validate_rejects_unknown_version` |
| No partial repartition code exists in the codebase | Code review gate |

---

## 5. Out Of Scope (R6)

- Live rescaling (changing parallelism without a savepoint stop/restore cycle).
- Coordinator-side state repartition during restore.
- Migration CLI for upgrading savepoints across incompatible versions.
- Schema evolution for RocksDB state snapshots.
- Key-range repartitioning or consistent hashing for state redistribution.
