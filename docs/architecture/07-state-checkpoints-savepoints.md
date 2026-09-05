# State, checkpoints, and savepoints

`krishiv-state` is keyed operator state and its durability. This document
covers the state backends, key groups and rescaling, timers and TTL, the
checkpoint storage format, the barrier protocol from operator to coordinator,
exactly-once sinks, savepoints, and restore. Rule stated in the crate root:
state is accessed only on the operator loop (`process_batch`,
`flush_triggered_windows`), never from timer callbacks.

## State backends

`StateBackend` is synchronous (the caller chooses async dispatch) and keyed
by `(Namespace{op_id, state_name}, key bytes)`:

| Method group | Purpose |
|---|---|
| `get` / `put` / `delete` / `clear_namespace` | point access |
| `list_namespaces` / `list_keys` | inspection (`StateInspector`, `StateReader`) |
| `snapshot` / `load_snapshot` | portable byte snapshot: `[u32 version=1][u64 count][entries…]`, entry = `[op_id][name][key][value]` with LE length prefixes; `load_snapshot` is transactional (a failure leaves prior data intact) |
| `put_batch` / `delete_batch` / `get_batch` | single-transaction batches (RocksDB overrides) |
| `purge_expired` / `set_watermark` | TTL eviction, wall-clock or event-time |
| `sync` | make buffered writes durable — called once per checkpoint epoch |
| `as_rocksdb` | downcast hook for incremental SST checkpoints |

| Backend | Use |
|---|---|
| `InMemoryStateBackend` | tests, embedded, in-process cluster (`open_state_backend(None, …)`) |
| `RocksDbStateBackend` | production; key = `[op_id_len][op_id][name_len][name][raw key]`, identical to the snapshot layout so snapshots port between backends. Options: dynamic level compaction, bloom filters, `KRISHIV_ROCKSDB_WRITE_BUFFER_MB` (64), `KRISHIV_ROCKSDB_MAX_OPEN_FILES` (512). `durable_fsync`: `true` = fsync per write (historical default for `open_for_profile`), `false` = WAL-buffered, synced once per epoch by `sync()` — what the window operators use, so the crash boundary is "state at the last checkpoint" |
| `TtlStateBackend<B>` | wraps any backend; value = `[u64 expires_at_ms][bytes]`; lazy expiry on read plus eager `purge_expired`; `set_watermark` switches "now" to event time |
| `DisaggregatedStateBackend` | DFS-primary (S3/HDFS/GCS) with a local LRU cache (`DisaggregatedConfig`: 1 GiB cache, 64 MiB max entry); checkpoint = manifest of SST files, recovery fetches lazily (the ForSt shape) |
| `BroadcastBackend` / `BroadcastState` | Flink-style broadcast state for connected streams |
| `QueryableStateStore` / `QueryableStateHandle` | read-only external access to keyed state (HTTP `queryable_state_http.rs`) |

`operator_runtime::open_state_backend` picks the backend from placement:
`state_dir = None` → in-memory (the operator's active state is already an
in-memory map; the backend is touched only at checkpoint and purge), `Some`
→ RocksDB with `durable_fsync = false`, optionally TTL-wrapped.

## Key groups and rescaling

`NUM_KEY_GROUPS` = 32 768. `key_group_for_key` hashes with the shared keyed
SHA-256 domain (`krishiv_common::partition`) — the same function the shuffle
partitioner and IVM sharding use. `key_group_ranges_for_parallelism(p)`
assigns contiguous ranges; a subtask owns one `KeyGroupRange` and the
distributed run-loop forwards out-of-range rows to the owner (`05`).
Rescaling (`checkpoint::rescaling`: `KeyGroupRescaler`,
`redistribute_snapshots`, `EntryRouting`, `RescaleChecksum`) re-routes
snapshot entries to the new ranges and checksums the redistribution so a
rescale that loses or duplicates an entry is detected. The hash family changed
from XxHash64 once; checkpoints from before that need a fresh snapshot.

## Timers and processing time

`TimerService` — event-time timers keyed `(deadline_ms, namespace, key)` in a
`BTreeMap`, so `drain_fired_timers(watermark)` is a prefix split.
`ProcessingTimeTimerService` is the wall-clock counterpart. Timer state is
part of the operator snapshot.

## Compatibility and migration

`OperatorStateDescriptor { operator_id, state_name, serializer_version }` is
the identity a restore checks. Renaming an operator creates new state unless
a migration is registered. `StateMigrationRegistry` holds
`(from, to) → fn(bytes) → bytes` steps and `migrate_snapshot` chains them
`from → from+1 → … → to`; downgrades are refused;
`CURRENT_STATE_SCHEMA_VERSION` names the writer's version.

## Checkpoint storage

```
{base}/{job_id}/checkpoints/{epoch:020}/metadata.json
{base}/{job_id}/checkpoints/{epoch:020}/{op_id}/{task_id}/state.bin
{base}/{job_id}/checkpoints/{epoch:020}/manifest.sha256
```

An epoch is **complete** only when `manifest.sha256` exists, covers
`metadata.json`, the metadata names this job and epoch, and every listed file
passes its SHA-256. Anything less is invisible to `latest_valid_epoch` and to
restore — so a crash between the last `state.bin` and the manifest yields the
previous epoch, never a torn one. `CheckpointMetadata` carries the fencing
token, `OperatorSnapshotRef`s, `SourceOffsetRecord`s (encoded connector
offsets), `SinkTransactionRef`s (prepared 2PC handles), and the savepoint
label when there is one. `validate_fencing_token_for_restore` rejects a
metadata file written by a token newer than the restorer's.

| `CheckpointStorage` | Profile |
|---|---|
| `EphemeralCheckpointStorage` | `dev-local` |
| `LocalFsCheckpointStorage` | `single-node-durable` |
| `ObjectStoreCheckpointStorage` | `distributed-durable` (`open_checkpoint_storage_from_uri`, `KRISHIV_CHECKPOINT_URI`) |

**Incremental checkpoints** (`incremental_checkpoint.rs`,
`RocksDbIncrementalCheckpointer`): a native RocksDB checkpoint per epoch,
uploading only SST files not present in the previous epoch's
`SstEpochManifest`; `EpochMetaFile` records the file set so a restore
downloads one epoch's closure. Portable snapshots remain the fallback for
every non-RocksDB backend. `incremental_trace.rs` records what each epoch
uploaded for observability.

The `proptest_checkpoint_kill` suite stops the write sequence after any
prefix and demands that recovery lands on the last sealed epoch, and flips
any byte in a sealed epoch and demands the manifest fences it without
bricking recovery.

## The barrier protocol

1. The coordinator's `CheckpointCoordinator` (`04`) initiates epoch *n* and
   injects a barrier at every source task.
2. Inside a task, `OperatorQueue` (`krishiv-dataflow::queue`) carries data on
   a bounded channel and barriers on an **unbounded** one: a barrier is never
   blocked behind backpressure, so the protocol cannot deadlock a full
   pipeline. Barriers that arrive while the receiver waits on data are
   deferred in a `VecDeque` and processed before the next data item.
3. A multi-input operator aligns (`BarrierAligner`, Chandy–Lamport): the
   first input to deliver the barrier is blocked and its post-barrier data
   buffered until every input has delivered; `Aligned` triggers the
   snapshot and unblocks; stale or duplicate barriers are `Ignored`.
4. The operator writes its snapshot (`write_operator_snapshot`), calls
   `sync()` once, and the run-loop acknowledges with the snapshot refs, the
   source offsets (`SourceReader::checkpoint_offset`, plus
   `snapshot_in_flight` for prefetched-but-unemitted records), and any
   prepared sink refs.
5. On quorum the coordinator writes `metadata.json` then `manifest.sha256`
   and sends checkpoint-complete; sinks commit (below); the epoch hint
   (`write_epoch_hint`) advances.

Snapshots happen **only** at barrier epochs. The run-loop does not ship state
per cycle.

## Exactly-once sinks

`TwoPhaseCommitSink` (`krishiv-connectors::two_phase`): `prepare(epoch,
batch) → Handle` stages output under the epoch; `commit(handle)` makes it
visible (an atomic rename or an Iceberg `fast_append`); `abort(handle)`
discards it. Repeated commit/abort on the same handle must be idempotent and
a conflicting decision must never reverse visible data. For recovery across
an executor restart — where the in-memory handle is gone — a sink reports a
durable `prepare_path` in the ack (`PreparedSinkRef`) and implements
`finalize_prepared(path, commit)`; a sink that cannot is refused under a
durable profile. The streaming participant form
(`TransactionalSinkParticipant`: `stage`, `pre_commit(epoch)`,
`commit_through(epoch)`, `abort_after(epoch)`) is what the run-loop drives.
Certified implementations: local/S3 Parquet, Iceberg (`IcebergStreamingSink`,
append or copy-on-write upsert), Kafka transactional, Delta and Hudi
two-phase writers (`10`).

The effective guarantee is the weakest of source, sink, checkpoint storage,
and profile — `DeliveryGuarantee::{BestEffort, AtLeastOnce, EffectivelyOnce,
ExactlyOnce}` — and is reported per job, not asserted globally.

## Savepoints

A savepoint is a named, user-triggered checkpoint retained until deleted
(`SavepointCoordinator`, `SavepointMeta { format_version = 1, savepoint_id,
label, job_id, epoch, operator_versions, created_at_secs }`). `krishiv
savepoint create|list|delete` and the HTTP jobs API drive it; `create_savepoint`
copies the epoch's files under a savepoint key, `restore_savepoint` validates
the metadata version and operator identities before use; `savepoint_rename`
handles operator renames with an explicit mapping.

## Restore

`krishiv restore` or an automatic restart after failure: pick the epoch
(`latest_valid_epoch` or a named savepoint) → validate fencing and
`OperatorStateDescriptor` compatibility → run migrations → the coordinator
issues a `RestoreDirective` naming the epoch and, per prepared sink
transaction, commit or abort → each task loads its snapshot
(`RestoredJobCheckpoint`, `RestoredSourceOffset`), rewinds its sources to
the recorded offsets, and resumes. `abort_after(epoch)` on sinks deletes
staged files from epochs past the restore point; the rewound source
re-delivers them.

## Related

- `08-streaming.md` — the operators whose state this is.
- `10-connectors-and-lakehouse.md` — sink implementations and offsets.
- `14-deployment-and-durability.md` — which storage each profile requires.
