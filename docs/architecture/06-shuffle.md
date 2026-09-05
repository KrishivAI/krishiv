# Shuffle

`krishiv-shuffle` moves inter-stage data. A `ShuffleMap` task hash-partitions
its output by the exchange keys and writes one partition per reducer; the
downstream stage reads them through `ShuffleReadExec`. This document covers
the store contract, the backends, fencing, tiering, cleanup, and the
side-channels (runtime filters, compression) that ride on it.

## Identity and contract

A partition is `PartitionId { job_id, stage_id, partition: u32 }`. Ids are
validated with `validate_safe_id` before they become paths — empty strings,
separators, NUL, and `..` are rejected so an untrusted id cannot traverse the
store's directory.

`ShuffleStore` (async, object-safe):

| Method | Role |
|---|---|
| `register_partition_lease(id, token)` | record the current assignment's lease token before the task starts |
| `write_partition(partition, token)` | write a complete partition; a stale token is rejected |
| `write_partition_stream(id, schema, stream, token)` | write incrementally so the producer never holds the partition resident; disk stores override it (`ArrowWriter`), memory stores collect |
| `read_partition(id)` / streaming read | `None` until written |
| delete / list by job | reclamation |

**Lease fencing.** The coordinator hands each attempt a lease token and the
executor registers it at launch. A zombie attempt (an executor that lost its
heartbeat but is still running) cannot win a write race, because its token
is no longer current when the replacement attempt registers. Fencing is
enforced in the store, not by the scheduler's belief about who is alive.

## Backends

| Store | Medium | Profile | Notes |
|---|---|---|---|
| `InMemoryShuffleStore` | process memory | `dev-local` | tests, embedded |
| `LocalDiskShuffleStore` | local Parquet/IPC files | `single-node-durable` | streaming writes; page-cache eviction after durable write |
| `ObjectStoreShuffleStore` | S3/GCS/Azure/local via `object_store` | remote tier | BLAKE3 content hash verified on read |
| `TieredShuffleStore` | local + object store | `distributed-durable` | write acknowledged only after **both** tiers commit; read local first, fall back to remote on miss *or* on `ContentHashMismatch` |
| `PushShuffleStore` | memory, bounded by `KRISHIV_SHUFFLE_STORE_BYTES` | any | map output pushed to the reducer's executor ahead of the read |

`open_shuffle_backend_from_uri` (`storage_uri.rs`) selects a backend from
`KRISHIV_SHUFFLE_URI` (`memory://`, `file:///path`, `s3://bucket/prefix`,
…); `open_tiered_shuffle_backend` composes the tiered store. The mapping from
durability profile to `ShuffleDurability` (`Memory`, `LocalDisk`, `Tiered`)
is fixed in `krishiv_common::durability` (`14`).

Two details in the tiered store are deliberate: lease registration and writes
use `tokio::join!`, never `try_join!`, so a failure on one tier cannot cancel
the other mid-flight and leave the tiers holding different fencing tokens;
and a local corruption error falls through to the remote copy because the
remote tier verifies its own hash independently.

## Writers and indexes

- `HashPartitioner` partitions by the shared keyed hash
  (`krishiv_common::partition`, SHA-256 keyed domain) — the same family that
  routes streaming key groups and IVM shards, so a hash-partitioned batch and
  a keyed stream agree on where a key lives.
- `SortShuffleWriter` produces one sorted file plus a `SortShuffleIndex`
  (`SortShuffleFiles`) for range reads, the Spark sort-shuffle shape; used
  when a stage has many reducers so the writer opens one file, not one per
  partition.
- `partition_size.rs` gives `logical_batch_bytes` /
  `logical_partition_bytes` (what the data *is*, not what Arrow buffers
  happen to hold) and `compact_shared_buffers` for slices that pin large
  parents. AQE sizing decisions read these numbers.
- `ShuffleCompression` / `CompressionCodec` (LZ4, Zstd, none) apply per
  partition; configured by `KRISHIV_SHUFFLE_COMPRESSION`.

## Metadata and reclamation

`ShuffleMetadata` tracks `PartitionState::{Pending, Available, Failed}` per
`ShufflePath` in the coordinator's job record. There is deliberately no
partition cap: the only bound that could refuse a partition would refuse one
already written, making the consumer recompute its producer. The real bound is
the partition count the physical plan declares.

Orphan reclamation (`orphan.rs`) runs on each executor against the
coordinator's live-job set. A job directory is garbage only when it has been
absent from **both** `DEFAULT_RECLAIM_MIN_ABSENCES` = 3 consecutive
observations **and** for `DEFAULT_RECLAIM_MIN_ABSENT` = 120 s; the policy
clamps upward, so a caller cannot configure the grace away. The reason is a
real loop: a coordinator failover produced a live-job set missing a running
job, the sweep deleted its committed output, the consumer reported it
missing, the producer regenerated, the next sweep deleted it again, until the
regeneration budget was exhausted. `reclaim_foreign_spills` handles spill
files (`spill_file_name`, `spill_owner_id`) left by a dead executor on shared
disk. Job-level cleanup waits `KRISHIV_JOB_GC_GRACE_SECS` after terminal
state.

## Runtime filters

`runtime_filter.rs` builds a Bloom filter over the build side's join keys
(`RuntimeFilterBuilder`, `FilterKeyType`, `DEFAULT_FPP`, capped at
`MAX_FILTER_BYTES`; `plan_filter_bytes` sizes it) and ships it to the probe
side's scan so a selective dimension prunes fact-table row groups before the
shuffle. It is the distributed counterpart of the dynamic filter DataFusion
applies locally.

## Shuffle service

`shuffle_svc.rs` + `flight.rs` serve partitions over Arrow Flight, with
bearer-token auth (`token_auth.rs`, `12`). An executor reads a remote
partition by Flight ticket; `krishiv shuffle-svc` runs the service
standalone for external-shuffle deployments so executor pods can be
preempted without losing map output.

## Lease persistence

`lease_persistence.rs` writes the current lease token beside the partition so
a restarted executor recovers which token is valid without asking the
coordinator — a stale writer after restart is still fenced.

## Related

- `04` (stage cut, AQE), `05` (capacity fractions for the push store and
  page cache), `14` (which backend each profile uses), `16` (measurements).
