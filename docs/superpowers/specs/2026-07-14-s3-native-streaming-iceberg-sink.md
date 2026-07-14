# S3-native streaming Iceberg sink (DUR-2 live-cert enabler / Option B)

**Date:** 2026-07-14
**Status:** design approved (user chose Option B); implementation queued
**Risk:** HIGH — touches the DUR-2-certified two-phase commit path.

## Problem

`IcebergStreamingSink` (the streaming sink DUR-2 exercises) writes exclusively to
the **local filesystem**: `crates/krishiv-connectors/src/lakehouse/iceberg_native.rs`
is a 1036-line hand-rolled Iceberg two-phase commit backed by `std::fs`
(`fs::File::create` + `ArrowWriter` for staging, `fs::write`/`read_to_string` for
`version-hint.text`, a `LocalFsStorageFactory` for the `iceberg` crate catalog).
There are **33** `std::fs`/`Path` I/O sites.

Consequence (confirmed 2026-07-14): a cross-pod DUR-2 recover-commit cert cannot
run — when the subtask restores on a different pod, the staged parquet the
recovery re-stages from lives on the dead pod's local disk. MinIO is available
(`minio.krishiv-infra:9000`) but the sink cannot target it. See
[[dur2-checkpoint-sink-transactions]], [[flag-minimization]].

## Goal

`IcebergStreamingSink` reads/writes through `object_store` so its table root and
staged parquet can live on S3/MinIO (or any object store), making the staged
files + committed snapshot visible to whichever executor restores. Preserve the
**exact** DUR-2 two-phase semantics (offset-gated idempotent recover-commit,
per-instance nanosecond `snap_counter`, orphan cleanup) proven by the connectors
iceberg suite (378/0).

## Approach (chosen): uniform `object_store`, keep the sync surface via `block_on`

Route **all** file I/O in `iceberg_native.rs` through an `Arc<dyn ObjectStore>`
selected from the root URI (`file://` → `object_store::local::LocalFileSystem`,
`s3://` → the `cloud`-gated `AmazonS3Builder` path already in
`storage_factory.rs::build_s3`). Do **not** convert the `IcebergTwoPhaseCommit`
trait to async — bridge at each object_store call with
`krishiv_common::async_util::block_on` (the codebase's established sync-over-async
pattern), so the sink's blocking checkpoint-aligned contract is unchanged.

### Concrete conversion (the 33 sites)

1. **Store handle:** add `store: Arc<dyn ObjectStore>` + `prefix: object_store::path::Path`
   to `IcebergNativeTwoPhaseCommit`; build it in `open()` from the root URI via
   `StorageFactory` (extend it to return a store for a `file://` root too).
2. **`open()`:** replace `create_dir_all` (object stores are prefix-based — no
   dirs), `canonicalize` (URIs don't canonicalize), and the `version-hint`
   existence check/read with `store.head`/`store.get`.
3. **`stage_parquet()`:** write the Parquet bytes to an in-memory buffer
   (`ArrowWriter` over `Vec<u8>`), then `block_on(store.put(&path, bytes))`
   instead of `File::create`. Keep the nanosecond `snap_counter` naming.
4. **`read_staged_parquet()` / `read_all`:** `block_on(store.get(&path))` → bytes
   → `ParquetRecordBatchReader` over the buffer.
5. **`write_version_hint` / metadata:** `block_on(store.put(...))`.
6. **iceberg catalog FileIO:** build the `MemoryCatalog` with an S3-capable
   storage factory + `s3://` warehouse URI when the root is S3 (the iceberg crate
   supports S3 FileIO); keep `LocalFsStorageFactory` for `file://`.

### DUR-2 invariants to re-prove after conversion

- Recover-commit re-stages durable rows through the committing instance's own
  `stage_parquet` (foreign staged files are unresolvable by iceberg read FileIO —
  the ENOENT bug). Must hold with object_store paths too.
- `finalize_prepared(commit)` stays offset-gated idempotent via
  `committed_kafka_offsets()` (snapshot summary `krishiv.kafka.committed_offsets`).
- Orphan cleanup (`abort`, `remove_if_exists`, `.dur2.json` sidecar) → object_store
  `delete`.

## Feature gating & build

- The S3 path is `cloud`-gated (already the case in `storage_factory.rs`). The
  `file://` path stays always-available. `krishiv capabilities` must show
  `cloud=on` on the image that runs the cert (see [[flag-minimization]]).
- Build + ship the `prod` preset (`build-fast-engine.sh` already does this):
  kafka + cloud + iceberg + distributed.

## Testing

1. Unit (connectors, deterministic): parameterize the existing DUR-2 crash-recovery
   tests over BOTH a `file://` tempdir store AND an in-process memory object store
   (`object_store::memory::InMemory`) so recover-commit append+upsert+idempotent is
   proven on the object-store path with no external deps. Keep the iceberg suite green.
2. Live (krishiv-cert on MinIO): produce N Kafka rows → `POST
   /api/v1/continuous-register` run-loop with `sink.root = s3://<bucket>/dur2`,
   `checkpoint_storage_path = s3://<bucket>/dur2-ckpt` → wait barrier →
   `kubectl delete pod` the executor mid-run → restore on the sibling pod →
   assert Iceberg rowcount == N + offsets in the snapshot summary.

## Sequencing

1. Introduce the `object_store` seam + `InMemory`/`file://` unit coverage (no S3
   dep at test time) — the deterministic correctness gate.
2. Wire the `cloud`/S3 backend selection.
3. Build `prod`, deploy to krishiv-cert (coord+2 exec) with MinIO S3 env +
   `AWS_ENDPOINT_URL`/creds (`minio-root-creds`), run the live cert.

## Out of scope

- Converting the batch Iceberg path (already object-store-capable via StorageFactory).
- The platformd kafka_bridge offset protocol (#171) — orthogonal.
