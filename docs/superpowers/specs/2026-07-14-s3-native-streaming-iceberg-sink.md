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

## CORRECTION (2026-07-14, grounded in the code) — read before implementing

Two premises in the original draft were wrong or under-weighted; the corrected
facts drive the approach:

1. **iceberg 0.9.1 (pinned) has NO S3 storage backend.** Its `io/storage/`
   ships only `local_fs.rs` + `memory.rs`; the crate has zero
   `object_store`/`opendal` dependency (`default = []`). The original item 6
   ("the iceberg crate supports S3 FileIO") is false. **However**, a custom
   `object_store`→iceberg `Storage`/`StorageFactory` bridge ALREADY EXISTS in
   this repo — `krishiv-sql/src/catalog/object_store_io.rs`
   (`KrishivStorage` + `KrishivStorageFactory`), used by the REST-catalog and
   Postgres-catalog S3 warehouse paths. It dispatches on scheme (`s3://` →
   object store, else → `LocalFsStorage`), so swapping
   `LocalFsStorageFactory` → `KrishivStorageFactory` makes the iceberg-crate
   FileIO (manifests, manifest-lists, `metadata.json`) S3-capable while the
   `file://` path stays **byte-identical** (delegates to `LocalFsStorage`) —
   zero regression to the 378 certified tests on the local path.

2. **The blast radius is larger than "33 fs sites in one file."** It is:
   (a) relocate the bridge `krishiv-sql` → `krishiv-connectors` (the lowest
   crate that has the iceberg traits, so `iceberg_native.rs` can use it) and
   re-export it from `krishiv-sql` so the **verified** batch S3 path is
   untouched — this needs `iceberg`/`cloud` feature propagation across
   `rest-catalog`/`postgres-catalog`; (b) scheme-gated object-store branches in
   `iceberg_native.rs` (staging, read, version-hint) — keep the local `std::fs`
   branch byte-identical to preserve CONN-2/CONN-3 crash-atomicity; (c) the SAME
   in `streaming_sink.rs` (the `.dur2.json` sidecar, `remove_if_exists`,
   `read_staged_parquet`, the `PathBuf` staged paths); (d) root-URI plumbing so
   an `s3://…` sink `root` survives (today `IcebergSinkTarget.root: PathBuf`
   mangles `s3://` → `s3:/`).

**Design to contain the risk:** object stores have no rename/fsync/dir-sync, but
a `put` is atomic — so the object-store branch replaces the tmp+rename+fsync
crash-atomicity dance with a single atomic `put` (no torn-write window). The
local branch is unchanged. Reuse `KrishivStorage` for BOTH the iceberg-crate
FileIO (factory swap) and my own staging/version-hint I/O (its async
`read`/`write`/`exists`/`delete` wrapped in `block_on`).

## Approach: uniform `object_store` via the existing bridge, sync surface via `block_on`

Route the object-store branch of file I/O in `iceberg_native.rs` through the
relocated `KrishivStorage` (scheme-dispatching) rather than a raw
`Arc<dyn ObjectStore>`, so the iceberg-crate FileIO and our own I/O hit the same
backend. Do **not** convert the `IcebergTwoPhaseCommit` trait to async — bridge
at each object_store call with `krishiv_common::async_util::block_on`, so the
sink's blocking checkpoint-aligned contract is unchanged.

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

## Sequencing (staged, each gated on the connectors iceberg suite)

**Stage 1 — relocate the bridge (mechanical, compile-gated).** Move
`KrishivStorage`/`KrishivStorageFactory` from `krishiv-sql/src/catalog/object_store_io.rs`
into `krishiv-connectors/src/lakehouse/object_store_io.rs` (the lowest crate with
the iceberg traits, so `iceberg_native.rs` can use it), with a self-contained
`cloud`-gated `build_s3_object_store`. Re-export it from `krishiv-sql` so the
verified batch/REST S3 path is untouched. Wire `krishiv-connectors/iceberg` +
`krishiv-connectors/cloud` into krishiv-sql's `rest-catalog`/`postgres-catalog`
features (their `s3://` warehouses use the bridge). Add `typetag` to connectors
(the `Storage` trait is `#[typetag::serde]`). One canonical `#[typetag::serde]`
registration — no duplicate-tag collision. *Gate: `cargo check -p krishiv-connectors
--features iceberg,cloud` + `-p krishiv-sql --features rest-catalog`.*

**Stage 2 — scheme-dispatch `iceberg_native.rs`.** Hold a `KrishivStorage` +
`is_object_store` flag + `root_uri`. `open()` branches at the top: `s3://` skips
`create_dir_all`/`canonicalize` and builds the `MemoryCatalog` with
`KrishivStorageFactory` + the `s3://` warehouse URI; `file://` keeps today's code
byte-identical. `stage_parquet`/`write_version_hint`/`read_staged_parquet`/
`read_all`/`open`'s version-hint read each get an object-store branch (ArrowWriter
over `Vec<u8>` → `block_on(store.write(uri,bytes))`; a `put` is atomic so no
tmp+rename+fsync dance) and keep the local `std::fs` branch untouched.

**Stage 3 — scheme-dispatch `streaming_sink.rs`.** Route the `.dur2.json` sidecar
write/read, the staged-file cleanup (`remove_if_exists`/`remove_file`), and
`read_staged_parquet` through new `&self` helpers on
`IcebergNativeTwoPhaseCommit` that branch on scheme; the local branch calls the
identical `std::fs` so the 378 certified tests are behaviorally unchanged.

**Stage 4 — deterministic tests.** Parameterize the DUR-2 crash-recovery tests
over `file://` AND an in-process `object_store::memory::InMemory` store so
recover-commit (append+upsert+idempotent) is proven on the object-store path with
no external deps. Keep the iceberg suite green.

**Stage 5 — live cert.** Build `prod`, deploy to krishiv-cert (coord+2 exec) with
MinIO S3 env + `AWS_ENDPOINT_URL`/creds (`minio-root-creds`), run the live cert.

## Out of scope

- Converting the batch Iceberg path (already object-store-capable via StorageFactory).
- The platformd kafka_bridge offset protocol (#171) — orthogonal.
