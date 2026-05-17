# Krishiv Shuffle Deployment Model

**Status:** Decision — approved for R4 implementation.
**Owner:** Architecture team.
**Linked releases:** R4 (implementation), R7/R10 (local shuffle service optimization).

---

## Is An Object Store Required?

**No — not for any mode.** The object store is an optional durability layer, not the default.

| Execution Mode | Default Shuffle | Object Store Required? |
|---|---|---|
| **Embedded** | None — DataFusion handles in-process | Never |
| **Single-node** | Local filesystem | Never |
| **Distributed** | **Local disk on each executor** | No — opt-in for crash resilience only |

In distributed mode, the default is local disk shuffle. Executors serve their own shuffle partitions over Arrow Flight. Object store is a configurable durability upgrade you opt into when executor crash resilience is worth the extra write cost (e.g., long stages, spot/preemptible nodes).

---

## Two Durability Modes

### `local` (default — no external dependency)

```toml
[shuffle]
durability = "local"   # default, no other config needed
```

```
Write: Executor A writes partitions to local disk.
Read:  Stage N+1 opens Arrow Flight to Executor A's shuffle server
       and reads directly from A's local disk.
Crash: Executor A crashes → partition gone → coordinator re-runs Stage N.
```

Fast, zero external dependency. Correct for development, CI, and deployments where Stage N re-run cost is acceptable (typically small-to-medium stages).

### `object-store` (opt-in — crash resilience without re-running Stage N)

```toml
[shuffle]
durability = "object-store"
store     = "s3"          # or "file", "gcs", "abs"
bucket    = "my-bucket"   # for remote stores
```

```
Write: Executor A writes partitions to local disk (fast), then
       uploads each complete partition to the configured object store.
Read:  Stage N+1 reads always come from the object store via Arrow Flight.
       Executor A does not need to be alive after upload completes.
Crash: A crashes before upload → partition not in object store → re-run Stage N.
       A crashes after upload → partition in object store → no re-run needed.
```

Reduces re-computation on failure. Recommended when Stage N is expensive (minutes/hours) or executors are preemptible.

---

## ShuffleStore Abstraction

Both modes use the same `ShuffleStore` trait, backed by the `object_store` crate (the same crate DataFusion uses for Parquet reads). The `local` mode uses `object_store::local::LocalFileSystem`.

```rust
/// Abstraction over shuffle storage backends.
pub trait ShuffleStore: Send + Sync {
    async fn write_partition(&self, path: &ShufflePath, data: Bytes) -> ShuffleResult<()>;
    async fn finalize_partition(&self, path: &ShufflePath) -> ShuffleResult<()>;
    async fn read_partition(&self, path: &ShufflePath) -> ShuffleResult<impl Stream<Item = ShuffleResult<Bytes>>>;
    async fn delete_job(&self, job_id: JobId) -> ShuffleResult<()>;
}
```

| Backend | Durability mode | Config |
|---|---|---|
| `LocalFilesystem` | `local` (default for all modes) | `durability = "local"` |
| `InMemory` | Integration tests only | `durability = "memory"` |
| `AmazonS3` / MinIO | `object-store` | `store = "s3"` |
| `GoogleCloudStorage` | `object-store` | `store = "gcs"` |
| `AzureBlobStorage` | `object-store` | `store = "abs"` |

---

## Failure Model

| Failure | `local` mode | `object-store` mode |
|---|---|---|
| Executor crashes before partition write completes | Re-run Stage N | Re-run Stage N |
| Executor crashes after write, before upload | Re-run Stage N | Re-run Stage N (partition not yet in object store) |
| Executor crashes after upload completes | Re-run Stage N | **No re-run** — Stage N+1 reads from object store |
| Executor alive but slow | Stage N+1 waits on Arrow Flight | Stage N+1 reads from object store (no wait on A) |

**Key invariant (both modes):** A partition is either fully available at its read location or not available at all. No partially-written partition is ever served to a reader.

---

## Why Not True Hybrid (Local Read + Object Store Fallback)

A true hybrid — write to both local disk and object store simultaneously, read from local disk in the happy path, fall back to object store on failure — creates a consistency problem:

```
Executor A writes partition P:
  1. Write to local disk ✓
  2. Start async object store upload...
  3. Executor A dies mid-upload ✗

Stage N+1 assigns to Executor B:
  - Local disk on A: gone
  - Object store: partial file (60% uploaded, not marked Available)
  - Result: neither copy is usable
```

This requires per-partition flush status tracking, conditional atomic writes to the object store, and a reconciliation loop for failed uploads — a mini-distributed filesystem. The performance benefit of local reads does not justify this complexity. Use `local` mode for speed, use `object-store` mode for durability, never both simultaneously.

---

## Write Path Detail

### `local` mode
```
1. Write Arrow IPC frames to local staging file:
   {staging_dir}/{job_id}/{stage_id}/{partition_id}.tmp
2. When partition complete: rename to final path (atomic on Linux ext4/xfs):
   {staging_dir}/{job_id}/{stage_id}/{partition_id}.ipc
3. Mark partition Available in shuffle metadata.
4. Arrow Flight server on executor serves reads directly from this file.
5. File deleted after coordinator confirms all downstream stages consumed it.
```

### `object-store` mode
```
1. Write Arrow IPC frames to local staging file (same as local mode, fast).
2. When partition complete: multipart upload to object store.
3. After confirmed upload: mark partition Available; local staging file deleted.
4. Arrow Flight server in executor reads from object store and streams to caller.
   (Executor A does not need to remain alive after step 3.)
```

---

## Configuration Examples

### Default (all modes — no config needed)
```toml
# No shuffle config needed. Defaults to local durability.
```

### Explicit local (distributed mode)
```toml
[shuffle]
durability = "local"
staging_dir = "/var/krishiv/shuffle"   # defaults to system temp
```

### Object store durability — MinIO (self-hosted, no cloud account)
```toml
[shuffle]
durability = "object-store"
store     = "s3"
endpoint  = "http://minio.krishiv.svc.cluster.local:9000"
bucket    = "krishiv-shuffle"
# credentials via Kubernetes Secret
```

MinIO runs as a single Kubernetes `Deployment` + `PersistentVolumeClaim`. Zero cloud dependency.

### Object store durability — AWS S3
```toml
[shuffle]
durability = "object-store"
store  = "s3"
bucket = "my-shuffle-bucket"
region = "us-east-1"
# credentials via IRSA
```

---

## When To Use Each Mode

| Situation | Recommended mode |
|---|---|
| Local development, CI, testing | `local` |
| Single-node production | `local` |
| Distributed, short stages (< 1 min), stable nodes | `local` |
| Distributed, long stages (> 5 min), or spot/preemptible nodes | `object-store` |
| Distributed, Stage N+1 must not wait on Stage N executor | `object-store` |

---

## Future Optimization Path

| Phase | What changes | Trigger |
|---|---|---|
| **R4** | `local` default + optional `object-store` | Correctness and simplicity first |
| **R7/R10** | Local DaemonSet shuffle service (Spark ESS equivalent) | Benchmarks show Arrow Flight to executor is a bottleneck at SF1000+ |
| **Post-GA** | Push-based shuffle (executor-to-executor direct Arrow Flight) | After DaemonSet shuffle service is proven; reduces S3 intermediary cost |

True hybrid (simultaneous local + object store writes with local-read fast path) is never planned. Use `object-store` mode instead, which achieves the same crash resilience without the consistency complexity.
