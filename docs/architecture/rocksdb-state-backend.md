# RocksDB State Backend

**Status:** Decision — approved for R5.2 implementation.
**Owner:** Architecture team.
**Linked releases:** R5.2 (implementation spec), R6 (checkpoint recovery extension).

Related documents:
- [Streaming Execution Model](./streaming-execution-model.md)
- [Checkpoint Protocol](./checkpoint-protocol.md)

---

## 1. Purpose

This document defines the deployment model, async isolation boundary, and
compaction thread budget for the RocksDB keyed state backend introduced in R5.2.
All RocksDB code must conform to these rules before review.

---

## 2. Async Isolation Boundary

RocksDB's C++ library is synchronous and can block for milliseconds during
compaction or flush.  Calling it directly from a Tokio async task starves the
worker thread pool.

**Rule:** Every call into `RocksDbStateBackend` that touches the RocksDB API
(get, put, delete, flush, snapshot) **must** be dispatched via
`tokio::task::spawn_blocking`.  No RocksDB call is permitted directly on a
Tokio worker thread.

```rust
// Correct — moves the blocking call off the Tokio worker thread.
let value = tokio::task::spawn_blocking(move || backend.get(&namespace, &key))
    .await??;

// Wrong — blocks the worker thread.
let value = backend.get(&namespace, &key)?;
```

The `StateBackend` trait methods are synchronous (they take `&self` / `&mut self`,
not `async fn`) so that callers control exactly where the `spawn_blocking` boundary
is.  The trait must not grow async methods.

---

## 3. Compaction Thread Budget

RocksDB uses background threads for compaction and flush.  These threads share
the OS thread pool with Tokio workers on the same node.  Uncontrolled compaction
threads can cause latency spikes on the Tokio side.

**Rule:** The RocksDB `Options::set_max_background_jobs` value must be set to
`max(1, physical_cpus / 4)`, capped at `4`.  This leaves at least three-quarters
of CPU cores available for Tokio workers under normal load.

```rust
let mut opts = Options::default();
let bg_jobs = std::cmp::min(4, std::cmp::max(1, num_cpus::get() / 4));
opts.set_max_background_jobs(bg_jobs as i32);
```

The compaction thread budget must be configurable at executor startup via the
`KRISHIV_ROCKSDB_BG_JOBS` environment variable to allow tuning in CI and in
production without recompilation.

---

## 4. Executor Deployment Model

### 4.1 Supported Model: Kubernetes Deployment + S3 Recovery

Executors running stateful streaming jobs are Kubernetes `Deployment` pods (not
`StatefulSet` pods).  This means:

- **RocksDB lives on the pod's local ephemeral disk.**  There is no persistent
  volume claim attached to the executor pod.
- **On pod restart, RocksDB state is rebuilt from the last successful checkpoint
  stored on S3** (or another object store).  The executor reads the checkpoint
  manifest, downloads the SST files, and reopens the RocksDB instance before
  resuming operator processing.
- **Checkpoint-to-S3 is the only recovery path.**  There is no direct pod-to-pod
  state transfer.

This model is the only supported deployment for stateful streaming executors in
R5.2.  It is the model validated in acceptance tests.

### 4.2 Explicitly Unsupported: StatefulSet With PVC-Backed RocksDB

`StatefulSet` pods with `PersistentVolumeClaim`-backed RocksDB are **explicitly
out of scope** in R5.2 and must not be used.  Rationale:

- StatefulSet pod scheduling is more constrained (pod identity binding), which
  reduces rescheduling flexibility when nodes fail.
- PVC-backed state bypasses the checkpoint protocol, making recovery semantics
  inconsistent with the rest of the system.
- There is no validated migration path from PVC state to object-store checkpoints.

StatefulSet deployment may be re-evaluated in a future release after the
checkpoint protocol is proven in production.

### 4.3 Pod Restart Recovery Sequence

```
1. Executor pod starts on a new or existing node.
2. Executor reads checkpoint manifest from S3:
   KRISHIV_CHECKPOINT_BUCKET/<job_id>/<stage_id>/latest.json
3. Executor downloads SST files listed in the manifest to local disk.
4. Executor opens RocksDB from the downloaded SST files.
5. Executor re-registers with the coordinator (re-attach protocol; see
   docs/architecture/streaming-execution-model.md §6).
6. Executor resumes processing from the offset stored in the checkpoint.
```

Steps 1–4 happen inside `spawn_blocking` before the async executor loop starts.

---

## 5. Key Encoding

RocksDB uses a single default column family.  Keys are encoded as:

```
[namespace_prefix (variable UTF-8 bytes)] ':' [record_key (raw bytes)]
```

where `namespace_prefix` is `{operator_id}:{state_name}` (the output of
`Namespace::column_family_name()`).  The `:` separator byte (`0x3A`) is reserved
and must not appear in `operator_id` or `state_name`.

Range scans for `list_keys(namespace)` use the namespace prefix as the key
range start; the end is the prefix with the last byte incremented by one.

---

## 6. TTL Cleanup

TTL in the RocksDB backend uses the same `TtlStateBackend<RocksDbStateBackend>`
wrapper defined in `krishiv-state`.  The wrapper encodes `[8-byte LE
expires_at_ms][value bytes]` in the stored value; expired entries are lazily
deleted on read.

Proactive compaction-time cleanup (RocksDB compaction filter) is a post-R5.2
optimisation.  For R5.2, lazy deletion is sufficient.

---

## 7. State Inspection Safety

State inspection via `StateInspector` holds an immutable borrow of the backend
and cannot mutate state.  On the executor, the inspection RPC must acquire a
read lock before constructing the `StateInspector`.  The lock must not be held
while a `process_batch` call is in progress.

The inspection RPC returns only:
- Namespace names.
- Key counts per namespace.
- Estimated key size in bytes per namespace.

Raw value bytes are never returned by the inspection API.

---

## 8. Key Invariants

| Invariant | Enforcement |
|---|---|
| No RocksDB calls on Tokio worker threads | `spawn_blocking` at every call site |
| Compaction threads ≤ `min(4, cpus/4)` | Configured at executor startup |
| RocksDB state is ephemeral on pod | `Deployment` (not `StatefulSet`) |
| Recovery from last S3 checkpoint | Executor pod restart sequence |
| State inspection is read-only | `StateInspector` holds `&B` not `&mut B` |

---

## 9. Out Of Scope

| Topic | Reference |
|---|---|
| Exactly-once with durable checkpoints | `docs/implementation/r6-checkpoints-and-savepoints.md` |
| StatefulSet + PVC deployment | Explicitly unsupported in R5.2 |
| Compaction-filter TTL cleanup | Post-R5.2 optimisation |
| State rescaling on topology change | Post-R6 design document |
| Multi-coordinator failover | `docs/implementation/r9-governance-and-operations.md` |
