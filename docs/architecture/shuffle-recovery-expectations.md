# Krishiv Shuffle Recovery Expectations

**Status:** Decision — approved for R4 implementation.
**Owner:** Architecture team.
**Linked releases:** R4 (shuffle), R6 (checkpoint-based exactly-once).

Related documents:
- [Shuffle Deployment Model](./shuffle-deployment-model.md)
- [Shuffle Retry Lineage Policy](./shuffle-retry-lineage.md)
- [Stage-Local Execution](./stage-local-execution.md)

---

## 1. Overview

Shuffle recovery behaviour depends on the configured shuffle durability mode (`local` or `object-store`) and the point in the write lifecycle at which an executor failure occurs. This document defines the expected recovery outcome for each scenario so that tests and coordinator logic can be verified against a shared specification.

---

## 2. Failure Points In The Shuffle Write Lifecycle

The shuffle write lifecycle for a single partition has four phases:

```
Phase 1: lease registered     — coordinator records Pending; executor begins writing
Phase 2: data writing         — executor streams Arrow IPC frames to staging file
Phase 3: finalize (rename)    — executor atomically renames .tmp → .ipc
Phase 4: Available reported   — executor reports TaskSucceeded; coordinator marks Available
```

---

## 3. Local Mode Recovery Scenarios

### 3a. Crash During Phase 1 or 2 (before finalize)

- The staging `.tmp` file is incomplete or absent on executor's local disk.
- The coordinator receives no `TaskSucceeded` update (heartbeat times out).
- **Coordinator action:** marks executor `Lost`; stage transitions to `Failed`; shuffle partitions for the stage are cleaned up; Stage N re-runs from source per the retry lineage policy.
- **Stage N+1:** blocked; never receives stale partial data.

### 3b. Crash During Phase 3 (mid-rename)

- Linux `rename(2)` is atomic on a single filesystem. Either the old `.tmp` name exists or the new `.ipc` name exists — never an intermediate state.
- If the rename completed: the `.ipc` file is present, but the executor has not reported `TaskSucceeded`. Coordinator does not know the partition is complete.
- **Coordinator action:** same as 3a — executor lost; Stage N re-runs; the duplicate `.ipc` file is treated as an orphan and cleaned up by `cleanup_orphans` on the next scan.
- **Note:** This means Stage N+1 may not start even if the partition data is physically present. This is the safe choice — the coordinator's `ShuffleMetadata` is the authoritative gate, not the presence of a file.

### 3c. Crash During Phase 4 (after finalize, before status report)

- Same as 3b from the coordinator's perspective.
- `.ipc` file exists but `TaskSucceeded` was never received.
- **Coordinator action:** executor lost; Stage N re-runs; the `.ipc` file is orphaned and cleaned on next scan.
- **Implication:** in `local` mode, the coordinator's `ShuffleMetadata` gate is the single source of truth. File presence alone never makes a partition Available.

### 3d. Executor Crash After `TaskSucceeded` (partition Available)

- Partition is `Available` in coordinator's `ShuffleMetadata`.
- Stage N+1 may have already started reading the partition via Arrow Flight from the crashed executor's local disk. The Flight server on the crashed executor is now gone.
- **Coordinator action:** marks executor `Lost`; any Stage N+1 tasks assigned to this executor are reassigned.
- **Stage N+1 partition reads from the crashed executor:** Arrow Flight reads will fail with connection error. The Stage N+1 task is retried on another executor. However, a different executor cannot read the crashed executor's local disk.
- **Recovery:** Stage N must re-run (Case 2 lineage) to reproduce the lost partition on a surviving executor. Stage N+1 then reads the new partition.
- **Implication:** in `local` mode, a partition's durability is bounded by the executor's lifetime. If the executor that wrote a partition crashes after Stage N+1 has started, Stage N re-run is required.

### 3e. Planned Executor Graceful Shutdown (SIGTERM)

- Executor finishes current RecordBatch, deregisters via `DeregisterExecutor` RPC, exits.
- If the executor had finished all assigned tasks and all partitions are `Available`, graceful shutdown has no recovery impact.
- If a task was still running at SIGTERM, the task is abandoned; the executor deregisters before exiting. Coordinator marks executor `Removed`; running tasks on that executor are reassigned.

---

## 4. Object-Store Mode Recovery Scenarios

### 4a. Crash Before Upload Starts (Phase 1 or 2)

- Same as local mode 3a. Partition is `Pending`; coordinator triggers Stage N re-run.

### 4b. Crash During Upload (partial object-store write)

- The object store write is not atomic at the application level (multipart upload may be in progress).
- The coordinator has not yet received `TaskSucceeded`.
- **Coordinator action:** executor lost; Stage N re-runs; the partial object-store upload is treated as an orphan (the object key exists but is incomplete or was aborted). `cleanup_orphans` removes it on the next TTL scan.
- Object stores with multipart upload abort support (S3, GCS, Azure) will eventually abort the partial upload if not completed. Krishiv does not rely on this; it triggers explicit cleanup.

### 4c. Crash After Successful Upload And Before `TaskSucceeded`

- Object is fully uploaded. The executor crashes before reporting `TaskSucceeded`.
- Coordinator has not marked the partition `Available`.
- **Coordinator action:** executor lost; Stage N re-runs; the uploaded object is an orphan and is cleaned on the next scan.
- **Implication:** same as local mode 3c — the coordinator's metadata gate prevents Stage N+1 from consuming an unacknowledged partition.

### 4d. Executor Crash After `TaskSucceeded` (partition Available in object store)

- Partition is `Available` in coordinator's `ShuffleMetadata`.
- The partition data lives in the object store, not on the executor's local disk.
- **Stage N+1 reads** via Arrow Flight from the object store can continue from any executor — the failed executor's local disk is not needed.
- **Coordinator action:** marks executor `Lost`; Stage N+1 tasks assigned to the failed executor are reassigned to surviving executors. Surviving executors read the object-store partition directly.
- **Key advantage over local mode:** Stage N does not need to re-run. Stage N+1 recovery requires only task reassignment, not data re-generation.

---

## 5. Summary Table

| Scenario | Local Mode Recovery | Object-Store Mode Recovery |
|---|---|---|
| Crash before finalize (Phase 1–2) | Stage N re-run | Stage N re-run |
| Crash mid-finalize (Phase 3) | Stage N re-run; orphan file cleaned | Stage N re-run; orphan upload cleaned |
| Crash before status report (Phase 4) | Stage N re-run; orphan file cleaned | Stage N re-run; orphan upload cleaned |
| Executor crash after partition Available | Stage N re-run (partition lost on local disk) | Stage N+1 task reassignment only (object store survives) |
| Planned graceful shutdown | No impact if tasks finished; task reassignment if mid-task | Same as local |

---

## 6. Coordinator Metadata Is The Authoritative Gate

In both modes, the coordinator's `ShuffleMetadata` is the single source of truth for partition availability. File or object presence alone does not make a partition available for Stage N+1 consumption. This invariant:

- Prevents partial or uncommitted data from reaching downstream stages.
- Simplifies the Stage N+1 start protocol: wait for `all_available()` on the required partition set, then launch.
- Enables orphan cleanup to be a background operation without affecting correctness.

---

## 7. Interaction With Checkpoint Epochs (R6)

R6 introduces checkpoint epochs for stateful streaming jobs. Checkpoint epochs add a finer-grained recovery boundary than stage boundaries. The recovery expectations above apply to R4 batch shuffle. For R5/R6 streaming, checkpoint epoch ownership determines which shuffle and state data to recover from.
