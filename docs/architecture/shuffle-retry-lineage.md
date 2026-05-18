# Krishiv Shuffle Retry Lineage Policy

**Status:** Decision — approved for R4 implementation.
**Owner:** Architecture team.
**Linked releases:** R4 (shuffle), R5 (streaming retry), R6 (checkpoint-based recovery).

---

## 1. Problem

When a multi-stage job fails, the coordinator must decide which stages to re-run and whether upstream shuffle output can be reused. Two extreme options exist:

- **Option A (full restart):** discard all shuffle output for the job and re-run every stage from the beginning.
- **Option B (partial restart):** retain completed upstream shuffle output and re-run only the failed stage(s).

Krishiv uses **Option B** as the default for downstream stage failures and **forced Option A** for upstream stage failures, as documented below.

---

## 2. Case 1: Downstream Stage Failure

**Definition:** Stage N has completed and its output shuffle partitions are marked `Available`. Stage N+1 (a downstream consumer) fails during execution.

**Policy (Option B):** Retain Stage N shuffle output. Re-run only Stage N+1, reading Stage N partitions from the existing shuffle store. Stage N is not re-executed.

**Rationale:**
- Stage N output is already durable in the shuffle store (`LocalShuffleStore` on local disk, or object store in `object-store` mode).
- Re-running Stage N would waste the compute already invested and unnecessarily extend recovery time.
- Stage N+1 tasks are idempotent: they read the same shuffle partitions, produce the same output given the same input, and overwrite any previous partial output.

**Implementation requirements:**
- Coordinator retains shuffle partition metadata (`ShuffleMetadata`) for Stage N until the job completes or is explicitly cancelled.
- `StageRecord` for Stage N+1 transitions back to `Scheduling` on failure; shuffle reads for Stage N are served from the existing store.
- Stage retry count is tracked per stage; after `max_stage_retries`, the stage (and job) transition to `Failed`.

---

## 3. Case 2: Upstream Stage Failure

**Definition:** Stage N fails during execution. Stage N+1 has not started (because Stage N output was not yet fully `Available`).

**Policy (forced Option A for the failed upstream stage):** Discard any partial shuffle output from Stage N (partitions in `Pending` or `Failed` state). Re-run Stage N from its source inputs. Stage N+1 remains blocked until Stage N produces fully `Available` partitions.

**Rationale:**
- Partial shuffle output from a failed Stage N is incomplete and cannot be trusted for Stage N+1 consumption.
- Stage N source inputs are always re-readable (Parquet files, object store objects, or rewindable Kafka offsets from the commit log).
- Retaining partial output would require Stage N+1 to distinguish "this partition is complete" from "this partition is partial", adding complex protocol state.

**Implementation requirements:**
- On Stage N failure: coordinator calls `delete_job_partitions` for Stage N's `ShufflePath` set to clean up partial data.
- Stage N is re-planned and re-assigned on the next available executors.
- Partitions for Stage N are re-registered as `Pending` before re-launch.

---

## 4. Mixed Case: Mid-Stage Upstream Failure

**Scenario:** Stage N has 4 tasks. Tasks 0–2 finish and produce `Available` partitions. Task 3 fails. Stage N+1 has not started.

**Policy:** This is treated as Case 2 (upstream failure). Coordinator discards all 4 Stage N partitions (including the completed ones from Tasks 0–2) and re-runs all 4 Stage N tasks from source.

**Rationale:** Mixing old and new Stage N partitions in Stage N+1 risks incorrect results if any Stage N executor processed a partially overlapping input range. A clean re-run is safer and avoids tracking per-partition provenance across retry generations.

**Future:** R6 checkpoint epochs will enable finer-grained provenance tracking. After R6, a certified checkpoint-aware retry path may allow partial Stage N reuse in specific conditions. Until then, full Stage N re-run is the safe default.

---

## 5. Lineage For Cancelled Jobs

When a job is cancelled via `CancelJob`, all shuffle partitions for all stages are deleted. No retry or lineage preservation applies.

Cleanup order: `cancel_job` → `push_cancel_task` to executors → `delete_job_partitions` for each stage → mark job `Cancelled`.

---

## 6. Stage Retry Budget

Each stage has a per-stage retry budget of `max_stage_retries` (configured in `CoordinatorConfig`). The retry budget is consumed when the stage re-runs due to failure, regardless of whether it is a Case 1 or Case 2 retry. When the budget is exhausted, the stage transitions to `Failed` and the job transitions to `Failed`.

---

## 7. Interaction With R6 Checkpoints

R6 introduces checkpoint epochs for stateful streaming jobs. The retry lineage policy above applies to R4 batch jobs only.

For R5/R6 streaming jobs, the coordinator uses checkpoint barriers to define the recovery boundary. Stage retry in a streaming context means re-processing from the last committed checkpoint epoch, not from the original source. The R5 streaming execution model document defines this separately.
