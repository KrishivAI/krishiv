# Convergent continuous registration + capacity-derived scheduling

**Date:** 2026-07-09
**Repo:** krishiv (engine)
**Status:** approved-by-directive (user: "eliminate this task slot knob with correct
architectural approach and fix bugs, then proceed with soak"; "make sure our approach
is optimized for batch, delta-batch and streaming mode as well")

## Problem

Two coupled defects surfaced while bringing up a **second** concurrent continuous
streaming pipeline (`wiki`) alongside a running one (`orders`):

1. **Non-convergent continuous registration (the wedge).** A continuous streaming job
   is a declarative, desired-state object keyed by `job_id` — the platform pipeline
   reconciler re-drives `POST /continuous-register-sql` to make the running job match a
   spec. But registration bottoms out in `Coordinator::submit_job`, which returns
   `SchedulerError::DuplicateJob` for *any* non-terminal job of the same id. Once an
   entry is present and non-terminal (a healthy job, or a wedged/limbo one whose
   `continuous_job_view` can't render), the reconciler can never converge:
   re-register → `409`, and teardown can't always free the id. Result: the second
   pipeline never runs.

2. **The `KRISHIV_TASK_SLOTS` knob is the wrong abstraction.** Operators must hand-set a
   per-executor slot count with no principled "ideal value." Yet `SlotAwareScheduler::
   place_task_ids_with_load` never rejects for lack of slots — when the per-placement
   budget is exhausted it resets to full. Slots are purely a *load-distribution bias*
   across executors; on a single-executor deployment they are inert. Real overload
   protection already exists as memory-estimate admission in `evaluate_admission`
   (queues a job whose `memory_limit_bytes` ask exceeds cluster-available memory).

## Design

### A. Continuous registration becomes an upsert (fixes the wedge)

Change **only the continuous-streaming registration path**
(`register_continuous_stream_coordinated`, and route `api_continuous_register` through
it). Generic `submit_job` keeps `DuplicateJob` semantics — a duplicate *batch/delta*
submission is still a genuine conflict.

Before submitting, inspect any existing job with the same id:

- **No existing job** → submit fresh (unchanged).
- **Existing non-streaming job** → `DuplicateJob` (id collides with a batch/delta job).
- **Existing streaming job, decodes to the *same* `WindowExecutionSpec`, non-terminal**
  → idempotent success (no-op). Critically, this preserves streaming continuity: a
  steady-state reconcile must NOT tear down and recreate a healthy stream (that would
  reset window state and watermarks).
- **Existing streaming job that is terminal, undecodable (limbo), or decodes to a
  *different* spec** → tear it down (`push_cancel_job` best-effort → `evict_completed_job`
  → `remove_continuous_snapshot`), then submit fresh. This heals limbo entries and
  applies genuine spec changes.

Also harden `api_continuous_deregister`: if the job is present in `job_coordinators` but
`continuous_job_view` fails to render (limbo), still force cancel+evict so the id is
freed, instead of returning `409`. Belt-and-suspenders behind the upsert.

### B. Capacity-derived scheduling weight (eliminates the knob)

The executor advertises a **task capacity** equal to its available CPU parallelism
(`std::thread::available_parallelism()`, which honors cgroup CPU limits) instead of a
hand-set `KRISHIV_TASK_SLOTS`. The env var remains an optional override for advanced
oversubscription/capping, but is no longer required and is removed from the prod deploy.

This weight is task-based and job-kind-agnostic, so it is correct for all three modes:

- **Batch / delta-batch:** multi-task jobs spread across executors proportionally to
  free capacity (`capacity − active_tasks`), the greedy most-free-first placement.
- **Streaming:** each continuous job's long-lived `stream:loop` task counts as active
  load, biasing the next job onto a less-loaded executor.

Actual resource protection stays with the existing memory-estimate admission — the
placement weight is scheduling hygiene, not an admission gate.

## Scope / non-goals

- No change to generic `submit_job` duplicate semantics for batch/delta jobs.
- No new admission gate; memory-estimate admission already covers overload.
- Single-executor prod: the capacity weight is inert there (one executor); the value of
  this change is removing operator guesswork + correctness for the multi-executor path.

## Verification

- Unit: same-spec re-register is idempotent; changed-spec re-register replaces; deregister
  frees a limbo id. Executor capacity defaults to `available_parallelism` when the env is
  unset and honors the override when set.
- `cargo test -p krishiv-scheduler -p krishiv-executor`; clippy clean (engine convention).
- Prod: rebuild engine image, deploy with `KRISHIV_TASK_SLOTS` removed, bring up `wiki`
  and `orders` continuous pipelines **concurrently**, confirm both register + emit windows.
