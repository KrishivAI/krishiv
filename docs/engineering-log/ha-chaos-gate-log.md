# Phase 58 HA and shuffle-loss runbook

This runbook covers the supported `distributed-durable` coordinator profile,
active/standby failover, shuffle regeneration, and the scheduled chaos gate.

## Supported topology

- Run three `krishiv clusterd` replicas on distinct nodes.
- Use the same three-or-more-member etcd cluster for `--leader-backend etcd`
  and `--metadata-backend etcd`.
- Give every coordinator a unique `KRISHIV_COORDINATOR_ID` (the Kubernetes
  pod name), a shared lease key, and authenticated executor/task endpoints.
- Route coordinator traffic only to `/leaderz`-ready pods. `/healthz` means
  the process is alive; `/readyz` additionally requires a healthy executor.
- Executors and their local shuffle/state directories are replaceable.
  Durable checkpoints belong in object storage.

The runnable certification topology is
[`deploy/k8s/phase58/ha-cert.yaml`](../../deploy/k8s/phase58/ha-cert.yaml).

## Coordinator failover

1. Identify the active coordinator from the Service EndpointSlice. Exactly one
   endpoint must have `conditions.ready=true`.
2. Before promotion, a standby refreshes jobs, executor descriptors,
   continuous snapshots, IVM snapshots, and completed-job history from etcd.
   Promotion fails closed if that refresh fails.
3. The promoted coordinator receives a new etcd fencing token. Old assignments
   and checkpoint acknowledgements are rejected by lease generation/fencing
   checks.
4. Executors reconnect through the coordinator Service and unfinished work is
   rescheduled. The production SLO is one routable leader and resumed
   scheduling within 30 seconds.

If no leader appears within 30 seconds, inspect the etcd quorum first, then
coordinator logs for `promotion recovery failed`. Do not bypass fencing or
route traffic to a standby. Restore etcd quorum and allow election to retry.

## Shuffle loss

Shuffle output is owned by its producing map task and executor. When a reduce
reports a missing partition or its producer is lost, the coordinator
invalidates that output, resets the producing maps, and regenerates the stage.
Regeneration is bounded by the task/stage retry budget; exhaustion is a typed
terminal failure rather than an infinite loop.

For diagnosis, correlate the job history record with coordinator messages for
`missing shuffle`, map-task resets, executor loss, and the final typed
`failure_class`/`failure_code`. Replacing an executor is safe; copying its
partial shuffle directory into another executor is not.

## Certification and scheduled gate

Run `./scripts/phase58_chaos.sh`. It covers batch multi-stage, parallel
streaming, and resident IVM workloads against executor kill, active coordinator
kill, shuffle-producer kill, and an isolated-pod network partition. Defaults
are two consecutive runs of 25 iterations. It asserts one active endpoint,
failover within 30 seconds, real multi-executor batch stages, durable IVM state,
and completed-job history after another coordinator restart.

The scheduled workflow is `.github/workflows/phase58-chaos.yml` and requires a
self-hosted runner labelled `krishiv-chaos` with `kubectl` access to the
certification namespace. Preserve the complete harness log as release evidence.

## Rollout and rollback

Roll standbys first and verify they are alive but absent from the ready
EndpointSlice. Then delete the old active pod so a new-version standby is
promoted. Never force all three coordinator pods down together.

For rollback, restore the previous image on every node, replace the two
standbys, then replace the active. etcd keys are per-record and forward reads
fail closed; take an etcd snapshot before any release that changes persisted
schema versions.

## 2026-08-08 — gate re-run: coordinator-kill REGRESSED (KRV_SHUFFLE_MISSING)

First gate run since the 2026-07-20 clean pass, on a kind cluster (topology
adapted: executor/driver `nodeName` pins repointed from the CI rig's s1–s3,
`phase58-tokens`/`minio-s3-creds` recreated, events/changes datasets
reseeded, image built from `--features prod`).

- PASS: batch, streaming, and ivm through executor-kill (iterations 0–2),
  leader election <30s, one active endpoint, two-executor re-registration.
- **FAIL, twice, from a clean 3-coordinator topology: batch ×
  coordinator-kill.** After the standby promotes, reduce tasks retry into
  `KRV_SHUFFLE_MISSING(stage=s0.mN)` — "the coordinator attached no location
  for this stage key at all" — because the map-output locations lived only
  in the dead leader's memory. The producer stage is never regenerated, so
  the job lands `failed` with 1 failed task (of 9–16). Steady-state
  submit→Succeeded on the same topology works (9/9 tasks).
- Reading: in-flight jobs' shuffle-location registry is not part of the
  durable state the promoted coordinator recovers, and there is no
  fetch-failed → regenerate-producer-stage path. One of the two must exist
  for the failover story to hold for running batch jobs. The 2026-07-20 run
  passed this cell; the regression window is the three weeks of scheduler /
  shuffle / runtime-filter work since — the gate simply did not run across
  it. Bisect before fixing forward.

### Correction, same day: NOT a regression — the 2026-07-20 pass was false

Code archaeology (full `-S` history over `store.rs`): shuffle map-output
locations have NEVER been part of the etcd-persisted job state.
`PersistedShufflePartition` is `{stage_id, partition_id}` only;
`PersistedTaskOutputMetadata` drops `shuffle_partitions` (and their
`flight_endpoint`s) on the floor (`store.rs:876-882`, `:1183-1192`), and
`PersistedTaskSpec` does not carry `shuffle_write` (`store.rs:854-865`).
After promotion, both guards in `missing_report_addresses_task`
(`job/record.rs:218-233`) read exactly those absent fields, so the
regenerate-producer path — which exists and works for executor-kill —
diagnoses `NoneAffected` and does nothing.

What changed in the window is DETECTION: before `74fcae1` (2026-07-27,
"an unlocatable shuffle partition is an error, not empty"), this same
promotion scenario read the missing partitions as EMPTY and completed
`Succeeded` with silently short results. The gate asserts only
`"state":"Succeeded"` — no row count, no digest — so 2026-07-20 scored
silent data loss as a pass.

Do NOT bisect (it lands on `74fcae1`, which is a fix, not a fault) and do
not revert it. Fix options, either sufficient:
  a) persist `(stage_key, partition, flight_endpoint, executor_id,
     size_bytes)` in `PersistedTaskOutputMetadata` + `shuffle_write` in
     `PersistedTaskSpec`, so recovery rebuilds the location registry; or
  b) have executors re-advertise their held shuffle output on
     re-registration (`executor_ops.rs:19-88`) — the data plane still has
     the partitions; only the coordinator's map of them died.
Also: the gate needs a row-count/digest assertion so an empty-read bug can
never score PASS again.

### 2026-08-08 — FIXED (option a: persist the locations)

`PersistedTaskOutputMetadata` now carries `shuffle_partitions`
(partition_id, size_bytes, flight_endpoint) and `PersistedTaskSpec` carries
`shuffle_write` (stage_id, num_partitions, key_columns, lease_token), both
`#[serde(default)]` so pre-fix stores still load with the fields empty. The
four store.rs conversions round-trip them; recovery reconstructs the
location registry, so `shuffle_location_inputs` attaches locations to reduce
specs and `missing_report_addresses_task` can name the producer again.
Round-trip + legacy-load unit tests in store.rs; `cargo clippy
-p krishiv-scheduler -D warnings` clean.

**Verified live**: rebuilt the phase58-ha image on the fix, brought the
control plane fully onto it, re-ran `scripts/phase58_chaos.sh` — the
`batch × coordinator-kill` cell (iteration 3), which failed twice before,
PASSES, along with `streaming` and `ivm × coordinator-kill` (iterations
4–5). The whole coordinator-kill fault class recovers.

Still owed (unchanged): the gate's own missing correctness assertion — it
asserts `Succeeded`, never a row count or digest, which is exactly why the
pre-`74fcae1` empty-read scored PASS. Add one before trusting a green run.

### 2026-08-08 — separately surfaced: streaming × shuffle-kill is FLAKY

The post-fix full 2×25 ran 44 consecutive PASS (all of run 1, 19 of run 2)
— every coordinator-kill cell green, confirming the shuffle-location fix —
then failed at run 2 iteration 19 on `streaming × shuffle-kill`:
`do_action ... unknown job: phase58-...-streaming`. This cell PASSED three
other times the same run (run1 i7, run1 i19, run2 i7), so it is
**intermittent, not deterministic** — a distinct pre-existing streaming
recovery bug, NOT the shuffle-location regression (which never flaked).

Signature: after a shuffle-write executor is killed, `stream push` returns
`scheduler error: unknown job` where a batch job in the same slot recovers.

**Root-caused 2026-08-09 (code analysis, not the runbook's earlier
peer-table guess).** Registration IS durable — `submit_job` persists to
etcd synchronously before the in-memory insert, and `recover_from_store`
rebuilds it on every promotion — and executor loss never drops a job from
the registry (that needs 5 consecutive losses). The defect is an
asymmetry: `register` leader-fences up front (`ensure_active()`), but the
**push path checked job existence BEFORE leadership**. So a push landing
on a demoted/standby/not-yet-recovered replica during a leadership
transition (executor churn can park the etcd bridge on `block_in_place`
and cost the leader its 9s lease) fell into `run_loop_targets`'
`UnknownJob` and surfaced as a hard, non-retryable `unknown job` — which
`retry_engine` treats as terminal — instead of a retryable `Unavailable`
whose retry the client's Service routes to the real leader.

**Partial fix 2026-08-09** (leader-fence): both push entry points now
`ensure_active()` before the existence check, so a push to a NON-leader is
a retryable 503. Unit-tested
(`continuous_push_to_a_standby_is_retryable_not_unknown_job`). Correct, but
**the gate re-run (2×25, fixed image) STILL FAILED** the same cell — so
this was NOT the operative cause. The failing push's `unknown job` came
*past* the fence (`ensure_active` passed → the coordinator WAS active),
proving the active leader genuinely lacked the job.

**Actual mechanism, from the failing iteration's coordinator logs
(2026-08-09, job `…-r1-i19-streaming` on pod `657458454-7tqnx`):**
the coordinator that owned the job was **freshly created at 18:00:03** (a
promotion — the executor-kill churn cost the prior leader its 9s lease).
The job was submitted to it at 18:00:17 and registered; its task went
`Running` on `exec-s3` at 18:00:18.073, then **`Failed` at 18:00:18.098**
when the shuffle-kill killed `exec-s3`. **The streaming task terminated
`Failed` instead of being reset-to-`Pending` and reassigned to a survivor**
— which is what `reset_running_tasks_for_lost_executor` does for a
continuous task under normal conditions. The job then evicts on the
terminal failure, and the next push reports `unknown job`.

The bug: `refresh_state` (job/record.rs) marks a job `Failed` on ANY
`Failed` stage with no streaming exception, while the executor-loss reset
(`reset_running_tasks_for_lost_executor`) only runs on the SLOW
heartbeat-timeout path and only matches `Running`/`Assigned`/idle-`Succeeded`
tasks. A streaming task whose executor is SIGTERM'd (chaos `shuffle-kill`
= `kubectl delete pod`) SELF-reports `Failed` in ~25 ms — far ahead of the
9-tick heartbeat — so the fast failure drives the job terminal and evicts
it before recovery can act; the next push gets `unknown job`.

**FIXED + CONFIRMED 2026-08-09.** `rescue_failed_continuous_task`
(`coordinator/executor_ops.rs`, called from `apply_task_update`) resets a
continuous task's fast `Failed` back to `Pending` and reassigns it — within
the same `MAX_EXECUTOR_LOSSES_BEFORE_FAIL = 5` budget, seeding the
checkpoint restore — keeping the streaming job `Running`. A genuinely
broken task still fails after 5 consecutive failures. All 543 scheduler
tests pass; clippy clean. **The full 2×25 gate now PASSES on the fixed
image: 51/51, 0 failures, all 16 streaming cells green across both runs
(including every streaming×shuffle-kill and ×executor-kill), plus the
final failover-≤30s / one-leader / durable-history check.** Phase 58's
twice-consecutive exit gate is met. The leader-fence fix stays too (it
closes a real, separate non-leader push window).

### 2026-08-10 — digest assertions land; they immediately kill two hollow cells

The gate's owed correctness assertion is now built in
(`scripts/phase58_chaos.sh`): before the matrix, every workload runs twice
in steady state — capture + determinism check — and each fault iteration
must then reproduce the baseline content digest to PASS. `Succeeded` alone
no longer scores. Batch verifies the fault-surviving job's OWN result rows
(`GET /api/v1/batch-sql/{id}`, sha256 over sorted rows); streaming digests
the poll's windowed rows (main push, then an `advance.parquet` push that
closes the tail window); IVM asserts the post-fault re-run is EXACTLY 2×
the pre-fault view per key (incremental semantics: 1× = state lost,
3× = double-applied) plus a baseline digest on the pre-fault view.
Loud, bounded carve-outs where delivery is best-effort by design
(coordinator-kill: batch inline results and undrained stream output live
in leader RAM — DUR-5).

Two findings the first baseline run surfaced immediately, before any fault:

1. **Every prior streaming cell was hollow.** The gate submitted
   `--event-time-column ts` against a dataset whose column is
   `event_time`; every poll returned 0 rows and exit-code-only scoring
   called that PASS — including all 16 "green" streaming cells of the
   2026-08-09 51/51 run. The fixed workload digests 87,623 windowed rows.
2. **The Flight drain path silently lost oversized cycle output** (engine
   fix, same day). `ContinuousDrain` consumed the store, THEN size-checked:
   a >64 MiB response was rejected client-side and the client's streaming
   fallback re-drained an EMPTY store — 87k rows reported as "0 rows", no
   error anywhere. Fixed server-side: the action now measures the encoded
   response against a 48 MiB budget (`KRISHIV_FLIGHT_DRAIN_ACTION_MAX_BYTES`)
   and, when oversized, PUTS THE PAYLOAD BACK
   (`Coordinator::unshift_job_inline_results`, order-preserving; host-level
   buffer for the in-process backend) before returning
   `resource_exhausted` — so the existing streaming fallback then delivers
   it. Regression tests: scheduler unshift ordering; flight-sql
   end-to-end oversize→retry-delivers-all-rows.

The rescue fix (`4f20d74`) also gained its missing unit test
(`fast_failed_continuous_task_is_rescued_not_terminal`: rescued below the
loss budget, terminal at it). Rig standup for kind is now scripted:
`scripts/phase58_kind_up.sh` (fresh secrets, MinIO + DUR-2 bucket,
deterministic datasets incl. `advance.parquet`, nodeName repoints).

### 2026-08-10 — TWICE-CONSECUTIVE 2×25 PASS on the digest-hardened gate

Both invocations ran on the same image (pushed HEAD + this session's fixes)
on the kind rig stood up by `scripts/phase58_kind_up.sh`:

- **Invocation 1: exit 0 — 50/50 iterations PASS**, final failover-≤30s /
  one-leader / durable-history check green.
- **Invocation 2 (immediately after, fresh MATRIX_ID): exit 0 — 50/50
  PASS**, same final check green.
- **Zero digest downgrades and zero carve-out skips across both runs**:
  every one of the 100 fault iterations reproduced its steady-state
  content digest exactly (batch rows=1000, streaming rows=8750, ivm
  rows=500 with the 2× incremental check) — including every
  coordinator-kill cell, where the loud DUR-5 carve-outs never even fired.
  Baselines were bit-identical across both invocations and across a full
  rig rebuild.

En route, the digest gate forced two more engine fixes that are part of
this certification: the oversized-drain put-back (a >64 MiB cycle output
was silently lost as "0 rows") and the shuffle open-attempt bound (a
black-holed producer IP cost 405 s of kernel connect timeout before
regeneration; now ≤15 s per attempt, recovery well inside the workload
budget — batch × executor/shuffle-kill cells pass with the fetch retry
loop in charge of its own schedule).

Known residual, deliberately out of this gate's scope: the drain's
streaming SQL/do_get fallback still consumes before delivery completes, so
a client killed mid-transfer of a >48 MiB drain loses that payload —
structural fix tracked as Phase 55 streamed-results / drain-ack. The
gate's streaming workload rides the bounded atomic path
(`events_stream.parquet`), and the oversized path is pinned by engine
unit/service tests.

**Phase 58's twice-consecutive 2×25 exit gate: met, this time on a gate
that verifies content.** (The 2026-08-09 "51/51" run predates the digest
assertions; its streaming cells were vacuous — see above.)
