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

Signature: after a shuffle-write executor is killed, the continuous job's
registration intermittently vanishes from the coordinator (`unknown job`),
where a batch job in the same slot recovers. Likely the run-loop
peer-table refresh residual already filed on Phase 55
(`parse_stream_peers` is start-of-task only) surfacing as lost
registration under executor loss. Needs its own investigation; it blocks
the *twice-consecutive* 2×25 that Phase 58's exit gate wants, but does not
reopen the coordinator-kill fix. Filed here so the next chaos run does not
mistake it for a durability regression.
