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
