# Phase 62 GA soak — restart report & injected-fault log (2026-08-10)

## The honest finding first: the prior "running since 07-25" claim is unverifiable

The tracker carried "soak has run since 07-25, unreported." What the record
actually supports:

- The 07-25 honesty audit itself (`phase62-honesty-audit-2026-07-25.md`)
  states the 7-day soak **had not run** at that date and that the
  long-lived `cert-soak-driver` was a liveness loop on a stale image.
- The real soak machinery landed 07-25 (`983a2ce`, `a64cd7f`) and got a
  cronjob fix on 07-30 (`99f0350`) — so *a* soak ran somewhere in that
  window. Its evidence store was **pod logs in the `krishiv-cert`
  namespace**, and no cluster carrying that namespace exists today (the
  current kind cluster is 2 days old). Whatever the July soak observed did
  not survive.
- **A second hollow-claim class found while restarting**: the chaos
  CronJob's victim selectors named the old hand-rolled topology
  (`app=cert-v2-exec-a/-b`). Against a helm-chart deployment those match
  nothing — every kill would log `CHAOS SKIP … no-pod` forever while the
  driver kept reporting healthy. A chart-based "soak with chaos" could
  have run green for weeks without a single injected fault. Fixed in
  `soak.yaml` (selectors now match the chart's
  `app.kubernetes.io/instance=cert-v2,component=…` labels), and the fix
  was proven live (see the fault log — a real pod died).

Conclusion: **the 7-day window starts today.** This report records the
restart, the first hours of evidence, the injected-fault log so far, and
the runbook rehearsals executed live against the same estate.

## Restart record (all times UTC, 2026-08-10)

| Time | Event |
|---|---|
| ~17:20 | `cert-v2` engine deployed on kind from the helm chart (`deploy/k8s/helm/krishiv`, release `cert-v2`, ns `krishiv-cert`, image = current main content): 1 coordinator, 3 executors spread across the 3 kind nodes. |
| 17:33:50 | `ga-soak-driver` START (Flight-SQL correctness leg) + `ga-soak-modes` START (IVM/batch/stream lifecycle leg). |
| 17:36–17:40 | 2 AVAILABILITY_FAIL on each leg — the coordinator port realignment rollout (chart defaults 7070/7072/7073 → the soak's contractual 2001/2002/2003). Self-healed; classified correctly as availability, never correctness. |
| 17:49:26 | First HEALTH heartbeat: `n=20 ok=18 availability_fail=2 correctness_fail=0`. Modes leg: `n=10 ivm_ok=9 batch_ok=9 stream_ok=9` (the 1 failure each = the same rollout window). |
| 20:15:17 | Manual chaos trigger #1: `CHAOS SKIP selector=app=cert-v2-exec-a no-pod` — the stale-selector finding above, caught red-handed. |
| 20:16:45 | After the selector fix: `CHAOS KILL … pod=cert-v2-executor-5b7469749c-pggtt` — a Running executor genuinely deleted; replacement scheduled immediately. |
| 20:23:59 | The kill's blast radius, classified correctly: one `AVAILABILITY_FAIL` ("job finished in state Cancelled" — the in-flight query on the killed executor). |
| 20:26:34 | Post-kill heartbeat: `HEALTH n=20 ok=19 availability_fail=1 correctness_fail=0` — **recovery with zero wrong answers, the exact verdict semantics the gate defines.** |

**Verdict criteria (unchanged from the soak's own manifest):** zero
`CORRECTNESS_FAIL` over the 7-day window; `AVAILABILITY_FAIL` during a
chaos kill is expected and is what recovery is measured on. Chaos cadence:
`17 */6 * * *` (executors; the 05:00 slot takes the coordinator).

**Read the evidence:**
`kubectl logs -n krishiv-cert deploy/ga-soak-driver -c driver | grep -E 'HEALTH|FAIL'`
(and `-c modes` for the lifecycle leg;
`kubectl logs -n krishiv-cert job/<chaos-job>` for kills).

## Runbook rehearsals — executed live (Phase 62's second open item)

All three procedures from the HA runbook were performed against live rigs
today, as an operator would:

1. **Failover** (phase58 HA rig: 3 coordinators, etcd×3): identified the
   leader from the EndpointSlice, deleted it, watched promotion.
   **New leader in 5 s** (SLO ≤30 s), exactly one ready endpoint after.
   (The same procedure was also exercised ~25× coordinator-kill iterations
   by the digest-hardened chaos gate earlier today, 100/100 green.)
2. **Rescale** (cert-v2): executors 3→5 (all registered Healthy at the
   coordinator), then →1 **with a query submitted mid-scale-down — it
   succeeded**, then →3. Executor registry tracked every step.
3. **Savepoint-upgrade** (phase58 rig, durable MinIO checkpoint storage):
   registered a checkpointed continuous job → pushed 10k events (8,513
   window rows drained) → `stop-with-savepoint` returned
   `savepoint_epoch: 1` → **rolling-restarted every coordinator and
   executor** (the upgrade cut) → re-registered → a single
   watermark-advancing push emitted **237 rows from the pre-upgrade
   window state** — state continuity across the upgrade proven
   behaviorally, not by assertion.

## Honest scope notes

- The estate is a 3-node kind cluster on one physical box: chaos kills,
  failover timing, and recovery semantics are real; wall-clock performance
  numbers from this estate are not load-isolated.
- The cert-v2 profile is the chart default (`dev-local`). Durability under
  fault is certified separately and more strictly by the phase-58
  digest-hardened chaos gate (twice-consecutive 2×25, content-exact,
  2026-08-10) and the DUR-2 test suite.
- Day-7 verdict lands on 2026-08-17. Until then this phase's soak box
  stays open with this report as the interim record; nothing before
  2026-08-10 counts as soak evidence.
