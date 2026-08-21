# Task #147 — Distributed streaming: plan of record (2026-08-20)

All 22 NEXMark query classes through the distributed path, then benchmarked
on the 3-node k3s rig. Produced by the architecture scout; line numbers
verified 2026-08-20. Execute as NINE revert-proven commits, in order:

1. krishiv-plan: `StreamingTaskSpec` enum (Window/Join/Pipeline/Stateless) +
   `StatelessQuerySpec { sql, source, side_tables }` in new stream_task.rs.
2. krishiv-sql: move `StatelessBatchExecutor` in from krishiv-engines
   (re-export back); executor already depends on krishiv-sql + DataFusion.
3. krishiv-executor: run_loop_classes.rs — `stream:rjoin:` + `stream:rbatch:`
   fragments (payload = raw JSON spec, `prefix<job>|<sub>/<par>|<json>`),
   side-tagged input keys `{job}#{task}#L`/`#R`, per-side SplitWatermarks
   with min-of-sides advancement (NOT the window-join fragment's max rule),
   join_executors re-keyed by rloop_state_key. Verify grpc.rs input-key
   handling first (risk d).
4. krishiv-executor: `stream:rpipe:` — parallelism 1 ENFORCED with named
   refusal (Q4 stage-1 re-keys category, not a function of the join key;
   parallel pipelines need an inter-stage exchange — follow-up). No
   checkpoint: JoinAggPipeline lacks snapshot (follow-up before checkpointed
   pipelines).
5. krishiv-scheduler: `ContinuousRegisterRequest.stream_spec:
   Option<StreamingTaskSpec>` (exactly one of spec/stream_spec),
   build_continuous_job_spec class-matched to the three new prefixes,
   decode_continuous_job_shape extended, ack echoes `class`. Deliberate test
   flips: run_loop_registration_builds_parallel_subtasks (:2603),
   reregistration convergence, the "does not use a stream:loop or
   stream:rloop fragment" pin (:560).
6. krishiv-runtime: execute_coordinator_continuous_register_task (Window
   keeps legacy byte-identical `spec`; other classes send only stream_spec →
   old coordinators 400 = fail-closed), verify_ack class echo, push gains
   `side`; in-process rig: `LocalStreamOperator` enum in
   continuous_stream.rs + register_stream_task/push_input_side.
7. krishiv-api: submit_streaming_distributed (:3072) flips to the same
   routing ladder as engines/bench (pipeline → join → window → stateless);
   sink + bounded-source refusals stay; parallelism per class (join =
   per-side max, pipeline = 1). Pin the NEW routing with tests.
8. krishiv-bench: distributed harness arm (in-process rig for all 22 +
   real-gRPC fragment integration tests per new kind, modeled on
   coordinator_eos_conformance.rs — the in-process rig bypasses fragments).
9. Live rig: env-gated arm (KRISHIV_COORDINATOR_URL → Session distributed),
   deploy per runbook (fast images → ctr import s1/s2/s3 → roll rig;
   GA-soak CronJob + ports 18086/32010 untouched), grammar.rs placement
   marker (:1033/:1167) rewritten deliberately, harness footer updated.

Key seams (verified): run_loop.rs :787-891 untagged source merge (factor,
don't reuse); continuous_stream_http.rs :1463 build_continuous_job_spec,
:515 decode, :1535 register signature, :1618 launch-failure wedge (risk b);
session.rs :3072 windowed hard-require; coordinator_http_client.rs :814
options / :894 verify_ack; window-join: fragment (streaming.rs :242) is a
producer-less cycle escape hatch — leave it, give it no new producer.
Risks: rpipe checkpoint gap; launch wedge widens; Q13 side-table IPC size
cap; grpc.rs input-key strings.

## Live k3s run — RESULT (2026-08-21, commit 4ed77ea)

Deployed the dedicated helm chart (`deploy/k8s/helm/krishiv`) into the fresh
`krishiv-bench` namespace on the 3-node rig (s1 coordinator + 1 executor per
node, image `localhost/krishiv:fast-<sha>` via ctr import; GA-soak CronJob,
krishiv-platform, and ports 18086/32010 untouched). **Completeness gate:
PASS — all 22 NEXMark query classes registered, pushed, executed, flushed,
and drained end to end across the cluster** at 11k–20.6k ev/sec median
(1000-row batches over the HTTP push path + ssh tunnel; measured end to end
incl. registration and drain).

Six defects the live rig surfaced, each fixed at the seam with a
revert-proven test (commits a8045c2, 2b07881, 2d41988, 640d689, bed5833,
4ed77ea):

1. Harness never deregistered jobs → 3 jobs exhausted the 9 executor
   slots. New `execute_coordinator_continuous_deregister`; harness tears
   down per rep. Plus: coordinator-HTTP client errors now carry the
   response body (the first failure reported only "HTTP 503" while the
   reason sat in the dropped body).
2. Backpressure surfaced as generic 503 → producers treated flow control
   as fatal. Executor ResourceExhausted now maps to a Backpressure error
   and HTTP 429; the push client retries with capped backoff.
3. **Cancel never stopped run-loop fragments** (forget_job purges the
   cancel tombstone before the loop can observe it) → every deregistered
   job leaked its runner slot; each executor ran exactly `slots` jobs and
   then never another. Loops now exit on Arc-identity liveness of their
   own state entry; cancel retires ALL FOUR class state maps (the classed
   maps weren't shared with the gRPC service at all — SharedClassExecutors
   bundle). Helm chart: executor topologySpreadConstraints (a roll had
   co-scheduled all executors onto one node).
4. Pushed run-loop input skipped `coerce_batch_for_window` (owned-split
   reads had it) → any window aggregating a pushed unsigned column died
   with "pre-downcast: UInt64" (q7/q16).
5. `encode_window_execution_spec` injected the legacy default-count into
   top-N specs and then refused its own output → every compiled top-N spec
   400'd at distributed registration (q18/q19; embedded never encodes).
6. **No end-of-stream verb on the run-loop surface**: a bounded stream
   whose event-time span fits inside one window can never close it from
   data alone, so pipelines (q9/q4) emitted nothing and then dumped their
   state into egress at deregister where teardown destroyed it. New EOS
   directive (`POST /api/v1/continuous-flush` → reserved `stream-eos` task
   id on the push RPC → executors flush window/pipeline operators into
   egress), the run-loop sibling of cycle mode's `stream-eos:` partition.
   rpipe's flush-at-cancel removed (partial aggregates are not final —
   the window loop's stance, now uniform). q13: the harness now carries
   the side table in the spec (`side_tables`), the wire-native form of the
   embedded harness's out-of-band registration.

Recorded follow-ups (unchanged): JoinAggPipeline snapshot/restore,
parallel-pipeline inter-stage exchange, join projection from CTE SELECT
list, grammar.rs placement-marker rewrite, batch-at-a-time join
optimization, cached-logical-plan for stateless. New from this run: egress
ring cap (512) truncates large EOS flushes (q9 retained exactly cap batches)
— drain-before-flush or raise KRISHIV_RLOOP_EGRESS_CAP; distributed rows_out
for window queries under-reports vs embedded EOS-flush totals when watermark
closes lag behind (documented, not a defect).

## Durable-mode runs (2026-08-21, commit a6936ac) — BOTH PASS

Re-ran with executors at `single-node-durable` (RocksDB state, batched-WAL
fsync per checkpoint epoch) and 1s barrier checkpointing on every job
(KRISHIV_BENCH_CHECKPOINT_INTERVAL_MS=1000). RocksDB SSTs + per-job
checkpoint dirs verified on disk in both environments.

- 3-node k3s rig (chart: durable executors + emptyDir at /var/lib/krishiv):
  gate PASS 22/22 at 10.4–23.2K ev/s — ~10–20% under dev-local, the
  RocksDB + checkpoint cost.
- Single node (local clusterd + 1 executor, 12 slots, same profile, same
  harness over loopback): gate PASS 22/22 at 82–164K ev/s.

New follow-ups surfaced by durable mode (recorded, not fixed):
- A fragment whose checkpoint storage cannot be created (mkdir EACCES)
  kills the executor DAEMON, not the task — the process exits and the
  coordinator sees Lost.
- The durable executor auto-selects its checkpoint URI at startup and the
  job registration's checkpoint_storage_path is silently ignored;
  --checkpoint-uri is the only override. Registration should either honor
  the job's path or refuse it loudly.
- Stale-binary defense proved out live: the class-echo verify_ack refused a
  week-old local clusterd that would have silently registered cycle jobs.

## Streaming fix pass, task #149 (2026-08-21) — 12 issues, 11 fixed + 1 verified-not-a-defect

The ranked issue list from the durable-mode runs, fixed in full. Every fix
carries a test proven red against the pre-fix behavior; each landed as its
own commit with fmt + clippy(-D warnings) + workspace tests green.

1. **Daemon death on checkpoint-storage failure** (1c46c59): executor
   start-up is now pre-flight — checkpoint storage, state backend, and
   shuffle backend/flight bind are acquired BEFORE registration, so a
   broken mount fails the process before the coordinator ever sees it
   (no more register-then-vanish → Lost).
2. **Registration's checkpoint_storage_path honored** (1c46c59):
   CheckpointBarrier carries the job's storage path (proto field 6);
   executors open/cache per-job storages and an unopenable path fails
   that barrier only, never the daemon.
3. **EOS flush uncapped + paged drain** (66b5840): the egress ring cap no
   longer truncates flush output (q9's loss); drain pages at
   KRISHIV_RLOOP_EGRESS_CAP per call.
4. **JoinAggPipeline snapshot/restore** (7f279ed): pipelines actually
   checkpoint (join + per-stage state, b64; stage-count drift refused).
5. **Run-loop Running state**: verified NOT a defect — the earlier
   0-running observation was the fix-6 slot-leak wedge; live probe shows
   running_task_count 3/3.
6. **Run-loop family lifecycle** (66b5840): is_run_loop_family() governs
   dispatch and terminal states for all four class prefixes — classed
   loops now report Cancelled on cancel, not Succeeded.
7. **Direct-to-executor ingest** (380a7c1): coordinator serves target
   discovery only (GET /api/v1/continuous/{job}/targets); producers push
   Arrow IPC straight to executor task gRPC (bearer-authed, #L/#R side
   suffix in the task id). Harness: KRISHIV_BENCH_DIRECT_PUSH=1. The
   coordinator HTTP hop + base64/JSON re-encode is no longer the
   distributed ingest ceiling. Reaching executor endpoints requires
   in-cluster or loopback producers — not a coordinator-only tunnel.
8. **Batch-at-a-time join output** (94e7ede): match pairs concat into one
   output batch per input batch instead of per-match slice/concat.
9. **Stateless plan caching** (e1cbdb5): the optimized LOGICAL plan is
   compiled once per job (physical planning per batch — DataFusion
   physical plans are single-execution); schema drift refused.
10. **Parallel pipelines** (2e2c839): join-keyed pipelines run at N>1 —
    rpipe gained the rjoin keyed exchange; stage re-keying cases are
    refused by name via parallel_unsafe_reason().
11. **Input buffer dial** (66b5840): KRISHIV_RLOOP_INPUT_BUFFER_CAP
    (default 64), declared in the env registry.
12. **Long-poll drain** (ae12163): DrainContinuousOutputRequest.wait_ms —
    consumers park on the egress notify instead of busy-polling; the
    coordinator grants the wait budget to the first executor only.

The rig image predates this pass; a durable-mode re-benchmark needs a
fresh image build/ship/roll.

## Post-fix durable re-benchmark (2026-08-21, image fast-551d1ba) — ALL PASS

Rebuilt and redeployed after the 12-fix pass; the re-run itself surfaced and
fixed two more defects the fixes had been masking:

- **EOS flush raced pending input** (9fdd8dc): flushing over the head of
  queued-but-unapplied batches lost the windows they would have extended
  (direct push made it a ~40% rows_out deficit). Now a distributed EOS
  barrier: loops raise a busy count while applying taken input; executors
  refuse to flush until buffers-empty + busy==0 (loud DeadlineExceeded on a
  wedged loop); the coordinator runs two full quiesce rounds across all
  executors before any flush (`stream-eos-quiesce` directive). Direct push
  also gained the coordinator path's ResourceExhausted backoff envelope.
- **Stale cancel tombstones across job incarnations** (551d1ba): only the
  FIRST per-subtask cancel RPC of a teardown still found run-loop state, so
  later RPCs skipped forget_job and their tombstones survived — a recreated
  job reusing the deterministic ids had those subtasks insta-cancelled at
  pickup (a 3-parallel job silently ran 2 subtasks; pre-quiesce its pushes
  were silently discarded — likely the cause of historical cross-attempt
  flakiness). The retirement gate now also fires on the `task-streaming[-N]`
  id shape.

Numbers (RocksDB + 1s checkpointing, 22/22 gate PASS everywhere):

- Single node, coordinator push (attempt15): 28–73K ev/s.
- Single node, DIRECT push (attempt16, same stack, full id reuse): windows
  ≈parity (37–42K), join/pipeline classes +10–15% (q3 65K vs 57K, q20 64K
  vs 58K) — the coordinator hop + base64 was the two-source ceiling.
- 3-node rig (executors now properly 1/node incl. CPU-starved s3, ~1.3
  cores free): 5.6–15.8K ev/s. Slower than the 08-21 morning run (10–23K)
  whose executors happened to avoid s3 — placement, not regression; and
  pipelines now pay for REAL 1s snapshots (fix 4).

## q4 parallel (2026-08-21, commit 43ae03a)

The last parallelism-1 holdout runs at N: split pipelines re-key once at
the stage boundary (`parallel_plan()` → `#S` exchange → prestage EOS leg;
see the commit for the full design). Verified live (attempt18, single-node
durable): q4 registered at parallelism 3 (12 rpipe loop starts = 4 reps ×
3 subtasks), gate PASS 22/22, q4 rows_out identical to parallelism-1
(10), throughput ~71K ev/s (flat vs 72K at parallelism 1 on one machine —
the win is distributing stage state and work across nodes, not single-box
speed; the exchange costs ~3% locally). The rig image fast-551d1ba
predates this commit; q4 runs at 1 there until the next roll.
