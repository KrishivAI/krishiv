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
