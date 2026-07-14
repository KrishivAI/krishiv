# Distributed Kafka→Iceberg streaming submission front-door

**Date:** 2026-07-14
**Author:** engine work (DUR-2 live-cert enablement / Phase 60 #183 + #197)
**Status:** SUPERSEDED — the front-door already exists; this doc is now a
correction + a live-cert runbook.

## CORRECTION (2026-07-14)

The original version of this design claimed there was **no engine-direct
submission path** for a distributed Kafka→Iceberg `stream:rloop` job and
proposed building a new coordinator HTTP `kafka-iceberg` handler. **That premise
was wrong.** It came from an incomplete read of
`crates/krishiv-scheduler/src/continuous_stream_http.rs`: I had only looked at
the cycle-model path and the `unified_jobs_http` streaming shim (which forwards
a bare `WindowExecutionSpec`), and missed the run-loop path.

The distributed front-door is already built and HTTP-reachable via **two**
endpoints (both registered in `coordinator_daemon.rs` ~line 500):

- `POST /api/v1/continuous-register` (`api_continuous_register`) — JSON body
  `ContinuousRegisterRequest { job_id, spec, sink?, parallelism?, mode?,
  sources[], checkpoint_interval_ms?, checkpoint_storage_path? }`.
- `POST /api/v1/continuous-register-sql` (`api_continuous_register_sql`) — the
  **SQL front door**: a windowed streaming SQL string is compiled by the
  coordinator (`krishiv_sql::streaming_window_plan::compile_streaming_window_sql`)
  into the `WindowExecutionSpec`, with the same run-loop/Kafka/Iceberg/checkpoint
  options.

Both map their body → `ContinuousRegistrationOptions` →
`register_continuous_stream_with_options` → `build_continuous_job_spec`, which
for `mode = "run-loop"` builds exactly the intended spec:

- **Fragment:** `stream:rloop:<job_id>|<subtask>/<parallelism>|<encoded_spec>`
  (`build_continuous_job_spec`, one `task-streaming-<i>` per subtask).
- **Source:** registry connector sources (`sources: [{kind:"kafka", table,
  config:{...}}]`) are carried as input partitions and opened by the run-loop
  subtask via the connector registry (`launch_run_loop_job`).
- **Sink:** `ContinuousSinkSpec` → `iceberg-sink:<root>|<table>|mode=...` contract
  string on every task (`OutputContractDescriptor::parse_iceberg_sink`
  round-trip-validated at registration).
- **Barrier checkpoints:** when both `checkpoint_interval_ms` and
  `checkpoint_storage_path` are set, `JobSpec::with_checkpoint(interval, path)`
  wires the coordinator-driven barrier pipeline.
- **Launch:** run-loop jobs are assigned + peer-wired + launched in
  `launch_run_loop_job`; from there the coordinator is control-plane-only.

The executor-side run-loop registers `IcebergStreamingSink` into the
`transaction_log` (`fragment/streaming.rs` ~813), so the DUR-2 report + recover
chain fires on this path. **No new submission surface is required.**

## What this means for DUR-2 (#188)

The DUR-2 recover-commit code (`30bcbe8f`) is already deployed to `krishiv-cert`
(`fast-30bcbe8f`). The live distributed streaming→kill→restore exactly-once cert
is therefore runnable **now** against the existing endpoint + image — it does not
depend on any unbuilt feature. See the runbook below.

Phase 60 residual (#183/#197) is **not** "build the front-door" (done). It is the
narrower "one SQL front door across the three engines" convergence + connector
one-registry dispatch (#197) — a separate, smaller scope tracked under those
tasks.

## Live DUR-2 cert runbook (krishiv-cert, engine-only)

Precondition: `krishiv-cert` healthy on `fast-30bcbe8f` (coordinator +
≥2 executors), redpanda in `krishiv-infra`, a coordinator HTTP route reachable
from a driver pod with a bearer token.

1. `rpk topic create dur2-cert`; produce N=known rows (id, payload).
2. From a driver pod, `POST /api/v1/continuous-register` with:
   ```json
   { "job_id": "dur2-cert", "spec": <pass-through window spec>,
     "mode": "run-loop", "parallelism": 1,
     "sources": [{ "kind": "kafka", "table": "events",
       "config": { "bootstrap.servers": "redpanda.krishiv-infra:9092",
                   "topic": "dur2-cert", "group.id": "dur2-cert" } }],
     "sink": { "root": "/warehouse/dur2", "table": "events", "mode": "append" },
     "checkpoint_interval_ms": 2000,
     "checkpoint_storage_path": "file:///warehouse/dur2-ckpt" }
   ```
3. Wait for ≥1 barrier checkpoint (coordinator checkpoint list / logs).
4. `kubectl delete pod` an executor running a subtask, mid-run (produce across a
   barrier first so an epoch is prepared-but-not-committed).
5. Coordinator reassigns + sends `RestoreFromCheckpointCommand`; the executor
   drives `recover_prepared_refs` → `IcebergStreamingSink::finalize_prepared`
   (offset-gated idempotent).
6. **Assert exactly-once:** Iceberg row count == N, no duplicate ids, no lost
   ids; committed offsets in the snapshot summary
   (`krishiv.kafka.committed_offsets`) cover exactly the produced range.

## Testing

- Existing connectors DUR-2 crash-recovery unit tests remain the deterministic
  exactly-once proof of the recovery logic (append + upsert recover-commit +
  idempotent re-run; connectors iceberg suite 378/0).
- This runbook adds the live distributed wire validation on real Kafka + real
  executor kill.

## Risks

- The pass-through (no-window) `WindowExecutionSpec` encoding for a plain
  Kafka→Iceberg forward must be the one the run-loop accepts; the
  `continuous-register-sql` path sidesteps this by compiling from SQL.
- Timing the kill between `pre_commit` and `commit_through` requires producing
  across a barrier; if the window is missed the cert is inconclusive (retry),
  not a failure.
