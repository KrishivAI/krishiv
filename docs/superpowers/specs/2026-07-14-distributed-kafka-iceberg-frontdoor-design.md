# Distributed Kafka→Iceberg streaming submission front-door

**Date:** 2026-07-14
**Author:** engine work (DUR-2 live-cert enablement / Phase 60 #183 + #197)
**Status:** design approved (approach #1), pending implementation plan

## Problem

DUR-2 (#188) recover-commit for the production `IcebergStreamingSink` is code-complete
and **deterministically** unit-certified (append + upsert recover-commit + idempotent
re-run; connectors iceberg suite 378/0), committed (`9ed0e7bb`, `30bcbe8f`), and
deployed to the `krishiv-cert` cluster (`fast-30bcbe8f`, healthy, no regression).

To run the **live distributed** streaming→sink→kill→restore exactly-once cert we need a
running distributed Kafka→Iceberg `stream:rloop` job on the engine-only cert cluster.
There is currently **no engine-direct submission path** for that: `krishiv stream`,
`krishiv pipeline`, and `krishiv submit` are all in-process; the coordinator's
`unified_jobs_http` streaming handler only accepts a windowed `WindowExecutionSpec`
(push-fed); the coordinator Flight SQL rejects `CREATE TABLE ... WITH (connector=...)`.
The distributed Kafka→Iceberg `JobSpec` is constructed only by platformd today (#171).

## Goal

A remote client can POST a small JSON to the coordinator to launch a distributed,
checkpointing `stream:rloop` job that reads a Kafka topic and writes an Iceberg table,
registering `IcebergStreamingSink` into the executor `transaction_log` so DUR-2
recover-commit fires on restore. Enables the live cert and advances the "one SQL front
door" (#183) + connector reachability (#197).

## Approach (chosen: #1 — coordinator HTTP endpoint)

Extend `krishiv-scheduler/src/unified_jobs_http.rs` (or a sibling module) with a
`handle_kafka_iceberg` handler on the existing router (reuses auth + `state.coordinator`).

Request:
```json
{ "job_id": "...", "topic": "dur2-cert", "brokers": "redpanda.krishiv-infra:9092",
  "group_id": "dur2-cert", "table_root": "/warehouse/dur2", "table": "events",
  "mode": "append", "schema": [ {"name":"id","type":"long"}, {"name":"payload","type":"string"} ],
  "parallelism": 1 }
```

Handler builds the distributed `JobSpec` and calls `coord.submit_job(spec)`:

- **Fragment**: `stream:rloop:<job_id>|0/<parallelism>|<window_fragment>`
  (`run_loop::STREAM_RLOOP_PREFIX`). The `<window_fragment>` is a
  `krishiv_plan::window::WindowExecutionSpec` encoded string — for the cert a
  **pass-through** (no windowing) spec that forwards source rows to the sink.
- **Source**: a registry Kafka partition spec parsed by
  `fragment/common.rs::parse_registry_partition_specs` — a `ConnectorConfig{kind:"kafka",
  topic, bootstrap.servers, group.id}` carried as the stage's `InputPartition`
  descriptor (registry connector source; run_loop opens it via
  `connector_registry.open_source`).
- **Sink**: an `OutputContract` with `OutputContractDescriptor::IcebergSink{ root, table,
  mode, key_columns, op_column }` (streaming.rs:791 decode site).
- **Kind**: streaming; **durability**: the coordinator's active profile
  (distributed-durable) drives barrier checkpoints.

### Implementation's first task (to finalize exactly)

The precise construction of: (a) the pass-through `WindowExecutionSpec` encoding, (b) the
registry Kafka `InputPartition` descriptor shape `parse_registry_partition_specs` expects,
(c) the `IcebergSink` `OutputContract` string/descriptor, and (d) how `parallelism`/
key-group ranges are set for a single-subtask job. These are read from
`fragment/run_loop.rs`, `fragment/common.rs`, `fragment/streaming.rs`,
`krishiv-plan/src/window.rs`, and `in_process.rs` (the existing JobSpec builder model at
`in_process.rs:606-644`). A **unit test** in the scheduler asserting the built JobSpec
round-trips + names the Kafka source and Iceberg sink is the correctness gate before deploy.

## Cert flow (live, on krishiv-cert)

1. `rpk topic create dur2-cert` (done); produce N=known rows.
2. `curl` the endpoint from a driver pod → job launches distributed.
3. Wait for ≥1 barrier checkpoint (coordinator logs / checkpoint list).
4. `kubectl delete pod` an executor mid-run (between a `pre_commit` and its
   `commit_through`, forced by producing across a barrier then killing).
5. Coordinator reassigns + sends `RestoreFromCheckpointCommand`; executor drives
   `recover_prepared_refs` → `IcebergStreamingSink::finalize_prepared`.
6. **Assert exactly-once**: query the Iceberg table row count == N, no duplicate ids,
   no lost ids. (Compare committed offsets in the snapshot summary vs produced.)

## Testing

- Scheduler unit test: JobSpec builder produces a well-formed `stream:rloop` spec with the
  Kafka source + Iceberg sink (round-trip / field assertions).
- Existing connectors DUR-2 crash-recovery unit tests remain the deterministic exactly-once
  proof of the recovery logic; this front-door adds the live distributed wire validation.

## Out of scope

- SQL text front-door (`CREATE SINK ... INTO iceberg(...) FROM kafka(...)`) — the endpoint
  takes structured JSON; SQL parsing into this spec is a later Phase-60 layer.
- Multi-subtask key-group parallelism beyond `parallelism=1` (cert uses 1).
- platformd kafka_bridge offset-consultation protocol (#171) — orthogonal.

## Risks

- Getting the internal spec encodings wrong → job rejected or mis-runs. Mitigated by the
  scheduler unit test + a dry-run submit before the kill step.
- Touching the coordinator job-submission path near a certified streaming runtime. Mitigated
  by additive endpoint (no change to existing handlers) + isolated cert cluster.
