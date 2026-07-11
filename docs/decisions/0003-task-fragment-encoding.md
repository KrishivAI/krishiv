# 0003: Task fragment encoding — DataFusion plan proto in a typed envelope

- Status: Accepted
- Date: 2026-07-11
- Owners: project maintainers

## Context

Every distributed task the coordinator dispatches carries its work as a
string: `TypedTaskFragment { version, execution_kind, body: String }`
(`krishiv-plan/src/task_fragment.rs`). The bodies in production today are

- `sql: <query>` — batch: the executor re-parses, re-plans, and re-optimizes
  the full query per task;
- `stream:loop:<…>` / window-spec strings — streaming;
- `delta:step:{job}|{deltas_b64}|{specs_b64}|{state_b64}` — delta-batch/IVM.

A batch SQL job is one stage with one task (`batch_sql.rs`), so "distributed"
batch is remote execution, not scale-out. Partition-parallel stages (Phase 52)
need a fragment that addresses *a partition of a plan subtree*, which a SQL
string cannot express: the map task for partition 7 of a hash-repartitioned
aggregate has no SQL spelling. Ballista proves the architecture on the same
DataFusion base with protobuf-encoded physical-plan fragments, stage-per-
exchange, and task-per-partition.

Two candidate encodings were evaluated against the pinned DataFusion 54.0.0:

1. **DataFusion plan proto** (`datafusion-proto`, version-locked to the
   `datafusion` workspace pin). Serializes *physical* plans, including
   parquet scans with their file groups, filters, joins, and partial/final
   aggregates. Extension nodes (our shuffle reader) plug in through
   `PhysicalExtensionCodec`.
2. **Substrait** (`datafusion-substrait`). Cross-engine standard, but DF's
   consumer/producer covers *logical* plans; physical round-tripping —
   which stage splitting requires, because stages are cut at *physical*
   repartition boundaries — is not supported. Using it would force each
   executor to re-run physical planning per task, reintroducing the
   per-task planning tax Phase 52 exists to remove.

The risk gate in the Phase 52 plan ("if DF plan-proto round-tripping fights
the pinned DF version, land on Substrait") was exercised as a spike:
scan→filter→partial/final-aggregate and hash-join physical plans round-trip
byte-stable through `datafusion-proto` 54.0.0 and execute identically from a
fresh context with no tables registered
(`krishiv-sql/src/distributed_plan.rs` tests). The risk did not materialize.

## Decision

### 1. The envelope stays; bodies get typed kinds

`TypedTaskFragment` (version 1) remains the single wire carrier for all task
bodies. The `body` string's *kind* is determined by its prefix, and exactly
three kinds are canonical:

| Kind | Prefix | Engine | Payload |
| --- | --- | --- | --- |
| Batch plan fragment | `dfplan:v1:` | batch | base64 of `datafusion-proto` physical-plan bytes |
| Streaming loop | `stream:loop:` (+ window-spec forms) | streaming | operator/loop spec (Phase 55 reworks onto a proto body under the same envelope) |
| Delta step | `delta:step:` | delta-batch/IVM | tick payload (Phase 57 reworks onto a proto body under the same envelope) |

`sql: <query>` remains valid **only** as the single-task fallback body
(§4 of the Phase 52 plan): queries the stage builder cannot split run
exactly as today. New multi-stage plans never emit it.

The stringly-typed protocol therefore dies once, at the envelope level:
Phases 55 and 57 adopt new body kinds by defining a new prefix + payload
under the same `TypedTaskFragment`, not by inventing a parallel envelope.

### 2. Batch fragments are proto-encoded physical-plan subtrees

- Encoding: `dfplan:v1:` + base64(`physical_plan_to_bytes_with_extension_codec`).
  The `v1` segment versions the *payload* independently of the envelope
  version, so a DataFusion upgrade that changes proto semantics is detected
  as an explicit kind mismatch rather than a prost decode failure deep in a
  task.
- The codec module lives in `krishiv-sql` (`distributed_plan.rs`), the crate
  that owns the DataFusion dependency. `krishiv-plan` stays arrow-only.
- Custom leaves (shuffle reads) are extension nodes serialized through a
  Krishiv `PhysicalExtensionCodec`; the reduce-side fragment's shuffle
  input is part of the plan, not a side-channel table registration.
- One task per *output partition* of the stage subtree: the assignment's
  existing partition addressing (`ShuffleWriteConfig`/`ShuffleReadConfig`)
  names which partition of the decoded plan `execute(partition, ctx)` runs.
- Inline base64 IPC tables (`BatchSqlInlineTable` /
  `InputPartitionDescriptor::InlineIpc`) remain **only** for small
  client-supplied tables; they are not a data-plane transport between
  stages.

### 3. Version discipline

`datafusion-proto` is pinned to the same version as `datafusion` in the
workspace (`54.0.0`) and must be bumped in lockstep. Coordinator and
executors already run the same build in every supported deployment (single
binary set); a mixed-version cluster is out of scope until the Phase 59 wire
protocol ADR, which owns cross-version negotiation. Until then the `v1`
payload tag plus the envelope version is the whole compatibility story: an
executor that cannot decode a fragment fails the task with an explicit
validation error, and the job falls back per §4 of the phase plan.

## Consequences

- Executors stop re-parsing SQL per task; they decode a physical plan and
  execute one partition of it. Per-task planning cost becomes proto decode
  (µs) instead of parse+plan+optimize (ms).
- The stage builder can cut plans at physical repartition boundaries and
  address partitions individually — the prerequisite for shuffle-connected
  ShuffleMap/Result stages (Phase 52) and for locality/AQE work (Phases
  53–54).
- We accept coupling the fragment format to DataFusion's proto stability.
  Mitigations: the payload version tag, the version-locked dependency, and
  the recorded fallback (Substrait producer/consumer at the *logical* level
  plus executor-side physical planning) if a future DF upgrade breaks
  physical-plan round-tripping.
- Streaming and delta-batch fragments are unchanged today; their Phase 55/57
  reworks inherit this envelope decision instead of re-litigating it.
