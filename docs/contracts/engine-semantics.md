# Krishiv Engine Semantic Contract

This document defines the guarantees of the open-source Krishiv compute engine.
A feature is not a production guarantee merely because an implementation type
exists. Production claims require the runtime profile, connector maturity, and
source/sink/checkpoint combination documented below.

## Batch semantics

- A submitted batch job is a finite DAG of versioned task fragments.
- A task attempt may execute more than once after executor loss. Only the
  coordinator-fenced winning attempt may publish scheduler-visible completion.
- Intermediate shuffle data may be regenerated or refetched. Consumers must not
  treat an uncommitted attempt as authoritative.
- Batch reads are snapshot-consistent only when the source or table format
  provides a stable snapshot. Plain files can change during planning/execution.
- A successful query result means every required stage completed. Partial output
  is an error unless the API explicitly returns a partial-result type.
- Distributed writes require a commit-capable sink. Client-side
  collect-and-write helpers are local convenience APIs and are not atomic
  distributed writes.

## Streaming semantics

- Streaming jobs process an unbounded or incrementally supplied sequence of
  Arrow record batches using the same scheduler/executor runtime as batch jobs.
- Event time is derived from an explicitly configured column. Watermarks are
  lower bounds on future event timestamps, not wall-clock completion promises.
- Records later than the effective watermark may be dropped or sent to a side
  output according to the operator configuration.
- Stateful operators must use a stable operator ID and state name. Persisted
  state is directly restorable only when the serializer version is unchanged;
  otherwise a registered migration is required.
- Checkpoints are coordinator-fenced epochs. A checkpoint is restorable only
  after all required operator snapshots and source offsets are durably committed.
- Savepoints are user-retained checkpoints and follow the same operator identity
  and state compatibility rules.
- Executor/task retries may replay records. The end-to-end delivery guarantee is
  the weakest guarantee supplied by the source, sink, checkpoint storage, and
  selected durability profile.

## Delivery guarantee definitions

- **Best effort:** failure can lose or duplicate records.
- **At least once:** acknowledged source positions are not advanced before
  durable output, but replay may duplicate output.
- **Effectively once:** replay can occur, but deterministic/idempotent sink keys
  make repeated writes converge on one externally visible result.
- **Exactly once:** source position and sink publication are coordinated by the
  checkpoint protocol and a transactional/two-phase sink.

Krishiv does not make a blanket exactly-once claim. Only certified combinations
in the matrix below may be described as exactly-once.

## Exactly-once support matrix

| Source | Sink | Required checkpoint/runtime profile | Current claim |
|---|---|---|---|
| Kafka checkpoint source | Iceberg two-phase commit | Durable checkpoint storage; `single-node-durable` or `distributed-durable` | Preview exactly-once; not certified until broker/object-store failure tests pass |
| Kafka checkpoint source | Transactional Kafka sink | Durable checkpoint storage and stable transactional IDs | Preview exactly-once |
| Kafka checkpoint source | Two-phase Parquet/object-store sink | Durable checkpoint storage and atomic publication protocol | Preview exactly-once |
| Rewindable source | Idempotent sink | Durable checkpoint storage | Effectively once |
| Rewindable/checkpointed source | Non-idempotent sink | Any supported checkpoint storage | At least once; duplicates possible |
| Non-rewindable source | Any sink | Any | Best effort after source/task failure |
| Any source | Elasticsearch, Cassandra, HBase, vector sink | Any | At least once or effectively once only when user-selected keys are idempotent; never a blanket exactly-once claim |

## Version compatibility

| Artifact | Current writer | Restore compatibility |
|---|---:|---|
| Typed task fragment envelope | 1 | Version 1 only; durable profiles reject legacy untyped fragments |
| Checkpoint metadata | 2 | Versions 1-2 |
| Savepoint metadata | 1 | Version 1 only |
| Operator state serializer | Per operator/state | Exact version match or an explicitly registered migration |

Unknown future versions are rejected rather than guessed. Backward-compatible
fields must be optional/defaulted; semantic changes require a format-version
increment and migration tests.

## Operator identity and state compatibility

An operator state address is the tuple:

```text
(job_id, stable_operator_id, state_name, key_group)
```

Rules:

1. `stable_operator_id` must be deterministic and must not be derived from a
   display label or task attempt number.
2. Parallel task instances share the operator ID and are distinguished by key
   group/task assignment.
3. Renaming an operator is a state-breaking change unless an explicit mapping is
   supplied during restore.
4. `state_name` identifies independent values owned by one operator.
5. `serializer_version` starts at 1 and changes whenever persisted bytes change.
6. Direct restore requires matching operator ID, state name, and serializer
   version. Other changes require `StateMigrationRegistry`.
7. Removing state is allowed only when restore explicitly permits dropping that
   state; silent state loss is forbidden.

## Iceberg-first lakehouse policy

Apache Iceberg is Krishiv's primary lakehouse format. New catalog, distributed
write, schema/partition evolution, time-travel, and exactly-once lakehouse work
must target Iceberg first. Delta Lake and Hudi remain optional experimental
compatibility integrations and must not block Iceberg correctness.
