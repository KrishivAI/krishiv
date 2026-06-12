# Connector SDK and Certification

Krishiv connectors implement explicit source and sink contracts. A connector's
name alone never implies exactly-once delivery: guarantees are derived from its
capabilities and the checkpoint path used by a job.

## Core contracts

A source is responsible for:

- discovering and assigning bounded splits or streaming partitions;
- producing Arrow `RecordBatch` values;
- exposing replayable positions when it claims checkpoint support;
- snapshotting and restoring offsets without data loss;
- declaring boundedness, projection/filter pushdown, and replay capabilities.

A sink is responsible for:

- consuming Arrow batches;
- declaring append, overwrite, idempotence, and transaction capabilities;
- staging writes before a checkpoint completes when transactional commit is
  supported;
- committing or aborting a checkpoint transaction deterministically;
- making retries safe for every advertised delivery guarantee.

Use the public source/sink traits and `ConnectorCapabilities` in
`krishiv-connectors`; do not route connector behavior through connector-name
string comparisons.

## Maturity levels

| Level | Meaning |
|---|---|
| Experimental | API and behavior can change; intended for evaluation. |
| Preview | Main paths are implemented and tested, but production certification is incomplete. |
| Certified | A named source/checkpoint/sink combination has passed correctness, recovery, compatibility, and performance gates. |

Certification applies to a versioned combination, not an entire connector
family. Documentation must not describe an experimental or preview connector as
production-ready.

## Adding a connector

1. Add the connector behind its own Cargo feature unless it is a core file
   format.
2. Implement capability declarations conservatively.
3. Add bounded read/write tests and malformed-input tests.
4. For streaming sources, test offset snapshot, restore, reassignment, and
   duplicate handling.
5. For transactional sinks, test prepare, commit, abort, coordinator restart,
   and idempotent recovery.
6. Register a descriptor with an explicit maturity level.
7. Add the combination to the exactly-once matrix as unsupported until all
   certification evidence exists.
8. Document external services and test-container requirements.

## Certification evidence

A certification pull request must contain or link to:

- fault-injection tests at every checkpoint boundary;
- source offset and sink transaction recovery tests;
- upgrade/restore results for supported metadata versions;
- schema evolution and partition evolution tests where applicable;
- sustained throughput and backpressure results;
- dependency and license review;
- an owner for regressions and security reports.

Iceberg is Krishiv's primary lakehouse format. Delta Lake, Hudi, vector stores,
and AI-specific integrations remain optional extensions and do not belong in
the standard full-engine build.
