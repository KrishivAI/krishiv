# Phase D — Typed I/O, connectors, and Iceberg commits

Phase D moves I/O configuration out of unvalidated string maps and establishes
one lazy builder contract for files, Kafka, databases, and Iceberg. Execution is
async at the canonical Rust boundary; synchronous methods remain compatibility
wrappers until the Phase E blocking facade is introduced.

## Implemented contract

- Typed Parquet, CSV, JSON, Kafka, database, layout, write-mode, distribution,
  sizing, and schema-evolution options.
- `DataFrameReader::load_async`, `DataFrameWriter::save_async`, async typed table
  resolution, multiple file paths, projection/filter planning, CSV header and
  delimiter configuration, and Iceberg table reads/writes.
- Atomic local file publication through temporary files and rename, with
  overwrite/ignore/append behavior, partition directories, hash buckets, sort
  keys, row-count sizing, and target-size-derived splitting.
- Common endpoint capability metadata. Kafka and database endpoints fail before
  execution when no matching driver is registered; bounded Kafka execution is
  intentionally deferred to the structured-streaming contract.
- Coordinator-owned distributed Iceberg epoch aggregation: task output remains
  invisible until every expected task stages successfully, one combined
  snapshot is committed, retries are idempotent, and incomplete epochs abort.
- In-memory Iceberg conformance model for append, overwrite, equality delete,
  equality update, key-based merge, additive schema evolution, partition-spec
  evolution, and branch/tag references.

## Certification boundary

The in-memory and local-file suites validate commit/abort, task retry,
incomplete-epoch visibility, duplicate suppression, row-level mutation, schema
changes, partition layout, and recovery metadata. Native Iceberg row-level DML,
object-store failure loops, Kafka replay/backpressure, and JDBC driver
certification remain release blockers. They are not advertised as certified or
exactly-once combinations.

## Architectural boundary

Phase D does not add streaming triggers, query lifecycle handles, or a generic
platform catalog. Kafka streaming belongs to Phase F, cancellation/progress to
Phase E, and SQL DDL/DML grammar completeness to Phase H.
