# krishiv-connectors

Every way data enters or leaves the engine: `Source`/`Sink` contracts,
`CheckpointSource`, `Offset`/`OffsetCommitter`, `TwoPhaseCommitSink` and
`TransactionalSinkParticipant`, `ConnectorCapabilities` /
`DeliveryGuarantee` / `ConnectorMaturity`, the driver registry
(`ConnectorKind`, `ConnectorRole`, `default_registry()`), CDC (Debezium →
Iceberg), data-quality rules, vector sinks, and the lakehouse module
(Iceberg native two-phase commit and streaming sink, Delta Lake, Hudi).

Parquet and CSV are unconditional. Leaf feature flags are **defined here and
forwarded upward**: `cloud`, `kafka`, `schema-registry`, `avro`, `lakehouse`,
`iceberg`, `state`, `two-phase`, `vortex`, `kinesis`, `pulsar-source`,
`elasticsearch`, `cassandra`, `hbase`, `jdbc`, `vector-sinks`, `qdrant`,
`pgvector`. There are no presets on this crate.

Documentation: `docs/architecture/10-connectors-and-lakehouse.md`,
`docs/contracts/connectors.md`, `docs/connector-sdk.md`, and the generated
`docs/reference/certification-matrix.md` /
`docs/reference/connector-reachability-matrix.md`.

License: Apache-2.0.
