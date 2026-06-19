# Connector Contract and Maturity

## Capability interface

Every source and sink must report `ConnectorCapabilities`. Sources additionally
implement checkpoint offset capture/restore when rewindable. Dynamic sinks must
expose the same capability metadata as concrete sinks. Capability flags describe
what a connector can participate in; they do not independently establish an
end-to-end job guarantee.

Required source behaviors:

- Declare bounded or unbounded mode.
- Return Arrow `RecordBatch` values.
- Apply backpressure by awaiting consumer capacity.
- For checkpoint support, capture and restore an exact durable source position.
- Never advance an externally committed position before downstream durability.

Required sink behaviors:

- Declare idempotent, transactional, checkpoint, and two-phase capabilities.
- `flush` means previously accepted output is durable according to the sink.
- Non-idempotent sinks must document duplicate behavior.
- Exactly-once sinks must prepare on a checkpoint epoch and publish only after
  coordinator commit; abort must make prepared output non-visible or reclaimable.

## Maturity definitions

- **Experimental:** API/semantics may change and failure recovery is not certified.
- **Preview:** intended for evaluation; contract tests exist but the complete
  external-system failure matrix has not passed.
- **Certified:** passes the common contract, restart, replay, duplicate, schema,
  credential, and failure-injection suite for its published guarantee.

No connector is labeled certified in Phase 1 because the reusable external-system failure harness is still pending.

## Connector inventory

| Connector | Role | Maturity | Published guarantee/status |
|---|---|---|---|
| Parquet | Source/sink | Preview | Bounded; local write helpers are not distributed atomic writes |
| S3/object store | Source/sink/storage | Preview | Depends on concrete file/table commit protocol |
| CSV/NDJSON | Source | Preview | Bounded, best effort on mutable files |
| Avro | Source/sink | Preview | Bounded file semantics |
| Kafka | Source/sink | Preview | Checkpoint/transaction paths exist; certification pending |
| Schema Registry | Decoder integration | Preview | Avro/Protobuf compatibility subset |
| Iceberg | Lakehouse source/sink | Preview | Primary lakehouse target; two-phase path available |
| Delta Lake | Lakehouse source/sink | Experimental | Optional compatibility integration |
| Hudi | Lakehouse source/sink | Experimental | Optional compatibility integration |
| Two-phase Parquet/S3 | Sink | Preview | Exactly-once candidate with durable checkpoints |
| CDC router | Source integration | Experimental | Guarantee depends on upstream broker and decoder |
| Kinesis | Source | Experimental | Checkpoint/recovery certification pending |
| Pulsar | Source | Experimental | Checkpoint/recovery certification pending |
| Elasticsearch/OpenSearch | Sink | Experimental | At least once; effectively once with stable document IDs |
| Cassandra/ScyllaDB | Sink | Experimental | At least once; effectively once with idempotent primary keys |
| HBase | Sink | Experimental | At least once; effectively once with idempotent row keys |
| Qdrant/pgvector/LanceDB/Weaviate/Pinecone | Sink | Experimental | Platform-adjacent optional connectors; excluded from standard full build |

`ConnectorDescriptor::maturity` is the machine-readable maturity value for
registry-backed connectors. Documentation remains authoritative for integration
modules that are not yet opened through the common driver registry.
