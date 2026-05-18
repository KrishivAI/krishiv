# R1–R3 Architecture Review And Remediation Plan

## Purpose

This document captures the architectural decisions made while implementing R1–R3, the risks those decisions create for upcoming releases, and the remediation steps already applied before moving deeper into R4–R6.

## Decisions That Remain Directionally Correct

- **Rust + Tokio runtime foundation.** Keep stable Rust and Tokio for runtime/control-plane work.
- **Arrow + DataFusion execution foundation.** Keep Arrow as the in-memory batch format and DataFusion as the local SQL/expression/vectorized execution engine, but keep DataFusion types behind Krishiv crate boundaries.
- **One semantic model across embedded, single-node, and distributed modes.** Do not fork separate local and distributed engines.
- **Stage-local distributed execution.** Executors run local fragments over assigned partitions; shuffle carries data between stages.
- **Kubernetes isolation.** Kubernetes API access stays in `krishiv-operator`, manifests, and narrowly scoped CLI paths; core runtime remains deployment-agnostic.
- **Reliability pull-forward.** Task attempts, leases, stale-attempt rejection, durable metadata, event logs, finalizer cleanup, and basic metrics arrived in R3 rather than being bolted on later.
- **Connector capabilities and certification-first semantics.** Exactly-once remains certified per source/sink/checkpoint combination only.

## R1–R3 Risks Identified

| Risk | Adverse Effect If Left Unfixed | Remediation Status |
|---|---|---|
| Stringly typed task input/output descriptors | Brittle R4 shuffle, R5 streaming, R6 recovery, and R8 lakehouse descriptors | **Mitigated in R3 hardening** with typed input/output descriptor structs plus backward-compatible legacy descriptions |
| Unversioned JSON metadata | Upgrade and schema-compatibility pain in R6/R10 | **Mitigated in R3 hardening** with `schema_version` and `store_kind` envelope fields plus future-version rejection |
| DataFusion local context as the only executor shape | Stateful/continuous streaming operators become bolted on | **Tracked for R5**: add executor operator runtime abstraction before streaming execution expands |
| Process-local shuffle lease memory | Shuffle fencing metadata is lost across store restart | **Tracked for R4**: move shuffle partition ownership and lease state into durable shuffle metadata |
| Kafka semantics tested only by deterministic harness | Broker rebalance, commit failure, and transaction behavior remain unproven | **Tracked as post-R3/R4 hardening**: add feature-gated live Kafka integration tests |
| Static placement/monolithic coordinator | R7 ResourceManager and R9 HA become large refactors | **Tracked for R4–R7**: split `JobCoordinator`, `ResourceManager`, and shuffle metadata responsibilities incrementally |
| R1/R2 roadmap checklist drift | Confusing project state and inaccurate phase tracking | **Mitigated in R3 hardening** by reconciling R1/R2 roadmap checklist state with implementation status |

## Remediations Applied Before R4

### Typed task I/O descriptors

`krishiv-proto` now carries typed descriptors for known R3 input/output paths while retaining legacy human-readable descriptions for compatibility and CLI/test ergonomics:

- local Parquet input;
- connector Parquet input;
- object-store Parquet input;
- deterministic in-memory Kafka input;
- inline/local/shuffle/object-Parquet/Parquet-sink outputs.

Executors prefer typed descriptors when present and fall back to legacy strings only for compatibility.

### Versioned JSON metadata envelope

`JsonFileMetadataStore` now persists a schema envelope:

- `schema_version` for compatibility checks;
- `store_kind` for identifying the metadata document class;
- future schema versions are rejected rather than silently misread.

### Roadmap state reconciliation

The R1 and R2 roadmap checklist entries are marked complete to match the implementation status and tracker history. R4+ items remain unchecked and should continue to be completed through release-specific trackers.

## Remaining Follow-Ups

1. **R4 shuffle metadata:** durable partition ownership, lease state, garbage collection, and orphan cleanup.
2. **Live Kafka certification:** feature-gated integration tests against a real broker.
3. **Streaming execution model:** document and approve continuous operator, watermark, state, and lifecycle semantics before R5.
4. **Executor operator runtime abstraction:** keep DataFusion batch fragments as one runner, not the only runner.
5. **Metadata compatibility suite:** extend schema-version tests to checkpoint/savepoint metadata in R6 and all persisted metadata in R10.
