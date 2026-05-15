# Krishiv Engineering Standards

## Purpose

This document defines engineering standards for implementing Krishiv. It should be updated as the codebase matures, but the defaults here apply from R1 onward.

## Language And Runtime

- Use stable Rust unless a feature explicitly requires nightly and is approved in an RFC.
- Use Tokio for async runtime work.
- Keep CPU-heavy operator execution separate from async control-plane I/O.
- Do not block Tokio worker threads with long-running compute, file scans, RocksDB operations, or network copies.
- Prefer `Arc` for shared immutable state and explicit ownership for mutable runtime state.

## Crate Boundaries

- `krishiv-api` owns public Rust APIs such as `Session`, `DataFrame`, and `Stream`.
- `krishiv-sql` owns DataFusion integration and SQL compatibility behavior.
- `krishiv-plan` owns logical and physical Krishiv DAG structures.
- `krishiv-exec` owns Arrow physical operators.
- `krishiv-runtime` owns embedded, single-node, and distributed runtime traits.
- `krishiv-scheduler` owns coordinator, job leadership, placement, retries, and queues.
- `krishiv-connectors` owns source/sink contracts and connector implementations.
- Keep cross-crate dependencies acyclic where practical. Public API crates should not depend on Kubernetes, RocksDB, or connector-specific implementation details.

## API Design

- Prefer small, stable public traits over broad catch-all abstractions.
- Keep public types documented once they are exported from `krishiv-api`.
- Represent execution mode explicitly with an enum such as `ExecutionMode`.
- Represent connector guarantees with capability flags rather than comments.
- Do not promise exactly-once in generic APIs; expose it as a certified connector/runtime capability.
- Keep SQL compatibility documented in `docs/sql-compatibility/` as behavior is added.

## Error Handling

- Use `Result<T, E>` at fallible boundaries.
- Prefer domain-specific error enums for public crate boundaries.
- Preserve source errors where useful for debugging.
- Include job id, stage id, task id, checkpoint epoch, connector name, or table path in errors when available.
- Avoid `unwrap` and `expect` outside tests, examples, and startup-time invariant checks.

## Async And Concurrency

- Use bounded channels for runtime queues unless an unbounded queue is explicitly justified.
- Propagate cancellation through task handles or cancellation tokens.
- Add timeouts to network/control-plane operations.
- Use leases and fencing tokens for failover-sensitive coordinator actions.
- Make replay and retry idempotent where possible.

## Data And Memory

- Use Arrow record batches as the default in-memory data representation.
- Track memory at task/operator boundaries before adding large in-memory buffers.
- Prefer spillable operators for joins, aggregations, shuffle, and state-heavy operations.
- Avoid ad hoc binary formats for durable state or checkpoint metadata.

## Testing

- Add unit tests for core logic in the owning crate.
- Add integration tests for planner/runtime/connector behavior.
- Add golden tests for SQL plans and query results.
- Add deterministic replay tests for streaming and stateful behavior.
- Add failure injection tests for checkpoint, shuffle, scheduler, and connector guarantees.
- Every connector must eventually pass the connector certification suite before being documented as supported.

## Documentation

- Update roadmap checklists as features are implemented.
- Document public behavior, limitations, and accepted semantics.
- Keep architecture docs concise and decision-oriented.
- Add examples for headline features of each release.
