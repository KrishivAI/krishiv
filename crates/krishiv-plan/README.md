# krishiv-plan

The engine-owned plan IR that crosses crate and wire boundaries without
exposing DataFusion types: `LogicalPlan`/`PhysicalPlan`/`NodeOp`, the
versioned public expression and scalar-type AST, deterministic lowering,
`TypedTaskFragment` (the single wire carrier for work, ADR-0003), streaming
task specs (`WindowExecutionSpec`, `StreamingTaskSpec`, interval joins), the
IR optimizer (predicate pushdown, constant folding, join reorder, broadcast,
statistics registry, AQE rules), UDF contracts, the CEP pattern builder, and
the `governance` auth/policy interfaces.

Documentation: `docs/architecture/03-planning-and-optimization.md`,
`docs/decisions/0002-public-api-shape-and-execution-semantics.md`,
`docs/decisions/0003-task-fragment-encoding.md`.

License: Apache-2.0.
