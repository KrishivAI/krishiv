# 0002: Public API shape and execution semantics

- Status: Accepted
- Date: 2026-06-12
- Owners: project maintainers

## Context

Krishiv currently exposes overlapping `DataFrame`, `Relation`, `Stream`, and
`StreamingDataFrame` abstractions. Rust provides both synchronous and
asynchronous actions, while several Python methods named `*_async` block on a
Rust runtime instead of returning Python awaitables. Expressions are presented
as typed wrappers but are represented internally as SQL strings. SQL, Rust, and
Python therefore do not yet share one versionable public plan contract.

A stable API must work consistently in embedded, single-node, and distributed
modes. It must also preserve one runtime for batch and streaming while offering
both a relational API and the lower-level state/time primitives that relational
queries cannot express.

Apache Spark's SQL API provides a useful breadth baseline for DataFrame,
column/expression, catalog, reader/writer, UDF, and structured-streaming
surfaces. Apache Flink's Table API and SQL demonstrate one relational contract
for bounded and unbounded inputs, while its DataStream API demonstrates the
state, timers, watermarks, partitioning, and process functions that an engine
must provide below the relational layer.

## Decision

### 1. One canonical relational API

`Session`, `DataFrame`, `Expr`, `GroupedDataFrame`, `DataFrameReader`, and
`DataFrameWriter` are the canonical relational concepts in Rust and Python.
`DataFrame` may represent bounded or unbounded input; boundedness is plan
metadata, not a different public class identity.

`Relation`, `Stream`, and `StreamingDataFrame` may remain temporarily as
compatibility adapters, but new relational features are implemented first on
the canonical API. They will not evolve as independent engines or competing
public contracts.

### 2. A separate lower-level stream-processing API

Krishiv will expose a lower-level `DataStream`-style API for behavior that cannot
be expressed relationally: process functions, keyed state, event-time and
processing-time timers, side outputs, connected streams, broadcast state,
async I/O, and explicit operator identity. The lower-level API shares sources,
sinks, plans, runtime, state, checkpointing, and query handles with the
relational API.

### 3. Plan construction is synchronous

Expression construction and lazy transformations are synchronous and must not
perform hidden network or storage I/O. This includes `select`, `filter`, `join`,
`group_by`, `window`, `with_watermark`, and reader/writer configuration.

If catalog or schema resolution requires I/O, it occurs at an explicit async
boundary such as `load`, `table`, `analyze`, or execution. Local cached metadata
accessors remain synchronous; remote refresh operations are explicitly async.

### 4. Rust execution is async-first

Rust terminal operations involving execution, RPC, connectors, object storage,
checkpointing, savepoints, or job control are canonical `async fn` APIs. Normal
method names are used (`collect().await`, `start().await`, `stop().await`), not
`*_async` suffixes.

A separately named `krishiv::blocking` facade may provide finite batch and
administrative convenience methods. It delegates to the same async engine using
a reusable owned runtime and must reject unsafe runtime nesting. Ordinary
library methods must not silently create Tokio runtimes or call `block_on`.

### 5. Python provides sync convenience and genuine asyncio APIs

Python transformations remain synchronous. Finite batch actions may provide a
blocking convenience method and an `*_async` method. Every Python method named
`*_async` returns a genuine Python awaitable; it never blocks until completion
before returning.

Streaming execution, progress streams, termination, savepoints, and remote job
control are async-first. Sync variants, where offered, release the GIL and call
the same canonical Rust async implementation. Python async iteration is used for
streamed results and progress events.

### 6. SQL, DataFrame, and Python share one plan contract

SQL text and language-native expressions lower into the same versioned,
engine-owned expression and relational-plan AST. DataFusion remains the local
parser/planner/execution implementation, but DataFusion types are not public API
or wire format.

`Expr::raw`/`expr(...)` remains an explicit SQL escape hatch. Normal expression
constructors retain structured identifiers, literals, operators, functions,
types, nullability, sort semantics, and window definitions instead of flattening
every expression into a string.

### 7. Execution returns a query handle

All submitted work has a typed `QueryHandle`/`StreamingQuery` contract with job
ID, cached status, async status refresh, result stream, progress stream,
cancellation, timeout, completion, failure details, checkpoint/savepoint
operations where applicable, and coordinator fencing metadata.

`collect` is a convenience implemented through the query handle; it is not a
separate execution path.

### 8. Stability is explicit and generated

Every public item belongs to one audience and stability class:

- `stable`: semantic-versioning compatibility applies;
- `preview`: expected to converge, with documented migration;
- `experimental`: opt-in and may change without a compatibility shim;
- `internal`: not re-exported from public facade crates.

The Rust facade, Python module, SQL grammar, configuration keys, connector
options, and wire/durable formats each publish an inventory. CI compares that
inventory with an approved baseline and requires changelog plus compatibility
metadata for removals or semantic changes.

## Consequences

- Rust applications use idiomatic Tokio for execution while scripts retain an
  explicit blocking option.
- Python remains approachable for batch users without misleadingly synchronous
  `*_async` methods.
- Embedded and distributed modes have the same public execution semantics.
- API duplication must be reduced before 1.0; compatibility wrappers are
  temporary and carry removal milestones.
- A structured expression AST and query handle become prerequisites for stable
  remote APIs.
- The lower-level streaming API increases implementation work but prevents
  relational APIs from absorbing untyped, engine-specific process semantics.
- Spark/Flink naming may be adopted where it improves familiarity, but binary,
  wire, and exhaustive method compatibility are not goals.

## Rejected alternatives

### Synchronous Rust as the primary API

Rejected because distributed execution, connectors, shuffle, object storage,
and streaming are naturally asynchronous. Hiding runtime creation in library
methods creates nesting, cancellation, and deadlock hazards.

### Async plan construction everywhere

Rejected because lazy transformations are pure plan construction. Making them
async adds ceremony and encourages hidden I/O.

### Separate batch and streaming DataFrame engines

Rejected because it violates Krishiv's shared-runtime invariant and makes SQL,
state, connectors, and optimizer behavior diverge.

### SQL strings as the only cross-language expression contract

Rejected because strings cannot provide stable serialization, typed validation,
safe identifier handling, or optimizer-visible structure.

### Exact Spark or Flink API compatibility

Rejected because Krishiv is Rust-native and must preserve its own typed errors,
Arrow model, runtime, connector contracts, and compatibility guarantees.

## Validation

This decision is complete when the stable-API plan has executable inventory,
parity, compatibility, conformance, and release gates; duplicate public types
are removed or deprecated; Python async methods are genuine awaitables; and all
execution paths delegate to one async query-submission contract.
