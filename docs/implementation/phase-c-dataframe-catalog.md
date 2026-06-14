# Phase C — Canonical DataFrame and catalog API

Phase C makes `DataFrame` the relational identity across Rust, Python, and SQL.
It keeps bounded and unbounded inputs in the same type and exposes boundedness
as plan metadata rather than selecting a different public class.

## Canonical DataFrame

- `Boundedness::{Bounded, Unbounded}` is derived from the logical plan's
  execution kind and is available from Rust and Python.
- Canonical DataFrame entry points now cover typed expressions, joins, union and
  distinct union, intersection, exception, null dropping/filling, sampling,
  ordering, limits, aliases, column operations, statistics, grouping sets,
  cube, rollup, pivot, unpivot, and expression windows.
- Event-time, keying, and tumbling/sliding/session window configuration can be
  entered from `DataFrame`; compatibility stream builders remain adapters and
  must not acquire independent relational semantics.
- `describe()` remains lazy. `show()` and collection remain executing actions.
  Cache/persist/checkpoint are intentionally deferred until distributed memory
  accounting and storage semantics are contracted.

## Catalog contract

`Identifier`, `Namespace`, `TableIdentifier`, `ViewIdentifier`, and
`FunctionIdentifier` provide validated, quoted names. `Session` supports typed
table resolution, table metadata, table listing, current catalog/namespace,
temporary views, relation drops, and scalar-function metadata/registration.

## Prepared SQL

`PreparedStatement` validates one-based positional parameters and binds only
engine-owned `ScalarValue` values. Binding ignores placeholders inside quoted
strings and identifiers and renders literals through the Phase B scalar
contract. Python exposes the same prepared statement using `lit(...)` values.

## Conformance

Focused conformance tests compare SQL, Rust DataFrame, and Python DataFrame
results for the canonical operations. Boundedness metadata is checked in
embedded, single-node, and distributed session configurations; distributed
result execution remains covered by the runtime/Flight suites rather than
silently falling back to local execution.
