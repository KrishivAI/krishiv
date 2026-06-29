# Phase B — Canonical expression and type AST

Phase B establishes one engine-owned expression contract shared by Rust,
Python, serialized plans, and the DataFusion integration boundary.

## Completed contract

- `krishiv-plan::expression` owns the versioned AST, scalar values, logical data
  types, field nullability, decimal precision/scale, timestamp units/timezones,
  interval units, and nested list/map/struct types.
- Serialized expressions use an explicit envelope version and reject unknown
  versions or invalid nodes before restore or lowering.
- `krishiv-api::Expr` is a typed wrapper. Column, literal, comparison, boolean,
  arithmetic, aggregate, cast, sort, function, and window constructors create
  AST nodes rather than concatenating SQL strings.
- `krishiv-sql` is the only DataFusion lowering boundary. Core nodes lower
  structurally; generic functions, aggregates, windows, interval literals, and
  the explicit `RawSql` preview node pass through DataFusion's analyzer until
  dedicated typed registries cover those families.
- Python exposes `Column`, `col`, `lit`, aggregate helpers, generic function
  calls, operators, casts, ordering, windows, normalized AST inspection, and
  typed DataFrame projection/filter/grouping entry points.
- Raw SQL remains available through Rust `Expr::raw`, Python `expr()`, and the
  compatibility string methods. It is a preview escape hatch, not the stable
  representation used by ordinary constructors.

## Validation and compatibility

`EXPRESSION_FORMAT_VERSION` changes only when the serialized expression
contract changes incompatibly. Decoders accept only explicitly supported
versions. Validation currently rejects empty identifiers/functions/raw SQL,
invalid decimal definitions, empty timestamp timezones, and malformed nested
struct definitions.

Golden tests cover deterministic versioned round trips, Rust/Python normalized
AST parity, and equivalent execution through typed and raw-SQL lowering paths.

## Deferred expansion

Phase B intentionally does not claim a complete Spark-compatible function
catalog. Dedicated typed nodes and metadata for conditional, JSON, collection,
regular-expression, UDF volatility/nullability, and full SQL window-frame
families remain additive API work. These additions must preserve the versioned
AST contract or introduce an explicitly migrated envelope version.
