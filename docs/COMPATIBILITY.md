# Compatibility Policy

Krishiv is pre-1.0 software. This policy separates public API compatibility from
durable-data compatibility so users can evaluate upgrades without assuming that
every surface has the same stability.

## Public API stabilization

The pre-1.0 migration from the current preview surface to the intended stable
Rust, Python, and SQL contracts follows
[`implementation/stable-public-api-plan.md`](implementation/stable-public-api-plan.md)
and [ADR-0002](decisions/0002-public-api-shape-and-execution-semantics.md).
Items are not stable merely because they are public today.

## Compatibility classes

| Surface | Current contract | Upgrade expectation |
|---|---|---|
| Rust API | Preview | Breaking changes are allowed in a minor release and must be called out in `CHANGELOG.md`. |
| Python API | Preview | Follows the Rust API where practical; changed names or behavior require release notes. |
| CLI and configuration | Preview | Existing flags and keys should be deprecated for one minor release before removal when feasible. |
| Task-fragment envelope | Versioned | Readers reject unsupported versions instead of silently interpreting them. |
| Checkpoint metadata | Versioned | Writers emit v2; readers accept supported v1-v2 metadata. |
| Savepoint metadata | Versioned | Import validates the declared format version before restore. |
| Operator state | Explicitly described | Stable operator identity and serializer compatibility are required for restore. |
| Connector capability API | Preview | Capability declarations are conservative and connector certification is combination-specific. |
| SQL behavior | DataFusion-based preview | Intentional semantic changes must be documented and covered by conformance tests. |

## Durable artifact rules

1. Every durable envelope carries a format version.
2. Unknown versions fail with a typed error; they are never treated as the newest
   known version.
3. A writer may advance only after the previous reader remains available or an
   explicit migration tool exists.
4. Savepoint portability requires compatible operator IDs, state names, key
   schema, and serializer versions.
5. Connector offsets and sink transaction metadata are part of a checkpoint's
   compatibility boundary.

The exact delivery combinations are published in
[`contracts/engine-semantics.md`](contracts/engine-semantics.md), and connector
requirements are described in [`connector-sdk.md`](connector-sdk.md).

## Deprecation policy

Before 1.0, Krishiv aims to announce public API removals in `CHANGELOG.md` and
retain a deprecated path for one minor release when the maintenance cost is
reasonable. Security fixes, unsound APIs, and incorrect durability behavior may
require immediate removal.

After 1.0, semantic-versioning rules apply to stable public APIs. Experimental
and preview connectors remain outside that guarantee until promoted to
certified maturity.

## Upgrade checklist

Before upgrading a production deployment:

1. Read the changelog entries between the installed and target versions.
2. Verify the checkpoint/savepoint versions accepted by the target release.
3. Verify every source/checkpoint/sink combination against the exactly-once
   matrix.
4. Take a savepoint and test restore with production-like state.
5. Run plan and SQL conformance tests for critical queries.
6. Roll executors and coordinators according to the release notes; mixed-version
   clusters are unsupported unless a release explicitly says otherwise.
