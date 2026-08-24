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
| Task-fragment envelope | Versioned | Readers reject unsupported versions (current v1) instead of silently interpreting them. |
| Checkpoint metadata | Versioned | Writers emit v3; readers accept supported v1-v3 metadata. |
| Savepoint metadata | Versioned | Import validates the declared format version (v1) before restore. |
| Wire protocol (coordinator↔executor) | Versioned | Handshake carries the transport contract version (current R3.1); a peer announcing an unsupported version is rejected rather than assumed compatible. |
| IVM resident tick wire | Versioned | Every resident IVM tick result carries a format magic (`IVMD1` deltas only, `IVMD2` deltas + per-view health). Readers reject unsupported tick-result versions (current v2) instead of silently interpreting them. The coordinator negotiates the dialect from the executor's attach reply and falls back to v1 when it hears nothing. **That guarantee is per-attach and process-local, and it does not hold across a multi-executor rolling upgrade**: there is no placement pin (IVM-AUD-DIST-A2), so a job that negotiated v2 can have its next tick land on a not-yet-upgraded executor, which fails to decode the binary payload — a failed tick, `attached = false`, and a full `checkpoint_full` re-attach, not a graceful "health not reported". Single-executor and fully-upgraded fleets degrade as described; mixed fleets should set `KRISHIV_IVM_LEGACY_TICK_WIRE=1` on the coordinator for the duration of the roll. |
| Metadata store | Explicitly described | Schema migrations run forward-only at boot; a store written by a newer engine is not downgraded. |
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

### These promises are CI-enforced

`scripts/compatibility_gate.py` (run by the `compat-gate` CI job) fails the
build when a version number stated above stops matching the constant the engine
compiles, or when the test that enforces a promise disappears:

| Promise | Code constant | Enforcing test |
|---|---|---|
| Checkpoint metadata | `CheckpointMetadata::{MIN_SUPPORTED_VERSION, VERSION}` | `write_epoch_metadata_rejects_incompatible_version` |
| Savepoint metadata | `SAVEPOINT_FORMAT_VERSION` | `import_rejects_unknown_format_version` |
| Task-fragment envelope | `TASK_FRAGMENT_VERSION` | `rejects_unknown_fragment_version` |
| Wire protocol | `TransportVersion::CURRENT` | `transport_version_exposes_compatibility` (the reject path itself is `ensure_transport_version` in the coordinator gRPC service, mirrored in the executor) |
| IVM resident tick wire | `IVM_TICK_WIRE_VERSION` | `decode_tick_result_rejects_unknown_magic` |

The prose above was wrong before this gate existed — it promised checkpoint
metadata v2 while the engine had been writing v3 — which is exactly the drift
the gate now prevents.

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
