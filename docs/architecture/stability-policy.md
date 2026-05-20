# Krishiv GA Stability Policy

## Versioning Scheme

Krishiv uses Semantic Versioning (SemVer): `MAJOR.MINOR.PATCH`.

- **PATCH**: backward-compatible bug fixes. No public API changes.
- **MINOR**: backward-compatible additions. New `pub` items, new optional config fields, new connector capabilities. Pre-GA (v0.x) minors may break.
- **MAJOR**: breaking changes to the stable surface. Any removal, rename, or signature change on a stable-surface item requires a major version bump.

GA is v1.0. All pre-1.0 crates are exempt from SemVer stability promises on minors.

---

## Stable Surface

The following crates and their public APIs are covered by the SemVer stability guarantee starting at v1.0:

| Crate | Covered Surface |
|---|---|
| `krishiv-api` | `Session`, `DataFrame`, `Stream`, all `pub` types and trait impls |
| `krishiv-sql` | `SqlEngine`, `RegisteredTable`, `MaterializedViewDefinition`, all `pub` types |
| `krishiv-proto` | All `.proto` message types and gRPC service definitions (wire-level compatibility) |
| `krishiv-runtime` | `Runtime`, `EmbeddedRuntime`, `DistributedRuntime`, all `pub` traits |
| `krishiv-connectors` | `Source`, `Sink`, `ConnectorCapabilities`, `SinkConfig`, `SourceConfig` capability flags |

**Wire-level stability**: `krishiv-proto` guarantees that existing fields in `.proto` files are never removed or type-changed without a major version bump. Field numbers are permanent.

---

## Internal Surface (No Stability Promise)

The following crates have no SemVer stability promise. Their APIs may change on any minor or patch release:

- `krishiv-scheduler` — coordinator internals, task dispatch, gRPC server impl
- `krishiv-executor` — task runner internals, operator evaluation
- `krishiv-operator` — Kubernetes CRD reconciliation
- `krishiv-plan` — logical/physical plan DAG nodes
- `krishiv-exec` — Arrow physical operator descriptors
- `krishiv-state` — state backend implementations
- `krishiv-shuffle` — shuffle store and partition routing

Consumers of internal crates must pin exact versions and accept breakage on upgrade. Internal crates are not published to crates.io until promoted.

---

## Non-Exhaustive Requirement

All stable-surface public enums that may acquire new variants in future releases MUST be annotated `#[non_exhaustive]`. This applies to:

- Error enums (e.g., `KrishivError`, `ConnectorError`)
- Status/state enums (e.g., `JobState`, `TaskState`)
- Configuration variant enums (e.g., `RefreshPolicy`, `QualityAction`)
- Any `pub enum` in `krishiv-api`, `krishiv-sql`, `krishiv-runtime`, `krishiv-connectors`

Failure to annotate `#[non_exhaustive]` on a stable enum is a build-gate violation. The CI lint step enforces this via a custom `clippy` lint or audit script.

---

## Deprecation Policy

1. A `pub` item slated for removal must be annotated `#[deprecated(since = "X.Y.0", note = "use Foo instead")]` in the same release that starts the deprecation window.
2. The deprecated item must remain functional for at least one full minor release cycle after the deprecation annotation is added.
3. Removal occurs in the next **major** version bump after the deprecation window closes.
4. Docs for the deprecated item must point to the replacement.
5. Items deprecated in pre-GA (v0.x) may be removed on any subsequent minor release; the one-minor-notice rule still applies within the v0 series.

---

## Public API Freeze

At v1.0 GA, the stable surface is frozen. Additions are allowed (new `pub` items, new trait methods with default implementations). The following require a major version bump:

- Removing any `pub` type, function, method, field, or variant (unless `#[non_exhaustive]`).
- Changing any `pub` function or method signature.
- Adding a required method to a stable trait.
- Changing any `.proto` field number, field type, or service method signature.
- Removing any `ConnectorCapabilities` flag.

**Adding a new `pub` item** is always semver-compatible. Adding a new variant to a `#[non_exhaustive]` enum is semver-compatible.

---

## Pre-GA Crates (v0.x)

Crates with version `0.x` may break their public API on any minor release. The following constraints still apply:

- Breaking changes require a minor version bump (never a patch bump).
- Breaking changes must be documented in `CHANGELOG.md` under the releasing version.
- Pre-GA crates that are upstream dependencies of stable crates must not expose breakage to callers of the stable crate.

---

## Enforcement

- `cargo semver-checks` runs in CI on every PR targeting stable crates.
- A custom audit step flags `pub enum` items missing `#[non_exhaustive]` in stable crates.
- The stable surface list above is the authoritative scope for semver-checks. Internal crates are excluded from semver-checks CI.
