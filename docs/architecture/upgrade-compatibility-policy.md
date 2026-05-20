# Upgrade Compatibility and Metadata Schema Policy

## Decision

Every persisted metadata blob carries a `schema_version: u32`. Readers enforce a strict upper bound: a blob with `schema_version > CURRENT_VERSION` is rejected with an error, not silently ignored. Forward compatibility (new reader reads old blob) is required. Backward compatibility (old reader reads new blob) is not required but should be preserved where cost-free.

Rolling upgrades across at most one minor version are supported. Major version upgrades require a full cluster restart and, in some cases, a migration tool invocation.

---

## Metadata Families

Each family has an owning crate, a `CURRENT_VERSION` constant, and an automated upgrade test:

| Family | Owning Crate | Persisted In | Current `schema_version` |
|---|---|---|---|
| Job metadata | `krishiv-proto` | Coordinator state store | 1 |
| Event log | `krishiv-scheduler` | `RedbStateBackend` event log table | 1 |
| Checkpoint state | `krishiv-checkpoint` | `RedbStateBackend` checkpoint store | 1 |
| Savepoint | `krishiv-checkpoint` | Local filesystem (Parquet + manifest) | 1 |
| Connector offset metadata | `krishiv-connectors` | `RedbStateBackend` offset table | 1 |
| Catalog metadata | `krishiv-catalog` | `RedbStateBackend` catalog table | 1 |

All `schema_version` values start at 1. Version 0 is reserved for pre-R10 blobs and is treated as unversioned legacy; readers may reject or attempt best-effort decode at their discretion.

---

## Schema Version Field Requirement

Every persisted blob structure (Protobuf message, Serde-serialized struct, Arrow IPC metadata) that is written to durable storage MUST include a top-level `schema_version: u32` field. For Protobuf, this is field number 1 on the top-level message. For Serde JSON/MessagePack, it is a top-level key named `schema_version`.

Rejection rule: if `schema_version > CURRENT_VERSION`, return `Err(SchemaVersionTooNew { found, current })`. Do not attempt to deserialize the rest of the blob.

---

## Forward Compatibility (New Reads Old)

When `schema_version < CURRENT_VERSION`, the reader must provide defaults for fields that did not exist in older versions. Pattern: use `Option<T>` for new fields and treat `None` as the documented default. Required fields introduced after v1 must be added as optional in the schema type with a documented default.

New fields must never change the semantics of existing fields. If semantics change, that is a new `schema_version`.

---

## Rolling Upgrade Protocol

R10 supports rolling upgrade across **at most one minor version gap** (e.g., `1.2.x → 1.3.x`).

During a rolling upgrade:
1. New coordinator pods are started before old coordinator pods are stopped.
2. Both versions read from the same durable state store.
3. New version reads old-versioned blobs via forward compatibility defaults.
4. Old version MUST NOT encounter blobs written by the new version with `schema_version > CURRENT_VERSION` (the new version must not write new-version blobs until all old pods are terminated, or must write old-version-compatible blobs during the mixed-version window).

**Major version upgrades** require a full cluster stop, optional migration tool invocation, and full cluster start. Rolling upgrade across a major version boundary is not supported.

---

## Breaking Change Process

1. Add or change a field in the persisted structure.
2. Increment `CURRENT_VERSION` by 1 in the owning crate.
3. Add a migration function `migrate_v{N}_to_v{N+1}(blob: &[u8]) -> Result<Blob>` in the owning crate's `migrations` module.
4. Update the upgrade test in `crates/krishiv-upgrade-tests` to cover the new version transition.
5. Document the change in `CHANGELOG.md` under the releasing version.
6. If the change removes a field that callers depend on, it is a major version change and requires a semver major bump of the owning crate.

---

## Upgrade Test Requirements

Automated upgrade tests live in `crates/krishiv-upgrade-tests`. The test harness:

1. Serializes a representative metadata blob at version N using the old schema type.
2. Decodes the blob using the new schema type (version N+1 reader).
3. Asserts that all fields from the N blob are preserved with correct values.
4. Asserts that new fields introduced in N+1 have their documented defaults.

Tests must cover every GA-supported metadata family listed in the table above. Tests run in CI on every PR touching any owning crate.

---

## Migration Tool

For schema changes that require active data transformation (e.g., field type changes, field moves), a migration tool is provided in `tools/krishiv-migrate`. The tool:

- Reads a `RedbStateBackend` database.
- Detects blobs with `schema_version < CURRENT_VERSION` for each family.
- Applies migration functions sequentially (v1 → v2 → v3, etc.).
- Writes upgraded blobs back to the same store.
- Emits a migration report: count of blobs upgraded per family, errors encountered.

The migration tool is idempotent: re-running it after a partial failure produces the same result.

---

## Compatibility Test Matrix

| Transition | Supported | Mechanism |
|---|---|---|
| v1.0 → v1.1 (minor) | Yes (rolling) | Forward-compat defaults |
| v1.1 → v1.0 (downgrade) | Not guaranteed | Old reader may reject new-version blobs |
| v1.x → v2.0 (major) | Yes (stop-migrate-start) | Migration tool |
| v0.x → v1.0 (GA promotion) | Best-effort | Legacy blob handling, no guarantee |
