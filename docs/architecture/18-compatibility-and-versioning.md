# Compatibility and versioning

This is the summary; `docs/COMPATIBILITY.md` is the authoritative,
CI-parsed statement of supported versions and guarantees.

## What is versioned

| Surface | Contract | Where |
|---|---|---|
| Rust public API | stability labels per item (`stable`, `beta`, `experimental`) and phases A–F in `api/stable-api.toml`, validated by `just api-inventory` in CI | `11` |
| Python API | same inventory; PySpark parity per method in the generated matrix | `11`, `../reference/pyspark-parity.md` |
| SQL dialect | the generated feature matrix; Spark SQL compatibility documented per construct | `02`, `../reference/sql-feature-matrix.md`, `../reference/krishiv-vs-spark-sql.md` |
| wire: task fragments | `TypedTaskFragment { version, execution_kind, body }` (ADR-0003); legacy untyped bodies refused in production | `05` |
| wire: IVM | `IVMD1` / `IVMD2` dialects with capability echo; a new executor answers in the dialect it was asked in | `05`, `09` |
| wire: Flight SQL, gRPC, HTTP | Arrow Flight SQL is the front door (ADR-0004); `/api/v1` is versioned by path; `openapi.json` is generated | `11` |
| checkpoints and savepoints | `CheckpointMetadata` version, `SAVEPOINT_FORMAT_VERSION` = 1, `CURRENT_STATE_SCHEMA_VERSION` with registered migrations | `07` |
| persisted metadata | `PersistedIvmJob.version` (additive fields use `serde(default)`, not a bump, so existing snapshots load) | `04` |
| connectors | `ConnectorMaturity`; certified connectors follow the connector contract (`../contracts/connectors.md`) | `10` |
| engine semantics | `../contracts/engine-semantics.md` | `00` |

## Policy in one paragraph

Rust is async-first and `DataFrame` is the canonical relational type
(ADR-0002); DataFusion types are an implementation detail, never public API
or wire format. Breaking changes to a `stable` item require a major version
and a migration note; `beta` items may change between minor releases and say
so in their doc comment; `experimental` items carry no guarantee. Wire
formats are forward-compatible one version: roll executors before
coordinators. State formats ship a migration or are documented as a break
(the key-group hash change is the one such break).

## Dependency baseline

The workspace pins DataFusion 54.1 / Arrow 54 and the Rust toolchain in
`rust-toolchain.toml`; the DataFusion 55 / Arrow 59 migration is prepared as
a patch (`../implementation/patches/datafusion-55-arrow-59-migration.patch`)
and, like the MSRV bump, is a deliberate release decision recorded in
`COMPATIBILITY.md` when taken.

## Deprecation

A deprecated item keeps working for one minor release with a
`#[deprecated]` note naming its replacement, appears in `CHANGELOG.md`, and
is removed in the next major. The `wire-or-delete` review
(`../engineering-log/wire-or-delete-2026-07.md`) is the record of public items
that had no callers and what was decided for each.

## Related

- `../COMPATIBILITY.md`, `../RELEASE.md`, `../decisions/`, `CHANGELOG.md`.
