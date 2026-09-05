# krishiv-api

The public Rust API: `SessionBuilder`/`Session`, `DataFrame` (the canonical
relational type), the versioned `Expr` facade, `QueryHandle`,
`PreparedStatement`, `StreamingDataFrame`, `IncrementalDataFrame`,
`Pipeline`, catalog operations, typed I/O options, `BlockingSession`, and the
`ComputeEngine`/`CompiledJob` re-exports. Rust is async-first; every sync
method is a documented convenience over an async one.

Features: `kafka`, `iceberg-catalog`, and the connector forwarders (see
`docs/architecture/15-configuration.md`).

Documentation: `docs/architecture/11-public-interfaces.md`,
`docs/architecture/01-execution-modes.md` (session building and routing),
`docs/architecture/18-compatibility-and-versioning.md` (stability labels,
`api/stable-api.toml`).

License: Apache-2.0.
