# krishiv-flight-sql

Arrow Flight SQL service over the `Session` API — the wire front door for
JDBC (Arrow Flight SQL driver), ADBC, and dbt (ADR-0004). Statements and
prepared statements, parameter binding, table upload, catalog metadata
commands, transactions, bounded sessions (`SessionRegistry`), API-key
authentication and table policy hooks. `FlightExecutionHost` binds the
service to an embedded engine, a local daemon, or a distributed coordinator.
Binary: `krishiv-flight-server` (`krishiv flight-server`).

Documentation: `docs/architecture/11-public-interfaces.md`,
`docs/architecture/12-security.md`, `docs/reference/jdbc-connectivity.md`,
`docs/decisions/0004-wire-protocol-front-door.md`.

License: Apache-2.0.
