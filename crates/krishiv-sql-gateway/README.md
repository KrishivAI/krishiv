# krishiv-sql-gateway

An in-process library facade over `BlockingSession` with SQLSTATE error
mapping, connection pooling, and a gateway-style API (`GatewaySession`,
`SessionPool`, `GatewayQueryResult`) for tools that embed Krishiv. It is
**not** a JDBC/ODBC wire server: external drivers connect through Arrow
Flight SQL (`krishiv-flight-sql`).

Documentation: `docs/architecture/11-public-interfaces.md`,
`docs/reference/jdbc-connectivity.md`.

License: Apache-2.0.
