# JDBC/ODBC Gateway Architecture

## Summary

Krishiv exposes a single query gateway for BI tools, JDBC clients, and ODBC clients via the Apache Arrow Flight SQL protocol. There is no separate ODBC server: ODBC clients connect through the Arrow ODBC driver, which speaks Flight SQL over gRPC. JDBC clients use the Apache Arrow JDBC driver.

All BI access flows through the same auth, policy, planner, and execution path as the native Rust and Python APIs. There is no separate code path that could bypass governance.

---

## Protocol and Drivers

| Client Type | Driver | Protocol |
|---|---|---|
| Java / JDBC | Apache Arrow JDBC driver (`org.apache.arrow.flight.sql`) | Arrow Flight SQL over gRPC/TLS |
| ODBC (Windows/Linux) | Arrow ODBC driver (`arrow-odbc`) | Arrow Flight SQL over gRPC/TLS |
| Python / ADBC | Arrow ADBC Flight SQL driver | Arrow Flight SQL over gRPC/TLS |

All variants speak the same Flight SQL gRPC service. No separate port per driver type.

---

## Server Component

`KrishivFlightSqlService` in crate `krishiv-flight-sql` is the server implementation. It wraps the Arrow Flight SQL gRPC service and delegates query execution to a `Session` from `krishiv-api`.

**R10 scope**: auth integration, policy hook wiring, basic query execution (SELECT, EXPLAIN). Named queries, prepared-statement cache, and parameter binding return `Status::UNIMPLEMENTED`.

---

## Request Flow

```
Client
  │
  ▼  TLS handshake (required in prod, optional in dev)
  │
  ▼  gRPC Handshake → AuthProvider::authenticate(token)
  │    ├─ Bearer token (JWT) verified against configured OIDC/static provider
  │    └─ Returns KrishivPrincipal{user, roles, tenant}
  │
  ▼  FlightDescriptor → CommandStatementQuery{query}
  │
  ▼  PolicyHook::check_table_access(principal, table_refs)
  │    └─ Rejects or allows; column-level masking rules applied here
  │
  ▼  Krishiv SQL planner (krishiv-sql::SqlEngine)
  │    └─ Produces LogicalPlan via DataFusion
  │
  ▼  DataFusion physical execution
  │    └─ PolicyHook::mask_record_batch(principal, batch) applied before streaming
  │
  ▼  Arrow IPC record batches streamed to client via FlightData
```

---

## Governance Invariants

Two invariants are non-negotiable and enforced at the service boundary:

1. **No session without auth**: `KrishivFlightSqlService` MUST call `AuthProvider::authenticate()` before processing any `DoGet`, `GetFlightInfo`, or `DoPut` request. A missing or invalid token returns `Status::UNAUTHENTICATED`. This is not optional and cannot be disabled in production mode.

2. **Masking before return**: `PolicyHook::mask_record_batch()` MUST be called on every `RecordBatch` before it is written to a `FlightData` response. The policy hook may redact columns, substitute constant values, or drop rows based on the principal's column-level access grants. Bypassing masking is a security defect.

Both invariants are verified by the integration test suite in `krishiv-flight-sql/tests/governance_invariants.rs`.

---

## TLS Configuration

| Mode | Behavior |
|---|---|
| `tls: required` | mTLS or server-side TLS enforced; connections without TLS are rejected at the TCP layer |
| `tls: optional` | TLS accepted but not required; intended for local dev only |
| `tls: disabled` | Compile-time error in release builds; only available in test builds |

Production deployments MUST use `tls: required`. The Helm chart defaults to `tls: required`.

---

## Port and Configuration

Default port: **31337**. Configurable via `FlightSqlConfig.port` in the server config YAML/TOML.

```toml
[flight_sql]
port = 31337
tls = "required"
tls_cert_path = "/etc/krishiv/tls/server.crt"
tls_key_path  = "/etc/krishiv/tls/server.key"
auth_provider = "oidc"      # or "static" for dev
max_connections = 256
```

---

## Out of Scope for R10

The following Flight SQL features return `Status::UNIMPLEMENTED` in R10 and are deferred to R11:

- Named (persisted) prepared statements.
- Server-side cursor pagination for large result sets beyond stream continuation.
- `DoPut` for INSERT via Flight SQL.
- Catalog/schema introspection via `GetCatalogs`, `GetSchemas`, `GetTables` (stubbed; returns empty).
- Parameter binding in prepared statements.

---

## ODBC Note

ODBC clients require no separate server. The Arrow ODBC driver connects directly to the Flight SQL gRPC endpoint. ODBC DSN configuration points to `<host>:31337`. No ODBC-to-SQL translation layer is needed because Arrow IPC is the native wire format and DataFusion handles the SQL dialect.
