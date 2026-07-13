# 0004: Wire-protocol front door — Flight SQL for GA, PostgreSQL wire deferred behind a proxy

- Status: Accepted
- Date: 2026-07-13
- Owners: project maintainers

## Context

Phase 59 (engine interfaces at production grade) requires a decided
wire-protocol story before the GA gate: does the engine grow a **PostgreSQL
wire protocol** front door, or stay **Flight-SQL-only**? The choice is a public
contract and a long-lived maintenance commitment, so it is recorded here.

What exists today:

- **Arrow Flight SQL** is the engine's native query surface
  (`krishiv-flight-sql`). It carries typed Arrow results zero-copy, and its
  metadata RPCs (`GetSqlInfo`, `GetCatalogs`, `GetTables`, `GetTableTypes`) and
  `?`/`$N` parameter binding were closed in Phase 02 (G1/G12). It is behind the
  same bearer + TLS authz as every other listening surface (SEC-1/SEC-3 sweep).
- **ADBC** drivers (Go/Python/C++/Java) speak Flight SQL directly; the stock
  **Flight SQL JDBC driver** (18.x) connects and runs metadata + typed SELECT +
  large results (verified in the Phase 02 spikes). So Java/JDBC and
  ADBC-over-Arrow BI tools already reach the engine.
- `krishiv-sql-gateway` already maps engine errors to **SQLSTATE** codes — the
  seed a pg-wire implementation would need.

The adoption gap a pg-wire front door would close is the **psql / pg-only BI
long tail**: tools and libraries (psycopg, `psql`, some dashboards, ORMs) that
speak *only* the PostgreSQL wire protocol and cannot use JDBC/ODBC/ADBC.

The cost of pg-wire in the engine core is large and open-ended (the phase notes
it is "a scope magnet"): startup/auth (SCRAM), the simple **and** extended query
sub-protocols, portals/cursors, prepared-statement lifecycle, the full type-OID
↔ Arrow type mapping, `COPY`, error/notice framing, and a parallel authz +
session surface that must stay in lockstep with the Flight SQL one forever.

## Decision

**GA ships on Arrow Flight SQL as the single engine-native wire protocol.**
The PostgreSQL wire protocol is **deferred, not rejected**, and — if built — is
delivered as a **proxy in front of the engine, never in the engine core**.

Concretely:

1. **Flight SQL is the front door.** All first-party surfaces (Rust API, Python
   `krishiv`, HTTP, MCP) and all JDBC/ODBC/ADBC BI tooling resolve through it.
   Any new wire surface must pass the *same* authz review as Flight SQL
   (bearer + TLS parity) before it ships — that requirement is inherited by
   anything built under this ADR.

2. **The JDBC/ODBC long tail is covered now** by the Flight SQL JDBC driver and
   ADBC's ODBC bridge; the BI certification matrix (Metabase/Superset) is run
   against these, not against a bespoke pg-wire endpoint.

3. **The pg-only long tail's named adoption path is a pg-wire *proxy*** — a
   separate process that terminates the PostgreSQL simple/extended query
   protocol and translates `SELECT`-path traffic onto a Flight SQL session. It
   is **demand-triggered** (built when a concrete pg-only tool requirement
   appears), and kept out-of-core so the engine's authz/session/type surface
   stays single-sourced. A v1 proxy is **SELECT + prepared statements only**;
   DML/`COPY`/cursors are explicitly out of scope for v1 and must be their own
   decision.

## Consequences

- **Easier:** the engine keeps one authz surface, one session model, and one
  type system to secure and evolve; Arrow stays zero-copy end to end; the GA
  gate does not carry a half-built second protocol.
- **Harder / unsupported:** pg-only clients cannot connect at GA. That is an
  accepted, documented limitation, not an accident — the adoption path above is
  the answer when the demand is concrete.
- **Migration / governance:** if the proxy is later built, the platform decides
  *separately* whether to expose it (the governance door, platform ADR-0011);
  this engine ADR only fixes *where* the protocol may live (out-of-core) and the
  authz bar it must clear.
- **Reversibility:** deferring is cheap to reverse (add the proxy); shipping a
  half pg-wire surface in-core would have been expensive to reverse. This
  decision is superseded by a new ADR if pg-wire is promoted into the core.

## Validation

- Phase 02 spikes: ADBC and the Flight SQL JDBC driver connect and run
  metadata + typed SELECT + large results against the engine (evidence for
  "JDBC/ODBC long tail covered now").
- BI certification matrix (Metabase/Superset) runs over JDBC/ADBC, not pg-wire.
- No engine code ships a PostgreSQL wire listener at GA; the listening-surface
  auth sweep (SEC-3) therefore has one query protocol to certify, not two.
- A future pg-wire proxy, if built, is gated on: SELECT-corpus parity, the same
  bearer+TLS authz review as Flight SQL, and a mainstream pg-only tool
  completing its connection + SELECT script.
