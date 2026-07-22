# JDBC / Flight SQL client connectivity

The engine exposes an Arrow **Flight SQL** endpoint that the stock Apache Arrow
Flight SQL **JDBC driver** connects to directly — so JDBC/ODBC-oriented BI tools
(DBeaver, Tableau via the Arrow driver, JetBrains DataGrip, plain JDBC apps) work
without a translation layer.

Verified end-to-end 2026-07-22 against a deployed coordinator (Flight on
`:2003`): connect, `DatabaseMetaData` (vendor/version, table types), typed
multi-column results, and large aggregations all succeed.

## Connection

```
jdbc:arrow-flight-sql://<coordinator-host>:<flight-port>?useEncryption=false
```

- `<flight-port>` is the coordinator's Flight SQL port (`--flight-addr`, default
  `2003` in the certified deployment).
- **Auth**: pass the Flight API key as the `token` connection property (bearer).
  It is validated against `KRISHIV_API_KEYS` / `KRISHIV_FLIGHT_API_KEY` on the
  server. Basic `user`/`password` is also accepted where a policy provider maps
  it.
- `useEncryption=false` for a plaintext port; set TLS properties instead when the
  server is started with `--tls-cert`/`--tls-key`.

## Required JVM flag (client side, not an engine setting)

Apache Arrow's Java memory layer needs the `java.nio` module opened on JDK 9+
(mandatory on JDK 17/21). This is a **client** requirement of the Arrow JDBC
driver itself — omitting it fails at driver init with
`Failed to initialize MemoryUtil` / `InaccessibleObjectException`, *before* any
RPC reaches the engine:

```
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp app.jar:flight-sql-jdbc-driver.jar ...
```

BI tools that bundle their own JRE typically set this in their `.vmoptions` /
launcher config. See https://arrow.apache.org/docs/java/install.html.

## Proven capability (2026-07-22, driver 17.0.0, JDK 21)

| Surface | Result |
|---|---|
| `DriverManager.getConnection` + `token` | connects |
| `SELECT 1` | typed row returned |
| `DatabaseMetaData.getDatabaseProductName/Version` | `Krishiv` / `0.1.0` (GetSqlInfo) |
| `DatabaseMetaData.getTableTypes` | `TABLE` (GetTableTypes) |
| Typed multi-column result (`ResultSetMetaData`) | column count + SQL types (e.g. `BIGINT`) |
| Large aggregation (`GROUP BY` over 300k rows) | correct grouped results streamed |

## Prepared statements with parameters

`PreparedStatement` with `?` placeholders works: the server rewrites JDBC `?`
positional placeholders to its `$1 … $N` machinery (string-literal- and
quoted-identifier-aware), and the bound values are substituted into the query
before execution — through **both** the DoGet-prepared path and the
`GetFlightInfo → DoGet(ticket)` path the Arrow JDBC driver actually uses.

- **String parameters** (`PreparedStatement.setString`) are fully supported.
- **Non-string typed parameters** (`setInt`, `setLong`, `setDouble`, …) are
  currently **rejected by the JDBC driver client-side**: the server advertises a
  parameter schema of `Utf8` (it cannot infer per-parameter types from a bare
  `?`/`$N` without planning the statement), and the driver enforces that schema
  before sending the value. Until per-parameter type inference lands, bind typed
  params as strings (`setString`) — the server renders each bound Arrow value as
  the correct SQL literal regardless — or inline the literal. (Residual: G12
  parameter-type inference.)

Non-parameterized prepared statements and all metadata paths work fully.
