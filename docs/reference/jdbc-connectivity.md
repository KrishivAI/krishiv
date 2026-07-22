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

## Known limitation

Server-side prepared-statement parameter binding uses `$N` placeholders, not the
JDBC `?` placeholder, so `PreparedStatement.setX(...)` with `?` is not yet mapped
(tracked as G12). Inline the values, or use `$1`-style parameters, until the `?`→
`$N` mapping lands. Non-parameterized prepared statements and all metadata paths
work.
