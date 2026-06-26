# krishiv-flight-sql

Arrow Flight SQL server implementation for Krishiv.

## Overview

`krishiv-flight-sql` exposes Krishiv's SQL engine over the Arrow Flight SQL
protocol, enabling JDBC/ODBC clients and tools like `adbc` to connect.

- Statement execution (query, DDL, DML)
- Parameter binding
- Transaction support
- Prepared statement caching

## Binary

`krishiv-flight-server`

## Features

| Feature | Description |
|---------|-------------|
| `kafka` | Kafka connector support |

## Usage

```bash
krishiv-flight-server --listen 0.0.0.0:50051 --coordinator http://localhost:50051
```

## License

Apache-2.0
