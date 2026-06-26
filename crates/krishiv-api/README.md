# krishiv-api

Public API surface for interacting with Krishiv programmatically.

## Overview

`krishiv-api` provides the user-facing API for:

- Session management and SQL execution
- DataFrame operations (select, filter, join, write)
- Catalog operations (list tables, describe, drop)
- Streaming pipeline management

## Features

| Feature | Description |
|---------|-------------|
| `kafka` | Kafka connector support |
| `iceberg-catalog` | Iceberg catalog API support |

## Usage

```rust
use krishiv_api::Session;

let session = Session::new();
let result = session.sql("SELECT * FROM orders WHERE amount > 100").await?;
```

## License

Apache-2.0
