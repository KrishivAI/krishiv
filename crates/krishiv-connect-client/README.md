# krishiv-connect-client

Lightweight Spark Connect-style client for Krishiv over Flight SQL.

## Overview

`krishiv-connect-client` provides a thin client for submitting SQL queries and
fetching results from a remote Krishiv coordinator via the Arrow Flight SQL
protocol.

## Usage

```rust
use krishiv_connect_client::ConnectClient;

let client = ConnectClient::connect("http://localhost:50051").await?;
let batches = client.sql("SELECT 1").await?;
```

## License

Apache-2.0
