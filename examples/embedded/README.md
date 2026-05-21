# Embedded Examples

Embedded examples use the `krishiv` crate directly — the unified user-facing library
that re-exports all public APIs under a single `use krishiv::...` import.

Run local SQL over a generated Parquet file:

```bash
cargo run -p krishiv --example batch_sql
```

Run a bounded in-memory stream pipeline:

```bash
cargo run -p krishiv --example memory_stream
```

The source files live under `crates/krishiv/examples/`.

## Rust API Quick-Start

```rust
use krishiv::prelude::*;

// Batch SQL
let session = Session::builder().build()?;
let result = session.sql("SELECT 1 AS n")?.collect()?;
println!("{}", result.pretty()?);

// Local stream
let stream = session.memory_stream("events", batches);
let out = stream.collect_bounded()?;
```
