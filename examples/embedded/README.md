# Embedded Examples

R1 embedded examples are compiled as Cargo examples on the `krishiv-api`
crate, where they can reuse the public Rust API directly.

Run local SQL over a generated Parquet file:

```bash
cargo run -p krishiv-api --example local_sql_parquet
```

Run a bounded in-memory stream pipeline:

```bash
cargo run -p krishiv-api --example memory_stream
```

The source files live under `crates/krishiv-api/examples/` so Cargo can build
and validate them with the rest of the workspace.
