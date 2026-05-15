# Batch SQL Examples

R1 supports local SQL through `krishiv sql` and `krishiv explain`.

Run a literal query:

```bash
cargo run -p krishiv-cli -- sql --query "select 1 as value"
```

Explain the logical and physical plan:

```bash
cargo run -p krishiv-cli -- explain --query "select 1 as value"
```

Run the embedded Parquet example to generate a small Parquet-backed query using
the same public API:

```bash
cargo run -p krishiv-api --example local_sql_parquet
```

For CLI Parquet registration, use:

```bash
cargo run -p krishiv-cli -- sql --parquet people=./people.parquet --query "select count(*) from people"
```
