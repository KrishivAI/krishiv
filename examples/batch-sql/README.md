# Batch SQL Examples

Run a literal query:

```bash
cargo run -p krishiv -- sql --query "select 1 as value"
```

Explain the logical and physical plan:

```bash
cargo run -p krishiv -- explain --query "select 1 as value"
```

Submit a distributed job to the local scheduler:

```bash
cargo run -p krishiv -- submit --job-id job-demo --name demo --tasks 2 --launch
```

Show distributed status for the current process:

```bash
cargo run -p krishiv -- jobs --distributed
```

Run the embedded Parquet example using the unified `krishiv` crate:

```bash
cargo run -p krishiv --example batch_sql
```

For CLI Parquet registration:

```bash
cargo run -p krishiv -- sql --parquet people=./people.parquet --query "select count(*) from people"
```
