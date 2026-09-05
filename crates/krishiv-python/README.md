# krishiv-python

PyO3 bindings (`pip install krishiv`) over the same `Session` the Rust API
uses: `Session`, `BlockingSession`, `DataFrame`, `StreamingDataFrame`,
`IncrementalDataFrame`, `IvmJob`, `DeltaBatch`, `Pipeline`, `QueryHandle`,
`PreparedStatement`, `Column`, keyed process state, connector sources and
sinks, Rust and Python UDFs, and a PySpark-compatible surface
(`krishiv.sql.functions`, `Row`). Python is sync-convenience plus asyncio.

Optional native features: `kafka`, `iceberg`, `kinesis`, `pulsar`,
`cassandra`, `elasticsearch`, `hbase`, `vector-sinks`, `qdrant`, `pgvector`.

```bash
maturin develop --manifest-path crates/krishiv-python/Cargo.toml --release
```

```python
import krishiv as ks
session = ks.Session()
print(session.sql("SELECT 42 AS answer").collect().pretty())
```

Excluded from the workspace `cargo test`/clippy runs (needs a Python
toolchain); tested by the required `test-python` CI job. Lint with
`cargo check -p krishiv-python`.

Documentation: `docs/architecture/11-public-interfaces.md`,
`docs/reference/pyspark-parity.md` (generated).

License: Apache-2.0.
