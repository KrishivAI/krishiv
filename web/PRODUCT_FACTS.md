# Krishiv Product Facts

Internal notes for maintainers. Do not link this file publicly.

| Capability | Maturity | Evidence/source path | Safe website wording |
|---|---|---|---|
| Rust-native hybrid compute framework | Available | `docs/README.md`, `Cargo.toml`, crate map | Krishiv is a Rust-native compute framework for batch SQL, streaming pipelines, and lakehouse-oriented data work. |
| Arrow `RecordBatch` internal model | Available | `docs/README.md`, `README.md`, connector contracts | Apache Arrow `RecordBatch` is the in-memory and IPC data model. |
| DataFusion SQL planning/execution | Available | `docs/README.md`, `crates/krishiv-sql`, `README.md` | DataFusion handles SQL parsing, planning, expressions, and local execution. |
| Batch SQL | Available | `README.md`, `docs/running-examples.md` | Batch SQL is available over registered Arrow/Parquet-style sources. |
| Streaming execution | Available | `docs/running-examples.md`, `examples/python/*stream*`, `crates/krishiv-dataflow` | Streaming sessions, windows, and stream job push/poll APIs exist. |
| Delta batch mode | Experimental | `README.md`, `docs/implementation/incremental-computing.md`, `crates/krishiv-delta` | DeltaBatch represents weighted Arrow change batches for incremental processing. |
| Incremental view maintenance | Experimental | `docs/implementation/incremental-computing.md`, `docs/implementation/status.md`, `crates/krishiv-ivm`, `crates/krishiv-api` | IncrementalFlow supports experimental IVM with views, steps, snapshots, watches, and partitioning. |
| Distributed executor-side IVM | In Progress | `docs/implementation/status.md` notes deferred distributed IVM compute | Do not advertise distributed executor-side IVM as complete. |
| Embedded/local mode | Available | `docs/README.md`, `docs/running-examples.md` | Embedded mode runs in process for tests, examples, and local API use. |
| Single-node mode | Available | `docs/README.md`, examples/single-node | Single-node mode runs core engine components on one host. |
| Distributed mode | In Progress | `docs/README.md`, k8s manifests, scheduler/executor crates | Distributed mode uses explicit remote coordinator/executor transport; describe operationally and conservatively. |
| Scheduler | Available | `docs/README.md`, `crates/krishiv-scheduler` | Scheduler owns coordinator, job/task lifecycle, metadata stores, leadership, and gRPC server. |
| Shuffle | Preview | `docs/README.md`, `crates/krishiv-shuffle` | Shuffle support includes in-memory, local disk, object-store, and Flight-oriented paths. |
| Checkpointing and state | Preview | `docs/README.md`, `crates/krishiv-state`, engine contracts | State and checkpoint primitives exist; guarantees depend on durability profile and certified connectors. |
| Iceberg | Preview | `docs/README.md`, `docs/contracts/connectors.md`, `docs/contracts/engine-semantics.md` | Iceberg is the primary lakehouse target; certification work continues. |
| REST/Hive/Glue catalogs | Preview | `README.md`, `docs/README.md` | Catalog integration includes REST, Hive, and Glue paths. Use “Polaris-compatible REST catalog” only when tested. |
| Parquet | Preview | `docs/contracts/connectors.md` | Parquet source/sink paths exist; avoid claiming distributed atomic writes. |
| Kafka | Preview | `docs/contracts/connectors.md`, engine semantics | Kafka source/sink and transactional/checkpoint paths exist; certification pending. |
| S3/object store | Preview | `docs/contracts/connectors.md`, durability profiles | Object-store storage paths exist; correctness depends on table/file commit protocol. |
| ADLS | Preview | `docs/README.md`, docs/examples references | Mention ADLS conservatively as a storage target; do not overstate certification. |
| Python API | Available | `docs/README.md`, `crates/krishiv-python`, examples/python | Python bindings expose Session/DataFrame/streaming and incremental wrappers; some integrations are feature gated. |
| Exactly-once semantics | In Progress | `docs/contracts/engine-semantics.md`, `docs/contracts/connectors.md` | Do not claim global exactly-once. Use preview/candidate wording for specific source/sink/checkpoint combinations. |
| Krishiv Cloud | Planned | User request only; no product code | Managed compute is planned for the future. No pricing or cloud functionality claims. |
