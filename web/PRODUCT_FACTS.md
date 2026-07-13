# Krishiv Product Facts

Internal notes for maintainers. Do not link this file publicly.

| Capability | Maturity | Evidence/source path | Safe website wording |
|---|---|---|---|
| Rust-native hybrid compute framework | Available | `docs/README.md`, `Cargo.toml`, crate map | Krishiv is a Rust-native compute framework for batch SQL, streaming pipelines, and lakehouse-oriented data work. |
| Arrow `RecordBatch` internal model | Available | `docs/README.md`, `README.md`, connector contracts | Apache Arrow `RecordBatch` is the in-memory and IPC data model. |
| DataFusion SQL planning/execution | Available | `docs/README.md`, `crates/krishiv-sql`, `README.md` | DataFusion handles SQL parsing, planning, expressions, and local execution. |
| Batch SQL | Available | `README.md`, `docs/running-examples.md` | Batch SQL is available over registered Arrow/Parquet-style sources. |
| Streaming execution | Preview | `docs/running-examples.md`, `examples/python/*stream*`, `crates/krishiv-dataflow` | Streaming sessions, windows, and stream-job APIs exist; recovery and connector certification continue. |
| Delta batch mode | Experimental | `README.md`, `docs/implementation/incremental-computing.md`, `crates/krishiv-delta` | DeltaBatch represents weighted Arrow change batches for incremental processing. |
| Incremental view maintenance | Experimental | `docs/implementation/incremental-computing.md`, `docs/implementation/status.md`, `crates/krishiv-ivm`, `crates/krishiv-api` | IncrementalFlow supports experimental IVM with views, steps, snapshots, watches, and partitioning. |
| Distributed executor-side IVM | In Progress | `docs/implementation/status.md` notes deferred distributed IVM compute | Do not advertise distributed executor-side IVM as complete. |
| Embedded/local mode | Available | `docs/README.md`, `docs/running-examples.md` | Embedded mode runs in process for tests, examples, and local API use. |
| Single-node mode | Available | `docs/README.md`, examples/single-node | Single-node mode runs core engine components on one host. |
| Distributed mode | Preview | `docs/README.md`, k8s manifests, scheduler/executor crates | Coordinator/executor and transport foundations exist; do not claim production readiness, high availability, or elastic scale. |
| Scheduler | Available | `docs/README.md`, `crates/krishiv-scheduler` | Scheduler owns coordinator, job/task lifecycle, metadata stores, leadership, and gRPC server. |
| Shuffle | Preview | `docs/README.md`, `crates/krishiv-shuffle` | Shuffle support includes in-memory, local disk, object-store, and Flight-oriented paths. |
| Checkpointing and state | Preview | `docs/README.md`, `crates/krishiv-state`, engine contracts | State and checkpoint primitives exist; guarantees depend on durability profile and certified connectors. |
| Iceberg | Preview | `docs/README.md`, `docs/contracts/connectors.md`, `docs/contracts/engine-semantics.md` | Iceberg is the primary lakehouse target; certification work continues. |
| Iceberg catalog paths | Preview | `README.md`, `docs/README.md` | Catalog-oriented code exists; name a specific catalog backend only when its current path has been verified. |
| Parquet | Preview | `docs/contracts/connectors.md` | Parquet source/sink paths exist; avoid claiming distributed atomic writes. |
| Kafka | Preview | `docs/contracts/connectors.md`, engine semantics | Kafka source/sink and transactional/checkpoint paths exist; certification pending. |
| S3/object store | Preview | `docs/contracts/connectors.md`, `crates/krishiv-connectors/src/storage_factory.rs`, registry drivers | Object-store paths exist. The named S3 registry driver is local-backed; AWS construction requires the separate `cloud` feature. |
| Python API | Preview | `docs/README.md`, `crates/krishiv-python`, examples/python | Source-built Python bindings expose Session/DataFrame APIs; connector selection and packaging are not yet stable. |
| Exactly-once semantics | In Progress | `docs/contracts/engine-semantics.md`, `docs/contracts/connectors.md` | Do not claim global exactly-once. Use preview/candidate wording for specific source/sink/checkpoint combinations. |
| Krishiv Platform | Coming Soon | `../krishiv-platform` source tree | Platform is a separate, self-hosted, source-available control plane and workspace under development. Do not publish install steps, dates, availability, or production claims. |
