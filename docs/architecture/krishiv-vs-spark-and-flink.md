# Krishiv vs Apache Spark vs Apache Flink: Architectural Comparison

**Date**: 2026-05-30 (analysis performed during R12-R18 stability sprint)  
**Scope**: Core architectural decisions and bottlenecks, grounded in Krishiv source code, invariants, and published roadmap. Spark/Flink sections draw from public architecture, production analyses, and official docs (Flink 1.x/2.x concepts as of 2025-2026).

**Primary Sources for Krishiv**:
- [AGENTS.md](/home/code/krishiv/AGENTS.md) (invariants)
- [docs/architecture/krishiv-roadmap.md](/home/code/krishiv/docs/architecture/krishiv-roadmap.md)
- [docs/architecture/unified-execution-model.md](/home/code/krishiv/docs/architecture/unified-execution-model.md)
- [docs/architecture/crate-map.md](/home/code/krishiv/docs/architecture/crate-map.md)
- [docs/architecture/architectural-decisions-r12-r20.md](/home/code/krishiv/docs/architecture/architectural-decisions-r12-r20.md) (ADRs)
- [docs/implementation/status.md](/home/code/krishiv/docs/implementation/status.md) (maturity evidence)
- Concrete code: `krishiv-runtime`, `krishiv-scheduler` (JobCoordinator), `krishiv-state`, `krishiv-shuffle`, `krishiv-exec`, `krishiv-plan`

Krishiv's stated goal (roadmap): "a Rust-native hybrid compute framework ... combines Spark-style distributed SQL and adaptive batch execution with Flink-style event-time streaming, keyed state, checkpointing, and exactly-once-capable sink coordination" using **one shared planning and runtime model**.

## 1. Core Architectural Philosophy & Decisions

| Dimension                  | Krishiv (code-grounded)                                                                 | Apache Spark                                      | Apache Flink                                          |
|----------------------------|-----------------------------------------------------------------------------------------|---------------------------------------------------|-------------------------------------------------------|
| **Batch + Streaming Model**| **Single unified DAG execution model**. Batch = bounded DAGs; streaming = unbounded + watermarks/state/checkpoints. Same `PhysicalPlan`, `NodeOp`, operators, runtime path. No separate engines. | Two models historically (RDD vs DStream; now Structured Streaming uses micro-batching on Catalyst). "Continuous Processing" experimental/limited. | Native streaming first (DataStream API, ProcessFunction, CEP). Batch is bounded streams or Table API. Unified Table/SQL later. |
| **Evidence in Krishiv**    | [crates/krishiv-runtime/src/execution_runtime.rs:60](crates/krishiv-runtime/src/execution_runtime.rs) (`trait ExecutionRuntime { accept_plan, collect_bounded_window }` + `RuntimeMode { Embedded, SingleNode, Distributed }`). [unified-execution-model.md](/home/code/krishiv/docs/architecture/unified-execution-model.md). AGENTS.md: "Do not build separate engines for batch and streaming. Model both as DAG execution modes in one runtime." | Catalyst optimizer + Tungsten for batch; microbatch engine for streaming. | One runtime (TaskManagers execute chained operators); batch as special case of streaming. |
| **Data Model / Memory**    | **Apache Arrow** columnar, zero-copy IPC (Flight), Rust ownership. `#![forbid(unsafe_code)]` in core crates. | JVM objects (Tungsten: off-heap binary format + codegen). Arrow support added later (via Arrow DataSource, PyArrow). | JVM (managed memory segments, off-heap for network/RocksDB). Some Arrow interop but not primary internal format. |
| **Language / Runtime**     | **Rust + Tokio** (async-first, no GC, structured concurrency via tasks/abort handles). | Scala/Java on JVM (GC, JIT). Python via PySpark (serialization cost). | Java/Scala on JVM (heavy managed memory tuning, GC mitigation via segments). |
| **Control Plane**          | Active-active API servers. **Exactly one active `JobCoordinator` per job** (cell-based). Executors = replaceable data-plane workers. Leases, fencing tokens, epoch metadata for failover. CCP (ClusterControlPlane) + per-job JCP split in later design. | Driver (per-application, runs user code + scheduler) + Master + Workers. Coarse-grained. | JobManager(s) (high-availability via ZooKeeper/K8s) + TaskManagers (slots). Fine-grained resource mgmt in newer versions. |
| **Evidence in Krishiv**    | [crates/krishiv-scheduler/src/job_coordinator.rs:1-100](crates/krishiv-scheduler/src/job_coordinator.rs) ("Per-job coordinator facade (ADR-DIST-01)", `JobCoordinator` scopes to one `JobId` over `SharedCoordinator`, `spawn_job_orchestration_loops_with_handles` + abort on demotion). [distributed-unified-mitigation-plan.md](/home/code/krishiv/docs/architecture/distributed-unified-mitigation-plan.md) (ADR-DIST-01). AGENTS.md: "Use active-active API servers with exactly one active `JobCoordinator` per job." "Do not implement full active-active multi-master scheduling for the same job." | Driver is single point of coordination + metadata for the app. | JobManager coordinates; can have standby via HA services. |
| **Shuffle**                | Pluggable independent backend abstraction: `InMemoryShuffleStore`, `LocalDiskShuffleStore`, `ObjectStoreShuffleStore` (Arrow IPC over Flight or object store). Compression (LZ4/Zstd) negotiation. Explicit `validate_safe_id` (path traversal defense). | Shuffle via local disk or External Shuffle Service (ESS). Heavy serialization (Kryo). | Network shuffle (pipelined, credit-based flow control). Chained operators reduce some shuffles. |
| **Evidence**               | [crates/krishiv-shuffle/src/lib.rs:1-50](crates/krishiv-shuffle/src/lib.rs) (re-exports multiple `*ShuffleStore`, Flight module, `validate_safe_id` for S4 security fix). | ESS is separate process; shuffle write/read phases explicit. | Credit-based; buffer pools; operator chaining. |
| **State Management**       | Pluggable `StateBackend` trait: `InMemoryStateBackend`, `RedbStateBackend` (pure-Rust embedded B-tree, ACID, file-backed). TTL, timers, snapshots. "State must be accessed only within `process_batch`..." Synchronous I/O requires `spawn_blocking`. | Limited in RDD/streaming (mapWithState, updateStateByKey); Structured Streaming uses HDFS-backed state store (RocksDB-like options later). | Rich keyed state + timers. Pluggable backends: Heap (HashMap), RocksDB (embedded LSM), (ForSt disaggregated in 2.0+). |
| **Evidence**               | [crates/krishiv-state/src/lib.rs:1-56](crates/krishiv-state/src/lib.rs) (`StateBackend` trait + InMemory + Redb/RocksDb alias; forbid unsafe; explicit access rules). | State store pluggable in Structured but secondary to batch focus. | Core to the programming model (ValueState, ListState, MapState, timers). |
| **Fault Tolerance / Exactly-Once** | Epoch-based checkpoints + barriers. **Certified only per source/sink/checkpoint combination** (no blanket promise). Fencing tokens, leases, durable metadata (fsync + atomic rename in stores). Savepoints. | RDD lineage + checkpointing (microbatch). Structured Streaming: checkpoint dir (WAL + state). Exactly-once for sinks with idempotency or transactions (Kafka 0.11+). | Lightweight asynchronous snapshots (barriers). Exactly-once by default for state + certified sinks. Unaligned checkpoints for speed. |
| **Evidence**               | Checkpoint crates, `validate_fencing_token`, `latest_valid_epoch`, two-phase sink commits in connectors (see status.md CDC→Iceberg). AGENTS.md: "Document exactly-once only for certified source/sink/checkpoint combinations." | Driver failure = full restart or recovery from checkpoint. | Barrier-based; highly tuned for low overhead in steady state. |

**Key Krishiv Differentiator (from code)**: The unified model + pluggable backends + Rust memory safety + "exactly one active coordinator per job + fencing" is a deliberate synthesis that avoids the microbatch tax (Spark) and JVM GC + RocksDB tuning burden (Flink) while keeping a simpler HA story than full multi-master.

## 2. Architectural Bottlenecks Comparison

### Krishiv Bottlenecks (Grounded in Current Code + Plans)

1. **Per-Job Coordinator Centralization (Correctness vs Scalability Tradeoff)**
   - One active `JobCoordinator` owns the full state machine, task assignment, barrier dispatch, and checkpoint coordination for that job.
   - Evidence: `job_coordinator.rs:63` (`coordinator_tick` only for its job), `spawn_job_orchestration_loops_with_handles` (per-job Tokio tasks), `SharedCoordinator` still holds global maps (executors, jobs).
   - Mitigation in flight: CCP + per-job JCP split (distributed-unified-mitigation-plan.md ADR-DIST-01/02).
   - Impact: For jobs with 10k+ tasks or extreme churn, the single coordinator process (even Tokio) can become CPU or lock-contention bottleneck. Heartbeat/ tick interval (500ms in current spawn) adds scheduling latency.
   - Contrast: Avoids split-brain complexity of full active-active; simpler fencing/epoch semantics.

2. **Maturity & Production Hardening Surface (Current Phase Evidence)**
   - Status (2026-05-30): R12-R18 "API Stability, Local-Only Boundaries, & Observability Sprint". Recent work: 250 findings from full source review (21 critical), dozens of P0 security/correctness fixes (path traversal, injection, fail-open executor, fsync, lease races, spill races, etc.).
   - Evidence: [status.md](/home/code/krishiv/docs/implementation/status.md) (S1-S8, C1-C11, lease generation, shuffle spill, Hudi CoW, async emitter, etc.). Many "fail-closed" hardenings and `block_in_place` / `spawn_blocking` for sync I/O.
   - Impact: Lower operational confidence than Spark (10+ yrs at hyperscale) or Flink (mature exactly-once + huge connector ecosystem). Rust compile times, smaller talent pool, fewer battle-tested connectors.
   - Positive: Rapid gap closure; explicit "no stubs" policy in recent sprints.

3. **State & Shuffle Backend Maturity**
   - State: Redb excellent for local/single-node (ACID, no JNI), but distributed rescaling/queryable state and incremental checkpointing less advanced than Flink 2.0 ForSt or Spark state store.
   - Shuffle: Strong Arrow Flight path (zero-copy friendly); object-store path has latency/cost for fine-grained shuffle. No mature disaggregated "shuffle service" equivalent yet for ultra-high throughput low-latency.
   - Evidence: Multiple backends exist and are selectable (good), but status shows repeated spill races, capacity, orphan, fsync fixes in 2026-05.

4. **Ecosystem & Connector Surface**
   - Strong Parquet/Kafka/CDC/Iceberg/Hudi direction, but R15 is future Spark compat layer. Python (GIL + pyo3 FFI) and dbt/Airflow integration still maturing (R13+ focus).
   - Fewer SQL functions, UDF ecosystems, and monitoring integrations than Spark/Flink.

5. **Coordinator/Executor Heartbeat & Placement**
   - Lease generation, fencing, and 500ms ticks provide safety but can lag vs fully data-driven or finer-grained scheduling in mature systems.

### Apache Spark Bottlenecks (Well-Documented Production Realities)

1. **JVM GC + Memory Model (Fundamental)**
   - Large heaps for caching/shuffle/state → long stop-the-world pauses (seconds). Tungsten/off-heap mitigates but does not eliminate (metadata, UDF objects, driver).
   - Python UDFs: high ser/de cost (even Arrow vectorized paths have overhead).
   - Impact: Latency spikes, stragglers, OOM during shuffle.

2. **Micro-Batch Streaming Latency Floor**
   - Structured Streaming: minimum practical latency hundreds of ms to seconds (trigger interval + processing + commit). Continuous Processing mode is experimental, supports limited operators/sources, and has seen limited adoption.
   - Coarse-grained tasks amplify straggler impact on entire stage.

3. **Shuffle & Serialization Tax**
   - Even with ESS and Kryo, shuffle is a dominant cost (disk or network). Adaptive Query Execution (AQE) helps but is post-hoc.
   - Driver can become metadata bottleneck for very large plans.

4. **Operational Complexity at Scale**
   - Dynamic allocation, speculation, AQE tuning, YARN/K8s integration all require expertise. "Debugging a slow Spark job" is a meme for a reason.

### Apache Flink Bottlenecks (From Official Docs + Analyses 2024-2026)

1. **Checkpointing Under Load / Backpressure (Primary Pain Point)**
   - Aligned checkpoints require barrier propagation + alignment. Under backpressure or skew, alignment time dominates; one slow subtask stalls the checkpoint.
   - Evidence (Flink docs): "Checkpointing under backpressure", unaligned checkpoints trade size/complexity for speed. Incremental RocksDB checkpoints still produce bursty large uploads and compaction interference.
   - Recovery for large state is slow (remote reads).

2. **RocksDB State Backend I/O & Compaction**
   - 5-10x slower than heap due to ser/de, JNI, disk. Compactions during checkpoints cause variability. Memory tuning (block cache, write buffers, managed memory split) is notoriously error-prone.
   - Hot keys or growing state → I/O or GC amplification.
   - Flink 2.0 ForSt (disaggregated/remote primary + async) is the major mitigation, but adoption lag and still complex.

3. **JVM GC & Memory Tuning Burden**
   - Even with managed memory segments (off-heap), large state + user objects + network buffers require deep expertise. Long GC pauses cause missed heartbeats, false failures, checkpoint timeouts.
   - Tuning: task heap vs managed vs network vs metaspace is a full-time job for large deployments.

4. **Backpressure Propagation Complexity & Observability**
   - Credit-based flow control is powerful but creates feedback loops. UI metrics (busy/backpressured time) help, but root-causing skew + state + GC + sink interactions is hard. Buffer debloating helps but is another knob.

5. **Operational & Skill Tax**
   - Steep curve: checkpoint/savepoint semantics, rescaling with state, exactly-once sink contracts, slot sharing, HA setup. Many orgs need dedicated Flink SREs.

## 3. Where Krishiv Intentionally Differs (and Why)

- **Avoids micro-batch tax** by construction (unified DAG from day 1 in `krishiv-plan`/`krishiv-exec`/`krishiv-runtime`).
- **Avoids JVM/GC entirely** via Rust + Arrow + Tokio (forbid unsafe in state/shuffle/runtime).
- **Pluggable backends as first-class** (shuffle/state/checkpoint) so operators choose latency vs durability without engine forks.
- **Simpler HA model** (exactly-1 active JobCoordinator + leases/fencing) trades potential multi-leader throughput for dramatically simpler correctness reasoning and recovery (see ADRs and mitigation plan).
- **"Certified exactly-once only"** stance is more conservative/honest than blanket claims.
- **Cell-based + per-job isolation** aims for better blast radius than a single giant driver/JobManager.

**Current reality (status 2026-05)**: Krishiv has invested heavily in closing the "production gap" (fail-closed, fsync, lease races, spill correctness, etc.) precisely because these architectural choices are sound only if the implementation details are hardened.

## 4. When to Choose Which (Pragmatic)

- **Choose Krishiv** when: You want Rust-native performance + Arrow ecosystem, true unified batch/streaming without microbatch latency floor, are willing to operate a younger system, value explicit backend choice and strong safety invariants, or are building lakehouse/CDC pipelines where Rust connectors shine.
- **Choose Spark** when: You need the broadest SQL ecosystem, mature AQE, Python-first data science workflows, massive existing investment in Spark SQL / Delta / Iceberg tooling, or batch analytics at extreme scale where microbatch is acceptable.
- **Choose Flink** when: You need the most mature low-latency exactly-once streaming with rich state/CEP/watermark semantics today, have budget for JVM tuning + dedicated ops, or already run large Flink deployments with RocksDB/ForSt expertise.

## 5. References & Further Reading

- Krishiv: `docs/architecture/krishiv-roadmap.md`, `unified-execution-model.md`, `distributed-unified-mitigation-plan.md`, `checkpoint-protocol.md`, `shuffle-deployment-model.md`
- Spark: Official docs on Structured Streaming, AQE, External Shuffle Service, Tungsten.
- Flink: "Checkpointing under backpressure", "Large State Tuning", "State Backends", Flink 2.0 disaggregated state papers.

---

**Analysis Validation**: This document was produced after full protocol-mandated reads (AGENTS/CLAUDE/status/README), repo inspection via `list_dir` + targeted `grep`/`read_file` of 20+ architecture and source files, and external Flink research. All Krishiv claims are directly traceable to specific files and line ranges above.

**Next**: When R15 Spark ecosystem work begins, cross-reference this doc against the compat matrix in `docs/reference/spark-sql-compat-matrix.md`.
