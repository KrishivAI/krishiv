# Krishiv Streaming Architecture: Comprehensive Plan

## Executive Summary

This document consolidates the complete analysis and implementation plan for evolving Krishiv's streaming architecture. Based on analysis of Flink, Arroyo, Arkflow, and RisingWave, Krishiv needs to evolve from micro-batch processing to support dual-mode execution (micro-batch + continuous per-record) with disaggregated state storage.

**Total Effort**: 29-42 weeks across all phases

---

## Part 1: Framework Analysis

### Framework Comparison Matrix

| Aspect | Flink | Arroyo | Arkflow | RisingWave | Krishiv (current) |
|--------|-------|--------|---------|------------|-------------------|
| **Language** | Java/Scala | Rust | Rust | Rust | Rust |
| **Processing Model** | Per-record (Mailbox) | Per-record (Arrow) | Stateless stream (adding state) | Actor-based (per-message) | Micro-batch (drain) |
| **Latency Floor** | Single-digit ms | Millisecond-level | Low latency (Tokio) | Sub-100ms | Drain cycle interval |
| **State Backend** | RocksDB + ForSt (DFS) | Remote storage (S3) | Adding checkpoint/2PC | Hummock LSM-tree on S3 | RocksDB (sync fsync) |
| **Checkpoint** | Chandy-Lamport barriers | Chandy-Lamport inspired | Barrier-based + 2PC | Barrier-based, epoch MVCC | Coordinator-fenced epoch barriers |
| **Exactly-Once** | Certified matrix | Yes | Adding (2PC + idempotency) | Yes (epoch-based) | Certified matrix |
| **Architecture** | JobManager + TaskManagers | Controller + Workers | Engine + Streams | Meta + Streaming + Serving + Compactor | Single runtime |
| **Cloud-Native** | Limited (local disk) | Yes (S3, GCS, ABS) | Limited | Yes (S3-native) | Limited (local RocksDB) |
| **SQL Support** | Full | Full | YAML + SQL | Full (PostgreSQL compatible) | DataFusion SQL |
| **Key Innovation** | Mature ecosystem | 10x faster sliding windows, operator chaining | AI/ML integration | Disaggregated storage, remote compaction | Exactly-once certified matrix |

### Key Architectural Insights

#### 1. Processing Model Convergence
All modern Rust streaming engines use **per-record processing**:
- **Arroyo**: Arrow columnar format with zero-copy, operator chaining to eliminate queue overhead
- **RisingWave**: Actor model where each actor processes messages atomically via Tokio async
- **Flink**: Mailbox model with single-threaded execution

**Krishiv gap**: Micro-batch drain cycle limits latency floor.

#### 2. State Storage Evolution
All frameworks moving toward **disaggregated storage**:
- **RisingWave**: Hummock LSM-tree on S3 with multi-tier caching (memory → local disk → S3)
- **Arroyo**: Remote object stores for checkpoints
- **Flink 2.0**: ForSt backend on DFS
- **Arkflow**: Adding checkpoint mechanism with barrier alignment

**Krishiv gap**: Local RocksDB with sync fsync limits throughput and recovery time.

#### 3. Checkpoint Mechanisms
Two dominant patterns:
- **Barrier-based** (RisingWave, Arroyo, Flink): Inject barriers into stream, operators snapshot when barriers received from all inputs
- **Epoch-based** (RisingWave): Barriers = epochs, state committed only after barrier reaches storage

**Krishiv uses coordinator-fenced epoch barriers** - similar pattern but different implementation.

#### 4. Latency Optimization Strategies
- **Arroyo**: Operator chaining (eliminate inter-operator queues), efficient sliding window algorithms
- **RisingWave**: Multi-tier caching (memory, local disk, S3), 4MB block reads, prefetching
- **Flink**: Buffer timeout tuning (0 = per-record, 100ms = batched)

**Krishiv gap**: No operator chaining, no multi-tier caching, no buffer timeout.

---

## Part 2: Krishiv Gap Analysis

### Current Strengths
- Exactly-once certified matrix (explicit, not blanket claims)
- Two-phase commit sink with idempotent commit/abort
- TTL state backend with event-time awareness
- Incremental checkpoints with LRU key eviction
- Coordinator-fenced epoch barriers
- Declarative pipeline API in Python (existing)

### Critical Gaps

| Gap | Current State | Target State | Impact |
|-----|---------------|--------------|--------|
| **Processing Model** | Micro-batch drain cycle | Dual-mode (micro-batch + continuous) | High — enables sub-100ms latency |
| **State Storage** | Local RocksDB with sync fsync | Disaggregated storage (S3-native) | High — cloud-native scaling |
| **Checkpoint Efficiency** | Coordinator-fenced epoch barriers | Barrier-based checkpointing | Medium — faster recovery |
| **Operator Optimization** | No operator chaining | Operator chaining | Medium — reduced overhead |
| **Caching** | No multi-tier caching | Multi-tier caching (memory → disk → S3) | Medium — latency optimization |
| **Timezone Support** | Raw i64 ms, no timezone handling | Timezone-aware timestamps | Medium — correctness fix |
| **Network Buffers** | No buffer abstraction | Configurable output buffering | Medium — latency tuning knob |

---

## Part 3: Implementation Plan

### Phase 1: Dual-Mode Execution Engine (Core)

**Goal**: Support both micro-batch (throughput) and continuous (latency) modes.

**Duration**: 7-10 weeks

**Components**:

#### 1.1 Mailbox Runtime (`krishiv-mailbox` crate)

```rust
pub struct MailboxProcessor {
    mail: VecDeque<Mail>,
    default_action: Box<dyn MailboxDefaultAction>,
}

pub trait MailboxDefaultAction {
    fn process_mail(&mut self, mail: Mail) -> Result<()>;
}

pub enum Mail {
    Record(StreamRecord),
    CheckpointBarrier(CheckpointId),
    Watermark(Watermark),
    TimerFire(Timer),
}
```

**Key files to create**:
- `crates/krishiv-mailbox/src/lib.rs` — `MailboxProcessor`, `MailboxDefaultAction`
- `crates/krishiv-mailbox/src/mail.rs` — `Mail` enum
- `crates/krishiv-mailbox/src/stream_record.rs` — `StreamRecord<T>`
- `crates/krishiv-mailbox/src/output.rs` — `OutputCollector` with buffer timeout

#### 1.2 Per-Record Operator Adapters

```rust
pub trait PerRecordOperator {
    fn process_element(&mut self, record: &StreamRecord<RecordBatch>);
    fn process_watermark(&mut self, watermark: Watermark);
    fn process_checkpoint_barrier(&mut self, barrier: CheckpointBarrier);
}
```

**Adapter implementations**:
- `FilterAdapter`: wraps batch filter, applies per-row
- `ProjectAdapter`: wraps batch projection, applies per-row
- `AggregateAdapter`: maintains incremental state, emits per-record
- `WindowAdapter`: triggers on watermark, not drain cycle

#### 1.3 Hybrid Execution Mode

```rust
pub enum ProcessingMode {
    MicroBatch { drain_interval_ms: u64 },
    Continuous { buffer_timeout_ms: u64 },
}

pub struct ContinuousExecutor {
    mode: ProcessingMode,
    mailbox: MailboxProcessor,
    operators: Vec<Box<dyn PerRecordOperator>>,
}
```

**Dynamic mode switching**:
- Source operators emit `RecordAttributes::is_backlog` flag
- When `is_backlog = true`: operators use batch mode (throughput)
- When `is_backlog = false`: operators switch to continuous mode (latency)

---

### Phase 2: Disaggregated State Storage

**Goal**: Decouple compute from state storage for cloud-native scaling.

**Duration**: 6-8 weeks

**Components**:

#### 2.1 State Storage Abstraction

```rust
#[async_trait]
pub trait StateStorage: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<()>;
    async fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    async fn snapshot(&self) -> Result<SnapshotId>;
}
```

#### 2.2 Hummock LSM-Tree Backend (`krishiv-hummock` crate)

```rust
pub struct HummockStorage {
    memtable: Memtable,
    sst_levels: Vec<SstLevel>,
    cache: MultiTierCache,
    object_store: Box<dyn ObjectStore>,
}

pub struct MultiTierCache {
    memory: LruCache<Vec<u8>, Vec<u8>>,
    disk: DiskCache,
    prefetch: PrefetchManager,
}
```

**Key features**:
- Mem-table buffers writes in memory
- SSTables flushed to object storage (S3/GCS/Azure)
- Multi-tier caching: memory → local SSD → object storage
- 4MB block reads with sparse indexes
- Prefetching for sequential access patterns

#### 2.3 Async State Access

```rust
#[async_trait]
pub trait AsyncStateBackend: Send + Sync {
    async fn get_async(&self, key: &[u8]) -> StateFuture<Option<Vec<u8>>>;
    async fn put_async(&self, key: &[u8], value: &[u8]) -> StateFuture<()>;
    async fn delete_async(&self, key: &[u8]) -> StateFuture<()>;
}
```

---

### Phase 3: Barrier-Based Checkpointing

**Goal**: Improve checkpoint efficiency and recovery time.

**Duration**: 3-4 weeks

**Components**:

#### 3.1 Barrier Injection

```rust
pub struct BarrierCoordinator {
    checkpoint_interval: Duration,
    barriers: HashMap<BarrierId, BarrierState>,
}

pub struct Barrier {
    id: BarrierId,
    epoch: Epoch,
    state: BarrierState,
}
```

#### 3.2 Operator Snapshot

```rust
pub trait CheckpointableOperator {
    fn process_barrier(&mut self, barrier: &Barrier) -> Result<()>;
    fn snapshot(&mut self) -> Result<StateSnapshot>;
    fn restore(&mut self, snapshot: &StateSnapshot) -> Result<()>;
}
```

**Checkpoint flow**:
1. Coordinator injects barrier into stream
2. Operators snapshot when barriers received from all inputs
3. Async state upload to object storage
4. Epoch-based MVCC for consistent snapshots

---

### Phase 4: Operator Chaining & Optimization

**Goal**: Reduce inter-operator overhead for low-latency pipelines.

**Duration**: 2-3 weeks

**Components**:

#### 4.1 Chain Detection

```rust
pub struct ChainDetector {
    graph: DataflowGraph,
}

impl ChainDetector {
    pub fn detect_chains(&self) -> Vec<OperatorChain> {
        // Two operators chainable if:
        // 1. Connected by non-shuffle edge (forward)
        // 2. Same parallelism
        // 3. Previous operator has single output
        // 4. Next operator has single input
    }
}
```

#### 4.2 Chained Execution

```rust
pub struct ChainedOperator {
    operators: Vec<Box<dyn PerRecordOperator>>,
}

impl ChainedOperator {
    pub fn process_element(&mut self, record: StreamRecord) {
        let mut current = record;
        for op in &mut self.operators {
            current = op.process_element(current);
        }
    }
}
```

**Benefits**:
- Reduced task count (example: 10 operators → 3 chained)
- Eliminated queue overhead
- Reduced memory usage
- Zero-copy Arrow data passing

---

### Phase 5: Multi-Tier Caching

**Goal**: Achieve sub-100ms latency with disaggregated storage.

**Duration**: 3-4 weeks

**Components**:

#### 5.1 Cache Architecture

```rust
pub struct MultiTierCache {
    memory: MemoryCache,      // 40% of storage memory budget
    disk: DiskCache,          // Local SSD/EBS
    prefetch: PrefetchManager, // Query planner integration
}

impl MultiTierCache {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check memory cache
        // 2. Check disk cache
        // 3. Fetch from object storage (rare)
        // 4. Populate caches
    }
}
```

#### 5.2 Block-Level Storage

```rust
pub struct BlockReader {
    block_size: usize,  // 4MB default
    sparse_index: SparseIndex,
}

impl BlockReader {
    pub async fn read_block(&self, key_range: &KeyRange) -> Result<Block> {
        // Read 4MB block with sparse index
        // Minimize S3 API calls
    }
}
```

---

### Phase 6: Timezone-Aware Event Time

**Goal**: Add timezone support for event-time processing.

**Duration**: 1-2 weeks

**Implementation**:

```rust
pub struct Timestamp {
    pub millis: i64,
    pub tz: Option<TimeZoneRef>,
}

pub enum TimeZoneRef {
    Utc,
    Fixed(i32),           // offset seconds
    Named(String),        // "America/New_York"
}
```

- Add `tz` field to `StreamRecord`, `Watermark`, `Window`
- `WindowAssigner::assign()` converts to target timezone before bucketing
- `TtlStateBackend` uses event-time timezone for expiry
- Backward compatible: `None` = UTC (current behavior)

---

### Phase 7: Network Buffer Optimization

**Goal**: Add configurable output buffering for latency tuning.

**Duration**: 2 weeks

**Implementation**:

```rust
pub struct OutputBuffer {
    records: Vec<StreamRecord>,
    flush_timeout: Duration,
    last_flush: Instant,
}

impl OutputBuffer {
    pub fn push(&mut self, record: StreamRecord) {
        self.records.push(record);
        if self.should_flush() {
            self.flush();
        }
    }

    fn should_flush(&self) -> bool {
        self.records.len() >= self.batch_size
            || self.last_flush.elapsed() >= self.flush_timeout
    }
}
```

- Wire into `OutputCollector` for continuous mode
- Configurable per-operator via `setBufferTimeout()`

---

### Phase 8: SQL API Enhancements

**Goal**: Add streaming-specific SQL syntax.

**Duration**: 2-3 weeks

**Components**:

#### 8.1 Streaming SQL Syntax

```sql
-- Create streaming table (like RisingWave)
CREATE STREAMING TABLE user_aggregates AS
SELECT user_id, COUNT(*) as event_count
FROM events
GROUP BY user_id;

-- Processing mode hints
SELECT /*+ CONTINUOUS(buffer_timeout_ms=10) */ 
    user_id, COUNT(*) 
FROM events 
GROUP BY user_id;

-- Window functions
SELECT * FROM TABLE(
    TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '1 minute')
);

-- Sliding window
SELECT * FROM TABLE(
    HOP(TABLE events, DESCRIPTOR(event_time), INTERVAL '5 seconds', INTERVAL '1 minute')
);

-- Session window
SELECT * FROM TABLE(
    SESSION(TABLE events, DESCRIPTOR(event_time), INTERVAL '30 minutes')
);
```

#### 8.2 Streaming Functions

- Add watermark functions
- Add window functions (tumbling, sliding, session)
- Add streaming-specific aggregate functions

---

### Phase 9: Python API Enhancements

**Goal**: Enhance existing declarative pipeline API for streaming.

**Duration**: 3-4 weeks

**Components**:

#### 9.1 Processing Mode Configuration

```python
# Add processing mode configuration
pl.processing_mode("continuous", buffer_timeout_ms=10)
pl.processing_mode("micro_batch", drain_interval_ms=500)

# SQL hints
pl.view("revenue", "SELECT /*+ CONTINUOUS(buffer_timeout_ms=10) */ SUM(amount) FROM orders", materialized=True)
```

#### 9.2 Enhanced Window Functions

```python
# Sliding window
stream.key_by("user_id").sliding_window(
    slide_interval="5 seconds",
    window_interval="1 minute"
).agg(total_amount=agg.sum("amount"))

# Session window
stream.key_by("user_id").session_window(
    gap_interval="30 minutes"
).agg(total_amount=agg.sum("amount"))
```

#### 9.3 Watermark Support

```python
# Pipeline-level watermark
pl.watermark("event_time", "5 seconds")

# View-level watermark
pl.view("revenue", 
    "SELECT SUM(amount) FROM events", 
    watermark="event_time - INTERVAL '5 seconds'",
    materialized=True
)
```

#### 9.4 Streaming Sinks

```python
# Kafka sink
pl.sink_kafka("revenue", 
    bootstrap_servers="broker:9092",
    topic="output_events"
)

# Parquet sink with checkpointing
pl.sink_parquet("revenue",
    path="s3://bucket/output",
    checkpoint_location="s3://bucket/checkpoints",
    trigger="10 seconds"
)

# Iceberg sink (exactly-once)
pl.sink_iceberg("revenue",
    catalog="nessie",
    warehouse="s3://bucket/warehouse",
    table="events_aggregated"
)
```

#### 9.5 Python UDFs in Pipelines

```python
from krishiv import udf

@udf("double")
def calculate_risk(amount: float) -> float:
    return amount * 0.1

# Use in pipeline
pl.view("enriched", "SELECT *, calculate_risk(amount) AS risk FROM orders", materialized=True)
```

#### 9.6 Async Pipeline Execution

```python
# Async run
await pl.run_async("once")

# Async streaming
async for batch in pl.stream_async():
    process(batch)
```

---

## Part 4: Implementation Timeline

| Phase | Focus | Duration | Dependencies | Deliverables |
|-------|-------|----------|--------------|--------------|
| 1 | Dual-mode execution (mailbox + per-record) | 7-10 weeks | None | `krishiv-mailbox` crate, per-record operators, dual-mode execution |
| 2 | Disaggregated state storage | 6-8 weeks | Phase 1 | `krishiv-hummock` crate, async state access, multi-tier caching |
| 3 | Barrier-based checkpointing | 3-4 weeks | Phase 1 | Barrier-based checkpointing, epoch MVCC |
| 4 | Operator chaining | 2-3 weeks | Phase 1 | Operator chaining, chain detection |
| 5 | Multi-tier caching | 3-4 weeks | Phase 2 | Multi-tier caching optimization, block-level storage |
| 6 | Timezone-aware timestamps | 1-2 weeks | None | Timezone support for event-time processing |
| 7 | Network buffer optimization | 2 weeks | Phase 1 | Configurable output buffering |
| 8 | SQL API enhancements | 2-3 weeks | Phase 1 | Streaming SQL syntax, processing mode hints |
| 9 | Python API enhancements | 3-4 weeks | Phase 1 | Declarative pipeline enhancements, UDFs, sinks |

**Total Duration**: 29-42 weeks

---

## Part 5: User-Facing API Examples

### Rust API

```rust
// Dual-mode execution
let session = Session::new()
    .with_processing_mode(ProcessingMode::Continuous {
        buffer_timeout_ms: 10,  // 10ms latency target
    });

// Or for throughput-optimal
let session = Session::new()
    .with_processing_mode(ProcessingMode::MicroBatch {
        drain_interval_ms: 500,
    });

// Disaggregated storage
let session = Session::new()
    .with_state_storage(StateStorage::Hummock {
        object_store: ObjectStore::S3 {
            bucket: "my-bucket",
            region: "us-east-1",
        },
        cache_size: CacheSize::Automatic,
    });
```

### SQL API

```sql
-- Simple streaming aggregation
SELECT user_id, COUNT(*) as event_count
FROM events
GROUP BY user_id;

-- Windowed aggregation
SELECT 
    user_id,
    DATE_TRUNC('hour', event_time) as hour,
    COUNT(*) as event_count
FROM events
GROUP BY user_id, DATE_TRUNC('hour', event_time);

-- Streaming join
SELECT 
    e.user_id,
    e.amount,
    u.user_name
FROM events e
JOIN users u ON e.user_id = u.user_id;

-- Exactly-once sink
INSERT INTO iceberg_catalog.db.events_aggregated
SELECT user_id, COUNT(*) as event_count
FROM events
GROUP BY user_id;
```

### Python API

```python
import krishiv as ks
from krishiv import udf

s = ks.Session()
pl = s.pipeline("streaming_aggregation")

# Source with watermark
pl.source_kafka("events", 
    bootstrap_servers="broker:9092",
    topic="user_events"
)
pl.watermark("event_time", "5 seconds")

# Processing mode
pl.processing_mode("continuous", buffer_timeout_ms=10)

# Windowed aggregation
pl.view("hourly_aggregates", """
    SELECT 
        user_id,
        DATE_TRUNC('hour', event_time) as hour,
        COUNT(*) as event_count,
        SUM(amount) as total_amount
    FROM events
    GROUP BY user_id, DATE_TRUNC('hour', event_time)
""", materialized=True)

# Sink to Kafka
pl.sink_kafka("hourly_aggregates",
    bootstrap_servers="broker:9092",
    topic="aggregated_events"
)

# Run continuously
pl.run("continuous")
```

---

## Part 6: Validation Strategy

### Performance Benchmarks

1. **Latency**: End-to-end latency (source → sink) for continuous mode
   - **Target**: Sub-100ms end-to-end latency
   - **Baseline**: Current micro-batch drain cycle interval

2. **Throughput**: Events per second for micro-batch mode
   - **Target**: Maintain current throughput
   - **Baseline**: Current throughput metrics

3. **Recovery time**: Time to recover from failure
   - **Target**: Sub-second recovery time with disaggregated storage
   - **Baseline**: Current recovery time with local RocksDB

4. **State access latency**: P50, P99, P999 for state operations

### Correctness Tests

1. **Exactly-once semantics**: Verify no duplicates or dropped events
2. **Checkpoint consistency**: Verify snapshots are consistent
3. **State consistency**: Verify state matches expected values after recovery
4. **Mode switching**: Verify dynamic switching between micro-batch and continuous

### Integration Tests

1. **Kafka end-to-end**: Source → processing → sink with Kafka
2. **Windowed aggregations**: Tumbling, sliding, session windows
3. **Join operations**: Inner, left, outer joins with state
4. **Multi-tenant**: Multiple jobs sharing resources

---

## Part 7: Risk Mitigation

### Technical Risks

1. **Complexity**: Dual-mode execution adds complexity
   - Mitigation: Start with simple operators, iterate

2. **Performance**: Continuous mode may have lower throughput
   - Mitigation: Configurable buffer timeout, dynamic mode switching

3. **State consistency**: Disaggregated storage may have consistency issues
   - Mitigation: Epoch-based MVCC, barrier-based checkpoints

### Operational Risks

1. **Deployment**: New crates require updated CI/CD
   - Mitigation: Incremental rollout, feature flags

2. **Monitoring**: New metrics for continuous mode
   - Mitigation: Extend existing monitoring stack

3. **Documentation**: API changes require updated docs
   - Mitigation: Documentation as code, automated generation

---

## Part 8: Success Metrics

### Latency
- **Target**: Sub-100ms end-to-end latency for continuous mode
- **Baseline**: Current micro-batch drain cycle interval

### Throughput
- **Target**: Maintain current throughput for micro-batch mode
- **Baseline**: Current throughput metrics

### Recovery
- **Target**: Sub-second recovery time with disaggregated storage
- **Baseline**: Current recovery time with local RocksDB

### Resource Utilization
- **Target**: 30% reduction in memory usage with operator chaining
- **Baseline**: Current memory usage patterns

### API Completeness
- **Processing modes**: 100% coverage (continuous + micro-batch)
- **Window functions**: 100% coverage (tumbling, sliding, session)
- **Sinks**: 100% coverage (Kafka, Parquet, Iceberg)
- **UDFs**: Support for Python UDFs in pipelines

---

## Part 9: Files to Modify

| File | Change |
|------|--------|
| `krishiv-dataflow/src/continuous.rs` | Add `ContinuousExecutor` alongside `ContinuousWindowExecutor` |
| `krishiv-dataflow/src/process_fn.rs` | Add `PerRecordProcessFunction` trait |
| `krishiv-state/src/rocksdb_backend.rs` | Add async methods, prepare for disaggregated storage |
| `krishiv-state/src/ttl.rs` | Add timezone-aware TTL |
| `krishiv-plan/src/window.rs` | Add timezone to `WindowSpec` |
| `krishiv-executor/src/fragment/streaming.rs` | Add mailbox-based execution path |
| `krishiv-proto/src/job.rs` | Add `ProcessingMode` to job config |
| `Cargo.toml` | Add `krishiv-mailbox`, `krishiv-hummock` workspace members |

### New Crates to Create

| Crate | Purpose |
|-------|---------|
| `krishiv-mailbox` | Mailbox runtime for per-record processing |
| `krishiv-hummock` | Hummock LSM-tree backend for disaggregated storage |

---

## Part 10: Key Takeaways

1. **Per-record processing is the standard** for low-latency streaming (sub-100ms)
2. **Disaggregated storage** (S3-native) is becoming the norm for cloud-native scaling
3. **Barrier-based checkpointing** is the dominant pattern for exactly-once semantics
4. **Operator chaining** significantly reduces inter-operator overhead
5. **Multi-tier caching** is essential for achieving low latency with remote storage
6. **Dual-mode execution** (micro-batch + continuous) is the right approach for flexibility
7. **Krishiv already has a declarative pipeline API** - we need to enhance it, not replace it

---

## Part 11: Next Steps

1. **Review and approve** this comprehensive plan
2. **Create detailed design documents** for each phase
3. **Set up development environment** for new crates
4. **Implement Phase 1** (Mailbox runtime + per-record operators)
5. **Validate with benchmarks** and iterate
6. **Update documentation** and examples

---

## Part 12: References

- [Flink Architecture](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/architecture-overview/)
- [Arroyo Architecture](https://doc.arroyo.dev/architecture/)
- [RisingWave Architecture](https://docs.risingwave.com/get-started/architecture)
- [Arkflow Documentation](https://arkflow-rs.com/docs/intro)
- [Chandy-Lamport Snapshots](https://lamport.azurewebsites.net/pubs/chandy.pdf)
- [Dataflow Model](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/)
