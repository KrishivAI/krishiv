# Phase 7: Certification, Observability, and Benchmarks

## Goal

Make performance and delivery claims measurable through metrics, chaos tests, and performance baselines.

## Design

### 1. Metrics

```rust
// In krishiv-metrics/src/streaming.rs

/// Streaming metrics.
pub struct StreamingMetrics {
    /// Source read latency (histogram).
    pub source_read_latency: Histogram,
    
    /// Output buffer flush reason (counter).
    pub output_buffer_flush_reason: Counter,
    
    /// Checkpoint alignment time (histogram).
    pub checkpoint_alignment_time: Histogram,
    
    /// Unaligned in-flight bytes (gauge).
    pub unaligned_in_flight_bytes: Gauge,
    
    /// Checkpoint upload time (histogram).
    pub checkpoint_upload_time: Histogram,
    
    /// Restore time (histogram).
    pub restore_time: Histogram,
    
    /// State cache hit/miss (counter).
    pub state_cache_hits: Counter,
    pub state_cache_misses: Counter,
    
    /// Object-store request count (counter).
    pub object_store_requests: Counter,
    
    /// Sink prepare/commit/abort duration (histogram).
    pub sink_prepare_duration: Histogram,
    pub sink_commit_duration: Histogram,
    pub sink_abort_duration: Histogram,
    
    /// Backpressure duration (histogram).
    pub backpressure_duration: Histogram,
}
```

### 2. Chaos Tests

```rust
// In krishiv-chaos/src/streaming.rs

/// Chaos test scenarios for streaming.
pub struct StreamingChaosTests {
    executor: ExecutorHandle,
    coordinator: CoordinatorHandle,
}

impl StreamingChaosTests {
    /// Test executor kill during checkpoint.
    pub async fn test_executor_kill_during_checkpoint(&self) -> ChaosTestResult {
        // 1. Start streaming job
        // 2. Wait for checkpoint to start
        // 3. Kill executor
        // 4. Verify recovery
        // 5. Verify no duplicates
    }
    
    /// Test coordinator failover during checkpoint.
    pub async fn test_coordinator_failover_during_checkpoint(&self) -> ChaosTestResult {
        // 1. Start streaming job
        // 2. Wait for checkpoint to start
        // 3. Failover coordinator
        // 4. Verify new coordinator takes over
        // 5. Verify checkpoint completes
    }
    
    /// Test sink prepare success followed by coordinator abort.
    pub async fn test_sink_prepare_then_coordinator_abort(&self) -> ChaosTestResult {
        // 1. Start streaming job with two-phase sink
        // 2. Wait for sink prepare
        // 3. Abort coordinator checkpoint
        // 4. Verify sink transaction is aborted
        // 5. Verify no partial writes
    }
    
    /// Test source offset restore after executor loss.
    pub async fn test_source_offset_restore(&self) -> ChaosTestResult {
        // 1. Start streaming job with checkpoint source
        // 2. Process some data
        // 3. Kill executor
        // 4. Restore from checkpoint
        // 5. Verify source offset is restored correctly
    }
    
    /// Test object-store transient failure during checkpoint upload.
    pub async fn test_object_store_transient_failure(&self) -> ChaosTestResult {
        // 1. Start streaming job with object-store checkpoint
        // 2. Inject transient object-store failures
        // 3. Verify checkpoint retries
        // 4. Verify eventual success
    }
}
```

### 3. Performance Baselines

```rust
// In krishiv-bench/src/streaming.rs

/// Performance baselines for streaming.
pub struct StreamingBaselines {
    /// Current drain-cycle latency.
    pub drain_cycle_latency: LatencyBaseline,
    
    /// Low-latency buffered path latency.
    pub low_latency_latency: LatencyBaseline,
    
    /// Throughput path rows/sec.
    pub throughput_rows_sec: ThroughputBaseline,
    
    /// Restore time by state size.
    pub restore_time: RestoreBaseline,
    
    /// Memory usage with and without fusion.
    pub memory_usage: MemoryBaseline,
}

/// Latency baseline.
pub struct LatencyBaseline {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

/// Throughput baseline.
pub struct ThroughputBaseline {
    pub rows_per_sec: f64,
    pub batches_per_sec: f64,
}

/// Restore baseline.
pub struct RestoreBaseline {
    pub restore_time_by_state_size: Vec<(u64, f64)>, // (state_size_bytes, restore_time_ms)
}

/// Memory baseline.
pub struct MemoryBaseline {
    pub with_fusion_mb: f64,
    pub without_fusion_mb: f64,
}
```

### 4. Deployment Mode Smoke Suites

```rust
// In krishiv-chaos/src/deployment_modes.rs

/// Deployment mode smoke tests.
pub struct DeploymentModeTests {
    /// Embedded mode tests.
    pub embedded: EmbeddedTests,
    
    /// Single-node durable mode tests.
    pub single_node: SingleNodeTests,
    
    /// Distributed durable mode tests.
    pub distributed: DistributedTests,
}

/// Embedded mode tests.
pub struct EmbeddedTests;

impl EmbeddedTests {
    pub async fn test_basic_streaming(&self) -> TestResult {
        // Test basic streaming in embedded mode
    }
    
    pub async fn test_cancellation(&self) -> TestResult {
        // Test cancellation in embedded mode
    }
    
    pub async fn test_no_durable_exactly_once(&self) -> TestResult {
        // Verify no durable exactly-once claim without explicit config
    }
}

/// Single-node durable mode tests.
pub struct SingleNodeTests;

impl SingleNodeTests {
    pub async fn test_process_restart_recovery(&self) -> TestResult {
        // Test recovery after process restart
    }
    
    pub async fn test_source_offset_persistence(&self) -> TestResult {
        // Test source offset persistence
    }
    
    pub async fn test_operator_snapshot_persistence(&self) -> TestResult {
        // Test operator snapshot persistence
    }
}

/// Distributed durable mode tests.
pub struct DistributedTests;

impl DistributedTests {
    pub async fn test_executor_replacement(&self) -> TestResult {
        // Test executor replacement without second active owner
    }
    
    pub async fn test_coordinator_fencing(&self) -> TestResult {
        // Test coordinator fencing
    }
    
    pub async fn test_object_store_checkpoint(&self) -> TestResult {
        // Test object-store checkpoint
    }
}
```

## Files to Modify

| File | Change |
|------|--------|
| `crates/krishiv-metrics/src/streaming.rs` | New file: Streaming metrics |
| `crates/krishiv-chaos/src/streaming.rs` | New file: Streaming chaos tests |
| `crates/krishiv-chaos/src/deployment_modes.rs` | New file: Deployment mode tests |
| `crates/krishiv-bench/src/streaming.rs` | New file: Streaming performance baselines |

## Acceptance Criteria

1. Published targets are backed by benchmark output
2. Connector maturity labels are not upgraded without passing the failure matrix
3. CI has narrow smoke tests and an opt-in longer certification suite
