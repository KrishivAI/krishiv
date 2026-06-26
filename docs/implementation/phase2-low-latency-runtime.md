# Phase 2: Low-Latency Batch-Preserving Runtime

## Goal

Reduce latency without abandoning Arrow batches by adding `StreamEnvelope`, output buffering, barrier prioritization, and operator fusion.

## Design

### 1. StreamEnvelope

```rust
// In krishiv-dataflow/src/envelope.rs

/// Typed envelope for streaming data and control messages.
pub enum StreamEnvelope {
    /// Data batch with optional metadata.
    Data {
        batch: RecordBatch,
        /// Source that produced this batch (for checkpoint tracking).
        source_id: Option<String>,
        /// Timestamp when batch was produced (for latency tracking).
        produced_at_ms: i64,
    },
    
    /// Watermark indicating event time progress.
    Watermark {
        /// Watermark value in epoch milliseconds.
        epoch_ms: i64,
        /// Source that produced this watermark.
        source_id: String,
    },
    
    /// Checkpoint barrier for coordinated snapshots.
    CheckpointBarrier {
        /// Epoch number for this checkpoint.
        epoch: u64,
        /// Alignment mode (aligned or unaligned).
        alignment: CheckpointAlignment,
    },
    
    /// Timer fire for stateful operators.
    Timer {
        /// Key that owns this timer.
        key: Vec<u8>,
        /// When the timer should fire (epoch ms).
        fire_time_ms: i64,
        /// Kind of timer (processing or event time).
        kind: TimerKind,
    },
    
    /// End of input signal (for bounded sources).
    EndOfInput,
}
```

### 2. Output Buffer Policy

```rust
// In krishiv-dataflow/src/buffer.rs

/// Controls when buffered data is flushed.
pub struct OutputBufferPolicy {
    /// Maximum rows before flush.
    pub max_rows: Option<usize>,
    
    /// Maximum bytes before flush.
    pub max_bytes: Option<u64>,
    
    /// Maximum time (ms) before flush.
    pub flush_interval_ms: Option<u64>,
    
    /// Flush on any condition (true) or all conditions (false).
    pub flush_on_any: bool,
}

impl OutputBufferPolicy {
    /// Check if buffer should be flushed.
    pub fn should_flush(&self, current_rows: usize, current_bytes: u64, elapsed_ms: u64) -> bool {
        let row_limit = self.max_rows.map_or(false, |limit| current_rows >= limit);
        let byte_limit = self.max_bytes.map_or(false, |limit| current_bytes >= limit);
        let time_limit = self.flush_interval_ms.map_or(false, |limit| elapsed_ms >= limit);
        
        if self.flush_on_any {
            row_limit || byte_limit || time_limit
        } else {
            row_limit && byte_limit && time_limit
        }
    }
}
```

### 3. Streaming Execution Profile

```rust
// In krishiv-dataflow/src/profile.rs

/// Runtime execution profile for streaming jobs.
pub enum StreamingExecutionProfile {
    /// Optimize for low latency (p99 < 100ms).
    LowLatency {
        max_rows: usize,
        max_bytes: usize,
        flush_interval_ms: u64,
    },
    
    /// Optimize for throughput (rows/sec).
    Throughput {
        max_rows: usize,
        max_bytes: usize,
        flush_interval_ms: u64,
    },
    
    /// Auto-switch based on backlog.
    Auto {
        backlog_threshold_bytes: usize,
        hysteresis: f64,
        min_switch_interval_ms: u64,
    },
}
```

### 4. Operator Fusion

```rust
// In krishiv-dataflow/src/fusion.rs

/// Detect chainable operators for fusion.
pub struct FusionDetector {
    graph: DataflowGraph,
}

impl FusionDetector {
    /// Detect operators that can be fused.
    pub fn detect_fusions(&self) -> Vec<OperatorFusion> {
        let mut fusions = Vec::new();
        
        for node in self.graph.nodes() {
            // Check if node can be fused with successor
            if let Some(successor) = self.graph.successor(node) {
                if self.can_fuse(node, successor) {
                    fusions.push(OperatorFusion {
                        source: node.clone(),
                        sink: successor.clone(),
                    });
                }
            }
        }
        
        fusions
    }
    
    /// Check if two operators can be fused.
    fn can_fuse(&self, source: &NodeId, sink: &NodeId) -> bool {
        // Fuse if:
        // 1. Same parallelism
        // 2. Forward edge (no shuffle)
        // 3. Source has single output
        // 4. Sink has single input
        // 5. Both are stateless or have compatible state
        
        self.same_parallelism(source, sink)
            && self.is_forward_edge(source, sink)
            && self.has_single_output(source)
            && self.has_single_input(sink)
            && self.state_compatible(source, sink)
    }
}

/// A fusion of two operators.
pub struct OperatorFusion {
    pub source: NodeId,
    pub sink: NodeId,
}
```

## Files to Modify

| File | Change |
|------|--------|
| `crates/krishiv-dataflow/src/envelope.rs` | New file: `StreamEnvelope` enum |
| `crates/krishiv-dataflow/src/buffer.rs` | New file: `OutputBufferPolicy` |
| `crates/krishiv-dataflow/src/profile.rs` | New file: `StreamingExecutionProfile` |
| `crates/krishiv-dataflow/src/fusion.rs` | New file: `FusionDetector` |
| `crates/krishiv-dataflow/src/queue.rs` | Integrate stream envelopes and checkpoint alignment |
| `crates/krishiv-dataflow/src/continuous.rs` | Adapt to envelope-driven runtime |
| `crates/krishiv-executor/src/fragment/streaming.rs` | Use stream envelopes in executor loop |

## Acceptance Tests

1. Low-latency mode flushes by timeout even when row threshold is not reached
2. Throughput mode preserves current bounded throughput baseline
3. Barriers overtake or align according to configured checkpoint alignment
4. Fused filter/project pipeline produces byte-identical batches compared with unfused execution
