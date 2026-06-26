# Design Note: OutputBufferPolicy

## Summary

Add a typed `OutputBufferPolicy` to control flush behavior for streaming output
buffers.

## Motivation

Output buffering is critical for balancing latency and throughput. The policy
controls when buffered data is flushed to downstream operators or sinks:

- By row count: flush when buffer reaches max_rows
- By byte size: flush when buffer reaches max_bytes
- By time: flush when flush_interval_ms has elapsed since last flush
- By any condition: flush when any of the above conditions is met

## Design

### New Types

```rust
/// Output buffer flush policy.
/// Controls when buffered data is flushed to downstream.
pub struct OutputBufferPolicy {
    /// Maximum rows before flush. None means no row limit.
    pub max_rows: Option<usize>,
    
    /// Maximum bytes before flush. None means no byte limit.
    pub max_bytes: Option<u64>,
    
    /// Maximum time (ms) before flush. None means no time limit.
    pub flush_interval_ms: Option<u64>,
    
    /// Flush when any condition is met (AND vs OR semantics).
    /// true = flush when ANY condition is met (default)
    /// false = flush when ALL conditions are met
    pub flush_on_any: bool,
}

impl OutputBufferPolicy {
    /// Create a policy that flushes on any condition (low-latency).
    pub fn low_latency() -> Self {
        Self {
            max_rows: Some(1000),
            max_bytes: Some(64 * 1024), // 64KB
            flush_interval_ms: Some(10),
            flush_on_any: true,
        }
    }
    
    /// Create a policy that flushes on any condition (throughput).
    pub fn throughput() -> Self {
        Self {
            max_rows: Some(10000),
            max_bytes: Some(1024 * 1024), // 1MB
            flush_interval_ms: Some(100),
            flush_on_any: true,
        }
    }
    
    /// Create a policy from a streaming execution profile.
    pub fn from_profile(profile: &StreamingExecutionProfile) -> Self {
        match profile {
            StreamingExecutionProfile::LowLatency { max_rows, max_bytes, flush_interval_ms } => {
                Self {
                    max_rows: Some(*max_rows),
                    max_bytes: Some(*max_bytes),
                    flush_interval_ms: Some(*flush_interval_ms),
                    flush_on_any: true,
                }
            }
            StreamingExecutionProfile::Throughput { max_rows, max_bytes, flush_interval_ms } => {
                Self {
                    max_rows: Some(*max_rows),
                    max_bytes: Some(*max_bytes),
                    flush_interval_ms: Some(*flush_interval_ms),
                    flush_on_any: true,
                }
            }
            StreamingExecutionProfile::Auto { .. } => {
                // Auto profile uses low-latency defaults, switching handled externally
                Self::low_latency()
            }
        }
    }
    
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

impl Default for OutputBufferPolicy {
    fn default() -> Self {
        Self::low_latency()
    }
}
```

### BacklogPolicy for Auto Mode

```rust
/// Backlog detection policy for Auto execution profile.
/// Controls when to switch between LowLatency and Throughput modes.
pub struct BacklogPolicy {
    /// Backlog threshold (bytes) to switch from LowLatency to Throughput.
    pub threshold_bytes: usize,
    
    /// Hysteresis factor (0.0-1.0) to prevent oscillation.
    /// Switch back to LowLatency when backlog drops below threshold * hysteresis.
    pub hysteresis: f64,
    
    /// Minimum time (ms) between profile switches.
    pub min_switch_interval_ms: u64,
}

impl BacklogPolicy {
    /// Create a default backlog policy.
    pub fn default() -> Self {
        Self {
            threshold_bytes: 1024 * 1024, // 1MB
            hysteresis: 0.8,
            min_switch_interval_ms: 1000,
        }
    }
    
    /// Determine if we should switch to throughput mode.
    pub fn should_switch_to_throughput(&self, current_backlog: u64, last_switch_ms: i64, now_ms: i64) -> bool {
        let backlog = current_backlog as usize;
        let time_since_switch = now_ms - last_switch_ms;
        
        backlog >= self.threshold_bytes && time_since_switch >= self.min_switch_interval_ms as i64
    }
    
    /// Determine if we should switch back to low-latency mode.
    pub fn should_switch_to_low_latency(&self, current_backlog: u64, last_switch_ms: i64, now_ms: i64) -> bool {
        let backlog = current_backlog as usize;
        let time_since_switch = now_ms - last_switch_ms;
        let low_latency_threshold = (self.threshold_bytes as f64 * self.hysteresis) as usize;
        
        backlog <= low_latency_threshold && time_since_switch >= self.min_switch_interval_ms as i64
    }
}
```

### Integration with StreamEnvelope

```rust
pub enum StreamEnvelope {
    Data(arrow::record_batch::RecordBatch),
    Watermark { epoch_ms: i64 },
    CheckpointBarrier { epoch: u64, alignment: CheckpointAlignment },
    Timer { key: Vec<u8>, fire_time_ms: i64, kind: TimerKind },
    EndOfInput,
    // New: flush control
    FlushRequest { reason: FlushReason },
}

pub enum FlushReason {
    MaxRows,
    MaxBytes,
    TimeInterval,
    BackpressureRelief,
    CheckpointBarrier,
}
```

## Files to Modify

| File | Change |
|------|--------|
| `crates/krishiv-dataflow/src/continuous.rs` | Add `OutputBufferPolicy`, `BacklogPolicy` |
| `crates/krishiv-dataflow/src/queue.rs` | Integrate flush policy into queue management |
| `crates/krishiv-executor/src/fragment/streaming.rs` | Use flush policy in executor loop |
| `crates/krishiv-api/src/pipeline/mod.rs` | Add builder methods for output buffer policy |

## Testing

- Unit tests for flush condition evaluation
- Integration tests for flush behavior under different policies
- Performance tests comparing low-latency vs throughput profiles
- Chaos tests for flush during checkpoint barriers
