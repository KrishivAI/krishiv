# Design Note: StreamingExecutionProfile

## Summary

Add a typed `StreamingExecutionProfile` to separate runtime execution behavior
from the API coalescing knob (`RunPolicy`).

## Motivation

Currently `RunPolicy` controls how often a pipeline advances after input is fed:
`Once`, `OnChange`, `EveryRows`, `EveryMs`. This is an API coalescing concern,
not a runtime execution concern. We need a separate knob for:

- Low-latency mode: small batches, frequent flushes, optimized for p99 latency
- Throughput mode: larger batches, less frequent flushes, optimized for rows/sec
- Auto mode: dynamic switching based on backlog detection with hysteresis

## Design

### New Types

```rust
/// Runtime execution profile for streaming jobs.
/// Controls batch sizing, output buffering, and flush behavior.
pub enum StreamingExecutionProfile {
    /// Optimize for low latency (p99 < 100ms target).
    /// Small batches, frequent flushes, bounded output buffers.
    LowLatency {
        /// Maximum rows per output buffer before flush.
        max_rows: usize,
        /// Maximum bytes per output buffer before flush.
        max_bytes: usize,
        /// Maximum time (ms) before flushing partial buffer.
        flush_interval_ms: u64,
    },
    /// Optimize for throughput (rows/sec).
    /// Larger batches, less frequent flushes.
    Throughput {
        /// Maximum rows per output buffer before flush.
        max_rows: usize,
        /// Maximum bytes per output buffer before flush.
        max_bytes: usize,
        /// Maximum time (ms) before flushing partial buffer.
        flush_interval_ms: u64,
    },
    /// Automatically switch between LowLatency and Throughput based on backlog.
    Auto {
        /// Backlog threshold (bytes) to switch from LowLatency to Throughput.
        backlog_threshold_bytes: usize,
        /// Hysteresis factor (0.0-1.0) to prevent oscillation.
        /// Switch back to LowLatency when backlog drops below threshold * hysteresis.
        hysteresis: f64,
        /// Minimum time (ms) between profile switches.
        min_switch_interval_ms: u64,
    },
}

impl Default for StreamingExecutionProfile {
    fn default() -> Self {
        Self::LowLatency {
            max_rows: 1000,
            max_bytes: 64 * 1024, // 64KB
            flush_interval_ms: 10,
        }
    }
}
```

### Relationship to RunPolicy

```
User API:
  RunPolicy::Once           -> run pipeline once
  RunPolicy::OnChange       -> run on each CDC change
  RunPolicy::EveryRows(n)   -> run after n rows
  RunPolicy::EveryMs(ms)    -> run after ms milliseconds

Runtime:
  StreamingExecutionProfile -> controls batch sizing and output buffering
  OutputBufferPolicy        -> controls flush behavior (derived from profile)
  BacklogPolicy             -> controls auto-switching (for Auto profile)
```

### Storage in Job Metadata

```rust
// In krishiv-proto/src/job.rs
pub struct StreamingJobConfig {
    pub run_policy: RunPolicy,
    pub execution_profile: StreamingExecutionProfile,
    pub output_buffer: OutputBufferPolicy,
    pub backlog: Option<BacklogPolicy>,
}
```

### Python API

```python
pl = s.pipeline("my_stream")

# Low-latency mode
pl.execution_profile("low_latency", max_rows=1000, flush_interval_ms=10)

# Throughput mode
pl.execution_profile("throughput", max_rows=10000, flush_interval_ms=100)

# Auto mode with hysteresis
pl.execution_profile("auto", 
    backlog_threshold_bytes=1024*1024,  # 1MB
    hysteresis=0.8,
    min_switch_interval_ms=1000
)

# Output buffer overrides (optional)
pl.output_buffer(max_rows=500, max_bytes=32*1024, flush_interval_ms=5)
```

## Files to Modify

| File | Change |
|------|--------|
| `crates/krishiv-dataflow/src/continuous.rs` | Add `StreamingExecutionProfile` enum and `OutputBufferPolicy` |
| `crates/krishiv-proto/src/job.rs` | Add `StreamingJobConfig` with profile fields |
| `crates/krishiv-api/src/pipeline/mod.rs` | Add builder methods for execution profile |
| `crates/krishiv-python/src/pipeline_api.rs` | Add Python bindings for execution profile |

## Backward Compatibility

- Default profile is `LowLatency` with conservative defaults
- Existing `RunPolicy` usage is unchanged
- New fields are optional in job metadata with defaults

## Testing

- Unit tests for profile derivation (Auto -> LowLatency/Throughput switching)
- Integration tests for output buffer flush behavior
- Benchmark tests comparing LowLatency vs Throughput profiles
