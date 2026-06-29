# Phase 6: Public Rust and Python API

## Goal

Expose the runtime model without creating a second pipeline API by adding typed execution profiles and output buffer configuration.

## Design

### 1. Rust API

```rust
// In krishiv-api/src/pipeline/mod.rs

impl PipelineBuilder {
    /// Set the streaming execution profile.
    pub fn execution_profile(mut self, profile: StreamingExecutionProfile) -> Self {
        self.execution_profile = Some(profile);
        self
    }
    
    /// Set the output buffer policy.
    pub fn output_buffer(mut self, policy: OutputBufferPolicy) -> Self {
        self.output_buffer = Some(policy);
        self
    }
    
    /// Set the backpressure policy.
    pub fn backpressure(mut self, policy: BackpressurePolicy) -> Self {
        self.backpressure = Some(policy);
        self
    }
    
    /// Run the pipeline with streaming configuration.
    pub async fn run_streaming(self, config: StreamingConfig) -> Result<()> {
        // ...
    }
}

/// Streaming configuration.
pub struct StreamingConfig {
    pub run_policy: RunPolicy,
    pub execution_profile: StreamingExecutionProfile,
    pub output_buffer: OutputBufferPolicy,
    pub backpressure: BackpressurePolicy,
    pub checkpoint: Option<CheckpointConfig>,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            run_policy: RunPolicy::EveryMs(100),
            execution_profile: StreamingExecutionProfile::default(),
            output_buffer: OutputBufferPolicy::default(),
            backpressure: BackpressurePolicy::default(),
            checkpoint: None,
        }
    }
}
```

### 2. Python API

```python
# In krishiv-python/src/pipeline_api.rs

impl Pipeline {
    /// Set the execution profile.
    pub fn execution_profile(&mut self, profile: &str, **kwargs) -> Result<()> {
        match profile {
            "low_latency" => {
                self.execution_profile = StreamingExecutionProfile::LowLatency {
                    max_rows: kwargs.get("max_rows").unwrap_or(1000),
                    max_bytes: kwargs.get("max_bytes").unwrap_or(64 * 1024),
                    flush_interval_ms: kwargs.get("flush_interval_ms").unwrap_or(10),
                };
            }
            "throughput" => {
                self.execution_profile = StreamingExecutionProfile::Throughput {
                    max_rows: kwargs.get("max_rows").unwrap_or(10000),
                    max_bytes: kwargs.get("max_bytes").unwrap_or(1024 * 1024),
                    flush_interval_ms: kwargs.get("flush_interval_ms").unwrap_or(100),
                };
            }
            "auto" => {
                self.execution_profile = StreamingExecutionProfile::Auto {
                    backlog_threshold_bytes: kwargs.get("backlog_threshold_bytes").unwrap_or(1024 * 1024),
                    hysteresis: kwargs.get("hysteresis").unwrap_or(0.8),
                    min_switch_interval_ms: kwargs.get("min_switch_interval_ms").unwrap_or(1000),
                };
            }
            _ => return Err(Error::InvalidProfile),
        }
        Ok(())
    }
    
    /// Set the output buffer policy.
    pub fn output_buffer(&mut self, **kwargs) -> Result<()> {
        self.output_buffer = OutputBufferPolicy {
            max_rows: kwargs.get("max_rows"),
            max_bytes: kwargs.get("max_bytes"),
            flush_interval_ms: kwargs.get("flush_interval_ms"),
            flush_on_any: kwargs.get("flush_on_any").unwrap_or(true),
        };
        Ok(())
    }
    
    /// Run the pipeline with streaming configuration.
    pub async fn run_streaming(&mut self, config: Option<StreamingConfig>) -> Result<()> {
        let config = config.unwrap_or_default();
        // ...
    }
}
```

### 3. Python Usage Examples

```python
import krishiv as ks

session = ks.Session()
pl = session.pipeline("my_stream")

# Low-latency mode
pl.execution_profile("low_latency", max_rows=1000, flush_interval_ms=10)
pl.output_buffer(max_rows=500, max_bytes=32*1024, flush_interval_ms=5)
pl.run_streaming()

# Throughput mode
pl.execution_profile("throughput", max_rows=10000, flush_interval_ms=100)
pl.run_streaming()

# Auto mode with hysteresis
pl.execution_profile("auto", 
    backlog_threshold_bytes=1024*1024,
    hysteresis=0.8,
    min_switch_interval_ms=1000
)
pl.run_streaming()

# With checkpoint
pl.execution_profile("low_latency")
pl.checkpoint(interval_ms=1000, storage="s3://bucket/checkpoints")
pl.run_streaming()
```

## Files to Modify

| File | Change |
|------|--------|
| `crates/krishiv-api/src/pipeline/mod.rs` | Add builder methods for execution profile, output buffer, backpressure |
| `crates/krishiv-api/src/streaming_builder.rs` | New file: Streaming builder implementation |
| `crates/krishiv-python/src/pipeline_api.rs` | Add Python bindings for execution profile, output buffer |
| `crates/krishiv-python/src/session.rs` | Add Python bindings for streaming configuration |

## Acceptance Tests

1. Rust and Python APIs produce the same typed profile config
2. Continuous run rejects bounded-only sources unless explicitly run as batch
3. Feature-gated connector errors name the missing Cargo feature and maturin develop command
