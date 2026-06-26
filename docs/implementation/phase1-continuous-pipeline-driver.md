# Phase 1: True Continuous Pipeline Driver

## Goal

Replace the current "stream mode drains connector sources to memory" limit with a true continuous source loop that supports backpressure, cancellation, and checkpoint-controlled source offsets.

## Current State

The current pipeline driver in `krishiv-api/src/pipeline/driver.rs` has:

1. **`normalize_sources()`** (line 130-146): Drains all connector sources to in-memory batches
2. **`run_incremental()`** (line 51-126): Feeds all sources, then steps once
3. **`Ingest::Connector`** (line 117): Marked as `unreachable!()` after normalization

The streaming executor in `krishiv-executor/src/fragment/streaming.rs` uses:
- `ContinuousWindowExecutor` for windowed aggregations
- `STREAM_LOOP_PREFIX` for continuous window loop execution
- `STREAM_KAFKA_PARTITION_PREFIX` for Kafka partitions

## Design

### 1. Streaming Source Loop

Add a new streaming driver that reads connector batches incrementally:

```rust
// In krishiv-api/src/pipeline/driver.rs

/// Streaming pipeline driver that reads sources incrementally.
/// Replaces the drain-to-memory approach for unbounded sources.
pub async fn run_streaming(pipeline: Pipeline, config: StreamingConfig) -> Result<()> {
    let Pipeline {
        session,
        name,
        sources,
        views,
        sinks,
        expectations,
        ..
    } = pipeline;

    // 1. Register views (same as incremental path)
    let job = session.ivm(&name).await?;
    for v in &views {
        let out_schema = infer_view_schema(&schemas, &v.sql).await?;
        job.register_view(IncrementalViewSpec {
            name: v.name.clone(),
            body_sql: v.sql.clone(),
            output_schema: out_schema,
            is_materialized: v.materialized,
            is_recursive: false,
            lateness: vec![],
        })
        .await?;
    }

    // 2. Create streaming sources with backpressure
    let mut streaming_sources = Vec::new();
    for (sname, ingest) in sources {
        match ingest {
            Ingest::Connector(mut src) => {
                let streaming_src = StreamingSource {
                    name: sname,
                    source: src,
                    offset: None,
                    buffer: Vec::new(),
                    backpressure: BackpressureController::new(config.backpressure_config),
                };
                streaming_sources.push(streaming_src);
            }
            other => {
                // Memory/CDC sources still use incremental path
                feed_source(&job, &sname, other, &config.run_policy).await?;
            }
        }
    }

    // 3. Run continuous loop with backpressure
    let mut last_step = std::time::Instant::now();
    let mut running = true;

    while running {
        // Check for cancellation
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Received cancellation signal, stopping gracefully");
            break;
        }

        // Read from streaming sources with backpressure
        for src in &mut streaming_sources {
            if src.backpressure.is_available() {
                match src.source.read_batch_dyn().await? {
                    Some(batch) => {
                        // Feed batch to job
                        let delta = DeltaBatch::from_inserts(batch).map_err(rt)?;
                        job.feed(&src.name, &delta).await?;
                        
                        // Update offset if checkpoint source
                        if let Some(checkpoint_src) = src.source.as_any().downcast_ref::<dyn CheckpointSource>() {
                            src.offset = Some(checkpoint_src.encoded_checkpoint_offset()?);
                        }
                        
                        // Apply backpressure
                        src.backpressure.record_batch(batch.num_rows());
                    }
                    None if src.source.capabilities().is_bounded() => {
                        // Bounded source exhausted
                        src.backpressure.mark_exhausted();
                    }
                    None => {
                        // Unbounded source returned None (temporary)
                        // This is normal for sources waiting for data
                    }
                }
            }
        }

        // Step job based on run policy
        let elapsed = last_step.elapsed();
        let should_step = match config.run_policy {
            RunPolicy::Once => false,
            RunPolicy::OnChange => true,
            RunPolicy::EveryRows(n) => {
                streaming_sources.iter().any(|s| s.backpressure.rows_since_step >= n)
            }
            RunPolicy::EveryMs(ms) => elapsed.as_millis() >= ms as u128,
        };

        if should_step {
            job.step().await?;
            last_step = std::time::Instant::now();
            
            // Write snapshots to sinks
            write_snapshots(&job, sinks.clone(), &expectations).await?;
            
            // Reset row counters
            for src in &mut streaming_sources {
                src.backpressure.rows_since_step = 0;
            }
        }

        // Check if all bounded sources are exhausted
        running = streaming_sources.iter().any(|s| 
            !s.source.capabilities().is_bounded() || !s.backpressure.is_exhausted()
        );
    }

    // Final flush
    job.step().await?;
    write_snapshots(&job, sinks, &expectations).await
}
```

### 2. Backpressure Controller

```rust
// In krishiv-api/src/pipeline/driver.rs

/// Controls backpressure for streaming sources.
pub struct BackpressureController {
    /// Maximum bytes in flight before applying backpressure.
    max_bytes_in_flight: usize,
    
    /// Current bytes in flight.
    current_bytes: usize,
    
    /// Maximum rows before applying backpressure.
    max_rows_in_flight: usize,
    
    /// Current rows in flight.
    current_rows: usize,
    
    /// Rows since last step (for EveryRows policy).
    pub rows_since_step: usize,
    
    /// Whether source is exhausted (bounded sources only).
    exhausted: bool,
}

impl BackpressureController {
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            max_bytes_in_flight: config.max_bytes_in_flight,
            current_bytes: 0,
            max_rows_in_flight: config.max_rows_in_flight,
            current_rows: 0,
            rows_since_step: 0,
            exhausted: false,
        }
    }
    
    /// Check if source can produce more data.
    pub fn is_available(&self) -> bool {
        !self.exhausted 
            && self.current_bytes < self.max_bytes_in_flight
            && self.current_rows < self.max_rows_in_flight
    }
    
    /// Record that a batch was produced.
    pub fn record_batch(&mut self, num_rows: usize) {
        self.current_rows += num_rows;
        self.rows_since_step += num_rows;
        // Approximate bytes (would need actual batch size in production)
        self.current_bytes += num_rows * 100; // rough estimate
    }
    
    /// Mark source as exhausted.
    pub fn mark_exhausted(&mut self) {
        self.exhausted = true;
    }
    
    /// Check if source is exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }
    
    /// Reset counters after step.
    pub fn reset_after_step(&mut self) {
        self.current_bytes = 0;
        self.current_rows = 0;
    }
}

/// Configuration for backpressure.
pub struct BackpressureConfig {
    pub max_bytes_in_flight: usize,
    pub max_rows_in_flight: usize,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_bytes_in_flight: 1024 * 1024 * 10, // 10MB
            max_rows_in_flight: 10000,
        }
    }
}
```

### 3. Streaming Source Wrapper

```rust
// In krishiv-api/src/pipeline/driver.rs

/// A streaming source that wraps a connector source.
pub struct StreamingSource {
    /// Source name (for feeding to job).
    pub name: String,
    
    /// The underlying connector source.
    pub source: Box<dyn DynSource>,
    
    /// Current checkpoint offset (if checkpoint source).
    pub offset: Option<Vec<u8>>,
    
    /// Buffer for read-ahead (optional).
    pub buffer: Vec<RecordBatch>,
    
    /// Backpressure controller.
    pub backpressure: BackpressureController,
}
```

### 4. Checkpoint Integration

```rust
// In krishiv-api/src/pipeline/driver.rs

/// Save checkpoint state for streaming sources.
pub async fn save_streaming_checkpoint(
    sources: &[StreamingSource],
    checkpoint_id: &str,
) -> Result<StreamingCheckpoint> {
    let mut source_offsets = HashMap::new();
    
    for src in sources {
        if let Some(offset) = &src.offset {
            source_offsets.insert(src.name.clone(), offset.clone());
        }
    }
    
    Ok(StreamingCheckpoint {
        checkpoint_id: checkpoint_id.to_string(),
        source_offsets,
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
    })
}

/// Restore streaming sources from checkpoint.
pub async fn restore_streaming_checkpoint(
    sources: &mut [StreamingSource],
    checkpoint: &StreamingCheckpoint,
) -> Result<()> {
    for src in sources {
        if let Some(offset) = checkpoint.source_offsets.get(&src.name) {
            if let Some(checkpoint_src) = src.source.as_any().downcast_mut::<dyn CheckpointSource>() {
                checkpoint_src.restore_encoded_offset(offset)?;
            }
        }
    }
    Ok(())
}
```

## Files to Modify

| File | Change |
|------|--------|
| `crates/krishiv-api/src/pipeline/driver.rs` | Add `run_streaming()`, `BackpressureController`, `StreamingSource`, checkpoint integration |
| `crates/krishiv-api/src/pipeline/mod.rs` | Add `StreamingConfig`, `BackpressureConfig` |
| `crates/krishiv-api/src/pipeline/source.rs` | Add `Ingest::StreamingConnector` variant |
| `crates/krishiv-connectors/src/source.rs` | Add `DynSource::as_any()` for downcasting |
| `crates/krishiv-executor/src/fragment/streaming.rs` | Integrate with streaming driver |

## Acceptance Tests

### 1. Unbounded Source Test

```rust
#[tokio::test]
async fn test_unbounded_source_no_materialization() {
    let session = Session::new();
    let source = UnboundedTestSource::new(vec![batch1, batch2, batch3]);
    
    let mut pipeline = session.pipeline("test");
    pipeline.source_connector("events", Box::new(source));
    pipeline.view("count", "SELECT COUNT(*) AS n FROM events", true);
    
    let result = pipeline.run(RunPolicy::EveryRows(10)).await;
    
    // Verify source was not materialized
    assert!(source.was_read_incrementally());
    assert_eq!(source.read_count(), 3);
}
```

### 2. Backpressure Test

```rust
#[tokio::test]
async fn test_backpressure_limits_concurrent_batches() {
    let session = Session::new();
    let source = FastSource::new(1000); // Produces 1000 batches quickly
    
    let config = StreamingConfig {
        backpressure: BackpressureConfig {
            max_bytes_in_flight: 1024 * 1024, // 1MB
            max_rows_in_flight: 100,
        },
        ..Default::default()
    };
    
    let mut pipeline = session.pipeline("test");
    pipeline.source_connector("events", Box::new(source));
    pipeline.view("passthrough", "SELECT * FROM events", true);
    
    // Run for limited time
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        pipeline.run_streaming(config)
    ).await;
    
    // Verify backpressure was applied
    assert!(source.max_concurrent_batches() <= 100);
}
```

### 3. Cancellation Test

```rust
#[tokio::test]
async fn test_cancellation_stops_cleanly() {
    let session = Session::new();
    let source = UnboundedTestSource::new(vec![]);
    
    let mut pipeline = session.pipeline("test");
    pipeline.source_connector("events", Box::new(source));
    pipeline.view("passthrough", "SELECT * FROM events", true);
    
    // Start streaming in background
    let handle = tokio::spawn(async move {
        pipeline.run_streaming(StreamingConfig::default()).await
    });
    
    // Send cancellation signal
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    // In real code, would send ctrl_c signal
    
    // Wait for handle
    let result = handle.await.unwrap();
    
    // Verify clean shutdown
    assert!(result.is_ok());
    assert!(source.was_stopped_cleanly());
}
```

### 4. Source Offset Test

```rust
#[tokio::test]
async fn test_source_offset_not_advanced_before_checkpoint() {
    let session = Session::new();
    let source = CheckpointableTestSource::new(vec![batch1, batch2]);
    
    let mut pipeline = session.pipeline("test");
    pipeline.source_connector("events", Box::new(source));
    pipeline.view("passthrough", "SELECT * FROM events", true);
    
    // Run streaming with checkpoint enabled
    let config = StreamingConfig {
        checkpoint_interval_ms: 100,
        ..Default::default()
    };
    
    pipeline.run_streaming(config).await;
    
    // Verify offset was not advanced before checkpoint commit
    assert!(!source.offset_was_advanced_prematurely());
}
```

## Implementation Steps

1. **Add `DynSource::as_any()` method** for downcasting to `CheckpointSource`
2. **Create `BackpressureController` struct** with configurable limits
3. **Create `StreamingSource` wrapper** with backpressure and offset tracking
4. **Add `run_streaming()` function** to pipeline driver
5. **Add `StreamingConfig` struct** with backpressure and checkpoint configuration
6. **Integrate checkpoint save/restore** with streaming sources
7. **Add acceptance tests** for unbounded sources, backpressure, cancellation, and offsets
8. **Update `Pipeline::run()`** to use streaming driver for unbounded sources

## Backward Compatibility

- Existing `Pipeline::run()` behavior unchanged for bounded sources
- New `run_streaming()` is opt-in for unbounded sources
- Backpressure defaults are conservative (10MB, 10K rows)
- Checkpoint integration is optional

## Validation

```bash
# Run Phase 1 tests
cargo test -p krishiv-api --test streaming_driver

# Verify backpressure works
cargo test -p krishiv-api --test backpressure

# Verify checkpoint integration
cargo test -p krishiv-api --test checkpoint
```
