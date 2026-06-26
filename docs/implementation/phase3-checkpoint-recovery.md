# Phase 3: Checkpoint and Recovery Integration

## Goal

Make barrier propagation, source offsets, operator snapshots, unaligned buffers, and sink prepare/commit one coherent protocol.

## Design

### 1. Extended Checkpoint Metadata

```rust
// In krishiv-state/src/checkpoint/metadata.rs

/// Extended checkpoint metadata for streaming jobs.
pub struct StreamingCheckpointMetadata {
    /// Base checkpoint metadata.
    pub base: CheckpointMetadata,
    
    /// Source offsets per source partition.
    pub source_offsets: HashMap<(SourceId, PartitionId), Vec<u8>>,
    
    /// Operator snapshot references.
    pub operator_snapshots: HashMap<(OperatorId, KeyGroup), SnapshotRef>,
    
    /// In-flight unaligned buffers.
    pub unaligned_buffers: Vec<UnalignedBufferRef>,
    
    /// Sink transaction references.
    pub sink_transactions: HashMap<SinkId, SinkTransactionRef>,
    
    /// Execution profile used for this epoch.
    pub execution_profile: StreamingExecutionProfile,
    
    /// Output buffer policy used for this epoch.
    pub output_buffer: OutputBufferPolicy,
}
```

### 2. Checkpoint Protocol

```
1. Coordinator injects CheckpointBarrier(epoch, alignment_mode)
2. Operators receive barrier and:
   - Aligned: Wait for barrier on all inputs, then snapshot
   - Unaligned: Buffer in-flight data, snapshot immediately
3. Executors collect:
   - Operator snapshots
   - In-flight buffer references
   - Source offsets
   - Sink transaction references
4. Executors upload all to checkpoint storage
5. Executors report metadata to coordinator
6. Coordinator commits epoch
7. Sink commits transactions
```

### 3. Restore Protocol

```
1. Coordinator selects checkpoint to restore
2. Coordinator verifies all required data is present
3. Coordinator distributes restore plan to executors
4. Executors restore:
   - Operator state from snapshots
   - In-flight buffers from storage references
   - Source positions from offsets
5. Sink aborts uncommitted transactions
6. Execution resumes
```

### 4. Storage Layout

```
checkpoint/
  {epoch}/
    metadata.json          # StreamingCheckpointMetadata
    operator/
      {operator_id}/
        {key_group}.snap   # Operator snapshot
    buffers/
      {source_id}/
        {partition_id}/
          {offset_range}.buf  # In-flight unaligned buffer
    sources/
      {source_id}/
        {partition}.offset   # Source offset
    sinks/
      {sink_id}/
        prepare.json      # Sink prepare record
```

## Files to Modify

| File | Change |
|------|--------|
| `crates/krishiv-state/src/checkpoint/metadata.rs` | Add `StreamingCheckpointMetadata`, `UnalignedBufferRef`, `SinkTransactionRef` |
| `crates/krishiv-state/src/checkpoint/storage.rs` | Add storage methods for unaligned buffers |
| `crates/krishiv-executor/src/fragment/streaming.rs` | Wire unaligned buffer collection into checkpoint |
| `crates/krishiv-scheduler/src/barrier_dispatch.rs` | Handle unaligned checkpoint metadata |
| `crates/krishiv-scheduler/src/job/scheduler.rs` | Add checkpoint validation for unaligned buffers |

## Acceptance Tests

1. Kill/restore preserves open window state and watermark
2. Unaligned checkpoint replay emits no duplicates for idempotent sink keys
3. Failed checkpoint aborts prepared sink output
4. Stale coordinator fencing token cannot commit an epoch
5. Savepoint restore preserves operator identity or fails with typed migration error
6. Distributed recovery test proves executor replacement does not create a second active owner
