# Design Note: Checkpoint Metadata for Unaligned Buffers

## Summary

Extend checkpoint metadata to include in-flight unaligned buffer references,
source offsets, sink transaction references, and execution profile configuration.

## Motivation

Current checkpoint metadata only includes operator snapshots. For unaligned
checkpoints, we need to also persist:

1. In-flight data buffers that haven't been processed yet
2. Source offsets for rewindable sources
3. Sink transaction/prepared-commit references
4. Execution profile and buffer policy used for the epoch

This ensures that a checkpoint is restorable only after all required data is
durably recorded.

## Design

### Extended Checkpoint Metadata

```rust
// In krishiv-state/src/checkpoint/metadata.rs

/// Checkpoint metadata version for unaligned buffer support.
pub const CHECKPOINT_METADATA_VERSION: u32 = 3;

/// Extended checkpoint metadata for streaming jobs.
pub struct StreamingCheckpointMetadata {
    /// Base checkpoint metadata (version, epoch, timestamp).
    pub base: CheckpointMetadata,
    
    /// Source offsets per source partition.
    /// Key: (source_id, partition_id)
    /// Value: offset bytes (source-specific format)
    pub source_offsets: HashMap<(SourceId, PartitionId), Vec<u8>>,
    
    /// Operator snapshot references.
    /// Key: (operator_id, key_group)
    /// Value: snapshot location in checkpoint storage
    pub operator_snapshots: HashMap<(OperatorId, KeyGroup), SnapshotRef>,
    
    /// In-flight unaligned buffers.
    /// Only populated when CheckpointAlignment::Unaligned is used.
    pub unaligned_buffers: Vec<UnalignedBufferRef>,
    
    /// Sink transaction references.
    /// For two-phase commit sinks: transaction ID and prepare record location.
    pub sink_transactions: HashMap<SinkId, SinkTransactionRef>,
    
    /// Execution profile used for this epoch.
    /// Used for debugging and restoring with same configuration.
    pub execution_profile: StreamingExecutionProfile,
    
    /// Output buffer policy used for this epoch.
    pub output_buffer: OutputBufferPolicy,
}

/// Reference to an in-flight unaligned buffer.
pub struct UnalignedBufferRef {
    /// Source that produced the buffered data.
    pub source_id: SourceId,
    
    /// Partition that produced the buffered data.
    pub partition_id: PartitionId,
    
    /// Offset range covered by this buffer.
    pub offset_range: OffsetRange,
    
    /// Location of buffered data in checkpoint storage.
    pub storage_ref: StorageRef,
    
    /// Size in bytes (for metrics and restore planning).
    pub size_bytes: u64,
}

/// Reference to a sink transaction.
pub struct SinkTransactionRef {
    /// Transaction ID (sink-specific format).
    pub transaction_id: Vec<u8>,
    
    /// Location of prepare record in checkpoint storage.
    pub prepare_record_ref: StorageRef,
    
    /// Timestamp when transaction was prepared.
    pub prepared_at_ms: i64,
}
```

### Checkpoint Protocol Changes

```
1. Coordinator injects CheckpointBarrier(epoch, Unaligned)
2. Operator snapshots state
3. Operator buffers in-flight data (not yet processed)
4. Executor collects:
   - Operator snapshots
   - In-flight buffer references
   - Source offsets
   - Sink transaction references
5. Executor uploads all to checkpoint storage
6. Executor reports metadata to coordinator
7. Coordinator commits epoch
8. Sink commits transactions
```

### Restore Protocol Changes

```
1. Coordinator selects checkpoint to restore
2. Coordinator verifies all required data is present:
   - Operator snapshots
   - In-flight buffer references
   - Source offsets
   - Sink transaction references
3. Coordinator distributes restore plan to executors
4. Executors restore:
   - Operator state from snapshots
   - In-flight buffers from storage references
   - Source positions from offsets
5. Sink aborts uncommitted transactions
6. Execution resumes
```

### Storage Layout

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

## Backward Compatibility

- Version 3 metadata is backward-incompatible with version 2
- Add migration path: version 2 checkpoints can be restored as aligned-only
- New fields are optional for aligned checkpoints

## Testing

- Unit tests for metadata serialization/deserialization
- Integration tests for unaligned checkpoint creation and restore
- Chaos tests for partial buffer restore scenarios
- Performance tests for large unaligned buffer sets
