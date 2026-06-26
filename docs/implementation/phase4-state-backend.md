# Phase 4: State Backend Evolution

## Goal

Provide cloud-native state without breaking existing state contracts by evolving `krishiv-state` with object-store LSM design.

## Design

### 1. Async State Trait

```rust
// In krishiv-state/src/async_backend.rs

/// Async state trait for remote/object-store backends.
#[async_trait]
pub trait AsyncStateBackend: Send + Sync {
    /// Get value by key (async, non-blocking).
    async fn get(&self, key: &[u8]) -> StateResult<Option<Vec<u8>>>;
    
    /// Put key-value pair (async, non-blocking).
    async fn put(&self, key: &[u8], value: &[u8]) -> StateResult<()>;
    
    /// Delete key (async, non-blocking).
    async fn delete(&self, key: &[u8]) -> StateResult<()>;
    
    /// Scan key range (async, non-blocking).
    async fn scan(&self, start: &[u8], end: &[u8]) -> StateResult<Vec<(Vec<u8>, Vec<u8>)>>;
    
    /// Create snapshot for checkpoint.
    async fn snapshot(&self) -> StateResult<SnapshotId>;
    
    /// Restore from snapshot.
    async fn restore(&self, snapshot_id: &SnapshotId) -> StateResult<()>;
}
```

### 2. Object-Store LSM Backend

```rust
// In krishiv-state/src/object_lsm.rs

/// Object-store LSM state backend.
pub struct ObjectLsmBackend {
    /// In-memory memtable.
    memtable: Memtable,
    
    /// SST levels on object storage.
    levels: Vec<SstLevel>,
    
    /// Block cache (memory).
    block_cache: BlockCache,
    
    /// Optional local disk cache.
    disk_cache: Option<DiskCache>,
    
    /// Object store client.
    object_store: Arc<dyn ObjectStore>,
    
    /// Manifest for versioning.
    manifest: Manifest,
    
    /// Key-group ownership.
    key_groups: KeyGroupOwnership,
}

/// SST level on object storage.
pub struct SstLevel {
    /// Level number (0 = immutable memtable, 1+ = compacted).
    pub level: u32,
    
    /// SST files in this level.
    pub files: Vec<SstFile>,
    
    /// Key range for this level.
    pub key_range: KeyRange,
}

/// SST file on object storage.
pub struct SstFile {
    /// File path in object storage.
    pub path: String,
    
    /// File size in bytes.
    pub size_bytes: u64,
    
    /// Key range covered by this file.
    pub key_range: KeyRange,
    
    /// Bloom filter for fast lookups.
    pub bloom_filter: BloomFilter,
    
    /// Block index for range scans.
    pub block_index: BlockIndex,
}

/// Manifest for versioning.
pub struct Manifest {
    /// Current version number.
    pub version: u64,
    
    /// Committed epochs.
    pub committed_epochs: Vec<u64>,
    
    /// SST files per epoch.
    pub epoch_files: HashMap<u64, Vec<String>>,
    
    /// Garbage collection candidates.
    pub gc_candidates: Vec<String>,
}
```

### 3. Compaction Worker

```rust
// In krishiv-state/src/compaction.rs

/// Compaction worker for object-store LSM.
pub struct CompactionWorker {
    /// Backend to compact.
    backend: Arc<ObjectLsmBackend>,
    
    /// Compaction strategy.
    strategy: CompactionStrategy,
    
    /// Worker handle.
    handle: JoinHandle<()>,
}

/// Compaction strategy.
pub enum CompactionStrategy {
    /// Size-tiered compaction (RisingWave style).
    SizeTiered {
        /// Size threshold for compaction.
        size_threshold: u64,
        /// Number of files to compact together.
        fanout: usize,
    },
    
    /// Leveled compaction (RocksDB style).
    Leveled {
        /// Size ratio between levels.
        size_ratio: f64,
        /// Maximum level size.
        max_level_size: u64,
    },
}
```

## Files to Modify

| File | Change |
|------|--------|
| `crates/krishiv-state/src/async_backend.rs` | Add `AsyncStateBackend` trait |
| `crates/krishiv-state/src/object_lsm.rs` | New file: Object-store LSM backend |
| `crates/krishiv-state/src/compaction.rs` | New file: Compaction worker |
| `crates/krishiv-state/src/backend.rs` | Add async wrapper for sync backends |
| `crates/krishiv-state/src/dfs_backend.rs` | Evolve or replace with object-store LSM |

## Acceptance Tests

1. Async state access does not block a Tokio worker thread
2. Restart from object-store manifest restores exactly the committed epoch
3. Compaction never removes files referenced by active checkpoints/savepoints
4. Cache eviction preserves correctness and exposes hit/miss metrics
5. Rescaling redistributes key groups without state loss
