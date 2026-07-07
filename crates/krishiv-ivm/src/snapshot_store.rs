#![forbid(unsafe_code)]

//! Spill-aware snapshot store for IVM source and view state.
//!
//! When `IncrementalFlow` accumulates millions of rows across sources, the
//! in-memory `HashMap<String, RecordBatch>` can exhaust RAM. This module
//! provides an optional disk-backed store that spills large batches to a
//! local directory via Arrow IPC files, keeping an LRU cache in memory.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;

const DEFAULT_MAX_CACHE_BYTES: usize = 256 * 1024 * 1024; // 256 MiB
const DEFAULT_MAX_BATCH_MEMORY_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Where a particular snapshot lives.
enum Location {
    /// Held in memory in the LRU cache.
    Memory,
    /// Spilled to disk at the given path.
    Disk(PathBuf),
}

/// A store for named snapshots that can spill large records to disk.
///
/// Snapshots under `max_batch_memory` bytes are kept in an in-memory LRU
/// cache. Snapshots exceeding the threshold are written to disk as Arrow
/// IPC streams and loaded on demand.
pub struct SnapshotStore {
    /// In-memory cache: name → (batch, byte_size, location).
    cache: HashMap<String, (Arc<RecordBatch>, usize, Location)>,
    /// Total bytes held in the in-memory cache.
    cache_bytes: usize,
    /// Maximum bytes to retain in the in-memory cache before evicting.
    max_cache_bytes: usize,
    /// Batches larger than this are spilled to disk on insert.
    max_batch_memory: usize,
    /// Directory for spilled IPC files. Created on first spill.
    spill_dir: Option<PathBuf>,
}

impl SnapshotStore {
    /// Create an in-memory-only store (no disk spilling).
    pub fn in_memory() -> Self {
        Self {
            cache: HashMap::new(),
            cache_bytes: 0,
            max_cache_bytes: DEFAULT_MAX_CACHE_BYTES,
            max_batch_memory: DEFAULT_MAX_BATCH_MEMORY_BYTES,
            spill_dir: None,
        }
    }

    /// Create a store that spills large batches to `spill_dir`.
    pub fn with_disk_spill(spill_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let dir = spill_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            cache: HashMap::new(),
            cache_bytes: 0,
            max_cache_bytes: DEFAULT_MAX_CACHE_BYTES,
            max_batch_memory: DEFAULT_MAX_BATCH_MEMORY_BYTES,
            spill_dir: Some(dir),
        })
    }

    /// Insert or replace a named snapshot. Large batches are spilled to disk.
    pub fn put(&mut self, name: &str, batch: RecordBatch) -> std::io::Result<()> {
        let batch_bytes = batch.get_array_memory_size();

        if batch_bytes >= self.max_batch_memory
            && let Some(ref spill_dir) = self.spill_dir
        {
            // Spill to disk: serialize to Arrow IPC and write to a temp file.
            let path = spill_dir.join(format!("{name}.ipc"));
            let mut file = std::fs::File::create(&path)?;
            {
                let mut writer = StreamWriter::try_new(&mut file, batch.schema().as_ref())
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                writer
                    .write(&batch)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                writer
                    .finish()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
            }
            file.sync_all()?;
            // Keep a zero-row placeholder in cache to mark existence.
            let placeholder = RecordBatch::new_empty(batch.schema());
            self.evict_if_needed(0);
            self.cache_bytes += 0;
            self.cache.insert(
                name.to_string(),
                (Arc::new(placeholder), 0, Location::Disk(path)),
            );
        } else {
            self.evict_if_needed(batch_bytes);
            let arc = Arc::new(batch);
            self.cache_bytes += batch_bytes;
            self.cache
                .insert(name.to_string(), (arc, batch_bytes, Location::Memory));
        }
        Ok(())
    }

    /// Get a snapshot by name. Returns `None` if not found.
    pub fn get(&self, name: &str) -> Option<Arc<RecordBatch>> {
        let (arc, _size, location) = self.cache.get(name)?;
        match location {
            Location::Memory => Some(Arc::clone(arc)),
            Location::Disk(path) => {
                // Load from disk on demand.
                let file = std::fs::File::open(path).ok()?;
                let reader = std::io::BufReader::new(file);
                let stream = arrow::ipc::reader::StreamReader::try_new(reader, None).ok()?;
                let batches: Vec<RecordBatch> = stream.collect::<Result<Vec<_>, _>>().ok()?;
                let combined =
                    arrow::compute::concat_batches(&batches.first()?.schema(), &batches).ok()?;
                Some(Arc::new(combined))
            }
        }
    }

    /// Remove a snapshot by name.
    pub fn remove(&mut self, name: &str) {
        if let Some((_, size, location)) = self.cache.remove(name) {
            if let Location::Disk(path) = location {
                let _ = std::fs::remove_file(path);
            } else {
                self.cache_bytes = self.cache_bytes.saturating_sub(size);
            }
        }
    }

    /// List all snapshot names.
    pub fn keys(&self) -> Vec<String> {
        self.cache.keys().cloned().collect()
    }

    /// Returns true if the named snapshot exists.
    pub fn contains(&self, name: &str) -> bool {
        self.cache.contains_key(name)
    }

    /// Evict the least-recently-used entry if the cache would exceed capacity.
    fn evict_if_needed(&mut self, incoming_bytes: usize) {
        while self.cache_bytes + incoming_bytes > self.max_cache_bytes {
            // Find the first in-memory entry and evict it.
            let key = self
                .cache
                .iter()
                .find(|(_, (_, _, loc))| matches!(loc, Location::Memory))
                .map(|(k, _)| k.clone());
            if let Some(key) = key {
                self.remove(&key);
            } else {
                break; // nothing left to evict
            }
        }
    }
}

impl Default for SnapshotStore {
    fn default() -> Self {
        Self::in_memory()
    }
}
