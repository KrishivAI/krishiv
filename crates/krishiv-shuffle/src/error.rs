/// Errors that can occur in shuffle operations.
#[derive(Debug, thiserror::Error)]
pub enum ShuffleError {
    /// I/O failure, wrapping the original error.
    #[error("shuffle I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The requested partition path does not exist on disk.
    #[error("shuffle partition not found: {path}")]
    PartitionNotFound { path: String },

    /// The partition exists in the metadata registry but is not yet available.
    #[error("shuffle partition not available: {path}")]
    PartitionNotAvailable { path: String },

    /// A stale lease token was used; the write was rejected.
    #[error("stale shuffle lease token: expected {expected}, actual {actual}")]
    StaleLeaseToken { expected: u64, actual: u64 },

    /// An object-store or generic path was not found.
    #[error("shuffle path not found: {path}")]
    NotFound { path: String },

    /// The shuffle partition cap was exceeded; no new partitions may be registered.
    #[error("shuffle partition limit exceeded: max {limit} partitions")]
    TooManyPartitions { limit: usize },

    /// The in-memory shuffle byte cap was exceeded and no safe spill/admission path exists.
    #[error(
        "shuffle memory limit exceeded: max {max_bytes} bytes, current {current_bytes} bytes, incoming {incoming_bytes} bytes"
    )]
    MemoryLimitExceeded {
        max_bytes: usize,
        current_bytes: usize,
        incoming_bytes: usize,
    },

    /// An internal `RwLock` was poisoned.
    #[error("shuffle lock poisoned")]
    LockPoisoned,

    /// Arrow column type does not match the expected downcast target.
    #[error("shuffle type mismatch: expected {expected}")]
    TypeMismatch { expected: String },

    /// The requested partition count was zero.
    #[error("invalid shuffle partition count: {buckets}")]
    InvalidPartitionCount { buckets: u32 },

    /// Content hash mismatch on read (strict determinism enforcement).
    #[error("shuffle content hash mismatch for {partition}")]
    ContentHashMismatch {
        partition: String,
        expected: String,
        actual: String,
    },

    /// The local disk is full; the write cannot proceed.
    ///
    /// Distinct from the generic `Io` variant so callers (the executor task
    /// runner) can surface a clear diagnostic rather than treating it as a
    /// transient I/O error and retrying indefinitely.
    #[error("shuffle disk full: {path}: {source}")]
    DiskFull {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// Acquire a write lock, mapping poison to [`ShuffleError::LockPoisoned`].
pub fn shuffle_write_lock<T>(
    lock: &std::sync::RwLock<T>,
) -> ShuffleResult<std::sync::RwLockWriteGuard<'_, T>> {
    lock.write().map_err(|_| ShuffleError::LockPoisoned)
}

pub fn shuffle_read_lock<T>(
    lock: &std::sync::RwLock<T>,
) -> ShuffleResult<std::sync::RwLockReadGuard<'_, T>> {
    lock.read().map_err(|_| ShuffleError::LockPoisoned)
}

/// Create a [`ShuffleError::Io`] from a string message by wrapping it in a
/// custom `std::io::Error`.
pub fn io_err(msg: impl Into<String>) -> ShuffleError {
    ShuffleError::Io(std::io::Error::other(msg.into()))
}

/// Maximum shuffle ticket line length (P3-3).
pub const MAX_SHUFFLE_TICKET_LEN: usize = 65_536;

/// Convenience alias for `Result<T, ShuffleError>`.
pub type ShuffleResult<T> = Result<T, ShuffleError>;

/// Convenience result alias for `ShuffleStore` operations.
pub type StoreResult<T> = ShuffleResult<T>;
