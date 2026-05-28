/// Errors that can occur in shuffle operations.
#[derive(Debug)]
pub enum ShuffleError {
    /// I/O failure, wrapping the original error.
    Io(std::io::Error),
    /// The requested partition path does not exist on disk.
    PartitionNotFound {
        /// String representation of the path.
        path: String,
    },
    /// The partition exists in the metadata registry but is not yet available.
    PartitionNotAvailable {
        /// String representation of the path.
        path: String,
    },
    /// A stale lease token was used; the write was rejected.
    StaleLeaseToken {
        /// The expected (current) lease token.
        expected: u64,
        /// The token actually presented by the caller.
        actual: u64,
    },
    /// An object-store or generic path was not found.
    ///
    /// Used as the `StoreError::PartitionNotFound` alias when the partition key
    /// has already been formatted into a path string.
    NotFound {
        /// String representation of the missing path.
        path: String,
    },
    /// The shuffle partition cap was exceeded; no new partitions may be registered.
    TooManyPartitions {
        /// The configured partition limit.
        limit: usize,
    },
    /// An internal `RwLock` was poisoned.
    LockPoisoned,
    /// Arrow column type does not match the expected downcast target.
    TypeMismatch { expected: String },
    /// The requested partition count was zero.
    InvalidPartitionCount { buckets: u32 },
}

impl std::fmt::Display for ShuffleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "shuffle I/O error: {e}"),
            Self::PartitionNotFound { path } => {
                write!(f, "shuffle partition not found: {path}")
            }
            Self::PartitionNotAvailable { path } => {
                write!(f, "shuffle partition not available: {path}")
            }
            Self::StaleLeaseToken { expected, actual } => write!(
                f,
                "stale shuffle lease token: expected {expected}, actual {actual}"
            ),
            Self::NotFound { path } => write!(f, "shuffle path not found: {path}"),
            Self::TooManyPartitions { limit } => {
                write!(
                    f,
                    "shuffle partition limit exceeded: max {limit} partitions"
                )
            }
            Self::LockPoisoned => f.write_str("shuffle lock poisoned"),
            Self::TypeMismatch { expected } => {
                write!(f, "shuffle type mismatch: expected {expected}")
            }
            Self::InvalidPartitionCount { buckets } => {
                write!(f, "invalid shuffle partition count: {buckets}")
            }
        }
    }
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
    ShuffleError::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        msg.into(),
    ))
}

/// Maximum shuffle ticket line length (P3-3).
pub const MAX_SHUFFLE_TICKET_LEN: usize = 65_536;

impl std::error::Error for ShuffleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ShuffleError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Convenience alias for `Result<T, ShuffleError>`.
pub type ShuffleResult<T> = Result<T, ShuffleError>;

/// Unified error type for `ShuffleStore` operations.
#[deprecated(since = "0.2.0", note = "Use `ShuffleError` directly")]
pub type StoreError = ShuffleError;

/// Convenience result alias for `ShuffleStore` operations.
pub type StoreResult<T> = ShuffleResult<T>;
