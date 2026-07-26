//! Page-cache control for write-once, read-once-elsewhere files.
//!
//! Shuffle partitions and spill files are written by one process and read by a
//! *different* one — usually on a different node, over Flight. The kernel has
//! no way to know that, so every byte written lands in the page cache and stays
//! there against a future local read that never comes.
//!
//! Under cgroup v2 that page cache is charged to the container's `memory.max`,
//! and it is charged to the *writer*. A benchmark executor therefore pays for
//! caching bytes it will never look at again.
//!
//! Measured on the SF100 TPC-H run (4500 MiB executor limit, `krishiv-apitest`):
//!
//! ```text
//! pod  memory.current   process VmRSS   cgroup `file`   after evicting shuffle
//! s1        3516 MB          1994 MB        1450 MB          2365 -> 997 MB
//! s2        2742 MB           910 MB        1793 MB
//! s3        2787 MB          1057 MB        1697 MB
//! ```
//!
//! Evicting only the shuffle directory returned 1368 MB — roughly 30% of the
//! container limit — with no effect on the running process. That gap is why
//! bounding heap-side shuffle buffers three times never stopped the OOM kills:
//! the memory was never in the heap. `ExecutorCapacity` sized the query pool
//! from the container limit on the assumption that the limit was available for
//! anonymous memory, and up to a third of it was not.
//!
//! # Why this is safe
//!
//! `POSIX_FADV_DONTNEED` drops only **clean** pages. Dirty pages are left alone
//! rather than discarded, so this can never lose a write. Callers must still
//! `fsync` before evicting — otherwise the pages are still dirty, the eviction
//! silently does nothing, and the cache stays charged. Every caller in this
//! workspace evicts immediately after an existing `sync_all()` for that reason.
//!
//! This is the same treatment Kafka, PostgreSQL and RocksDB apply to their own
//! write-once segments.

use std::path::Path;

/// Drop the clean page-cache pages backing `file`.
///
/// The caller is responsible for having durably written the file first; see the
/// module docs. Returns the underlying `io::Error` on failure so callers can
/// decide whether to warn — eviction is an optimisation, never a correctness
/// requirement, so failing to evict is not a reason to fail the operation.
///
/// On non-Unix targets this is a no-op returning `Ok(())`: there is no portable
/// equivalent, and the cost of not evicting is memory, not incorrectness.
pub fn evict_file(file: &std::fs::File) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        // `len: None` means "to end of file", so this covers the whole file
        // regardless of how much was written. rustix wraps posix_fadvise
        // safely, which matters here: the workspace denies `unsafe_code`.
        rustix::fs::fadvise(file, 0, None, rustix::fs::Advice::DontNeed)
            .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        Ok(())
    }
}

/// Drop the clean page-cache pages backing the file at `path`.
///
/// Convenience wrapper around [`evict_file`] for callers that have already
/// closed the file (for example after an atomic rename). Opening read-only to
/// evict does not re-populate the cache.
pub fn evict_path(path: &Path) -> std::io::Result<()> {
    let file = std::fs::File::open(path)?;
    evict_file(&file)
}

/// Evict `path`, logging at debug level instead of propagating on failure.
///
/// This is the form nearly every caller wants: eviction is best-effort, and a
/// failure to evict must never turn a successful write into a failed one. A
/// missing file is not worth mentioning at all — it means a concurrent cleanup
/// won the race, which is normal.
pub fn evict_path_best_effort(path: &Path) {
    match evict_path(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::debug!(
                path = %path.display(),
                error = %e,
                "could not evict file from page cache; it stays charged to this cgroup"
            );
        }
    }
}

/// Best-effort [`evict_file`] for callers holding an open handle.
pub fn evict_file_best_effort(file: &std::fs::File) {
    if let Err(e) = evict_file(file) {
        tracing::debug!(
            error = %e,
            "could not evict file from page cache; it stays charged to this cgroup"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Eviction must not damage the file. This is the property that matters:
    /// the optimisation is invisible to readers.
    #[test]
    fn evicting_a_file_leaves_its_contents_intact() {
        let dir = std::env::temp_dir().join(format!("krishiv-pgcache-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("payload.bin");

        let payload: Vec<u8> = (0..256u32).flat_map(|i| i.to_le_bytes()).collect();
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&payload).unwrap();
            // Clean pages only — evicting before this would be a no-op.
            f.sync_all().unwrap();
        }

        evict_path(&path).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), payload);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A missing path is the common benign race (concurrent cleanup), and the
    /// best-effort form must swallow it rather than log noise on every call.
    #[test]
    fn evicting_a_missing_path_is_not_an_error_for_callers() {
        let missing = std::env::temp_dir().join("krishiv-pgcache-does-not-exist-9e3f1a");
        assert!(evict_path(&missing).is_err());
        evict_path_best_effort(&missing); // must not panic
    }
}
