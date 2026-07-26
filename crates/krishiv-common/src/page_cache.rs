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

/// Environment override for the shuffle page-cache ceiling, in bytes.
pub const SHUFFLE_PAGE_CACHE_ENV: &str = "KRISHIV_SHUFFLE_PAGE_CACHE_BYTES";

/// Share of the container handed to cached shuffle output.
///
/// Deliberately small and inside the slack the capacity model already leaves
/// unclaimed: page cache is *reclaimable* (clean pages), unlike the heap
/// budgets, so it does not need its own carve-out from the accounted fractions.
/// On a 4500 MiB executor this is ~560 MB — a real working set, and a long way
/// from the 1.7 GB that was getting containers killed.
const SHUFFLE_PAGE_CACHE_FRACTION: f64 = 0.125;

/// Default ceiling when there is no cgroup limit to derive one from.
const DEFAULT_SHUFFLE_PAGE_CACHE_BYTES: u64 = 512 * 1024 * 1024;

/// A bounded ceiling on page cache held by write-once files.
///
/// # Why this is not "evict immediately"
///
/// Dropping every partition's cache the moment it is durable does bound memory,
/// but it also throws away the case the cache exists for: a reduce task reading
/// a partition produced on the *same* node, which then pays a disk round-trip
/// instead of a memory read. Remote fetches were always going to disk; same-node
/// reads were not, and making them do so is a real cost on a 270 MB/s disk.
///
/// The original bug was never that the cache existed — it was that nothing ever
/// dropped it, so a partition written during the first stage was still resident
/// ten stages later. Bounding the total keeps the recent working set (where the
/// hits are) and discards the stale tail (where the OOM was), which is what a
/// cache is supposed to do.
///
/// Eviction is FIFO by write order. That is deliberately not LRU: shuffle output
/// is read once, so "oldest written" is the best available proxy for "least
/// likely to still be wanted", and it needs no per-read bookkeeping on the hot
/// path.
#[derive(Debug)]
pub struct ShufflePageCacheBudget {
    limit_bytes: u64,
    state: std::sync::Mutex<CacheState>,
}

#[derive(Debug, Default)]
struct CacheState {
    /// Paths still believed to hold cache, oldest first.
    resident: std::collections::VecDeque<(std::path::PathBuf, u64)>,
    held_bytes: u64,
}

impl ShufflePageCacheBudget {
    /// Build a budget from the container limit, honouring
    /// [`SHUFFLE_PAGE_CACHE_ENV`].
    pub fn from_cgroup(cgroup_limit_bytes: Option<u64>) -> Self {
        let limit_bytes = std::env::var(SHUFFLE_PAGE_CACHE_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or_else(|| match cgroup_limit_bytes {
                Some(limit) => ((limit as f64) * SHUFFLE_PAGE_CACHE_FRACTION) as u64,
                None => DEFAULT_SHUFFLE_PAGE_CACHE_BYTES,
            });
        Self {
            limit_bytes,
            state: std::sync::Mutex::new(CacheState::default()),
        }
    }

    /// The ceiling this budget enforces.
    pub fn limit_bytes(&self) -> u64 {
        self.limit_bytes
    }

    /// Bytes currently believed resident.
    pub fn held_bytes(&self) -> u64 {
        self.state.lock().map(|s| s.held_bytes).unwrap_or(0)
    }

    /// Record a freshly-written, durable file and evict oldest entries until the
    /// total is back under the ceiling.
    ///
    /// The caller must have fsynced first: `DONTNEED` skips dirty pages, so
    /// evicting before the data is durable silently does nothing and the cache
    /// stays charged.
    pub fn record_written(&self, path: std::path::PathBuf, bytes: u64) {
        let Ok(mut state) = self.state.lock() else {
            // A poisoned lock must not fail a write that already succeeded;
            // evict directly so memory is still bounded.
            evict_path_best_effort(&path);
            return;
        };
        state.resident.push_back((path, bytes));
        state.held_bytes = state.held_bytes.saturating_add(bytes);
        while state.held_bytes > self.limit_bytes {
            let Some((old_path, old_bytes)) = state.resident.pop_front() else {
                break;
            };
            state.held_bytes = state.held_bytes.saturating_sub(old_bytes);
            evict_path_best_effort(&old_path);
        }
    }

    /// Record that a partition has been fully served, and drop its cache.
    ///
    /// Shuffle output is read once, so once it has been consumed the cache has
    /// no remaining value and is pure charge against the container.
    pub fn record_consumed(&self, path: &Path) {
        evict_path_best_effort(path);
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(pos) = state.resident.iter().position(|(p, _)| p == path) {
            let (_, bytes) = state.resident.remove(pos).unwrap_or_default();
            state.held_bytes = state.held_bytes.saturating_sub(bytes);
        }
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

    fn budget(limit: u64) -> ShufflePageCacheBudget {
        ShufflePageCacheBudget {
            limit_bytes: limit,
            state: std::sync::Mutex::new(CacheState::default()),
        }
    }

    /// The point of the bound: recent partitions stay cached so a same-node
    /// reduce read is served from RAM, and only the excess is dropped. A policy
    /// that evicted everything on write would bound memory too, but it would
    /// also turn every same-node read into a disk round-trip.
    #[test]
    fn recent_partitions_stay_cached_and_only_the_excess_is_dropped() {
        let b = budget(300);
        b.record_written("/tmp/krishiv-pc-a".into(), 100);
        b.record_written("/tmp/krishiv-pc-b".into(), 100);
        assert_eq!(b.held_bytes(), 200, "under the ceiling, nothing is dropped");

        b.record_written("/tmp/krishiv-pc-c".into(), 100);
        assert_eq!(b.held_bytes(), 300, "exactly at the ceiling is still fine");

        // One more pushes past it; the oldest is evicted, not the newest.
        b.record_written("/tmp/krishiv-pc-d".into(), 100);
        assert_eq!(b.held_bytes(), 300, "held stays bounded by the ceiling");
        let resident = b.state.lock().unwrap();
        let paths: Vec<_> = resident
            .resident
            .iter()
            .map(|(p, _)| p.to_string_lossy().into_owned())
            .collect();
        assert!(
            !paths.iter().any(|p| p.ends_with("-a")),
            "oldest should have been evicted first, got {paths:?}"
        );
        assert!(
            paths.iter().any(|p| p.ends_with("-d")),
            "newest must be retained, got {paths:?}"
        );
    }

    /// A partition larger than the whole ceiling must not wedge the budget: it
    /// gets recorded, immediately trimmed, and the accounting stays consistent.
    #[test]
    fn a_partition_larger_than_the_ceiling_does_not_wedge_the_budget() {
        let b = budget(100);
        b.record_written("/tmp/krishiv-pc-huge".into(), 10_000);
        assert_eq!(b.held_bytes(), 0, "oversized entry is trimmed straight back");
        b.record_written("/tmp/krishiv-pc-small".into(), 50);
        assert_eq!(b.held_bytes(), 50, "budget still usable afterwards");
    }

    /// Consuming a partition returns its budget, which is what keeps the
    /// ceiling from being reached by data that is still live.
    #[test]
    fn consuming_a_partition_returns_its_budget() {
        let b = budget(1000);
        b.record_written("/tmp/krishiv-pc-x".into(), 400);
        b.record_written("/tmp/krishiv-pc-y".into(), 400);
        assert_eq!(b.held_bytes(), 800);

        b.record_consumed(Path::new("/tmp/krishiv-pc-x"));
        assert_eq!(b.held_bytes(), 400, "consumed bytes are released");

        // Consuming something untracked must not corrupt the accounting.
        b.record_consumed(Path::new("/tmp/krishiv-pc-never-written"));
        assert_eq!(b.held_bytes(), 400);
    }

    /// The ceiling must come from the container, not a hard-coded constant that
    /// claims the same bytes on a 2 GiB executor and a 64 GiB one.
    #[test]
    fn the_ceiling_is_derived_from_the_container() {
        let small = ShufflePageCacheBudget::from_cgroup(Some(4_718_592_000));
        let large = ShufflePageCacheBudget::from_cgroup(Some(64 * 1024 * 1024 * 1024));
        assert!(
            large.limit_bytes() > small.limit_bytes(),
            "a bigger container should permit a bigger cache"
        );
        assert!(
            small.limit_bytes() < 4_718_592_000 / 4,
            "the cache must stay well inside the container's slack"
        );
        assert_eq!(
            ShufflePageCacheBudget::from_cgroup(None).limit_bytes(),
            DEFAULT_SHUFFLE_PAGE_CACHE_BYTES,
            "uncontained falls back to the documented default"
        );
    }
}
