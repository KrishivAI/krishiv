use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::ShuffleResult;

/// Consecutive live-job-set observations a job directory must be absent from
/// before it may be reclaimed.
pub const DEFAULT_RECLAIM_MIN_ABSENCES: u32 = 3;

/// Wall-clock time a job directory must have been absent before it may be
/// reclaimed.
pub const DEFAULT_RECLAIM_MIN_ABSENT: Duration = Duration::from_secs(120);

/// When a job directory becomes garbage (A8).
///
/// The absence semantics of a *missing* live-job set are handled by the caller
/// (`None` skips the sweep entirely). What this guards is a **present but
/// incomplete** set: a coordinator failover, a sharded coordinator, or any
/// window in which a running job is not yet in the map. Reclaiming on the first
/// such observation deletes a live job's committed shuffle output; the consumer
/// then reports it missing, the producer regenerates it, and the next sweep
/// deletes it again — a deterministic loop that used to end in "regeneration
/// budget exhausted" naming nothing.
///
/// Both conditions must hold, so the policy is "whichever is later": a burst of
/// fast sweeps cannot satisfy the count before the clock, and a single slow
/// sweep cannot satisfy the clock before the count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrphanReclaimPolicy {
    min_absences: u32,
    min_absent: Duration,
}

impl Default for OrphanReclaimPolicy {
    fn default() -> Self {
        Self {
            min_absences: DEFAULT_RECLAIM_MIN_ABSENCES,
            min_absent: DEFAULT_RECLAIM_MIN_ABSENT,
        }
    }
}

impl OrphanReclaimPolicy {
    /// Build a policy, clamped up to the defaults — the floors are the safety
    /// property, so a caller cannot configure the grace period away.
    #[must_use]
    pub fn new(min_absences: u32, min_absent: Duration) -> Self {
        Self {
            min_absences: min_absences.max(DEFAULT_RECLAIM_MIN_ABSENCES),
            min_absent: min_absent.max(DEFAULT_RECLAIM_MIN_ABSENT),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Absence {
    /// Consecutive observations that omitted this job.
    consecutive: u32,
    /// When the current run of absences started.
    since: Instant,
}

/// Applies [`OrphanReclaimPolicy`] across successive live-job-set observations.
///
/// One tracker per shuffle directory; the caller keeps it alive across sweeps,
/// which is what makes "consecutive" meaningful.
#[derive(Debug)]
pub struct OrphanReclaimTracker {
    policy: OrphanReclaimPolicy,
    absent: HashMap<String, Absence>,
}

impl OrphanReclaimTracker {
    #[must_use]
    pub fn new(policy: OrphanReclaimPolicy) -> Self {
        Self {
            policy,
            absent: HashMap::new(),
        }
    }

    /// Record one live-job-set observation and reclaim only what has been
    /// absent long enough. Returns the number of files deleted.
    pub fn observe_and_cleanup(
        &mut self,
        base_dir: &Path,
        active_job_ids: &HashSet<String>,
    ) -> ShuffleResult<usize> {
        self.observe_and_cleanup_at(base_dir, active_job_ids, Instant::now())
    }

    /// [`Self::observe_and_cleanup`] with the clock injected, so the grace
    /// period is testable without sleeping through it.
    pub fn observe_and_cleanup_at(
        &mut self,
        base_dir: &Path,
        active_job_ids: &HashSet<String>,
        now: Instant,
    ) -> ShuffleResult<usize> {
        if !base_dir.exists() {
            self.absent.clear();
            return Ok(0);
        }

        let mut on_disk: HashSet<String> = HashSet::new();
        for entry in std::fs::read_dir(base_dir)? {
            let entry = entry?;
            if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                continue;
            }
            let Some(job_id) = entry
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(str::to_owned)
            else {
                continue;
            };
            on_disk.insert(job_id);
        }

        // A job directory that is gone (reclaimed, or never existed) carries no
        // absence history; forgetting it keeps the map bounded by the disk.
        self.absent.retain(|job_id, _| on_disk.contains(job_id));

        // Everything the sweep must NOT touch: the jobs the coordinator listed,
        // plus every absent job still inside its grace period.
        let mut protected: HashSet<String> = active_job_ids.clone();
        for job_id in &on_disk {
            if active_job_ids.contains(job_id) {
                // Present again — the run of absences is broken, not merely
                // paused. A job that flickers out of one incomplete set must
                // re-earn the whole grace period.
                self.absent.remove(job_id);
                continue;
            }
            let entry = self.absent.entry(job_id.clone()).or_insert(Absence {
                consecutive: 0,
                since: now,
            });
            entry.consecutive = entry.consecutive.saturating_add(1);
            let reclaimable = entry.consecutive >= self.policy.min_absences
                && now.duration_since(entry.since) >= self.policy.min_absent;
            if reclaimable {
                tracing::info!(
                    job_id = %job_id,
                    consecutive_absences = entry.consecutive,
                    absent_secs = now.duration_since(entry.since).as_secs(),
                    dir = %base_dir.display(),
                    "reclaiming shuffle scratch for a job absent from the live-job set"
                );
            } else {
                protected.insert(job_id.clone());
            }
        }

        cleanup_orphans(base_dir, &protected)
    }
}

/// Scan `base_dir` for local shuffle artifacts whose job directory is not in
/// `active_job_ids`.
///
/// Returns a list of orphan file paths (absolute paths under `base_dir`).
pub fn scan_orphans(
    base_dir: &std::path::Path,
    active_job_ids: &std::collections::HashSet<String>,
) -> ShuffleResult<Vec<std::path::PathBuf>> {
    if !base_dir.exists() {
        return Ok(Vec::new());
    }

    let mut orphans = Vec::new();

    for entry in std::fs::read_dir(base_dir)? {
        let entry = entry?;
        // P2.16: use DirEntry::file_type() to avoid an extra stat syscall per entry.
        if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }
        let path = entry.path();
        let job_id = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };

        if !active_job_ids.contains(&job_id) {
            // Recursively collect all local shuffle artifacts in this job directory.
            collect_shuffle_files(&path, &mut orphans)?;
        }
    }

    Ok(orphans)
}

/// Recursively collect all local shuffle data, hash sidecar, and staging files under `dir`.
///
/// G3: Includes `.parquet` and `.lease` files produced by `LocalDiskShuffleStore`
/// in addition to the legacy `.ipc`, `.tmp`, and `.blake3` formats.
fn collect_shuffle_files(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> ShuffleResult<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // P2.16: use DirEntry::file_type() to avoid an extra stat syscall per entry.
        let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
        let path = entry.path();
        if is_dir {
            collect_shuffle_files(&path, out)?;
        } else {
            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let extension = path.extension().and_then(|e| e.to_str());
            match extension {
                // Legacy IPC format and temp files.
                Some("ipc") | Some("tmp") | Some("blake3") => out.push(path),
                // G3: Primary LocalDiskShuffleStore output extensions.
                Some("parquet") | Some("lease") => out.push(path),
                _ if file_name.contains(".tmp.blake3") => out.push(path),
                _ => {}
            }
        }
    }
    Ok(())
}

/// Delete all orphan artifacts found by `scan_orphans`.
///
/// Returns the number of files deleted.
///
/// G3 (ARCH-06): `io::ErrorKind::NotFound` on deletion is treated as success
/// (the file was already cleaned up by a concurrent worker).
pub fn cleanup_orphans(
    base_dir: &std::path::Path,
    active_job_ids: &std::collections::HashSet<String>,
) -> ShuffleResult<usize> {
    let orphans = scan_orphans(base_dir, active_job_ids)?;
    let mut deleted = 0usize;
    let mut already_gone = 0usize;
    for path in &orphans {
        match std::fs::remove_file(path) {
            Ok(()) => {
                deleted += 1;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                already_gone += 1;
            }
            Err(e) => return Err(e.into()),
        }
    }

    // Removing the files leaves the tree that held them — one directory per
    // job, per stage, per map task. A long-lived executor accumulates those
    // forever, and a directory entry costs an inode whether or not it holds
    // bytes, so a filesystem can run out of inodes with disk left free.
    for entry in std::fs::read_dir(base_dir)? {
        let entry = entry?;
        if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }
        let path = entry.path();
        let is_active = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|name| active_job_ids.contains(name));
        if !is_active {
            prune_empty_dirs(&path)?;
        }
    }

    Ok(deleted + already_gone)
}

/// Remove `dir` and its descendants, but only the ones that ended up empty.
///
/// Deliberately not `remove_dir_all`: [`scan_orphans`] only collects files it
/// recognises as shuffle artifacts, so anything else under a job directory was
/// put there by something other than the shuffle writer. Pruning bottom-up and
/// stopping at the first non-empty directory keeps that restraint — an
/// unrecognised file keeps its parents alive instead of being swept up.
fn prune_empty_dirs(dir: &std::path::Path) -> ShuffleResult<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            prune_empty_dirs(&entry.path())?;
        }
    }
    // Fails harmlessly with `DirectoryNotEmpty` when something survived.
    match std::fs::remove_dir(dir) {
        Ok(()) => Ok(()),
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                || e.kind() == std::io::ErrorKind::DirectoryNotEmpty =>
        {
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &std::path::Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"x").unwrap();
    }

    /// The leak that filled a benchmark node: deleting the partition files
    /// left every `job/stage/map` directory behind, so a long-lived executor
    /// accumulated directory entries for the lifetime of the process.
    #[test]
    fn cleanup_prunes_the_directories_it_emptied() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        write(&base.join("job-done/s1.m0/0.parquet"));
        write(&base.join("job-done/s1.m0/0.parquet.blake3"));
        write(&base.join("job-done/s2.m1/3.lease"));

        let active = HashSet::new();
        cleanup_orphans(base, &active).unwrap();

        assert!(
            !base.join("job-done").exists(),
            "the emptied job directory must be pruned, not just its files"
        );
    }

    /// `scan_orphans` only collects files it recognises as shuffle artifacts.
    /// Pruning must keep that restraint: an unrecognised file keeps its
    /// parents alive rather than being swept up by a blanket remove_dir_all.
    #[test]
    fn cleanup_keeps_directories_holding_files_it_does_not_own() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        write(&base.join("job-done/s1.m0/0.parquet"));
        write(&base.join("job-done/s1.m0/operator.log"));

        cleanup_orphans(base, &HashSet::new()).unwrap();

        assert!(!base.join("job-done/s1.m0/0.parquet").exists());
        assert!(
            base.join("job-done/s1.m0/operator.log").exists(),
            "a file the shuffle writer did not create must survive"
        );
        assert!(base.join("job-done/s1.m0").exists());
    }

    /// A8: a single live-job set that omits a job must never delete it.
    ///
    /// The set is `job_coordinators.keys()` on the coordinator. A failover, a
    /// sharded coordinator, or any window in which a running job is not yet in
    /// the map produces a *present but incomplete* set — and reclaiming on the
    /// first such observation deletes a live job's committed shuffle output
    /// within one tick. The consumer then reports it missing, the producer
    /// regenerates it, and the next tick deletes it again: the strongest single
    /// hypothesis on file for a partition that stays missing across a
    /// successful producer re-run.
    #[test]
    fn a_single_absence_never_reclaims() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        write(&base.join("job-live/s1.m0/0.parquet"));

        let mut tracker = OrphanReclaimTracker::new(OrphanReclaimPolicy::default());
        // The coordinator momentarily reports an incomplete set.
        tracker
            .observe_and_cleanup_at(base, &HashSet::new(), Instant::now())
            .unwrap();

        assert!(
            base.join("job-live/s1.m0/0.parquet").exists(),
            "one incomplete live-job set must not reclaim a running job's output"
        );
    }

    /// Both halves of the policy are load-bearing: the count guards against a
    /// burst of fast sweeps, the clock against one slow sweep. Reclaim happens
    /// only when whichever is later has passed.
    #[test]
    fn reclaim_needs_both_the_count_and_the_clock() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        write(&base.join("job-done/s1.m0/0.parquet"));

        let mut tracker = OrphanReclaimTracker::new(OrphanReclaimPolicy::default());
        let start = Instant::now();
        let empty = HashSet::new();

        // Three absences in quick succession satisfy the count but not the clock.
        for tick in 0..3u64 {
            tracker
                .observe_and_cleanup_at(base, &empty, start + Duration::from_secs(tick))
                .unwrap();
        }
        assert!(
            base.join("job-done/s1.m0/0.parquet").exists(),
            "the count alone must not reclaim — a fast sweep loop would defeat the grace period"
        );

        // Long enough elapsed, but on the very first observation: the count is
        // still 1, so nothing is reclaimed.
        let mut fresh = OrphanReclaimTracker::new(OrphanReclaimPolicy::default());
        write(&base.join("job-other/s1.m0/0.parquet"));
        fresh
            .observe_and_cleanup_at(base, &empty, start + Duration::from_secs(600))
            .unwrap();
        assert!(
            base.join("job-other/s1.m0/0.parquet").exists(),
            "the clock alone must not reclaim — a single missing set is not evidence"
        );

        // Both satisfied.
        tracker
            .observe_and_cleanup_at(base, &empty, start + Duration::from_secs(121))
            .unwrap();
        assert!(
            !base.join("job-done").exists(),
            "a job absent for 4 consecutive sweeps over >120 s must be reclaimed"
        );
    }

    /// The run of absences must be broken, not merely paused, when the job
    /// reappears — otherwise a job that flickers out of every other incomplete
    /// set accumulates absences and is eventually deleted while running.
    #[test]
    fn a_reappearing_job_re_earns_the_whole_grace_period() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        write(&base.join("job-flicker/s1.m0/0.parquet"));

        let mut tracker = OrphanReclaimTracker::new(OrphanReclaimPolicy::default());
        let start = Instant::now();
        let empty = HashSet::new();
        let live: HashSet<String> = ["job-flicker".to_string()].into_iter().collect();

        tracker.observe_and_cleanup_at(base, &empty, start).unwrap();
        tracker
            .observe_and_cleanup_at(base, &empty, start + Duration::from_secs(60))
            .unwrap();
        // Reappears: the coordinator's set was incomplete, not the job gone.
        tracker
            .observe_and_cleanup_at(base, &live, start + Duration::from_secs(120))
            .unwrap();
        // Two more absences, well past the wall clock — but only two.
        tracker
            .observe_and_cleanup_at(base, &empty, start + Duration::from_secs(180))
            .unwrap();
        tracker
            .observe_and_cleanup_at(base, &empty, start + Duration::from_secs(240))
            .unwrap();

        assert!(
            base.join("job-flicker/s1.m0/0.parquet").exists(),
            "reappearing must reset the absence run, not pause it"
        );
    }

    /// The floors are the safety property: a caller cannot configure the grace
    /// period below them.
    #[test]
    fn the_policy_floors_cannot_be_configured_away() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        write(&base.join("job-done/s1.m0/0.parquet"));

        let mut tracker =
            OrphanReclaimTracker::new(OrphanReclaimPolicy::new(0, Duration::from_secs(0)));
        tracker
            .observe_and_cleanup_at(base, &HashSet::new(), Instant::now())
            .unwrap();

        assert!(
            base.join("job-done/s1.m0/0.parquet").exists(),
            "a zero-grace policy must still be clamped to the defaults"
        );
    }

    /// The safety property the whole feature rests on: a running job's
    /// partitions are never reclaimed while the coordinator still lists it.
    #[test]
    fn cleanup_never_touches_a_live_job() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        write(&base.join("job-live/s1.m0/0.parquet"));
        write(&base.join("job-done/s1.m0/0.parquet"));

        let active: HashSet<String> = ["job-live".to_string()].into_iter().collect();
        cleanup_orphans(base, &active).unwrap();

        assert!(
            base.join("job-live/s1.m0/0.parquet").exists(),
            "a live job's shuffle output must survive GC"
        );
        assert!(!base.join("job-done").exists());
    }
}
