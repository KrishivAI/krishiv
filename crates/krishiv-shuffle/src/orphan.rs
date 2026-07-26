use crate::ShuffleResult;

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
    use std::collections::HashSet;

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
