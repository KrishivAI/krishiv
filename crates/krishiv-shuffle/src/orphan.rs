use crate::ShuffleResult;

/// Scan `base_dir` for `.ipc` files whose job directory is not in `active_job_ids`.
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
            // Recursively collect all .ipc files in this job directory.
            collect_ipc_files(&path, &mut orphans)?;
        }
    }

    Ok(orphans)
}

/// Recursively collect all `.ipc` files under `dir`.
fn collect_ipc_files(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> ShuffleResult<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // P2.16: use DirEntry::file_type() to avoid an extra stat syscall per entry.
        let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
        let path = entry.path();
        if is_dir {
            collect_ipc_files(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("ipc") {
            out.push(path);
        }
    }
    Ok(())
}

/// Delete all orphan artifacts found by `scan_orphans`.
///
/// Returns the number of files deleted.
pub fn cleanup_orphans(
    base_dir: &std::path::Path,
    active_job_ids: &std::collections::HashSet<String>,
) -> ShuffleResult<usize> {
    let orphans = scan_orphans(base_dir, active_job_ids)?;
    let count = orphans.len();
    for path in &orphans {
        std::fs::remove_file(path)?;
    }
    Ok(count)
}
