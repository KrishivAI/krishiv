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
    Ok(deleted + already_gone)
}
