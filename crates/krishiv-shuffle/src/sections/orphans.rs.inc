// ── Orphan detection ──────────────────────────────────────────────────

fn write_ipc_file(base: &std::path::Path, job_id: &str, stage_id: &str, partition_id: u32) {
    let dir = base.join(job_id).join(stage_id);
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join(format!("{partition_id}.ipc"));
    std::fs::write(file, b"dummy").unwrap();
}

#[test]
fn scan_orphans_empty_base_dir_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let active: HashSet<String> = HashSet::new();
    let result = scan_orphans(dir.path(), &active).unwrap();
    assert!(result.is_empty());
}

#[test]
fn scan_orphans_nonexistent_base_dir_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does_not_exist");
    let active: HashSet<String> = HashSet::new();
    let result = scan_orphans(&missing, &active).unwrap();
    assert!(result.is_empty());
}

#[test]
fn scan_orphans_all_active_no_orphans() {
    let dir = tempfile::tempdir().unwrap();
    write_ipc_file(dir.path(), "job1", "s0", 0);
    write_ipc_file(dir.path(), "job1", "s0", 1);

    let mut active: HashSet<String> = HashSet::new();
    active.insert("job1".into());

    let result = scan_orphans(dir.path(), &active).unwrap();
    assert!(result.is_empty());
}

#[test]
fn scan_orphans_inactive_job_returns_ipc_files() {
    let dir = tempfile::tempdir().unwrap();
    write_ipc_file(dir.path(), "dead_job", "s0", 0);
    write_ipc_file(dir.path(), "dead_job", "s0", 1);

    let active: HashSet<String> = HashSet::new();
    let mut result = scan_orphans(dir.path(), &active).unwrap();
    result.sort();

    assert_eq!(result.len(), 2);
    for path in &result {
        assert!(
            path.extension().and_then(|e| e.to_str()) == Some("ipc"),
            "expected .ipc extension"
        );
    }
}

#[test]
fn scan_orphans_mixed_active_and_inactive() {
    let dir = tempfile::tempdir().unwrap();
    write_ipc_file(dir.path(), "active_job", "s0", 0);
    write_ipc_file(dir.path(), "dead_job", "s0", 0);
    write_ipc_file(dir.path(), "dead_job", "s1", 0);

    let mut active: HashSet<String> = HashSet::new();
    active.insert("active_job".into());

    let result = scan_orphans(dir.path(), &active).unwrap();
    assert_eq!(result.len(), 2);
    // None of the orphans should be under active_job.
    for path in &result {
        assert!(
            !path.to_string_lossy().contains("active_job"),
            "active job files should not be orphans"
        );
    }
}

#[test]
fn cleanup_orphans_deletes_files_and_returns_count() {
    let dir = tempfile::tempdir().unwrap();
    write_ipc_file(dir.path(), "dead_job", "s0", 0);
    write_ipc_file(dir.path(), "dead_job", "s0", 1);

    let active: HashSet<String> = HashSet::new();
    let count = cleanup_orphans(dir.path(), &active).unwrap();
    assert_eq!(count, 2);

    // Files should be gone.
    let remaining = scan_orphans(dir.path(), &active).unwrap();
    assert!(remaining.is_empty());
}

#[test]
fn cleanup_orphans_removes_hash_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    write_ipc_file(dir.path(), "dead_with_hash", "s0", 0);
    let sidecar = dir.path().join("dead_with_hash/s0/0.ipc.blake3");
    std::fs::write(&sidecar, b"abcd").unwrap();

    let active: HashSet<String> = HashSet::new();
    let mut orphans = scan_orphans(dir.path(), &active).unwrap();
    orphans.sort();
    assert_eq!(orphans.len(), 2);
    assert!(orphans.iter().any(|p| p.ends_with("0.ipc")));
    assert!(orphans.iter().any(|p| p.ends_with("0.ipc.blake3")));

    let count = cleanup_orphans(dir.path(), &active).unwrap();
    assert_eq!(count, 2);
    assert!(!dir.path().join("dead_with_hash/s0/0.ipc").exists());
    assert!(!sidecar.exists());
}

