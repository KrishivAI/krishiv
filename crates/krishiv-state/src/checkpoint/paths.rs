/// Path to the epoch directory: `{job_id}/checkpoints/{epoch:020}`.
pub fn epoch_dir(job_id: &str, epoch: u64) -> String {
    format!("{job_id}/checkpoints/{epoch:020}")
}

/// Path to `metadata.json` for an epoch.
pub fn metadata_path(job_id: &str, epoch: u64) -> String {
    format!("{}/metadata.json", epoch_dir(job_id, epoch))
}

/// Path to `state.bin` for an operator instance in an epoch.
pub fn snapshot_path(job_id: &str, epoch: u64, op_id: &str, task_id: &str) -> String {
    format!("{}/{op_id}/{task_id}/state.bin", epoch_dir(job_id, epoch))
}

/// Path to `manifest.sha256` for an epoch.
pub fn manifest_path(job_id: &str, epoch: u64) -> String {
    format!("{}/manifest.sha256", epoch_dir(job_id, epoch))
}

pub(crate) fn latest_epoch_hint_path(job_id: &str) -> String {
    format!("{job_id}/checkpoints/latest_epoch.json")
}
