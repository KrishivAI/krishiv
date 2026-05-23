//! Incremental checkpoint manifest tracking (R16 S6.1).
//!
//! Tracks changed state file segments between epochs. For `RedbStateBackend` this
//! records snapshot blob hashes rather than RocksDB SST files.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Reference to one uploaded state segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSegmentRef {
    pub path: String,
    pub sha256_hex: String,
    pub size_bytes: u64,
}

/// Manifest of segments for one checkpoint epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalManifest {
    pub epoch: u64,
    pub segments: Vec<StateSegmentRef>,
}

/// Tracks manifests to compute incremental uploads.
#[derive(Debug, Default)]
pub struct IncrementalCheckpointWriter {
    manifests: BTreeMap<u64, IncrementalManifest>,
}

impl IncrementalCheckpointWriter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build manifest for `snapshot_bytes` and return only segments not present in `previous_epoch`.
    pub fn plan_incremental_upload(
        &self,
        epoch: u64,
        snapshot_bytes: &[u8],
        previous_epoch: Option<u64>,
    ) -> (IncrementalManifest, Vec<StateSegmentRef>) {
        let segment = StateSegmentRef {
            path: format!("state-{epoch}.bin"),
            sha256_hex: hex_sha256(snapshot_bytes),
            size_bytes: snapshot_bytes.len() as u64,
        };
        let manifest = IncrementalManifest {
            epoch,
            segments: vec![segment.clone()],
        };
        let upload = match previous_epoch.and_then(|e| self.manifests.get(&e)) {
            Some(prev) => {
                let prev_hashes: std::collections::HashSet<_> =
                    prev.segments.iter().map(|s| s.sha256_hex.clone()).collect();
                manifest
                    .segments
                    .iter()
                    .filter(|s| !prev_hashes.contains(&s.sha256_hex))
                    .cloned()
                    .collect()
            }
            None => manifest.segments.clone(),
        };
        (manifest, upload)
    }

    pub fn record_committed(&mut self, manifest: IncrementalManifest) {
        self.manifests.insert(manifest.epoch, manifest);
    }

    /// Remove manifests older than the newest `retain` epochs.
    pub fn gc(&mut self, retain: usize) {
        if self.manifests.len() <= retain {
            return;
        }
        let epochs: Vec<u64> = self.manifests.keys().copied().collect();
        let drop_count = epochs.len().saturating_sub(retain);
        for epoch in epochs.into_iter().take(drop_count) {
            self.manifests.remove(&epoch);
        }
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_skips_unchanged_snapshot() {
        let mut writer = IncrementalCheckpointWriter::new();
        let bytes = b"same-state";
        let (m1, up1) = writer.plan_incremental_upload(1, bytes, None);
        assert_eq!(up1.len(), 1);
        writer.record_committed(m1);
        let (m2, up2) = writer.plan_incremental_upload(2, bytes, Some(1));
        assert_eq!(up2.len(), 0);
        writer.record_committed(m2);
    }

    #[test]
    fn incremental_uploads_changed_snapshot() {
        let mut writer = IncrementalCheckpointWriter::new();
        let (m1, _) = writer.plan_incremental_upload(1, b"a", None);
        writer.record_committed(m1);
        let (_, up2) = writer.plan_incremental_upload(2, b"b", Some(1));
        assert_eq!(up2.len(), 1);
    }
}
