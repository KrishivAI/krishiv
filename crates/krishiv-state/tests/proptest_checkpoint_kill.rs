//! Commit-or-abort model tests for the checkpoint protocol (audit §14 TEST-3).
//!
//! A checkpoint epoch is written as an ordered sequence of atomic storage
//! writes: operator snapshots → `metadata.json` → `manifest.sha256` (the
//! commit point) → epoch hint.  A process kill can stop that sequence after
//! any prefix.  The property: recovery (`latest_valid_epoch`) returns the
//! last epoch whose manifest was fully written — never a partial epoch, and
//! never loses a sealed one — and restoring from it round-trips every
//! snapshot byte-for-byte.  A second property flips arbitrary bytes in a
//! sealed epoch's files and asserts the integrity manifest keeps recovery
//! away from it.

// Test harness: panicking on invariant violation is the assertion.
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    clippy::indexing_slicing
)]

use krishiv_state::checkpoint::{
    CheckpointError, CheckpointMetadata, IntegrityManifest, LocalFsCheckpointStorage,
    OperatorSnapshotRef, latest_valid_epoch, list_valid_epochs, read_operator_snapshot,
    snapshot_path, validate_epoch, write_epoch_hint, write_epoch_metadata, write_manifest,
    write_operator_snapshot,
};
use proptest::prelude::*;

const JOB: &str = "job-kill-model";

fn metadata_for(epoch: u64, snapshots: &[Vec<u8>]) -> CheckpointMetadata {
    CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch,
        job_id: JOB.to_owned(),
        fencing_token: 1,
        coordinator_id: None,
        timestamp_ms: epoch * 1_000,
        source_offsets: vec![],
        operator_snapshots: (0..snapshots.len())
            .map(|i| OperatorSnapshotRef {
                operator_id: format!("op-{i}"),
                task_id: "t0".to_owned(),
                snapshot_path: snapshot_path(JOB, epoch, &format!("op-{i}"), "t0"),
            })
            .collect(),
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
        unaligned_buffer_refs: vec![],
        sink_transactions: vec![],
        streaming_profile: None,
    }
}

/// Write epoch `epoch` following the documented protocol order, executing at
/// most `budget` storage writes (decremented per write).  Returns `true` if
/// the whole sequence ran (i.e. the kill did not land inside this epoch).
fn write_epoch_with_budget(
    storage: &dyn krishiv_state::checkpoint::CheckpointStorage,
    epoch: u64,
    snapshots: &[Vec<u8>],
    budget: &mut usize,
) -> bool {
    let mut spend = |writes: &mut dyn FnMut()| -> bool {
        if *budget == 0 {
            return false;
        }
        *budget -= 1;
        writes();
        true
    };

    for (i, bytes) in snapshots.iter().enumerate() {
        if !spend(&mut || {
            write_operator_snapshot(storage, JOB, epoch, &format!("op-{i}"), "t0", bytes)
                .expect("write snapshot");
        }) {
            return false;
        }
    }

    let metadata = metadata_for(epoch, snapshots);
    if !spend(&mut || {
        write_epoch_metadata(storage, JOB, epoch, &metadata).expect("write metadata");
    }) {
        return false;
    }

    let mut manifest = IntegrityManifest::new();
    manifest.insert_bytes(
        "metadata.json",
        &serde_json::to_vec_pretty(&metadata).expect("serialize metadata"),
    );
    for (i, bytes) in snapshots.iter().enumerate() {
        manifest.insert_bytes(format!("op-{i}/t0/state.bin"), bytes);
    }
    if !spend(&mut || {
        write_manifest(storage, JOB, epoch, &manifest).expect("write manifest");
    }) {
        return false;
    }

    if !spend(&mut || {
        write_epoch_hint(storage, JOB, epoch).expect("write hint");
    }) {
        return false;
    }
    true
}

/// Per-epoch write count: K snapshots + metadata + manifest + hint.
fn ops_per_epoch(num_snapshots: usize) -> usize {
    num_snapshots + 3
}

fn snapshots_strategy(count: usize) -> impl Strategy<Value = Vec<Vec<Vec<u8>>>> {
    // One Vec<Vec<u8>> (operator snapshots) per epoch, epochs 1..=3.
    proptest::collection::vec(
        proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..48), count..=count),
        1..=3,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Kill the writer after an arbitrary prefix of the last epoch's writes:
    /// recovery must land exactly on the last epoch whose manifest completed,
    /// and every recovered snapshot must round-trip byte-for-byte.
    #[test]
    fn kill_during_checkpoint_recovers_last_sealed_epoch(
        sized_epochs in (1_usize..=3).prop_flat_map(|k| snapshots_strategy(k).prop_map(move |s| (k, s))),
        cut_frac in 0.0_f64..=1.0,
    ) {
        let (k, epoch_snapshots) = sized_epochs;
        let storage = LocalFsCheckpointStorage::ephemeral().expect("ephemeral storage");

        let per_epoch = ops_per_epoch(k);
        let num_epochs = epoch_snapshots.len() as u64;
        // Kill lands somewhere inside the LAST epoch's write sequence
        // (0 ops … all ops); earlier epochs complete fully.
        let cut_in_last = ((per_epoch as f64) * cut_frac).floor() as usize;
        let mut budget = per_epoch * (num_epochs as usize - 1) + cut_in_last.min(per_epoch);

        let mut sealed_up_to: Option<u64> = None;
        for (idx, snapshots) in epoch_snapshots.iter().enumerate() {
            let epoch = idx as u64 + 1;
            let manifest_done = {
                let before = budget;
                let completed = write_epoch_with_budget(&storage, epoch, snapshots, &mut budget);
                // Manifest is write k+2 of this epoch (1-based): sealed iff at
                // least k+2 writes of this epoch executed.
                completed || (before - budget) >= k + 2
            };
            if manifest_done {
                sealed_up_to = Some(epoch);
            }
        }

        match sealed_up_to {
            None => {
                prop_assert!(matches!(
                    latest_valid_epoch(&storage, JOB),
                    Err(CheckpointError::NoValidEpoch)
                ));
            }
            Some(expected) => {
                let recovered = latest_valid_epoch(&storage, JOB).expect("recovery must succeed");
                prop_assert_eq!(recovered, expected, "recovery must land on the last sealed epoch");
                prop_assert!(validate_epoch(&storage, JOB, recovered).expect("validate"));

                // Round-trip every snapshot of the recovered epoch.
                let snaps = &epoch_snapshots[(recovered - 1) as usize];
                for (i, expected_bytes) in snaps.iter().enumerate() {
                    let got = read_operator_snapshot(&storage, JOB, recovered, &format!("op-{i}"), "t0")
                        .expect("read snapshot")
                        .expect("snapshot present in sealed epoch");
                    prop_assert_eq!(&got, expected_bytes);
                }

                // A partially written newer epoch must never validate.
                if recovered < num_epochs {
                    prop_assert!(!validate_epoch(&storage, JOB, recovered + 1).expect("validate partial"));
                }
                // And the valid-epoch listing agrees.
                let valid = list_valid_epochs(&storage, JOB).expect("list");
                prop_assert_eq!(valid, (1..=recovered).collect::<Vec<u64>>());
            }
        }
    }

    /// Flip one byte anywhere in the newest sealed epoch's files: the
    /// integrity manifest must fence recovery away from the damaged epoch —
    /// falling back to the previous sealed epoch instead of restoring
    /// corrupted state (and instead of erroring out when a fallback exists).
    #[test]
    fn corrupted_epoch_is_fenced_and_recovery_falls_back(
        epoch_snapshots in (1_usize..=2).prop_flat_map(snapshots_strategy),
        file_pick in any::<prop::sample::Index>(),
        byte_pick in any::<prop::sample::Index>(),
        flip in 1_u8..=255,
    ) {
        let storage = LocalFsCheckpointStorage::ephemeral().expect("ephemeral storage");
        let num_epochs = epoch_snapshots.len() as u64;

        for (idx, snapshots) in epoch_snapshots.iter().enumerate() {
            let mut budget = usize::MAX;
            prop_assert!(write_epoch_with_budget(&storage, idx as u64 + 1, snapshots, &mut budget));
        }

        // Collect the newest epoch's files (walk the epoch dir on disk).
        let epoch_dir = storage
            .base_dir()
            .join(JOB)
            .join("checkpoints")
            .join(format!("{:020}", num_epochs));
        let mut files: Vec<std::path::PathBuf> = walk_files(&epoch_dir);
        files.sort();
        prop_assert!(!files.is_empty());
        let target = &files[file_pick.index(files.len())];

        // Flip one byte (skip zero-length files — nothing to corrupt).
        let mut bytes = std::fs::read(target).expect("read target file");
        prop_assume!(!bytes.is_empty());
        let pos = byte_pick.index(bytes.len());
        bytes[pos] ^= flip;
        std::fs::write(target, &bytes).expect("write corrupted file");

        // The damaged epoch must not validate (corrupt parse errors allowed).
        match validate_epoch(&storage, JOB, num_epochs) {
            Ok(valid) => prop_assert!(!valid, "corrupted epoch must not validate"),
            Err(CheckpointError::Corrupt { .. }) => {}
            Err(e) => prop_assert!(false, "unexpected error: {e}"),
        }

        // Recovery falls back to the previous sealed epoch — or reports
        // NoValidEpoch — but never restores the damaged one and never errors.
        match latest_valid_epoch(&storage, JOB) {
            Ok(recovered) => {
                prop_assert!(recovered < num_epochs, "must not recover the corrupted epoch");
                prop_assert_eq!(recovered, num_epochs - 1);
            }
            Err(CheckpointError::NoValidEpoch) => {
                prop_assert_eq!(num_epochs, 1, "fallback epoch existed but was not used");
            }
            Err(e) => prop_assert!(false, "recovery must not error: {e}"),
        }
    }
}

fn walk_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

/// The stale-hint regression pinned as a deterministic test: seal epoch 2 but
/// crash before its hint write — the hint still names epoch 1 (valid!), yet
/// recovery must return the sealed epoch 2, not the hinted epoch.
#[test]
fn stale_valid_hint_does_not_hide_newer_sealed_epoch() {
    let storage = LocalFsCheckpointStorage::ephemeral().expect("ephemeral storage");
    let snaps = vec![vec![1_u8, 2, 3]];

    let mut budget = usize::MAX;
    assert!(write_epoch_with_budget(&storage, 1, &snaps, &mut budget));

    // Epoch 2: snapshots + metadata + manifest written, hint write killed.
    let mut budget = ops_per_epoch(snaps.len()) - 1;
    assert!(!write_epoch_with_budget(&storage, 2, &snaps, &mut budget));

    assert!(validate_epoch(&storage, JOB, 2).expect("epoch 2 is sealed"));
    assert_eq!(
        latest_valid_epoch(&storage, JOB).expect("recovery"),
        2,
        "a stale-but-valid hint must not hide the newer sealed epoch"
    );
}
