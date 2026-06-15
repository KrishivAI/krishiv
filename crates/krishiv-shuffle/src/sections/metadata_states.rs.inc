// ── ShuffleMetadata: comprehensive state transitions ─────────────────

#[test]
fn metadata_state_transitions_pending_available() {
    let mut meta = ShuffleMetadata::new();
    let p = ShufflePath::new("j", "s", 0);
    assert_eq!(meta.state(&p), None);
    meta.mark_pending(&p).unwrap();
    assert_eq!(meta.state(&p), Some(&PartitionState::Pending));
    meta.mark_available(&p);
    assert_eq!(meta.state(&p), Some(&PartitionState::Available));
}

#[test]
fn metadata_state_transitions_pending_failed() {
    let mut meta = ShuffleMetadata::new();
    let p = ShufflePath::new("j", "s", 1);
    meta.mark_pending(&p).unwrap();
    meta.mark_failed(&p, "timeout".into());
    assert_eq!(
        meta.state(&p),
        Some(&PartitionState::Failed {
            reason: "timeout".into()
        })
    );
}

#[test]
fn metadata_available_count_and_total_count() {
    let mut meta = ShuffleMetadata::new();
    let p0 = ShufflePath::new("j", "s", 0);
    let p1 = ShufflePath::new("j", "s", 1);
    let p2 = ShufflePath::new("j", "s", 2);

    meta.mark_pending(&p0).unwrap();
    meta.mark_pending(&p1).unwrap();
    assert_eq!(meta.total_count(), 2);
    assert_eq!(meta.available_count(), 0);

    meta.mark_available(&p0);
    assert_eq!(meta.available_count(), 1);

    meta.mark_pending(&p2).unwrap();
    meta.mark_available(&p2);
    assert_eq!(meta.available_count(), 2);
    assert_eq!(meta.total_count(), 3);
}

#[test]
fn metadata_available_count_excludes_failed() {
    let mut meta = ShuffleMetadata::new();
    let p = ShufflePath::new("j", "s", 0);
    meta.mark_pending(&p).unwrap();
    meta.mark_failed(&p, "oops".into());
    assert_eq!(meta.available_count(), 0);
    assert_eq!(meta.total_count(), 1);
}

#[test]
fn metadata_state_returns_none_for_unknown_path() {
    let meta = ShuffleMetadata::new();
    let p = ShufflePath::new("missing", "s", 0);
    assert!(meta.state(&p).is_none());
}

#[test]
fn metadata_cap_error_has_correct_limit() {
    let mut meta = ShuffleMetadata::new().with_max_partitions(3);
    meta.mark_pending(&ShufflePath::new("j", "s", 0)).unwrap();
    meta.mark_pending(&ShufflePath::new("j", "s", 1)).unwrap();
    meta.mark_pending(&ShufflePath::new("j", "s", 2)).unwrap();
    let err = meta
        .mark_pending(&ShufflePath::new("j", "s", 3))
        .unwrap_err();
    match err {
        ShuffleError::TooManyPartitions { limit } => assert_eq!(limit, 3),
        other => panic!("expected TooManyPartitions, got {other}"),
    }
}

#[test]
fn metadata_all_available_treats_pending_as_not_available() {
    let mut meta = ShuffleMetadata::new();
    let p = ShufflePath::new("j", "s", 0);
    meta.mark_pending(&p).unwrap();
    assert!(!meta.all_available(&[p]));
}

#[test]
fn metadata_all_available_treats_failed_as_not_available() {
    let mut meta = ShuffleMetadata::new();
    let p = ShufflePath::new("j", "s", 0);
    meta.mark_pending(&p).unwrap();
    meta.mark_failed(&p, "err".into());
    assert!(!meta.all_available(&[p]));
}

