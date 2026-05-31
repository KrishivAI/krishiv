use krishiv_checkpoint::CheckpointMetadata;

#[test]
fn checkpoint_metadata_roundtrip() {
    let meta = CheckpointMetadata {
        version: 1,
        epoch: 42,
        job_id: "job-1".into(),
        fencing_token: 7,
        coordinator_id: None,
        timestamp_ms: 1_700_000_000_000,
        source_offsets: vec![],
        operator_snapshots: vec![],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
    };
    let json = serde_json::to_string(&meta).expect("serialize");
    let decoded: CheckpointMetadata = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(meta.epoch, decoded.epoch);
    assert_eq!(meta.job_id, decoded.job_id);
}
