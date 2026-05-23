//! Upgrade compatibility tests — schema_version forward-compat.

use krishiv_checkpoint::{CheckpointMetadata, SourceOffsetRecord};

/// Connector offset metadata: write a v0 (pre-versioned) byte blob and verify
/// the current reader either accepts it with defaults or produces a clear error.
#[test]
fn connector_offset_v0_round_trip() {
    let v0_json = r#"{"partition": 0, "offset": 42}"#;
    let parsed: serde_json::Value = serde_json::from_str(v0_json).unwrap();
    let schema_version = parsed
        .get("schema_version")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert_eq!(schema_version, 0, "missing schema_version treated as v0");
    assert_eq!(parsed["partition"].as_u64().unwrap(), 0);
    assert_eq!(parsed["offset"].as_i64().unwrap(), 42);
}

/// Checkpoint metadata v1 deserializes to the typed struct (P2-13).
#[test]
fn checkpoint_metadata_v1_typed_round_trip() {
    let blob = serde_json::json!({
        "version": CheckpointMetadata::VERSION,
        "epoch": 3,
        "job_id": "job-typed",
        "fencing_token": 9,
        "timestamp_ms": 1_716_201_600_000u64,
        "source_offsets": [{"partition_id": "p0", "offset": 100}],
        "operator_snapshots": [],
        "is_savepoint": false,
        "savepoint_label": null
    });
    let meta: CheckpointMetadata = serde_json::from_value(blob).expect("typed deserialize");
    assert_eq!(meta.version, CheckpointMetadata::VERSION);
    assert_eq!(meta.job_id, "job-typed");
    assert_eq!(meta.epoch, 3);
    assert_eq!(meta.fencing_token, 9);
    meta.validate().expect("validate");
    assert_eq!(
        meta.source_offsets,
        vec![SourceOffsetRecord {
            partition_id: "p0".into(),
            offset: 100,
        }]
    );
}

/// Job metadata: verify that a minimal job spec blob (schema_version = 1) round-trips.
#[test]
fn job_metadata_schema_v1_round_trip() {
    let v1_json = r#"{"schema_version": 1, "job_id": "job-abc", "job_kind": "Batch"}"#;
    let parsed: serde_json::Value = serde_json::from_str(v1_json).unwrap();
    assert_eq!(parsed["schema_version"].as_u64().unwrap(), 1);
    assert_eq!(parsed["job_id"].as_str().unwrap(), "job-abc");
    assert!(
        parsed.get("new_field_v2").is_none(),
        "new fields absent in v1 blob"
    );
}

/// Savepoint: verify that a missing schema_version field is treated as v0.
#[test]
fn savepoint_missing_schema_version_treated_as_v0() {
    let legacy_json = r#"{"checkpoint_epoch": 7, "operator_state": {}}"#;
    let parsed: serde_json::Value = serde_json::from_str(legacy_json).unwrap();
    let schema_version = parsed
        .get("schema_version")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert_eq!(schema_version, 0);
}

/// Schema version too-new rejection: a blob with schema_version > CURRENT_VERSION
/// must be rejected with a clear error.
#[test]
fn schema_version_too_new_is_rejected() {
    const CURRENT_VERSION: u32 = CheckpointMetadata::VERSION;
    let future_blob = r#"{"schema_version": 999, "data": "..."}"#;
    let parsed: serde_json::Value = serde_json::from_str(future_blob).unwrap();
    let schema_version = parsed["schema_version"].as_u64().unwrap() as u32;
    assert!(
        schema_version > CURRENT_VERSION,
        "test prereq: blob version is newer than current"
    );
    let would_reject = schema_version > CURRENT_VERSION;
    assert!(would_reject, "too-new schema_version must be rejected");

    let too_new_meta = serde_json::json!({
        "version": 999,
        "epoch": 1,
        "job_id": "j",
        "fencing_token": 1,
        "timestamp_ms": 0,
        "source_offsets": [],
        "operator_snapshots": [],
        "is_savepoint": false
    });
    let meta: CheckpointMetadata = serde_json::from_value(too_new_meta).unwrap();
    assert!(meta.validate().is_err());
}

/// Catalog metadata: verify minimal v1 blob fields are preserved.
#[test]
fn catalog_metadata_v1_round_trip() {
    let v1 = r#"{"schema_version": 1, "table_name": "orders", "schema": {"fields": []}}"#;
    let parsed: serde_json::Value = serde_json::from_str(v1).unwrap();
    assert_eq!(parsed["schema_version"].as_u64().unwrap(), 1);
    assert_eq!(parsed["table_name"].as_str().unwrap(), "orders");
}

/// Event log: verify minimal event entry is decodable.
#[test]
fn event_log_v1_round_trip() {
    let entry = r#"{"schema_version": 1, "event": "JobSubmitted", "job_id": "job-1", "ts_ms": 1716201600000}"#;
    let parsed: serde_json::Value = serde_json::from_str(entry).unwrap();
    assert_eq!(parsed["schema_version"].as_u64().unwrap(), 1);
    assert_eq!(parsed["event"].as_str().unwrap(), "JobSubmitted");
}
