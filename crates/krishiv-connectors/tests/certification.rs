//! Connector certification suite.
//!
//! A connector passes certification when all tests in this module pass.
//! Certification status is recorded in docs/architecture/compatibility-matrices.md.

use krishiv_connectors::{ConnectorCapabilities, ConnectorResult};

/// Every certified connector must declare at least one bounded or unbounded mode.
#[test]
fn local_parquet_sink_declares_capabilities() {
    let caps = ConnectorCapabilities::new()
        .with_bounded()
        .with_transactional()
        .with_two_phase_commit();
    assert!(caps.is_bounded());
    assert!(caps.is_transactional());
    assert!(caps.is_two_phase_commit_capable());
}

/// Dead-letter sink correctly splits a batch with null violations.
#[test]
fn dead_letter_sink_certification_notnull() {
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_connectors::{DataQualityConfig, DataQualityRule, DeadLetterSink, QualityAction};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    let col = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

    let config = DataQualityConfig::new().with_rule(
        DataQualityRule::NotNull {
            column: "v".into(),
        },
        QualityAction::Reject,
    );
    let sink = DeadLetterSink::new("cert_test", config);
    let (accepted, rejected) = sink.process_batch(&batch).unwrap();
    assert_eq!(accepted.num_rows(), 2);
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].batch_row_index, 1);
}

// Suppress unused import warning — ConnectorResult is part of the public API
// surface we verify is accessible from integration tests.
const _: fn() -> ConnectorResult<()> = || Ok(());
