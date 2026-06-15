use std::sync::Arc;

use super::*;
use crate::ExecutorAssignmentInbox;

#[test]
fn format_failure_message_basic() {
        let msg = format_failure_message("sql: select 1", "table not found");
        assert!(msg.contains("executor failed fragment 'sql: select 1'"));
        assert!(msg.contains("table not found"));
}

#[test]
fn format_failure_message_trims_whitespace() {
        let msg = format_failure_message("  sql: select 1  ", "  error  ");
        assert!(msg.contains("sql: select 1"));
        assert!(msg.contains("error"));
        assert!(!msg.contains("  "));
}

#[test]
fn format_failure_message_truncates_long_message() {
        let long_error = "x".repeat(10000);
        let msg = format_failure_message("fragment", &long_error);
        assert!(msg.len() <= TASK_FAILURE_MESSAGE_MAX_BYTES + 10);
        assert!(msg.ends_with('…'));
}

#[test]
fn format_failure_message_within_limit() {
        let short_error = "short error";
        let msg = format_failure_message("fragment", &short_error);
        assert!(!msg.ends_with('…'));
        assert!(msg.contains("short error"));
}

// ── ExecutorTaskOutputKind tests ────────────────────────────────────────

#[test]
fn task_output_kind_as_str() {
        assert_eq!(ExecutorTaskOutputKind::Sql.as_str(), "sql");
        assert_eq!(
            ExecutorTaskOutputKind::ConnectorPipeline.as_str(),
            "connector_pipeline"
        );
        assert_eq!(ExecutorTaskOutputKind::Cancelled.as_str(), "cancelled");
        assert_eq!(
            ExecutorTaskOutputKind::ShuffleWrite.as_str(),
            "shuffle_write"
        );
        assert_eq!(
            ExecutorTaskOutputKind::StreamingWindow.as_str(),
            "streaming_window"
        );
}

#[test]
fn task_output_kind_debug() {
        let kind = ExecutorTaskOutputKind::Sql;
        let debug = format!("{:?}", kind);
        assert_eq!(debug, "Sql");
}

#[test]
fn task_output_kind_clone() {
        let kind = ExecutorTaskOutputKind::StreamingWindow;
        let cloned = kind;
        assert_eq!(kind, cloned);
}

// ── ExecutorTaskOutput tests ────────────────────────────────────────────

#[test]
fn task_output_sql_constructor() {
        let output = ExecutorTaskOutput::sql(10, 2, 3);
        assert_eq!(output.kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(output.row_count(), 10);
        assert_eq!(output.batch_count(), 2);
        assert_eq!(output.column_count(), 3);
        assert!(output.shuffle_partitions().is_empty());
        assert!(output.runtime_stats.is_none());
        assert!(output.record_batches().is_empty());
        assert!(output.watermark_ms().is_none());
}

#[test]
fn task_output_cancelled_constructor() {
        let output = ExecutorTaskOutput::cancelled();
        assert_eq!(output.kind(), ExecutorTaskOutputKind::Cancelled);
        assert_eq!(output.row_count(), 0);
}

#[test]
fn task_output_shuffle_write_constructor() {
        let output = ExecutorTaskOutput::shuffle_write(100, vec![]);
        assert_eq!(output.kind(), ExecutorTaskOutputKind::ShuffleWrite);
        assert_eq!(output.row_count(), 100);
        assert_eq!(output.batch_count(), 0);
}

#[test]
fn task_output_streaming_window_constructor() {
        let output = ExecutorTaskOutput::streaming_window(50, 3, 4, vec![]);
        assert_eq!(output.kind(), ExecutorTaskOutputKind::StreamingWindow);
        assert_eq!(output.row_count(), 50);
        assert_eq!(output.batch_count(), 3);
        assert_eq!(output.column_count(), 4);
}

#[test]
fn task_output_with_watermark() {
        let output = ExecutorTaskOutput::streaming_window(10, 1, 2, vec![]).with_watermark_ms(5000);
        assert_eq!(output.watermark_ms(), Some(5000));
}

#[test]
fn task_output_without_watermark() {
        let output = ExecutorTaskOutput::sql(1, 1, 1);
        assert!(output.watermark_ms().is_none());
}

#[test]
fn task_output_to_task_output_metadata() {
        let output = ExecutorTaskOutput::sql(10, 2, 3);
        let meta = output.to_task_output_metadata();
        assert_eq!(meta.output_kind(), "sql");
        assert_eq!(meta.row_count(), 10);
        assert_eq!(meta.batch_count(), 2);
        assert_eq!(meta.column_count(), 3);
}

#[test]
fn task_output_to_metadata_with_watermark() {
        let output = ExecutorTaskOutput::streaming_window(5, 1, 2, vec![]).with_watermark_ms(7777);
        let meta = output.to_task_output_metadata();
        assert_eq!(meta.watermark_ms(), Some(7777));
}

// ── LocalParquetPartition tests ─────────────────────────────────────────

#[test]
fn local_parquet_partition_parse_valid() {
        let partition = krishiv_proto::InputPartition::new(
            "part-1",
            "local-parquet:my_table:/data/file.parquet",
        );
        let parsed = LocalParquetPartition::parse(&partition).unwrap().unwrap();
        assert_eq!(parsed.table_name(), "my_table");
        assert_eq!(parsed.path(), std::path::Path::new("/data/file.parquet"));
}

#[test]
fn local_parquet_partition_parse_non_local_returns_none() {
        let partition = krishiv_proto::InputPartition::new("part-1", "not-a-local-parquet");
        let parsed = LocalParquetPartition::parse(&partition).unwrap();
        assert!(parsed.is_none());
}

#[test]
fn local_parquet_partition_parse_malformed_no_colon() {
        let partition =
            krishiv_proto::InputPartition::new("part-1", "local-parquet:only_table_name");
        let err = LocalParquetPartition::parse(&partition).unwrap_err();
        assert!(err.to_string().contains("local-parquet:<table>:<path>"));
}

#[test]
fn parse_local_parquet_partitions_empty() {
        let parsed = parse_local_parquet_partitions(&[]).unwrap();
        assert!(parsed.is_empty());
}

#[test]
fn parse_local_parquet_partitions_skips_non_local() {
        let partitions = vec![
            krishiv_proto::InputPartition::new("p1", "not-local"),
            krishiv_proto::InputPartition::new("p2", "local-parquet:t:/f.parquet"),
        ];
        let parsed = parse_local_parquet_partitions(&partitions).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].table_name(), "t");
}

#[test]
fn parse_local_parquet_partitions_duplicate_table_name() {
        let partitions = vec![
            krishiv_proto::InputPartition::new("p1", "local-parquet:people:/f1.parquet"),
            krishiv_proto::InputPartition::new("p2", "local-parquet:people:/f2.parquet"),
        ];
        let err = parse_local_parquet_partitions(&partitions).unwrap_err();
        assert!(
            err.to_string()
                .contains("duplicate local Parquet table name")
        );
}

// ── TaskRunner tests ────────────────────────────────────────────────────

#[test]
fn task_runner_new() {
        let task_id = krishiv_proto::TaskId::try_new("task-1").unwrap();
        let runner = TaskRunner::new(task_id.clone());
        assert_eq!(runner.task_id, task_id);
        assert_eq!(runner.last_acked_epoch, 0);
        assert!(runner.kafka_source_offsets.is_empty());
        assert!(runner.operator_id.starts_with("operator-"));
}

#[test]
fn task_runner_with_kafka_source_offsets() {
        use krishiv_connectors::kafka::KafkaOffset;
        let task_id = krishiv_proto::TaskId::try_new("task-1").unwrap();
        let offsets = vec![
            KafkaOffset {
                topic: "events".into(),
                partition: 0,
                offset: 42,
            },
            KafkaOffset {
                topic: "events".into(),
                partition: 1,
                offset: 7,
            },
        ];
        let runner = TaskRunner::new(task_id).with_kafka_source_offsets(offsets.clone());
        assert_eq!(runner.kafka_source_offsets, offsets);
}

#[test]
fn task_runner_handle_checkpoint_stale_epoch() {
        let task_id = krishiv_proto::TaskId::try_new("task-1").unwrap();
        let mut runner = TaskRunner::new(task_id);
        runner.last_acked_epoch = 5;

        let req = krishiv_proto::InitiateCheckpointRequest {
            job_id: krishiv_proto::JobId::try_new("job-1").unwrap(),
            epoch: 3,
            fencing_token: krishiv_proto::FencingToken::initial(),
        };
        let state = CheckpointStateHandle::from_backend(
            krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
        );
        let storage = krishiv_state::checkpoint::LocalFsCheckpointStorage::ephemeral().unwrap();
        let ack = runner
            .handle_initiate_checkpoint(req, &state, &storage)
            .unwrap();
        assert_eq!(ack.epoch, 5); // signals stale
}

#[test]
fn task_runner_handle_checkpoint_new_epoch() {
        let task_id = krishiv_proto::TaskId::try_new("task-1").unwrap();
        let mut runner = TaskRunner::new(task_id);

        let req = krishiv_proto::InitiateCheckpointRequest {
            job_id: krishiv_proto::JobId::try_new("job-1").unwrap(),
            epoch: 1,
            fencing_token: krishiv_proto::FencingToken::initial(),
        };
        let state = CheckpointStateHandle::from_backend(
            krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
        );
        let storage = krishiv_state::checkpoint::LocalFsCheckpointStorage::ephemeral().unwrap();
        let ack = runner
            .handle_initiate_checkpoint(req, &state, &storage)
            .unwrap();
        assert_eq!(ack.epoch, 1);
        assert_eq!(runner.last_acked_epoch, 1);
}

#[test]
fn progress_callback_invoked_with_snapshot() {
        use std::sync::Mutex;
        struct TestCallback {
            snapshots: Mutex<Vec<StreamingProgressSnapshot>>,
        }
        impl StreamingProgressCallback for TestCallback {
            fn on_progress(&self, snapshot: &StreamingProgressSnapshot) {
                self.snapshots.lock().unwrap().push(snapshot.clone());
            }
        }
        let callback = Arc::new(TestCallback {
            snapshots: Mutex::new(Vec::new()),
        });
        let inbox = ExecutorAssignmentInbox::new();
        let runner = ExecutorTaskRunner::new(inbox)
            .with_progress_callback(callback.clone() as SharedProgressCallback);

        let snapshot = StreamingProgressSnapshot {
            task_id: "t0".into(),
            job_id: "j0".into(),
            watermark_ms: 1000,
            rows_emitted: 42,
            batches_emitted: 7,
            state_bytes: 4096,
            source_offset: Some(vec![0, 1, 2]),
            timestamp_ms: 5000,
        };
        runner.report_streaming_progress(&snapshot);

        let captured = callback.snapshots.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].task_id, "t0");
        assert_eq!(captured[0].rows_emitted, 42);
        assert_eq!(captured[0].watermark_ms, 1000);
}
