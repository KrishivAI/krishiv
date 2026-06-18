//! Structured debug report for production incident diagnosis (GAP-OB-07).
//!
//! When a production job stalls or produces wrong output, an operator can request
//! an [`ObservabilityReport`] to inspect the full state graph: job/stage/task
//! lifecycles, watermarks, source offsets, checkpoint epochs, shuffle partitions,
//! executor health, and event log entries — in a single JSON blob.
//!
//! This module defines the schema.  The actual population logic lives in the
//! coordinator (`krishiv-scheduler`) and is exposed through the CLI
//! (`krishiv diagnose <job_id>`).

use serde::Serialize;
use std::collections::BTreeMap;

/// Complete state snapshot for debugging a production incident.
///
/// Serialize with `serde_json::to_string_pretty` to get a human-readable JSON dump
/// suitable for incident tickets, Slack threads, or structured log ingestion.
#[derive(Debug, Clone, Serialize)]
pub struct ObservabilityReport {
    /// Wall-clock time the report was generated (RFC 3339).
    pub generated_at: String,
    /// Coordinator identity that produced this report.
    pub coordinator_id: String,

    /// Top-level job state.
    pub job: ReportJob,
    /// Per-stage detail.
    pub stages: Vec<ReportStage>,
    /// Executor pool health.
    pub executors: Vec<ReportExecutor>,
    /// Latest checkpoint epoch metadata.
    pub checkpoint: Option<ReportCheckpoint>,
    /// Per-job shuffle partition status.
    pub shuffle_partitions: Option<ReportShuffle>,
    /// Watermark and source offset state for streaming jobs.
    pub streaming_state: Option<ReportStreamingState>,
    /// Recent structured log / event-log entries (most recent 50).
    pub recent_events: Vec<ReportEvent>,
    /// Connector metrics aggregated for this job.
    pub connector_metrics: Option<ReportConnectorMetrics>,
}

// Sub-structures

#[derive(Debug, Clone, Serialize)]
pub struct ReportJob {
    pub job_id: String,
    pub job_name: String,
    pub job_kind: String,
    pub state: String,
    /// UNIX ms since epoch when the job was submitted.
    pub submitted_at_ms: u64,
    /// Scheduling priority (0–255).
    pub priority: u8,
    /// Assigned namespace, if any.
    pub namespace_id: Option<String>,
    /// Total elapsed wall-clock time in ms since submission.
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportStage {
    pub stage_id: String,
    pub stage_name: String,
    pub state: String,
    /// Tasks broken down by state: { "pending": 3, "running": 1, ... }
    pub task_counts: BTreeMap<String, usize>,
    /// Upstream stage ids this stage depends on.
    pub upstream_stage_ids: Vec<String>,
    /// Expected shuffle output partition count, if declared.
    pub output_partition_count: Option<u32>,
    /// Per-task attempt detail for running/failed tasks.
    pub task_details: Vec<ReportTask>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportTask {
    pub task_id: String,
    pub state: String,
    /// Assigned executor id, if any.
    pub executor_id: Option<String>,
    /// Current attempt id.
    pub attempt_id: u32,
    /// Total failure count across all attempts.
    pub failure_count: u32,
    /// Most recent failure reason, if any.
    pub last_failure_reason: Option<String>,
    /// Watermark (ms) if this is a streaming task.
    pub watermark_ms: Option<i64>,
    /// Encoded source offset if this is a streaming source task.
    pub source_offset: Option<String>,
    /// Runtime statistics for the last completed attempt.
    pub runtime_stats: Option<ReportRuntimeStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportRuntimeStats {
    pub input_rows: u64,
    pub output_rows: u64,
    pub cpu_nanos: u64,
    pub memory_bytes: u64,
    pub spill_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportExecutor {
    pub executor_id: String,
    pub host: String,
    pub state: String,
    pub slots: usize,
    pub active_tasks: usize,
    /// Heartbeat age in ticks since last healthy heartbeat.
    pub heartbeat_age_ticks: u64,
    /// Advertised task endpoint, if any.
    pub task_endpoint: Option<String>,
    /// Reported memory usage, if available.
    pub memory_used_bytes: Option<u64>,
    /// Reported memory limit, if available.
    pub memory_limit_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportCheckpoint {
    /// Latest committed epoch.
    pub latest_epoch: u64,
    /// Fencing token at commit time.
    pub fencing_token: u64,
    /// Wall-clock commit time (ms since epoch).
    pub committed_at_ms: u64,
    /// Per-partition source offsets at commit time. { partition_id: offset }
    pub source_offsets: BTreeMap<String, i64>,
    /// Per-operator snapshot paths. { operator_id: snapshot_path }
    pub operator_snapshots: BTreeMap<String, String>,
    /// Whether the latest epoch is a savepoint.
    pub is_savepoint: bool,
    /// Savepoint label, if set.
    pub savepoint_label: Option<String>,
    /// Iceberg snapshot id for this epoch, if tracked.
    pub iceberg_snapshot_id: Option<u64>,
    /// Kafka topic→offset map, if tracked.
    pub kafka_offsets: Option<BTreeMap<String, i64>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportShuffle {
    /// Total partitions pending.
    pub pending: u64,
    /// Total partitions available.
    pub available: u64,
    /// Total partitions failed.
    pub failed: u64,
    /// Total bytes written to the shuffle store for this job.
    pub total_bytes_written: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportStreamingState {
    /// Global low watermark (ms) across all source tasks.
    pub low_watermark_ms: i64,
    /// Per-source watermark: { source_id: watermark_ms }
    pub source_watermarks: BTreeMap<String, i64>,
    /// Per-source offset lag: { source_id: lag }
    pub source_offsets_lag: BTreeMap<String, i64>,
    /// Total streaming rows emitted so far.
    pub total_rows_emitted: u64,
    /// State backend key count.
    pub state_key_count: u64,
    /// State backend byte size.
    pub state_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportEvent {
    /// UNIX ms timestamp.
    pub timestamp_ms: u64,
    /// Event kind (e.g. "JobSubmitted", "TaskFailed").
    pub event_kind: String,
    /// Human-readable detail string.
    pub detail: String,
    /// Optional job_id for correlation.
    pub job_id: Option<String>,
    /// Optional stage_id for correlation.
    pub stage_id: Option<String>,
    /// Optional task_id for correlation.
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportConnectorMetrics {
    /// Rows read from all sources.
    pub total_rows_read: u64,
    /// Bytes read from all sources.
    pub total_bytes_read: u64,
    /// Rows written to all sinks.
    pub total_rows_written: u64,
    /// Bytes written to all sinks.
    pub total_bytes_written: u64,
    /// Per-source offset lag.
    pub source_offset_lag: BTreeMap<String, i64>,
    /// Latest committed sink epoch.
    pub sink_commit_epoch: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_report_serializes_to_valid_json() {
        let report = ObservabilityReport {
            generated_at: String::new(),
            coordinator_id: "coord-1".into(),
            job: ReportJob {
                job_id: String::new(),
                job_name: String::new(),
                job_kind: String::new(),
                state: String::new(),
                submitted_at_ms: 0,
                priority: 128,
                namespace_id: None,
                elapsed_ms: 0,
            },
            stages: Vec::new(),
            executors: Vec::new(),
            checkpoint: None,
            shuffle_partitions: None,
            streaming_state: None,
            recent_events: Vec::new(),
            connector_metrics: None,
        };
        let json = serde_json::to_string_pretty(&report).expect("should serialize");
        assert!(json.contains("coordinator_id"));
        assert!(json.contains("coord-1"));
        assert!(json.contains("generated_at"));
    }

    #[test]
    fn report_with_stages_and_executors_serializes() {
        let mut report = ObservabilityReport {
            generated_at: String::new(),
            coordinator_id: "coord-1".into(),
            job: ReportJob {
                job_id: String::new(),
                job_name: String::new(),
                job_kind: String::new(),
                state: String::new(),
                submitted_at_ms: 0,
                priority: 128,
                namespace_id: None,
                elapsed_ms: 0,
            },
            stages: Vec::new(),
            executors: Vec::new(),
            checkpoint: None,
            shuffle_partitions: None,
            streaming_state: None,
            recent_events: Vec::new(),
            connector_metrics: None,
        };
        report.stages.push(ReportStage {
            stage_id: "stage-0".into(),
            stage_name: "sql-stage".into(),
            state: "running".into(),
            task_counts: {
                let mut m = BTreeMap::new();
                m.insert("running".into(), 1);
                m.insert("pending".into(), 2);
                m
            },
            upstream_stage_ids: vec![],
            output_partition_count: Some(8),
            task_details: vec![ReportTask {
                task_id: "task-0".into(),
                state: "running".into(),
                executor_id: Some("exec-1".into()),
                attempt_id: 1,
                failure_count: 0,
                last_failure_reason: None,
                watermark_ms: Some(1_620_000_000_000),
                source_offset: Some("a2V5OjEyMzQ=".into()),
                runtime_stats: Some(ReportRuntimeStats {
                    input_rows: 1000,
                    output_rows: 500,
                    cpu_nanos: 50_000_000,
                    memory_bytes: 1048576,
                    spill_bytes: 0,
                }),
            }],
        });
        report.executors.push(ReportExecutor {
            executor_id: "exec-1".into(),
            host: "localhost".into(),
            state: "healthy".into(),
            slots: 8,
            active_tasks: 1,
            heartbeat_age_ticks: 3,
            task_endpoint: Some("http://localhost:51001".into()),
            memory_used_bytes: Some(512_000_000),
            memory_limit_bytes: Some(4_294_967_296),
        });

        let json = serde_json::to_string_pretty(&report).expect("should serialize");
        assert!(json.contains("stage-0"));
        assert!(json.contains("exec-1"));
        assert!(json.contains("health"));
    }

    #[test]
    fn report_with_checkpoint_serializes() {
        let mut report = ObservabilityReport {
            generated_at: String::new(),
            coordinator_id: "coord-1".into(),
            job: ReportJob {
                job_id: String::new(),
                job_name: String::new(),
                job_kind: String::new(),
                state: String::new(),
                submitted_at_ms: 0,
                priority: 128,
                namespace_id: None,
                elapsed_ms: 0,
            },
            stages: Vec::new(),
            executors: Vec::new(),
            checkpoint: None,
            shuffle_partitions: None,
            streaming_state: None,
            recent_events: Vec::new(),
            connector_metrics: None,
        };
        report.checkpoint = Some(ReportCheckpoint {
            latest_epoch: 42,
            fencing_token: 7,
            committed_at_ms: 1_700_000_000_000,
            source_offsets: {
                let mut m = BTreeMap::new();
                m.insert("kafka-0".into(), 15_000i64);
                m
            },
            operator_snapshots: {
                let mut m = BTreeMap::new();
                m.insert(
                    "operator-task-0".into(),
                    "checkpoints/job-a/042/op-0/task-0/state.bin".into(),
                );
                m
            },
            is_savepoint: false,
            savepoint_label: None,
            iceberg_snapshot_id: Some(100),
            kafka_offsets: {
                let mut m = BTreeMap::new();
                m.insert("events".into(), 15_000);
                Some(m)
            },
        });

        let json = serde_json::to_string_pretty(&report).expect("should serialize");
        assert!(json.contains("fencing_token"));
        assert!(json.contains("kafka-0"));
        assert!(json.contains("15000"));
        assert!(json.contains("latest_epoch"));
    }

    #[test]
    fn report_with_streaming_state_serializes() {
        let mut report = ObservabilityReport {
            generated_at: String::new(),
            coordinator_id: "coord-1".into(),
            job: ReportJob {
                job_id: String::new(),
                job_name: String::new(),
                job_kind: String::new(),
                state: String::new(),
                submitted_at_ms: 0,
                priority: 128,
                namespace_id: None,
                elapsed_ms: 0,
            },
            stages: Vec::new(),
            executors: Vec::new(),
            checkpoint: None,
            shuffle_partitions: None,
            streaming_state: None,
            recent_events: Vec::new(),
            connector_metrics: None,
        };
        report.streaming_state = Some(ReportStreamingState {
            low_watermark_ms: 1_620_000_000_000,
            source_watermarks: {
                let mut m = BTreeMap::new();
                m.insert("kafka-topic-0".into(), 1_620_000_000_000);
                m
            },
            source_offsets_lag: {
                let mut m = BTreeMap::new();
                m.insert("kafka-topic-0".into(), 1200);
                m
            },
            total_rows_emitted: 5_000_000,
            state_key_count: 100_000,
            state_bytes: 50_000_000,
        });

        let json = serde_json::to_string_pretty(&report).expect("should serialize");
        assert!(json.contains("low_watermark_ms"));
        assert!(json.contains("5000000"));
    }

    #[test]
    fn report_with_recent_events_serializes() {
        let mut report = ObservabilityReport {
            generated_at: String::new(),
            coordinator_id: "coord-1".into(),
            job: ReportJob {
                job_id: String::new(),
                job_name: String::new(),
                job_kind: String::new(),
                state: String::new(),
                submitted_at_ms: 0,
                priority: 128,
                namespace_id: None,
                elapsed_ms: 0,
            },
            stages: Vec::new(),
            executors: Vec::new(),
            checkpoint: None,
            shuffle_partitions: None,
            streaming_state: None,
            recent_events: Vec::new(),
            connector_metrics: None,
        };
        report.recent_events.push(ReportEvent {
            timestamp_ms: 1_700_000_000_000,
            event_kind: "TaskFailed".into(),
            detail: "executor lost during task execution".into(),
            job_id: Some("job-a".into()),
            stage_id: Some("stage-1".into()),
            task_id: Some("task-3".into()),
        });

        let json = serde_json::to_string_pretty(&report).expect("should serialize");
        assert!(json.contains("TaskFailed"));
        assert!(json.contains("job-a"));
    }
}
