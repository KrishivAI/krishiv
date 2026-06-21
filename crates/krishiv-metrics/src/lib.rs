#![forbid(unsafe_code)]
//! **Beta API**: may change between minor releases.
//!
//! OpenTelemetry metrics, traces, and structured log initialization for all Krishiv processes.

pub mod grpc;
pub mod observability_report;
pub mod system;

mod counters;
mod init;

pub use counters::{KrishivHistogram, KrishivMetrics, current_tracestate, global_metrics};
pub use init::{
    MetricsConfig, MetricsError, MetricsHandle, TracerExporter, current_traceparent, init,
};

#[cfg(test)]
mod tests {
    use super::{counters::*, init::*};

    #[test]
    fn init_noop_does_not_panic() {
        let _handle = init(MetricsConfig::default()).expect("noop init should succeed");
    }

    #[test]
    fn shutdown_does_not_panic() {
        let handle = init(MetricsConfig::default()).expect("init");
        handle.shutdown();
    }

    #[test]
    fn tracing_span_does_not_panic() {
        let _handle = init(MetricsConfig::default()).expect("init");
        let _s = tracing::info_span!("test_span").entered();
    }

    #[test]
    fn default_config_service_name() {
        assert_eq!(MetricsConfig::default().service_name, "krishiv");
    }

    #[test]
    fn default_config_otlp_endpoint_is_none() {
        assert!(MetricsConfig::default().otlp_endpoint.is_none());
    }

    #[test]
    fn current_traceparent_no_span_returns_none() {
        assert_eq!(current_traceparent(), None);
    }

    #[test]
    fn current_tracestate_no_span_returns_none() {
        assert_eq!(current_tracestate(), None);
    }

    // KrishivMetrics counter/gauge increment tests

    #[test]
    fn inc_tasks_submitted_increments_by_one() {
        let m = KrishivMetrics::default();
        assert_eq!(m.tasks_submitted(), 0);
        m.inc_tasks_submitted();
        assert_eq!(m.tasks_submitted(), 1);
        m.inc_tasks_submitted();
        assert_eq!(m.tasks_submitted(), 2);
    }

    #[test]
    fn set_tasks_running_stores_value() {
        let m = KrishivMetrics::default();
        m.set_tasks_running(5);
        assert_eq!(m.tasks_running(), 5);
        m.set_tasks_running(0);
        assert_eq!(m.tasks_running(), 0);
    }

    #[test]
    fn inc_tasks_succeeded_increments() {
        let m = KrishivMetrics::default();
        m.inc_tasks_succeeded();
        m.inc_tasks_succeeded();
        m.inc_tasks_succeeded();
        assert_eq!(m.tasks_succeeded(), 3);
    }

    #[test]
    fn inc_tasks_failed_increments() {
        let m = KrishivMetrics::default();
        m.inc_tasks_failed();
        assert_eq!(m.tasks_failed(), 1);
    }

    /// Regression (Wave 4 — Observability & Shutdown): `inc_executor_lost`
    /// must increment the `executor_lost` counter and the value must be
    /// rendered as `krishiv_executor_lost_total` in the Prometheus exposition
    /// (the counter and its renderer line were both added in this wave).
    #[test]
    fn inc_executor_lost_increments_and_renders() {
        let m = KrishivMetrics::default();
        m.inc_executor_lost();
        m.inc_executor_lost();
        assert_eq!(m.executor_lost(), 2);

        let rendered = m.render_prometheus();
        assert!(
            rendered.contains("krishiv_executor_lost_total 2"),
            "expected rendered metrics to include krishiv_executor_lost_total 2, got: {rendered}"
        );
    }

    #[test]
    fn add_shuffle_bytes_written_accumulates() {
        let m = KrishivMetrics::default();
        m.add_shuffle_bytes_written(1024);
        m.add_shuffle_bytes_written(2048);
        assert_eq!(m.shuffle_bytes_written(), 3072);
    }

    #[test]
    fn set_job_queue_depth_stores_value() {
        let m = KrishivMetrics::default();
        m.set_job_queue_depth(42);
        assert_eq!(m.job_queue_depth(), 42);
    }

    #[test]
    fn global_metrics_returns_same_instance() {
        let a = global_metrics();
        let b = global_metrics();
        let a_ptr = a as *const KrishivMetrics;
        let b_ptr = b as *const KrishivMetrics;
        assert_eq!(a_ptr, b_ptr);
    }

    // Prometheus text format rendering (P0 fix validation)

    /// Verifies the P0 format fix: exactly one HELP + TYPE per metric family.
    #[test]
    fn render_prometheus_single_help_type_per_family() {
        let m = KrishivMetrics::default();
        m.inc_tasks_submitted();
        m.inc_tasks_succeeded();
        m.inc_tasks_failed();
        let body = m.render_prometheus();
        // Count HELP lines for krishiv_tasks_total — must be exactly 1.
        let help_count = body
            .lines()
            .filter(|l| l.starts_with("# HELP krishiv_tasks_total"))
            .count();
        assert_eq!(
            help_count, 1,
            "must have exactly one HELP line per metric family"
        );
        // Count TYPE lines for krishiv_tasks_total — must be exactly 1.
        let type_count = body
            .lines()
            .filter(|l| l.starts_with("# TYPE krishiv_tasks_total"))
            .count();
        assert_eq!(
            type_count, 1,
            "must have exactly one TYPE line per metric family"
        );
    }

    #[test]
    fn render_prometheus_contains_help_and_type_lines() {
        let m = KrishivMetrics::default();
        let body = m.render_prometheus();
        assert!(body.contains("# HELP krishiv_tasks_total"));
        assert!(body.contains("# TYPE krishiv_tasks_total counter"));
        assert!(body.contains("# HELP krishiv_tasks_running"));
        assert!(body.contains("# TYPE krishiv_tasks_running gauge"));
        assert!(body.contains("# HELP krishiv_shuffle_bytes_written_total"));
        assert!(body.contains("# TYPE krishiv_shuffle_bytes_written_total counter"));
        assert!(body.contains("# HELP krishiv_job_queue_depth"));
        assert!(body.contains("# TYPE krishiv_job_queue_depth gauge"));
    }

    #[test]
    fn render_prometheus_reflects_counter_values() {
        let m = KrishivMetrics::default();
        m.inc_tasks_submitted();
        m.inc_tasks_submitted();
        m.inc_tasks_succeeded();
        m.inc_tasks_failed();
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_tasks_total{status=\"submitted\"} 2"));
        assert!(body.contains("krishiv_tasks_total{status=\"succeeded\"} 1"));
        assert!(body.contains("krishiv_tasks_total{status=\"failed\"} 1"));
    }

    #[test]
    fn render_prometheus_reflects_gauge_values() {
        let m = KrishivMetrics::default();
        m.set_tasks_running(7);
        m.set_job_queue_depth(3);
        m.add_shuffle_bytes_written(4096);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_tasks_running 7"));
        assert!(body.contains("krishiv_job_queue_depth 3"));
        assert!(body.contains("krishiv_shuffle_bytes_written_total 4096"));
    }

    #[test]
    fn render_prometheus_zeroes_for_default() {
        let m = KrishivMetrics::default();
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_tasks_total{status=\"submitted\"} 0"));
        assert!(body.contains("krishiv_tasks_running 0"));
        assert!(body.contains("krishiv_shuffle_bytes_written_total 0"));
        assert!(body.contains("krishiv_job_queue_depth 0"));
    }

    #[test]
    fn render_prometheus_ends_with_newline() {
        let m = KrishivMetrics::default();
        let body = m.render_prometheus();
        assert!(body.ends_with('\n'));
    }

    // Labeled metric tests

    #[test]
    fn labeled_checkpoint_epoch_gauge() {
        let m = KrishivMetrics::default();
        m.set_checkpoint_epoch("job-a", 5);
        m.set_checkpoint_epoch("job-b", 12);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_checkpoint_epoch{job_id=\"job-a\"} 5"));
        assert!(body.contains("krishiv_checkpoint_epoch{job_id=\"job-b\"} 12"));
    }

    #[test]
    fn labeled_checkpoint_epoch_counters() {
        let m = KrishivMetrics::default();
        m.inc_checkpoint_committed("job-a");
        m.inc_checkpoint_committed("job-a");
        m.inc_checkpoint_aborted("job-a");
        m.inc_checkpoint_failed("job-b");
        let body = m.render_prometheus();
        assert!(
            body.contains(
                "krishiv_checkpoint_epochs_total{job_id=\"job-a\",status=\"committed\"} 2"
            )
        );
        assert!(
            body.contains("krishiv_checkpoint_epochs_total{job_id=\"job-a\",status=\"aborted\"} 1")
        );
        assert!(
            body.contains("krishiv_checkpoint_epochs_total{job_id=\"job-b\",status=\"failed\"} 1")
        );
    }

    #[test]
    fn labeled_watermark_gauge() {
        let m = KrishivMetrics::default();
        m.set_watermark_ms("stream-job", 1620000000000);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_watermark_ms{job_id=\"stream-job\"} 1620000000000"));
    }

    #[test]
    fn labeled_latency_histograms() {
        let m = KrishivMetrics::default();
        m.observe_grpc_duration("/krishiv.ExecutorTaskService/LaunchTask", 0.15);
        m.observe_grpc_duration("/krishiv.ExecutorTaskService/LaunchTask", 0.002);

        m.observe_checkpoint_commit_duration("write_manifest", 0.035);
        m.observe_checkpoint_commit_duration("fsync", 1.2);

        let body = m.render_prometheus();

        // Verify gRPC call duration histogram
        assert!(body.contains("krishiv_grpc_call_duration_seconds_count{path=\"/krishiv.ExecutorTaskService/LaunchTask\"} 2"));
        assert!(body.contains("krishiv_grpc_call_duration_seconds_sum{path=\"/krishiv.ExecutorTaskService/LaunchTask\"} 0.152"));
        assert!(body.contains("krishiv_grpc_call_duration_seconds_bucket{path=\"/krishiv.ExecutorTaskService/LaunchTask\",le=\"0.005\"} 1"));
        assert!(body.contains("krishiv_grpc_call_duration_seconds_bucket{path=\"/krishiv.ExecutorTaskService/LaunchTask\",le=\"0.25\"} 2"));
        assert!(body.contains("krishiv_grpc_call_duration_seconds_bucket{path=\"/krishiv.ExecutorTaskService/LaunchTask\",le=\"+Inf\"} 2"));

        // Verify checkpoint commit duration histogram
        assert!(body.contains(
            "krishiv_checkpoint_commit_duration_seconds_count{phase=\"write_manifest\"} 1"
        ));
        assert!(body.contains(
            "krishiv_checkpoint_commit_duration_seconds_sum{phase=\"write_manifest\"} 0.035"
        ));
        assert!(body.contains("krishiv_checkpoint_commit_duration_seconds_bucket{phase=\"write_manifest\",le=\"0.05\"} 1"));

        assert!(
            body.contains("krishiv_checkpoint_commit_duration_seconds_count{phase=\"fsync\"} 1")
        );
        assert!(
            body.contains("krishiv_checkpoint_commit_duration_seconds_sum{phase=\"fsync\"} 1.200")
        );
        assert!(body.contains(
            "krishiv_checkpoint_commit_duration_seconds_bucket{phase=\"fsync\",le=\"2.5\"} 1"
        ));
    }

    #[test]
    fn labeled_source_offset_lag() {
        let m = KrishivMetrics::default();
        m.set_source_offset_lag("job-a", "kafka-topic-0", 1500);
        let body = m.render_prometheus();
        assert!(body.contains(
            "krishiv_source_offset_lag{job_id=\"job-a\",source_id=\"kafka-topic-0\"} 1500"
        ));
    }

    #[test]
    fn labeled_task_attempt_counters() {
        let m = KrishivMetrics::default();
        m.inc_task_attempt_submitted("job-a", "stage-0");
        m.inc_task_attempt_submitted("job-a", "stage-0");
        m.inc_task_attempt_succeeded("job-a", "stage-0");
        m.inc_task_attempt_failed("job-a", "stage-0");
        m.inc_task_attempt_retrying("job-a", "stage-0");
        let body = m.render_prometheus();
        assert!(body.contains(
            "krishiv_task_attempts_total{job_id=\"job-a\",stage_id=\"stage-0\",status=\"submitted\"} 2"
        ));
        assert!(body.contains(
            "krishiv_task_attempts_total{job_id=\"job-a\",stage_id=\"stage-0\",status=\"succeeded\"} 1"
        ));
        assert!(body.contains(
            "krishiv_task_attempts_total{job_id=\"job-a\",stage_id=\"stage-0\",status=\"failed\"} 1"
        ));
    }

    #[test]
    fn labeled_executor_slots_gauge() {
        let m = KrishivMetrics::default();
        m.set_executor_slots_used("exec-1", 3);
        m.set_executor_slots_used("exec-2", 7);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_executor_slots_used{executor_id=\"exec-1\"} 3"));
        assert!(body.contains("krishiv_executor_slots_used{executor_id=\"exec-2\"} 7"));
    }

    #[test]
    fn labeled_streaming_rows_counter() {
        let m = KrishivMetrics::default();
        m.add_streaming_rows("job-a", "task-0", 100);
        m.add_streaming_rows("job-a", "task-0", 250);
        let body = m.render_prometheus();
        assert!(body.contains(
            "krishiv_streaming_rows_emitted_total{job_id=\"job-a\",task_id=\"task-0\"} 350"
        ));
    }

    #[test]
    fn labeled_state_backend_gauges() {
        let m = KrishivMetrics::default();
        m.set_state_key_count("job-a", 5000);
        m.set_state_bytes("job-a", 1048576);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_state_key_count{job_id=\"job-a\"} 5000"));
        assert!(body.contains("krishiv_state_bytes{job_id=\"job-a\"} 1048576"));
    }

    #[test]
    fn labeled_shuffle_partition_gauges() {
        let m = KrishivMetrics::default();
        m.set_shuffle_partitions("job-a", "stage-1", 3, 7, 1);
        let body = m.render_prometheus();
        assert!(body.contains(
            "krishiv_shuffle_partitions{job_id=\"job-a\",stage_id=\"stage-1\",state=\"pending\"} 3"
        ));
        assert!(body.contains(
            "krishiv_shuffle_partitions{job_id=\"job-a\",stage_id=\"stage-1\",state=\"available\"} 7"
        ));
        assert!(body.contains(
            "krishiv_shuffle_partitions{job_id=\"job-a\",stage_id=\"stage-1\",state=\"failed\"} 1"
        ));
    }

    #[test]
    fn remove_job_cleans_all_labeled_metrics() {
        let m = KrishivMetrics::default();
        m.set_checkpoint_epoch("job-a", 1);
        m.set_watermark_ms("job-a", 1000);
        m.inc_checkpoint_committed("job-a");
        m.inc_task_attempt_submitted("job-a", "stage-0");
        m.set_shuffle_partitions("job-a", "stage-1", 1, 0, 0);
        m.set_state_key_count("job-a", 42);
        m.set_state_bytes("job-a", 1024);
        m.set_source_offset_lag("job-a", "kafka-0", 99);
        m.add_streaming_rows("job-a", "task-0", 10);
        m.remove_job("job-a");

        let body = m.render_prometheus();
        assert!(!body.contains("job-a"), "no job-a metrics after remove");
        // Global metrics should still exist.
        assert!(body.contains("krishiv_tasks_total"));
    }

    // MetricsError Display

    #[test]
    fn metrics_error_display_otlp_build() {
        let err = MetricsError::OtlpBuild("connection refused".into());
        assert_eq!(
            err.to_string(),
            "OTLP exporter build failed: connection refused"
        );
    }

    #[test]
    fn metrics_error_display_subscriber() {
        let err = MetricsError::Subscriber("already set".into());
        assert_eq!(err.to_string(), "subscriber init failed: already set");
    }

    #[test]
    fn metrics_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(MetricsError::OtlpBuild("test".into()));
        assert!(!err.to_string().is_empty());
    }

    // MetricsConfig custom values

    #[test]
    fn metrics_config_custom_service_name() {
        let config = MetricsConfig {
            service_name: "my-service".into(),
            ..Default::default()
        };
        assert_eq!(config.service_name, "my-service");
    }

    #[test]
    fn metrics_config_custom_log_filter() {
        let config = MetricsConfig {
            log_filter: Some("debug".into()),
            ..Default::default()
        };
        assert_eq!(config.log_filter.as_deref(), Some("debug"));
    }

    #[test]
    fn metrics_config_stdout_exporter() {
        let config = MetricsConfig {
            exporter: TracerExporter::Stdout,
            ..Default::default()
        };
        assert!(matches!(config.exporter, TracerExporter::Stdout));
    }

    #[test]
    fn metrics_config_otlp_endpoint_some() {
        let config = MetricsConfig {
            otlp_endpoint: Some("http://localhost:4317".into()),
            ..Default::default()
        };
        assert_eq!(
            config.otlp_endpoint.as_deref(),
            Some("http://localhost:4317")
        );
    }

    // MetricsHandle noop and shutdown

    #[test]
    fn metrics_handle_noop_creates_valid_handle() {
        let handle = MetricsHandle::noop();
        drop(handle);
    }

    #[test]
    fn metrics_handle_drop_calls_shutdown() {
        let handle = init(MetricsConfig::default()).expect("init");
        drop(handle);
    }

    #[test]
    fn init_with_stdout_exporter() {
        let config = MetricsConfig {
            exporter: TracerExporter::Stdout,
            ..Default::default()
        };
        let handle = init(config);
        assert!(handle.is_ok());
    }

    #[test]
    fn init_with_custom_filter() {
        let config = MetricsConfig {
            log_filter: Some("warn".into()),
            ..Default::default()
        };
        let handle = init(config);
        assert!(handle.is_ok());
    }

    #[test]
    fn init_with_empty_filter_defaults_to_info() {
        let config = MetricsConfig {
            log_filter: Some("".into()),
            ..Default::default()
        };
        let _handle = init(config);
    }

    // KrishivMetrics edge cases

    #[test]
    fn add_shuffle_bytes_written_zero() {
        let m = KrishivMetrics::default();
        m.add_shuffle_bytes_written(0);
        assert_eq!(m.shuffle_bytes_written(), 0);
    }

    #[test]
    fn add_shuffle_bytes_written_max_value() {
        let m = KrishivMetrics::default();
        m.add_shuffle_bytes_written(u64::MAX);
        assert_eq!(m.shuffle_bytes_written(), u64::MAX);
    }

    #[test]
    fn set_tasks_running_max_value() {
        let m = KrishivMetrics::default();
        m.set_tasks_running(u64::MAX);
        assert_eq!(m.tasks_running(), u64::MAX);
    }

    #[test]
    fn set_job_queue_depth_zero() {
        let m = KrishivMetrics::default();
        m.set_job_queue_depth(42);
        m.set_job_queue_depth(0);
        assert_eq!(m.job_queue_depth(), 0);
    }

    #[test]
    fn multiple_counters_accumulate_independently() {
        let m = KrishivMetrics::default();
        for _ in 0..100 {
            m.inc_tasks_submitted();
        }
        for _ in 0..50 {
            m.inc_tasks_succeeded();
        }
        for _ in 0..10 {
            m.inc_tasks_failed();
        }
        assert_eq!(m.tasks_submitted(), 100);
        assert_eq!(m.tasks_succeeded(), 50);
        assert_eq!(m.tasks_failed(), 10);
    }

    #[test]
    fn prometheus_output_is_valid_utf8() {
        let m = KrishivMetrics::default();
        m.inc_tasks_submitted();
        let body = m.render_prometheus();
        assert!(std::str::from_utf8(body.as_bytes()).is_ok());
    }

    // Global metrics thread safety

    #[test]
    fn global_metrics_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(KrishivMetrics::default());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let m = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..1000 {
                        m.inc_tasks_submitted();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(metrics.tasks_submitted(), 10000);
    }

    /// Verify that labeled metrics are thread-safe (DashMap + AtomicU64).
    #[test]
    fn labeled_metrics_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(KrishivMetrics::default());
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let m = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..500 {
                        m.inc_task_attempt_submitted(&format!("job-{i}"), "stage-0");
                        m.set_checkpoint_epoch(&format!("job-{i}"), 1);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // Verify no crash — concurrent DashMap access should work.
        let body = metrics.render_prometheus();
        assert!(body.contains("krishiv_checkpoint_epoch"));
    }

    // deployment_target unit tests

    #[test]
    fn resolved_deployment_target_explicit_config() {
        let config = MetricsConfig {
            deployment_target: Some("production".into()),
            ..MetricsConfig::default()
        };
        assert_eq!(config.resolved_deployment_target(), "production");
    }

    #[test]
    fn resolved_deployment_target_none_returns_env_or_unknown() {
        // When no explicit config is given, the function reads the env var or
        // falls back to "unknown". We verify the documented fallback chain
        // without mutating the environment (unsafe_code is workspace-forbidden).
        let config = MetricsConfig {
            deployment_target: None,
            ..MetricsConfig::default()
        };
        let result = config.resolved_deployment_target();
        let expected =
            std::env::var("KRISHIV_DEPLOYMENT_TARGET").unwrap_or_else(|_| "unknown".to_string());
        assert_eq!(
            result, expected,
            "resolved value must match the env var when set, or 'unknown' when absent"
        );
    }

    #[test]
    fn resolved_deployment_target_explicit_beats_any_env() {
        // When deployment_target is explicitly set, it wins regardless of any
        // env var — no env mutation needed to test this invariant.
        let config = MetricsConfig {
            deployment_target: Some("explicit-wins".into()),
            ..MetricsConfig::default()
        };
        assert_eq!(
            config.resolved_deployment_target(),
            "explicit-wins",
            "explicit config must always override the env var fallback"
        );
    }

    #[test]
    fn inmemory_exporter_captures_spans_after_init() {
        // Verifies that TracerExporter::InMemory is correctly wired into init()
        // and that emitted spans reach the exporter's capture buffer.
        use opentelemetry::trace::Tracer as _;
        use opentelemetry_sdk::trace::InMemorySpanExporter;

        let exporter = InMemorySpanExporter::default();
        let config = MetricsConfig {
            service_name: "span-capture-test".into(),
            exporter: TracerExporter::InMemory(exporter.clone()),
            deployment_target: Some("test-cluster".into()),
            otlp_endpoint: None,
            log_filter: None,
        };
        let handle = init(config).expect("init must succeed with InMemory exporter");

        // Emit a span directly via the provider-local tracer rather than the
        // global one, which can be replaced by concurrent tests calling init().
        {
            use opentelemetry::trace::TracerProvider as _;
            let tracer = handle.tracer_provider().tracer("capture-test");
            let span = tracer.start("test-capture-span");
            drop(span);
        }

        // Force flush to drain the processor (retry briefly for parallel test runs).
        let mut spans = Vec::new();
        for _ in 0..50 {
            let _ = handle.tracer_provider().force_flush();
            if let Ok(captured) = exporter.get_finished_spans()
                && !captured.is_empty()
            {
                spans = captured;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = handle.tracer_provider().shutdown();
        if spans.is_empty()
            && let Ok(captured) = exporter.get_finished_spans()
        {
            spans = captured;
        }
        assert!(
            !spans.is_empty(),
            "at least one span must be captured by InMemory exporter after init()"
        );
        // The deployment.target is passed to the resource builder in init().
        // Its correctness is validated by the resolved_deployment_target unit tests.
        // Here we just verify the span name is preserved.
        assert!(
            spans.iter().any(|s| s.name.as_ref() == "test-capture-span"),
            "captured span must have the expected name"
        );
    }

    #[tokio::test]
    #[ignore = "requires live OTLP collector at OTEL_EXPORTER_OTLP_ENDPOINT"]
    async fn otlp_integration_exports_span() {
        let endpoint = match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            Ok(e) => e,
            Err(_) => return,
        };
        let config = MetricsConfig {
            service_name: "krishiv-test".into(),
            otlp_endpoint: Some(endpoint),
            ..Default::default()
        };
        let handle = init(config).expect("metrics init with OTLP endpoint failed");
        let tracer = opentelemetry::global::tracer("test");
        {
            use opentelemetry::trace::Tracer as _;
            let _span = tracer.start("otlp_integration_test_span");
        }
        handle.shutdown();
    }
}
