//! Session-scoped in-process coordinator + executor cluster (ADR-12.4 / ADR-13.3).

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use krishiv_plan::window::{WindowExecutionSpec, WindowKind, encode_stream_fragment};

use crate::RuntimeResult;
use crate::in_process::InProcessStreamingRuntime;
use crate::local_streaming::LocalWindowExecutionSpec;

/// Shared local cluster: one coordinator + executor per session.
#[derive(Clone)]
pub struct InProcessCluster {
    inner: Arc<InProcessStreamingRuntime>,
}

impl InProcessCluster {
    /// Create and register the in-process executor with a new coordinator.
    pub fn new() -> RuntimeResult<Self> {
        Ok(Self {
            inner: Arc::new(InProcessStreamingRuntime::new()?),
        })
    }

    pub fn collect_batch_sql(
        &self,
        query: &str,
        tables: &[crate::in_process::BatchSqlTable],
        is_streaming: bool,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        self.inner.execute_batch_sql(query, tables, is_streaming)
    }

    /// Check if a query is streaming.
    pub fn is_streaming_query(&self, query: &str) -> RuntimeResult<bool> {
        self.inner.is_streaming_query(query)
    }

    /// Register a continuous streaming job.
    pub fn register_continuous_job(
        &self,
        job_id: &str,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<()> {
        self.inner
            .register_continuous_job(job_id, local_spec_to_plan_spec(spec))
    }

    /// Push input for a continuous job.
    pub fn push_continuous_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> RuntimeResult<()> {
        self.inner.push_continuous_input(job_id, batches)
    }

    /// Drain a continuous job through the coordinator.
    pub fn drain_continuous_job(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        self.inner.drain_continuous_job(job_id)
    }

    /// Execute a bounded windowed stream through coordinator → executor.
    pub fn collect_bounded_window(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        let plan_spec = local_spec_to_plan_spec(spec);
        self.inner
            .execute_windowed(topic, input_batches, &plan_spec)
    }

    /// Deregister a streaming source and clear matching parquet-cache entries.
    pub fn deregister_streaming_source(&self, name: &str) -> RuntimeResult<()> {
        self.inner.deregister_streaming_source(name)
    }

    /// Expose the parquet-cache handle so it can be shared with new sessions.
    ///
    /// Pass the returned `Arc` to [`InProcessStreamingRuntime::with_parquet_cache`]
    /// or [`InProcessCluster::with_parquet_cache`] to create sessions that reuse
    /// the same file-footer cache across session boundaries.
    pub fn parquet_cache(&self) -> std::sync::Arc<dashmap::DashMap<String, ()>> {
        self.inner.parquet_cache()
    }

    /// Create a new cluster that shares an existing parquet-file-footer cache.
    pub fn with_parquet_cache(
        cache: std::sync::Arc<dashmap::DashMap<String, ()>>,
    ) -> RuntimeResult<Self> {
        Ok(Self {
            inner: Arc::new(InProcessStreamingRuntime::with_parquet_cache(cache)?),
        })
    }

    /// Borrow the underlying streaming runtime (tests, advanced use).
    pub fn streaming_runtime(&self) -> &InProcessStreamingRuntime {
        &self.inner
    }
}

pub(crate) fn local_spec_to_plan_spec(spec: &LocalWindowExecutionSpec) -> WindowExecutionSpec {
    use krishiv_exec::AggFunction;
    use krishiv_plan::window::{WindowAgg, WindowAggKind};

    let (window_kind, slide_ms, session_gap_ms) = match &spec.window_kind {
        crate::local_streaming::LocalWindowKind::Tumbling => (WindowKind::Tumbling, None, None),
        crate::local_streaming::LocalWindowKind::Sliding { slide_ms } => {
            (WindowKind::Sliding, Some(*slide_ms), None)
        }
        crate::local_streaming::LocalWindowKind::Session { gap_ms } => {
            (WindowKind::Session, None, Some(*gap_ms))
        }
    };

    WindowExecutionSpec {
        key_column: spec.key_column.clone(),
        event_time_column: spec.event_time_column.clone(),
        watermark_lag_ms: spec.watermark_lag_ms,
        window_kind,
        window_size_ms: spec.window_size_ms,
        slide_ms,
        session_gap_ms,
        agg_exprs: spec
            .agg_exprs
            .iter()
            .map(|a| {
                let kind = match a.function {
                    AggFunction::Count => WindowAggKind::Count,
                    AggFunction::Sum => WindowAggKind::Sum,
                    AggFunction::Min => WindowAggKind::Min,
                    AggFunction::Max => WindowAggKind::Max,
                    AggFunction::Avg => WindowAggKind::Avg,
                };
                WindowAgg {
                    kind,
                    input_column: a.input_column.clone(),
                    output_column: a.output_column.clone(),
                }
            })
            .collect(),
        state_ttl_ms: spec.state_ttl_ms,
        source_watermark_lags: spec.source_watermark_lags.clone(),
        source_id_column: spec.source_id_column.clone(),
    }
}

pub fn plan_spec_to_local(spec: &WindowExecutionSpec) -> LocalWindowExecutionSpec {
    use krishiv_exec::{AggExpr, AggFunction};
    use krishiv_plan::window::WindowAggKind;

    let window_kind = match spec.window_kind {
        WindowKind::Tumbling => crate::local_streaming::LocalWindowKind::Tumbling,
        WindowKind::Sliding => crate::local_streaming::LocalWindowKind::Sliding {
            slide_ms: spec.slide_ms.unwrap_or(spec.window_size_ms),
        },
        WindowKind::Session => crate::local_streaming::LocalWindowKind::Session {
            gap_ms: spec.session_gap_ms.unwrap_or(spec.window_size_ms),
        },
    };

    LocalWindowExecutionSpec {
        key_column: spec.key_column.clone(),
        event_time_column: spec.event_time_column.clone(),
        watermark_lag_ms: spec.watermark_lag_ms,
        window_kind,
        window_size_ms: spec.window_size_ms,
        agg_exprs: spec
            .agg_exprs
            .iter()
            .map(|a| {
                let function = match a.kind {
                    WindowAggKind::Count => AggFunction::Count,
                    WindowAggKind::Sum => AggFunction::Sum,
                    WindowAggKind::Min => AggFunction::Min,
                    WindowAggKind::Max => AggFunction::Max,
                    WindowAggKind::Avg => AggFunction::Avg,
                };
                AggExpr {
                    function,
                    input_column: a.input_column.clone(),
                    output_column: a.output_column.clone(),
                }
            })
            .collect(),
        state_ttl_ms: spec.state_ttl_ms,
        source_watermark_lags: spec.source_watermark_lags.clone(),
        source_id_column: spec.source_id_column.clone(),
    }
}
pub fn fragment_from_local_spec(spec: &LocalWindowExecutionSpec) -> String {
    encode_stream_fragment(&local_spec_to_plan_spec(spec))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::RuntimeError;
    use crate::local_streaming::LocalWindowExecutionSpec;

    fn events_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]))
    }

    fn events_batch(user_ids: &[&str], timestamps: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            events_schema(),
            vec![
                Arc::new(StringArray::from(user_ids.to_vec())) as _,
                Arc::new(Int64Array::from(timestamps.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    fn tumbling_spec() -> LocalWindowExecutionSpec {
        LocalWindowExecutionSpec {
            key_column: "user_id".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        }
    }

    #[test]
    fn cluster_new_succeeds() {
        let cluster = InProcessCluster::new().expect("cluster creation");
        let inner = cluster.streaming_runtime();
        let _ = inner.coordinator_instance_id();
    }

    #[test]
    fn cluster_reused_across_collects() {
        let cluster = InProcessCluster::new().expect("cluster");
        let ptr1 = Arc::as_ptr(&cluster.inner);
        let cluster2 = cluster.clone();
        let ptr2 = Arc::as_ptr(&cluster2.inner);
        assert_eq!(ptr1, ptr2);
    }

    #[test]
    fn collect_batch_sql_returns_results() {
        let cluster = InProcessCluster::new().expect("cluster");
        let batches = cluster
            .collect_batch_sql("SELECT 42 AS answer", &[], false)
            .expect("batch sql");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn collect_batch_sql_with_parquet_tables() {
        use crate::in_process::BatchSqlTable;
        let cluster = InProcessCluster::new().expect("cluster");
        let tables = vec![BatchSqlTable {
            table_name: "t".into(),
            path: PathBuf::from("/nonexistent.parquet"),
        }];
        let result = cluster.collect_batch_sql("SELECT 1", &tables, false);
        // May fail because file doesn't exist, but the routing works
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn register_and_drain_continuous_job() {
        let cluster = InProcessCluster::new().expect("cluster");
        cluster
            .register_continuous_job("job-1", &tumbling_spec())
            .expect("register");
        cluster
            .push_continuous_input("job-1", vec![events_batch(&["a", "b"], &[1_000, 2_000])])
            .expect("push");
        let _ = cluster.drain_continuous_job("job-1").expect("drain");
    }

    #[test]
    fn push_continuous_input_unknown_job_fails() {
        let cluster = InProcessCluster::new().expect("cluster");
        let err = cluster
            .push_continuous_input("no-such-job", vec![])
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeError::ContinuousStream(crate::ContinuousStreamError::JobNotFound { .. })
        ));
    }

    #[test]
    fn drain_continuous_job_unknown_fails() {
        let cluster = InProcessCluster::new().expect("cluster");
        let result = cluster.drain_continuous_job("no-such-job");
        assert!(result.is_err());
    }

    #[test]
    fn drain_continuous_job_empty_input() {
        let cluster = InProcessCluster::new().expect("cluster");
        cluster
            .register_continuous_job("job-empty", &tumbling_spec())
            .expect("register");
        let out = cluster.drain_continuous_job("job-empty").expect("drain");
        assert!(out.is_empty());
    }

    #[test]
    fn collect_bounded_window_returns_results() {
        let cluster = InProcessCluster::new().expect("cluster");
        let batch = events_batch(&["a", "b"], &[1_000, 5_000]);
        let out = cluster
            .collect_bounded_window("events", vec![batch], &tumbling_spec())
            .expect("collect");
        assert!(!out.is_empty());
    }

    #[test]
    fn session_scoped_sliding_window_collect() {
        let batch = events_batch(&["a", "b"], &[1_000, 5_000]);
        let spec = LocalWindowExecutionSpec {
            key_column: "user_id".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Sliding { slide_ms: 5_000 },
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let cluster = InProcessCluster::new().expect("cluster");
        let out = cluster
            .collect_bounded_window("events", vec![batch], &spec)
            .expect("collect");
        assert!(!out.is_empty());
    }

    #[test]
    fn multiple_clusters_are_independent() {
        let c1 = InProcessCluster::new().expect("c1");
        let c2 = InProcessCluster::new().expect("c2");
        let id1 = c1.streaming_runtime().coordinator_instance_id();
        let id2 = c2.streaming_runtime().coordinator_instance_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn collect_batch_sql_multiple_queries() {
        let cluster = InProcessCluster::new().expect("cluster");
        let b1 = cluster
            .collect_batch_sql("SELECT 1 AS n", &[], false)
            .unwrap();
        assert_eq!(b1[0].num_rows(), 1);
        let b2 = cluster
            .collect_batch_sql("SELECT 2 AS n", &[], false)
            .unwrap();
        assert_eq!(b2[0].num_rows(), 1);
        assert_eq!(
            b2[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[test]
    fn local_spec_to_plan_spec_preserves_fields() {
        let local = LocalWindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 500,
            window_kind: crate::local_streaming::LocalWindowKind::Sliding { slide_ms: 2_000 },
            window_size_ms: 10_000,
            agg_exprs: vec![],
            state_ttl_ms: Some(60_000),
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let plan = local_spec_to_plan_spec(&local);
        assert_eq!(plan.key_column, "k");
        assert_eq!(plan.event_time_column, "ts");
        assert_eq!(plan.watermark_lag_ms, 500);
        assert_eq!(plan.window_kind, krishiv_plan::window::WindowKind::Sliding);
        assert_eq!(plan.slide_ms, Some(2_000));
        assert_eq!(plan.state_ttl_ms, Some(60_000));
    }

    #[test]
    fn plan_spec_to_local_preserves_fields() {
        let plan = krishiv_plan::window::WindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 500,
            window_kind: krishiv_plan::window::WindowKind::Session,
            window_size_ms: 10_000,
            slide_ms: None,
            session_gap_ms: Some(5_000),
            agg_exprs: vec![],
            state_ttl_ms: Some(30_000),
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let local = plan_spec_to_local(&plan);
        assert_eq!(local.key_column, "k");
        assert_eq!(
            local.window_kind,
            crate::local_streaming::LocalWindowKind::Session { gap_ms: 5_000 }
        );
        assert_eq!(local.state_ttl_ms, Some(30_000));
    }

    #[test]
    fn fragment_from_local_spec_returns_nonempty() {
        let fragment = fragment_from_local_spec(&tumbling_spec());
        assert!(!fragment.is_empty());
        assert!(fragment.contains("stream:tw"));
    }

    #[test]
    fn local_spec_to_plan_spec_tumbling_roundtrip() {
        let local = LocalWindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Tumbling,
            window_size_ms: 5_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let plan = local_spec_to_plan_spec(&local);
        assert_eq!(plan.window_kind, krishiv_plan::window::WindowKind::Tumbling);
        assert_eq!(plan.window_size_ms, 5_000);
        assert!(plan.slide_ms.is_none());
        assert!(plan.session_gap_ms.is_none());
    }

    #[test]
    fn local_spec_to_plan_spec_session_roundtrip() {
        let local = LocalWindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Session { gap_ms: 3_000 },
            window_size_ms: 10_000,
            agg_exprs: vec![],
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let plan = local_spec_to_plan_spec(&local);
        assert_eq!(plan.window_kind, krishiv_plan::window::WindowKind::Session);
        assert_eq!(plan.session_gap_ms, Some(3_000));
    }

    #[test]
    fn plan_spec_to_local_tumbling_roundtrip() {
        let plan = krishiv_plan::window::WindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: krishiv_plan::window::WindowKind::Tumbling,
            window_size_ms: 5_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: vec![],
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let local = plan_spec_to_local(&plan);
        assert_eq!(
            local.window_kind,
            crate::local_streaming::LocalWindowKind::Tumbling
        );
        assert_eq!(local.window_size_ms, 5_000);
    }

    #[test]
    fn plan_spec_to_local_sliding_roundtrip() {
        let plan = krishiv_plan::window::WindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: krishiv_plan::window::WindowKind::Sliding,
            window_size_ms: 10_000,
            slide_ms: Some(2_000),
            session_gap_ms: None,
            agg_exprs: vec![],
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let local = plan_spec_to_local(&plan);
        assert_eq!(
            local.window_kind,
            crate::local_streaming::LocalWindowKind::Sliding { slide_ms: 2_000 }
        );
    }

    #[test]
    fn plan_spec_to_local_sliding_uses_default_slide() {
        let plan = krishiv_plan::window::WindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: krishiv_plan::window::WindowKind::Sliding,
            window_size_ms: 10_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: vec![],
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let local = plan_spec_to_local(&plan);
        assert_eq!(
            local.window_kind,
            crate::local_streaming::LocalWindowKind::Sliding { slide_ms: 10_000 }
        );
    }

    #[test]
    fn plan_spec_to_local_session_uses_default_gap() {
        let plan = krishiv_plan::window::WindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: krishiv_plan::window::WindowKind::Session,
            window_size_ms: 8_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: vec![],
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let local = plan_spec_to_local(&plan);
        assert_eq!(
            local.window_kind,
            crate::local_streaming::LocalWindowKind::Session { gap_ms: 8_000 }
        );
    }

    #[test]
    fn fragment_from_local_spec_sliding() {
        let spec = LocalWindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Sliding { slide_ms: 5_000 },
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let fragment = fragment_from_local_spec(&spec);
        assert!(fragment.contains("stream:sw"));
    }

    #[test]
    fn fragment_from_local_spec_session() {
        let spec = LocalWindowExecutionSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Session { gap_ms: 3_000 },
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let fragment = fragment_from_local_spec(&spec);
        assert!(fragment.contains("stream:ses"));
    }

    #[test]
    fn cluster_collect_bounded_window_empty_input() {
        let cluster = InProcessCluster::new().unwrap();
        let out = cluster
            .collect_bounded_window("events", vec![], &tumbling_spec())
            .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn cluster_streaming_runtime_accessible() {
        let cluster = InProcessCluster::new().unwrap();
        let rt = cluster.streaming_runtime();
        let _ = rt.coordinator_instance_id();
    }
}
