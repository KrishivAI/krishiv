use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_plan::{ExecutionKind, LogicalPlan, PhysicalPlan};
use krishiv_runtime::{
    BatchTableRegistration, ExecutionRuntime, JobId, JobState, JobStatus, LocalJobRegistry,
};
use krishiv_sql::KrishivDataFrameOps;

use crate::error::{KrishivError, Result};
use crate::types::{ExecutionMode, QueryResult};

/// DataFrame API backed by DataFusion for R1 local execution.
#[derive(Clone)]
pub struct DataFrame {
    logical_plan: LogicalPlan,
    sql_dataframe: Option<Arc<dyn KrishivDataFrameOps>>,
    sql_query: Option<String>,
    /// Pre-collected batches — set when the DataFrame is constructed from
    /// already-executed results (e.g. [`Session::sql_as`]).
    pre_collected: Option<Vec<RecordBatch>>,
    mode: ExecutionMode,
    jobs: Arc<Mutex<LocalJobRegistry>>,
    next_job_id: Arc<AtomicU64>,
    #[allow(dead_code)]
    coordinator_url: Option<String>,
    runtime: Arc<dyn ExecutionRuntime>,
    registered_parquet: Arc<DashMap<String, PathBuf>>,
    /// When true, always collect from the local DataFusion plan even in remote
    /// mode. Set for lakehouse reads (Delta, Hudi) whose table registrations
    /// live only in the local DataFusion context and cannot be forwarded to a
    /// remote executor.
    force_local: bool,
}

impl fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFrame")
            .field("logical_plan", &self.logical_plan)
            .field("mode", &self.mode)
            .field("has_sql_query", &self.sql_query.is_some())
            .field(
                "pre_collected",
                &self.pre_collected.as_ref().map(|b| b.len()),
            )
            .finish_non_exhaustive()
    }
}

impl DataFrame {
    /// Create a logical-only DataFrame.
    pub fn new(logical_plan: LogicalPlan) -> Self {
        Self {
            logical_plan,
            sql_dataframe: None,
            sql_query: None,
            pre_collected: None,
            mode: ExecutionMode::Embedded,
            jobs: Arc::new(Mutex::new(LocalJobRegistry::default())),
            next_job_id: Arc::new(AtomicU64::new(1)),
            coordinator_url: None,
            runtime: crate::session::shared_embedded_runtime(),
            registered_parquet: Arc::new(DashMap::new()),
            force_local: false,
        }
    }

    /// Force collection from the local DataFusion plan regardless of runtime mode.
    pub(crate) fn with_force_local(mut self) -> Self {
        self.force_local = true;
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_sql_dataframe(
        mode: ExecutionMode,
        sql_dataframe: impl KrishivDataFrameOps + 'static,
        sql_query: Option<String>,
        jobs: Arc<Mutex<LocalJobRegistry>>,
        next_job_id: Arc<AtomicU64>,
        coordinator_url: Option<String>,
        runtime: Arc<dyn ExecutionRuntime>,
        registered_parquet: Arc<DashMap<String, PathBuf>>,
    ) -> Self {
        let logical_plan = sql_dataframe.krishiv_logical_plan();
        Self {
            logical_plan,
            sql_dataframe: Some(Arc::new(sql_dataframe)),
            sql_query,
            pre_collected: None,
            mode,
            jobs,
            next_job_id,
            coordinator_url,
            runtime,
            registered_parquet,
            force_local: false,
        }
    }

    /// Construct a [`DataFrame`] from a pre-collected list of record batches.
    ///
    /// Used by [`Session::sql_as`] to wrap the results of a policy-enforced query.
    pub(crate) fn from_batches(
        mode: ExecutionMode,
        batches: Vec<RecordBatch>,
        jobs: Arc<Mutex<LocalJobRegistry>>,
        next_job_id: Arc<AtomicU64>,
        runtime: Arc<dyn ExecutionRuntime>,
        registered_parquet: Arc<DashMap<String, PathBuf>>,
    ) -> Self {
        let logical_plan = LogicalPlan::new("policy-enforced-query", ExecutionKind::Batch);
        Self {
            logical_plan,
            sql_dataframe: None,
            sql_query: None,
            pre_collected: Some(batches),
            mode,
            jobs,
            next_job_id,
            coordinator_url: None,
            runtime,
            registered_parquet,
            force_local: false,
        }
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    /// Explain the current plan.
    pub fn explain(&self) -> Result<String> {
        krishiv_common::async_util::block_on(self.explain_async())
    }

    /// Convert this DataFrame into a fluent `StreamingDataFrame` builder
    /// for executing async stream operations with windows and aggregations.
    pub fn stream(&self) -> crate::streaming_dataframe::StreamingDataFrame {
        crate::streaming_dataframe::StreamingDataFrame::new(self.clone())
    }

    pub async fn explain_async(&self) -> Result<String> {
        let is_local = !self.runtime.uses_remote_execution();
        if is_local {
            let df = &self.sql_dataframe;
            if let Some(dataframe) = df {
                return dataframe.explain().await.map_err(Into::into);
            }
        }
        if let Some(query) = self.sql_query.as_deref() {
            return self.runtime.explain_sql(query).map_err(KrishivError::from);
        }
        match &self.sql_dataframe {
            Some(dataframe) => dataframe.explain().await.map_err(Into::into),
            None => Ok(self.logical_plan.describe()),
        }
    }

    /// Explain the Krishiv logical wrapper only.
    pub fn explain_logical(&self) -> String {
        match &self.sql_dataframe {
            Some(dataframe) => dataframe.explain_logical(),
            None => self.logical_plan.describe(),
        }
    }

    /// Collect results.
    pub fn collect(&self) -> Result<QueryResult> {
        krishiv_common::async_util::block_on(self.collect_async())
    }

    /// Asynchronously collect results.
    pub async fn collect_async(&self) -> Result<QueryResult> {
        let job_id = self.start_job("local-dataframe");
        self.update_job(&job_id, "local-dataframe", JobState::Running);

        if let Some(batches) = &self.pre_collected {
            self.update_job(&job_id, "local-dataframe", JobState::Succeeded);
            return Ok(QueryResult::new(batches.clone()));
        }

        let uses_remote = self.runtime.uses_remote_execution() && !self.force_local;

        let result = if uses_remote && self.sql_query.is_some() {
            let query = self.sql_query.as_deref().unwrap();
            let tables = self
                .registered_parquet
                .iter()
                .map(|entry| {
                    BatchTableRegistration::new(entry.key().clone(), entry.value().clone())
                })
                .collect::<Vec<_>>();
            crate::session::runtime_collect_batch_sql(Arc::clone(&self.runtime), query, &tables)
                .await
                .map(QueryResult::new)
        } else if let Some(dataframe) = &self.sql_dataframe {
            if !self.force_local {
                self.runtime
                    .accept_plan(&PhysicalPlan::new(
                        self.logical_plan.name(),
                        self.logical_plan.kind(),
                    ))
                    .map_err(KrishivError::from)?;
            }
            dataframe
                .collect()
                .await
                .map(QueryResult::new)
                .map_err(Into::into)
        } else {
            self.runtime
                .accept_plan(&PhysicalPlan::new(
                    self.logical_plan.name(),
                    self.logical_plan.kind(),
                ))
                .map_err(KrishivError::from)?;
            Err(KrishivError::unsupported(
                "logical-only DataFrame cannot be collected",
            ))
        };

        match &result {
            Ok(_) => self.update_job(&job_id, "local-dataframe", JobState::Succeeded),
            Err(_) => self.update_job(&job_id, "local-dataframe", JobState::Failed),
        }

        result
    }

    /// Asynchronously execute and return a record batch stream.
    pub async fn execute_stream_async(&self) -> Result<krishiv_plan::SendableRecordBatchStream> {
        let job_id = self.start_job("local-streaming");
        self.update_job(&job_id, "local-streaming", JobState::Running);

        if let Some(batches) = &self.pre_collected {
            self.update_job(&job_id, "local-streaming", JobState::Succeeded);
            let stream = futures::stream::iter(batches.clone().into_iter().map(Ok));
            return Ok(Box::pin(stream));
        }

        let uses_remote = self.runtime.uses_remote_execution() && !self.force_local;

        let result = if uses_remote && self.sql_query.is_some() {
            let query = self.sql_query.as_deref().unwrap();
            let tables = self
                .registered_parquet
                .iter()
                .map(|entry| {
                    BatchTableRegistration::new(entry.key().clone(), entry.value().clone())
                })
                .collect::<Vec<_>>();
            let batches = crate::session::runtime_collect_batch_sql(Arc::clone(&self.runtime), query, &tables).await?;
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as krishiv_plan::SendableRecordBatchStream)
        } else if let Some(dataframe) = &self.sql_dataframe {
            if !self.force_local {
                self.runtime
                    .accept_plan(&PhysicalPlan::new(
                        self.logical_plan.name(),
                        self.logical_plan.kind(),
                    ))
                    .map_err(KrishivError::from)?;
            }
            dataframe
                .execute_stream()
                .await
                .map_err(Into::into)
        } else {
            self.runtime
                .accept_plan(&PhysicalPlan::new(
                    self.logical_plan.name(),
                    self.logical_plan.kind(),
                ))
                .map_err(KrishivError::from)?;
            Err(KrishivError::unsupported(
                "logical-only DataFrame cannot be streamed",
            ))
        };

        match &result {
            Ok(_) => self.update_job(&job_id, "local-streaming", JobState::Succeeded),
            Err(_) => self.update_job(&job_id, "local-streaming", JobState::Failed),
        }

        result
    }

    fn start_job(&self, name: &str) -> JobId {
        let id = JobId::new(format!(
            "local-{}",
            self.next_job_id.fetch_add(1, Ordering::SeqCst)
        ));
        self.update_job(&id, name, JobState::Pending);
        id
    }

    fn update_job(&self, id: &JobId, name: &str, state: JobState) {
        let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        jobs.upsert(JobStatus::new(id.clone(), name, state));
    }
}
