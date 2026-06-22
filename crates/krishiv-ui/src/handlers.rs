use askama::Template;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse};
use krishiv_proto::{JobId, JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec};
use krishiv_scheduler::{ExecutorRecord, JobDetailSnapshot, JobSnapshot, StabilityMetrics};

use crate::router::ui_auth_token;
use crate::views::*;
use crate::{UiError, UiResult, UiState};

pub(crate) async fn healthz() -> &'static str {
    "ok\n"
}

/// Prometheus-format metrics endpoint backed by live `StabilityMetrics`.
pub(crate) async fn metrics(State(state): State<UiState>) -> impl IntoResponse {
    let coordinator = state.coordinator.read().await;
    let mut cache = state
        .metrics_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let now = std::time::Instant::now();
    let body = if now.duration_since(cache.1).as_secs() >= 1 || cache.0.is_empty() {
        let mut body = format_stability_metrics(&coordinator.stability_metrics());
        body.push('\n');
        body.push_str(&krishiv_scheduler::metrics::render_prometheus_metrics());
        body.push('\n');
        body.push_str(&krishiv_metrics::global_metrics().render_prometheus());
        body.push('\n');
        let sm = krishiv_metrics::system::system_metrics();
        sm.refresh();
        body.push_str(&sm.render_prometheus());
        cache.0 = body.clone();
        cache.1 = now;
        body
    } else {
        cache.0.clone()
    };
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

fn format_stability_metrics(m: &StabilityMetrics) -> String {
    let max_heartbeat_age = m
        .heartbeat_ages()
        .iter()
        .map(|a| a.age_ticks())
        .max()
        .unwrap_or(0);
    format!(
        "\
# HELP krishiv_running_tasks Currently running task count
# TYPE krishiv_running_tasks gauge
krishiv_running_tasks {running}
# HELP krishiv_task_retries_total Total stage-level retries scheduled
# TYPE krishiv_task_retries_total counter
krishiv_task_retries_total {retries}
# HELP krishiv_failed_assignments_total Total failed task assignments
# TYPE krishiv_failed_assignments_total counter
krishiv_failed_assignments_total {failed}
# HELP krishiv_max_executor_heartbeat_age_ticks Max executor heartbeat age in scheduler ticks
# TYPE krishiv_max_executor_heartbeat_age_ticks gauge
krishiv_max_executor_heartbeat_age_ticks {hb_age}
# HELP krishiv_shuffle_partitions_available Total shuffle partitions available across all active stages
# TYPE krishiv_shuffle_partitions_available gauge
krishiv_shuffle_partitions_available {shuffle_parts}
",
        running = m.running_task_count(),
        retries = m.retry_count(),
        failed = m.failed_assignments(),
        hb_age = max_heartbeat_age,
        shuffle_parts = m.shuffle_partitions_available,
    )
}

pub(crate) async fn readyz(State(state): State<UiState>) -> Result<impl IntoResponse, UiError> {
    use krishiv_proto::CoordinatorState;
    let coordinator = state.coordinator.read().await;
    if coordinator.state() != CoordinatorState::Active {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            "coordinator is not active\n",
        ));
    }
    let _snapshot = status_snapshot(&state).await?;
    Ok((StatusCode::OK, "ready\n"))
}

pub(crate) async fn api_jobs(
    State(state): State<UiState>,
    pagination: Query<Pagination>,
) -> Result<Json<JobsResponse>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    let (limit, offset) = pagination.resolved();
    let total = snapshot.jobs.len();
    let jobs = snapshot.jobs.into_iter().skip(offset).take(limit).collect();
    Ok(Json(JobsResponse {
        jobs,
        total,
        limit,
        offset,
    }))
}

pub(crate) async fn api_job_detail(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobDetailResponse>, UiError> {
    Ok(Json(JobDetailResponse {
        job: job_detail(&state, &job_id).await?,
    }))
}

pub(crate) async fn api_executors(
    State(state): State<UiState>,
) -> Result<Json<ExecutorsResponse>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    Ok(Json(ExecutorsResponse {
        executors: snapshot.executors,
    }))
}

pub(crate) async fn api_queues(
    State(state): State<UiState>,
) -> Result<Json<QueuesResponse>, UiError> {
    let coordinator = state.coordinator.read().await;

    // Collect all distinct namespaces from active jobs plus the default namespace.
    let mut namespaces: Vec<Option<String>> = coordinator
        .job_snapshots()
        .iter()
        .filter(|j| !j.state().is_terminal())
        .map(|j| j.namespace_id().map(str::to_owned))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // Ensure the default namespace is always present.
    if !namespaces.contains(&None) {
        namespaces.push(None);
    }
    // Sort: default namespace first, then alphabetical.
    namespaces.sort_by(|a, b| match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, _) => std::cmp::Ordering::Less,
        (_, None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => a.cmp(b),
    });

    let quota_views = namespaces
        .iter()
        .map(|ns| {
            let snap = coordinator.namespace_quota_snapshot(ns.as_deref());
            NamespaceQuotaView::from_snapshot(&snap)
        })
        .collect();

    Ok(Json(QueuesResponse {
        namespaces: quota_views,
    }))
}

pub(crate) async fn ui_submit(State(state): State<UiState>) -> Result<Html<String>, UiError> {
    let template = SubmitTemplate {
        bearer_token: ui_auth_token(&state),
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn ui_health(State(state): State<UiState>) -> Result<Html<String>, UiError> {
    let coordinator = state.coordinator.read().await;
    let bearer_token = ui_auth_token(&state);
    let tick = coordinator.executors().current_tick();
    let template = HealthTemplate {
        executors: coordinator
            .executor_snapshots()
            .iter()
            .map(|r| ExecutorView::from_record(r, tick))
            .collect(),
        jobs: coordinator
            .job_snapshots()
            .iter()
            .map(JobSummaryView::from_snapshot)
            .collect(),
        bearer_token,
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn ui_metrics(State(state): State<UiState>) -> Result<Html<String>, UiError> {
    let coordinator = state.coordinator.read().await;
    let snapshot = status_snapshot_inner(&coordinator);
    let scheduler = krishiv_scheduler::metrics::scheduler_metrics();
    let stability = coordinator.stability_metrics();
    let avg = scheduler
        .task_assignment_duration_ms_sum
        .checked_div(scheduler.tasks_assigned_total)
        .unwrap_or(0);
    let gm = krishiv_metrics::global_metrics();
    let global = GlobalMetricsView {
        tasks_submitted: gm.tasks_submitted(),
        tasks_succeeded: gm.tasks_succeeded(),
        tasks_failed: gm.tasks_failed(),
        executor_lost: gm.executor_lost(),
        shuffle_bytes_written: gm.shuffle_bytes_written(),
        job_queue_depth: gm.job_queue_depth(),
        spill_bytes_total: gm.spill_bytes_total(),
        spill_files_total: gm.spill_files_total(),
        watermark_entry_count: gm.watermark_entry_count(),
        state_key_entry_count: gm.state_key_entry_count(),
    };
    let sm = krishiv_metrics::system::system_metrics();
    let system = SystemMetricsView {
        process_memory_bytes: sm.process_memory_bytes(),
        process_cpu_usage_x100: sm.process_cpu_usage_x100(),
        process_virtual_memory_bytes: sm.process_virtual_memory_bytes(),
        process_thread_count: sm.process_thread_count(),
        system_total_memory_bytes: sm.system_total_memory_bytes(),
        system_available_memory_bytes: sm.system_available_memory_bytes(),
        system_cpu_usage_x100: sm.system_cpu_usage_x100(),
    };
    let template = MetricsTemplate {
        scheduler,
        stability,
        jobs_count: snapshot.jobs.len(),
        executors_count: snapshot.executors.len(),
        avg_duration_ms: avg,
        global,
        system,
        bearer_token: ui_auth_token(&state),
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn api_sql_execute(
    State(state): State<UiState>,
    Json(req): Json<SqlQueryRequest>,
) -> Json<SqlQueryResponse> {
    let engine = match &state.sql {
        Some(e) => e.clone(),
        None => {
            return Json(SqlQueryResponse {
                columns: vec![],
                rows: vec![],
                error: Some(
                    "SQL engine not available. Start the UI with SQL support enabled.".to_string(),
                ),
                row_count: 0,
                elapsed_ms: 0,
            });
        }
    };

    let start = std::time::Instant::now();
    match engine.sql(&req.query).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                let (columns, rows) = extract_columns_and_rows(&batches);
                let elapsed = start.elapsed().as_millis() as u64;
                let row_count = rows.len();
                Json(SqlQueryResponse {
                    columns,
                    rows,
                    error: None,
                    row_count,
                    elapsed_ms: elapsed,
                })
            }
            Err(e) => Json(SqlQueryResponse {
                columns: vec![],
                rows: vec![],
                error: Some(format!("execution error: {e}")),
                row_count: 0,
                elapsed_ms: start.elapsed().as_millis() as u64,
            }),
        },
        Err(e) => Json(SqlQueryResponse {
            columns: vec![],
            rows: vec![],
            error: Some(format!("sql error: {e}")),
            row_count: 0,
            elapsed_ms: start.elapsed().as_millis() as u64,
        }),
    }
}

pub(crate) async fn api_job_checkpoints(
    State(state): State<UiState>,
    Path(job_id_str): Path<String>,
) -> Result<Json<JobCheckpointsResponse>, UiError> {
    let job_id = JobId::try_new(job_id_str.clone()).map_err(|e| UiError::Id(e.to_string()))?;
    let coordinator = state.coordinator.read().await;

    // Verify the job exists — returns UnknownJob (→ 404) if not.
    coordinator.job_detail_snapshot(&job_id)?;

    let epochs = coordinator.list_job_checkpoints(&job_id)?;
    let latest_epoch = epochs.last().copied();
    Ok(Json(JobCheckpointsResponse {
        job_id: job_id_str,
        epochs,
        latest_epoch,
    }))
}

pub(crate) async fn ui_jobs(
    State(state): State<UiState>,
    filter: Query<JobsFilter>,
) -> Result<Html<String>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    let jobs = if filter.has_any() {
        filter_jobs(snapshot.jobs, &filter)
    } else {
        snapshot.jobs
    };
    let template = JobsTemplate {
        jobs,
        executors: snapshot.executors,
        bearer_token: ui_auth_token(&state),
        cluster_total_slots: snapshot.cluster_total_slots,
        cluster_used_slots: snapshot.cluster_used_slots,
        cluster_memory_total_mb: snapshot.cluster_memory_total_mb,
        cluster_memory_used_mb: snapshot.cluster_memory_used_mb,
        healthy_executor_count: snapshot.healthy_executor_count,
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn ui_job_detail(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    let template = JobTemplate {
        job: job_detail(&state, &job_id).await?,
        executors: snapshot.executors,
        bearer_token: ui_auth_token(&state),
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn api_executor_detail(
    State(state): State<UiState>,
    Path(executor_id): Path<String>,
) -> Result<Json<ExecutorDetailResponse>, UiError> {
    let snapshot = api_executor_detail_inner(&state, &executor_id).await?;
    Ok(Json(ExecutorDetailResponse { executor: snapshot }))
}

pub(crate) async fn ui_executor_detail(
    State(state): State<UiState>,
    Path(executor_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let executor = api_executor_detail_inner(&state, &executor_id).await?;
    let template = ExecutorTemplate {
        executor,
        bearer_token: ui_auth_token(&state),
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn ui_job_checkpoints_page(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let coordinator = state.coordinator.read().await;
    let jid = JobId::try_new(job_id.clone()).map_err(|e| UiError::Id(e.to_string()))?;
    coordinator.job_detail_snapshot(&jid)?;
    let epochs = coordinator.list_job_checkpoints(&jid)?;
    let latest_epoch = epochs.last().copied();
    let template = CheckpointsTemplate {
        job_id: job_id.clone(),
        epochs,
        latest_epoch,
        bearer_token: ui_auth_token(&state),
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn api_job_diagnose(
    State(state): State<UiState>,
    Path(job_id_str): Path<String>,
) -> Result<Json<serde_json::Value>, UiError> {
    let job_id = JobId::try_new(job_id_str).map_err(|e| UiError::Id(e.to_string()))?;
    let coordinator = state.coordinator.read().await;
    let report = krishiv_scheduler::coordinator::observability::build_observability_report(
        &coordinator,
        &job_id,
    )?;
    let json =
        serde_json::to_value(&report).map_err(|e| UiError::Sql(format!("serialize error: {e}")))?;
    Ok(Json(json))
}

pub(crate) async fn ui_job_diagnose(
    State(state): State<UiState>,
    Path(job_id_str): Path<String>,
) -> Result<Html<String>, UiError> {
    let job_id = JobId::try_new(job_id_str.clone()).map_err(|e| UiError::Id(e.to_string()))?;
    let coordinator = state.coordinator.read().await;
    let report = krishiv_scheduler::coordinator::observability::build_observability_report(
        &coordinator,
        &job_id,
    )?;
    let report_json =
        serde_json::to_string_pretty(&report).unwrap_or_else(|e| format!("serialize error: {e}"));
    let template = JobDiagnoseTemplate {
        job_id: job_id_str,
        report_json,
        bearer_token: ui_auth_token(&state),
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn api_history(
    State(state): State<UiState>,
    pagination: Query<Pagination>,
) -> Result<Json<JobHistoryListResponse>, UiError> {
    let coordinator = state.coordinator.read().await;
    let all = coordinator.list_job_history();
    let (limit, offset) = pagination.resolved();
    let total = all.len();
    let records = all
        .iter()
        .skip(offset)
        .take(limit)
        .map(JobHistoryView::from_record)
        .collect();
    Ok(Json(JobHistoryListResponse {
        records,
        total,
        limit,
        offset,
    }))
}

pub(crate) async fn api_history_detail(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobHistoryView>, UiError> {
    let coordinator = state.coordinator.read().await;
    coordinator
        .get_job_history(&job_id)
        .map(|r| Json(JobHistoryView::from_record(&r)))
        .ok_or_else(|| {
            UiError::Scheduler(krishiv_scheduler::SchedulerError::UnknownJob {
                job_id: krishiv_proto::JobId::try_new(job_id)
                    .unwrap_or_else(|_| krishiv_proto::JobId::try_new("unknown").unwrap()),
            })
        })
}

pub(crate) async fn ui_history(
    State(state): State<UiState>,
    pagination: Query<Pagination>,
) -> Result<Html<String>, UiError> {
    let coordinator = state.coordinator.read().await;
    let all = coordinator.list_job_history();
    let (limit, offset) = pagination.resolved();
    let total = all.len();
    let records = all
        .iter()
        .skip(offset)
        .take(limit)
        .map(JobHistoryView::from_record)
        .collect();
    let template = HistoryTemplate {
        records,
        total,
        limit,
        offset,
        bearer_token: ui_auth_token(&state),
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn ui_history_detail(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let coordinator = state.coordinator.read().await;
    let record = coordinator
        .get_job_history(&job_id)
        .map(|r| JobHistoryView::from_record(&r))
        .ok_or_else(|| {
            UiError::Scheduler(krishiv_scheduler::SchedulerError::UnknownJob {
                job_id: krishiv_proto::JobId::try_new(job_id)
                    .unwrap_or_else(|_| krishiv_proto::JobId::try_new("unknown").unwrap()),
            })
        })?;
    let template = HistoryDetailTemplate {
        record,
        bearer_token: ui_auth_token(&state),
    };
    Ok(Html(template.render()?))
}

pub(crate) async fn api_executor_detail_inner(
    state: &UiState,
    executor_id: &str,
) -> UiResult<ExecutorView> {
    let coordinator = state.coordinator.read().await;
    let tick = coordinator.executors().current_tick();
    let executors = coordinator.executor_snapshots();
    let eid = krishiv_proto::ExecutorId::try_new(executor_id.to_owned())
        .map_err(|e| UiError::Id(e.to_string()))?;
    executors
        .iter()
        .find(|e| e.executor_id() == &eid)
        .map(|r| ExecutorView::from_record(r, tick))
        .ok_or_else(|| {
            UiError::Scheduler(krishiv_scheduler::SchedulerError::UnknownExecutor {
                executor_id: eid.clone(),
            })
        })
}

pub(crate) async fn stylesheet() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../static/style.css"),
    )
}

pub(crate) async fn auth_js() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../static/krishiv-auth.js"),
    )
}

/// Vendored live-refresh helper (fragment polling + theme toggle). Replaces the
/// former htmx CDN dependency so the UI works in air-gapped clusters and under
/// a strict CSP.
pub(crate) async fn live_js() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../static/krishiv-live.js"),
    )
}

/// Vendored SQL-editor script (plain `fetch`, no htmx).
pub(crate) async fn sql_js() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../static/krishiv-sql.js"),
    )
}

/// Hand-maintained OpenAPI 3.1 description of the `/api/v1` surface, served so
/// operators and codegen tooling can discover the API.
pub(crate) async fn openapi_json() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "application/json; charset=utf-8")],
        include_str!("../static/openapi.json"),
    )
}

async fn status_snapshot(state: &UiState) -> UiResult<StatusView> {
    let coordinator = state.coordinator.read().await;
    Ok(status_snapshot_inner(&coordinator))
}

fn status_snapshot_inner(coordinator: &krishiv_scheduler::Coordinator) -> StatusView {
    let tick = coordinator.executors().current_tick();
    let executors: Vec<ExecutorView> = coordinator
        .executor_snapshots()
        .iter()
        .map(|r| ExecutorView::from_record(r, tick))
        .collect();

    let cluster_total_slots: usize = executors.iter().map(|e| e.slots).sum();
    let cluster_used_slots: usize = executors.iter().map(|e| e.slots_used).sum();
    let cluster_memory_total_mb: u64 = executors
        .iter()
        .filter_map(|e| e.memory_limit_bytes)
        .map(|b| b / 1048576)
        .sum();
    let cluster_memory_used_mb: u64 = executors
        .iter()
        .filter_map(|e| e.memory_used_bytes)
        .map(|b| b / 1048576)
        .sum();
    let healthy_executor_count = executors
        .iter()
        .filter(|e| e.state == "healthy" || e.state == "active")
        .count();

    StatusView {
        jobs: coordinator
            .job_snapshots()
            .iter()
            .map(JobSummaryView::from_snapshot)
            .collect(),
        executors,
        cluster_total_slots,
        cluster_used_slots,
        cluster_memory_total_mb,
        cluster_memory_used_mb,
        healthy_executor_count,
    }
}

fn filter_jobs(jobs: Vec<JobSummaryView>, filter: &JobsFilter) -> Vec<JobSummaryView> {
    jobs.into_iter()
        .filter(|j| {
            if let Some(ref state) = filter.state
                && !j.state.eq_ignore_ascii_case(state)
            {
                return false;
            }
            if let Some(ref kind) = filter.kind
                && !j.kind.eq_ignore_ascii_case(kind)
            {
                return false;
            }
            true
        })
        .collect()
}

fn extract_columns_and_rows(
    batches: &[arrow::record_batch::RecordBatch],
) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    if batches.is_empty() {
        return (vec![], vec![]);
    }
    let columns: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let val = if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    scalar_array_to_json(array.as_ref(), row_idx)
                };
                row.push(val);
            }
            rows.push(row);
        }
    }
    (columns, rows)
}

fn scalar_array_to_json(array: &dyn arrow::array::Array, idx: usize) -> serde_json::Value {
    use arrow::array::*;
    use arrow::datatypes::*;
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| serde_json::Value::Number(a.value(idx).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| serde_json::Value::Number(a.value(idx).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| serde_json::Value::Number(a.value(idx).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| serde_json::Value::Number(a.value(idx).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| serde_json::Value::Number(a.value(idx).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| serde_json::Value::Number(a.value(idx).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| serde_json::Value::Number(a.value(idx).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| serde_json::Value::Number(a.value(idx).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| {
                serde_json::Value::Number(
                    serde_json::Number::from_f64(a.value(idx) as f64)
                        .unwrap_or(serde_json::Number::from(0)),
                )
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| {
                serde_json::Value::Number(
                    serde_json::Number::from_f64(a.value(idx))
                        .unwrap_or(serde_json::Number::from(0)),
                )
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| serde_json::Value::Bool(a.value(idx)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| serde_json::Value::String(a.value(idx).to_string()))
            .unwrap_or(serde_json::Value::Null),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| serde_json::Value::String(a.value(idx).to_string()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Timestamp(_, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(|a| serde_json::Value::Number(a.value(idx).into()))
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::String(format!("{:?}", array.data_type())),
    }
}

async fn job_detail(state: &UiState, job_id: &str) -> UiResult<JobDetailView> {
    let job_id =
        JobId::try_new(job_id.to_owned()).map_err(|error| UiError::Id(error.to_string()))?;
    let coordinator = state.coordinator.read().await;
    let detail = coordinator.job_detail_snapshot(&job_id)?;
    Ok(JobDetailView::from_snapshot(&detail))
}

#[derive(Debug, Clone, PartialEq)]
struct StatusView {
    jobs: Vec<JobSummaryView>,
    executors: Vec<ExecutorView>,
    /// Total slots across all executors.
    cluster_total_slots: usize,
    /// Total slots in use across all executors.
    cluster_used_slots: usize,
    /// Total memory (MB) across all executors.
    cluster_memory_total_mb: u64,
    /// Used memory (MB) across all executors.
    cluster_memory_used_mb: u64,
    /// Number of healthy executors.
    healthy_executor_count: usize,
}

impl JobSummaryView {
    fn from_snapshot(snapshot: &JobSnapshot) -> Self {
        Self {
            job_id: snapshot.job_id().to_string(),
            kind: snapshot.kind().to_string(),
            state: snapshot.state().to_string(),
            stage_count: snapshot.stage_count(),
            task_count: snapshot.task_count(),
            assigned_task_count: snapshot.assigned_task_count(),
            running_task_count: snapshot.running_task_count(),
            succeeded_task_count: snapshot.succeeded_task_count(),
            failed_task_count: snapshot.failed_task_count(),
            priority: snapshot.priority(),
            namespace_id: snapshot.namespace_id().map(str::to_owned),
            resource_usage: ResourceUsageView::from_usage(snapshot.resource_usage()),
            shuffle_bytes_written: snapshot.shuffle_bytes_written(),
            shuffle_partitions_available: snapshot.shuffle_partitions_available(),
        }
    }
}

impl JobDetailView {
    fn from_snapshot(snapshot: &JobDetailSnapshot) -> Self {
        Self {
            summary: JobSummaryView::from_snapshot(snapshot.job()),
            stages: snapshot
                .stages()
                .iter()
                .map(|stage| StageView {
                    stage_id: stage.stage_id().to_string(),
                    state: stage.state().to_string(),
                    retry_count: stage.retry_count(),
                    task_count: stage.task_count(),
                    tasks: stage
                        .tasks()
                        .iter()
                        .map(|task| {
                            let wm = task.last_watermark_ms();
                            let off = task.last_source_offset();
                            TaskView {
                                task_id: task.task_id().to_string(),
                                state: task.state().to_string(),
                                assigned_executor: task
                                    .assigned_executor()
                                    .map(ToString::to_string)
                                    .unwrap_or_else(|| String::from("-")),
                                attempt: task.attempt(),
                                failure_count: task.failure_count(),
                                failure_reason_display: task
                                    .last_failure_reason()
                                    .map(ToOwned::to_owned)
                                    .unwrap_or_default(),
                                source_capabilities: task
                                    .source_capabilities
                                    .as_ref()
                                    .map(ConnectorCapabilityView::from_flags),
                                sink_capabilities: task
                                    .sink_capabilities
                                    .as_ref()
                                    .map(ConnectorCapabilityView::from_flags),
                                last_watermark_display: match wm {
                                    Some(ms) => ms.to_string(),
                                    None => String::from("-"),
                                },
                                last_source_offset_display: match off {
                                    Some(b) => hex_encode(b),
                                    None => String::from("-"),
                                },
                            }
                        })
                        .collect(),
                    shuffle_bytes_written: stage.shuffle_bytes_written(),
                    shuffle_partitions_available: stage.shuffle_partitions_available(),
                })
                .collect(),
        }
    }
}

impl ExecutorView {
    fn from_record(record: &ExecutorRecord, current_tick: u64) -> Self {
        let health = record.health_snapshot();
        let memory_used_pct = health.and_then(|h| {
            let used = h.memory_used_bytes?;
            let limit = h.memory_limit_bytes?;
            if limit > 0 {
                Some(((used as f64) * 100.0 / limit as f64) as u64)
            } else {
                None
            }
        });
        let heartbeat_age_ticks = current_tick.saturating_sub(record.last_heartbeat_tick());
        Self {
            executor_id: record.executor_id().to_string(),
            state: record.state().to_string(),
            slots: record.descriptor().slots(),
            host: record.descriptor().host().to_owned(),
            running_tasks: record
                .running_tasks()
                .iter()
                .map(ToString::to_string)
                .collect(),
            last_heartbeat_tick: record.last_heartbeat_tick(),
            lease_generation: record.lease_generation().as_u64(),
            memory_used_bytes: health.and_then(|h| h.memory_used_bytes),
            memory_limit_bytes: health.and_then(|h| h.memory_limit_bytes),
            active_task_count: health.and_then(|h| h.active_task_count),
            consecutive_task_failures: record.consecutive_task_failures(),
            task_endpoint_display: record.descriptor().task_endpoint().unwrap_or("").to_owned(),
            barrier_endpoint_display: record
                .descriptor()
                .barrier_endpoint()
                .unwrap_or("")
                .to_owned(),
            memory_used_pct,
            heartbeat_age_ticks,
            slots_used: record.running_tasks().len(),
            cpu_cores: health.and_then(|h| h.cpu_cores_used),
            network_bytes_sent: health.and_then(|h| h.network_bytes_sent),
            network_bytes_recv: health.and_then(|h| h.network_bytes_recv),
        }
    }
}

pub(crate) fn demo_job(job_id: JobId) -> UiResult<JobSpec> {
    let stage = StageSpec::new(
        StageId::try_new("stage-1").map_err(|error| UiError::Id(error.to_string()))?,
        "demo-status-stage",
    )
    .with_task(TaskSpec::new(
        TaskId::try_new("task-1").map_err(|error| UiError::Id(error.to_string()))?,
        "demo scan task",
    ))
    .with_task(TaskSpec::new(
        TaskId::try_new("task-2").map_err(|error| UiError::Id(error.to_string()))?,
        "demo aggregate task",
    ));

    Ok(JobSpec::new(job_id, "demo-status-job", JobKind::Batch).with_stage(stage))
}
