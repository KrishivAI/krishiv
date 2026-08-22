// Hand-maintained mirrors of the coordinator's JSON response structs
// (crates/krishiv-scheduler/src/{coordinator_daemon,continuous_stream_http,
// batch_sql_http}.rs). Follow-up: generate from an OpenAPI export instead.

export interface LiveJobView {
  job_id: string;
  kind: string;
  state: string;
  stage_count: number;
  task_count: number;
  assigned_task_count: number;
  running_task_count: number;
  succeeded_task_count: number;
  failed_task_count: number;
  shuffle_bytes_written?: number;
}
export interface LiveJobsResponse {
  jobs: LiveJobView[];
}

export interface LiveExecutorView {
  executor_id: string;
  host: string;
  slots: number;
  state: string;
  lease_generation: number;
  running_task_count: number;
  last_heartbeat_tick: number;
  consecutive_task_failures: number;
}
export interface LiveExecutorsResponse {
  executors: LiveExecutorView[];
}

export interface ContinuousJobView {
  job_id: string;
  state: string;
  task_count: number;
  assigned_task_count: number;
  running_task_count: number;
  succeeded_task_count: number;
  failed_task_count: number;
  last_watermark_ms: number | null;
  persisted_watermark_ms: number | null;
  snapshot_available: boolean;
  cycle_in_flight: boolean;
  delivery_guarantee?: string;
}
export interface ContinuousListResponse {
  streams: ContinuousJobView[];
}

export interface BatchSqlSubmitResponse {
  job_id: string;
}
export interface BatchSqlPollResponse {
  job_id: string;
  state: string;
  /** Base64 Arrow IPC stream payloads (present when state == "Succeeded"). */
  inline_record_batch_ipc?: string[];
  error?: string;
  stage_count: number;
  task_count: number;
}

export interface JobHistoryView {
  job_id: string;
  job_kind: string;
  final_state: string;
  completed_at_ms: number;
  stage_count: number;
  task_count: number;
  succeeded_task_count: number;
  failed_task_count: number;
  cpu_nanos: number;
  memory_peak_task_bytes: number;
  namespace_id: string | null;
  priority: number;
}
export interface JobHistoryListResponse {
  records: JobHistoryView[];
  total: number;
  limit: number;
  offset: number;
}
