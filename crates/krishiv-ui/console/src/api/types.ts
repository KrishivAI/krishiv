// Hand-maintained mirrors of the coordinator's JSON response structs
// (crates/krishiv-scheduler/src/{coordinator_daemon,continuous_stream_http,
// batch_sql_http}.rs). Field names and semantics are verified against the
// live API (goal: no fake values) — when the Rust structs change, change
// these in the same commit.

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

export interface TaskTimingView {
  task_id: string;
  state: string;
  executor_id: string | null;
  attempt: number;
  failure_count: number;
  last_failure_reason: string | null;
  completed_duration_ms: number | null;
  last_watermark_ms: number | null;
}
export interface StageTimingView {
  stage_id: string;
  state: string;
  task_count: number;
  succeeded_task_count: number;
  total_task_ms: number;
  min_task_ms: number | null;
  median_task_ms: number | null;
  max_task_ms: number | null;
  shuffle_bytes_written: number;
  tasks: TaskTimingView[];
  /** Real DAG in-edges (upstream shuffle producers) from the stage spec. */
  upstream_stage_ids: string[];
}
export interface StageTimingResponse {
  job_id: string;
  stages: StageTimingView[];
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
  /** Coordinator heartbeat tick — the reference for heartbeat staleness. */
  current_tick: number;
}

export interface ContinuousDeliveryView {
  model: string;
  parallelism: number;
  sink?: string;
  sink_guarantee?: string;
  source_offsets_in_sink_transaction: boolean;
  effective: string;
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
  delivery: ContinuousDeliveryView;
  class: string;
}
export interface ContinuousListResponse {
  streams: ContinuousJobView[];
}
export interface ContinuousTarget {
  task_id: string;
  endpoint: string;
}
export interface ContinuousTargetsResponse {
  targets: ContinuousTarget[];
}
export interface ContinuousCheckpointResponse {
  job_id: string;
  snapshot_b64: string | null;
  watermark_ms: number | null;
  snapshot_available: boolean;
  /** Present for run-loop jobs: why this endpoint reports no snapshot
   *  (they checkpoint through the barrier pipeline instead). */
  snapshot_source?: string;
}
export interface ContinuousStopWithSavepointResponse {
  job_id: string;
  savepoint_epoch: number;
}
export interface ContinuousFlushResponse {
  success: boolean;
  inline_record_batch_ipc_b64: string[];
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

export interface MetricsSnapshot {
  at_ms: number;
  current_tick: number;
  executor_count: number;
  max_heartbeat_lag: number | null;
  running_jobs: number;
  failed_jobs: number;
  running_task_count: number;
  retry_count: number;
  failed_assignments: number;
  shuffle_bytes_written: number;
  shuffle_partitions_available: number;
}

export interface EventView {
  seq: number;
  kind: string;
  job_id?: string;
  stage_id?: string;
  task_id?: string;
  executor_id?: string;
  attempt?: number;
  detail?: string;
}
export interface EventsResponse {
  events: EventView[];
  total: number;
}

export interface ListStateNamesResponse {
  job_id: string;
  op_id: string;
  state_names: string[];
}
export type QueryStateResponse =
  | { found: "true"; job_id: string; op_id: string; state_name: string; key_hex: string; value_base64: string }
  | { found: "false"; job_id: string; op_id: string; state_name: string; key_hex: string };
