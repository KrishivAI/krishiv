#![forbid(unsafe_code)]

//! Public facade for `krishiv-scheduler`.
//!
//! Unified control-plane coordinator registry, scheduler loops, and gRPC endpoint.

// Implementation module: contains the root-level type definitions and core loops.
// Declare all scheduler sub-modules directly in lib.rs
pub mod adaptive;
pub mod admission;
pub mod auth;
pub mod config;
pub mod coordinator;

pub mod error;
#[cfg(feature = "etcd")]
pub mod etcd_lease;
#[cfg(feature = "etcd")]
pub mod etcd_metadata;
pub mod grpc;
pub mod http_auth;
pub mod leadership;
pub mod metrics;

pub mod barrier_client;
pub mod barrier_dispatch;
pub mod barrier_tracker;
pub mod batch_sql;
pub mod batch_sql_http;
pub mod bounded_window;
pub mod bounded_window_http;
pub mod checkpoint;
pub mod cluster_control;
pub mod continuous_stream_http;
pub mod coordinator_daemon;
pub mod coordinator_sharded;
pub mod heartbeat;
pub mod in_process;
pub mod job;
pub mod job_coordinator;
pub mod queryable_state_http;
pub mod rpc_drain;
pub mod store;
pub mod transport;

// Re-export the public API at the crate root for source compatibility.
pub use adaptive::{
    AdaptiveDecisionKind, AdaptiveDecisionLog, AdaptiveOverrideConfig, ExecutorHeartbeatEffects,
    ThrottleDecision,
};
pub use admission::{InMemoryQueueManager, QueueManager};
pub use auth::{
    AuthContext, COORDINATOR_AUTH_RELOAD_INTERVAL_SECS_ENV, COORDINATOR_BEARER_TOKEN_ENV,
    COORDINATOR_BEARER_TOKEN_FILE_ENV, COORDINATOR_BEARER_TOKENS_ENV,
    COORDINATOR_BEARER_TOKENS_FILE_ENV, JwtAuthProvider, OIDC_JWKS_URI_ENV,
    configure_grpc_auth_provider_from_env, configure_jwt_auth_provider_from_env,
    configured_coordinator_bearer_token, configured_coordinator_bearer_tokens,
    coordinator_bearer_auth_configured, extract_auth_context, reload_grpc_auth_provider_from_env,
    set_allow_anonymous, set_grpc_auth_provider, spawn_grpc_auth_reload_task_from_env,
    try_configured_coordinator_bearer_tokens, validate_grpc_admin, validate_grpc_auth,
    validate_grpc_auth_for_role, validate_grpc_writer,
};
pub use barrier_dispatch::{BarrierDispatchPlan, drive_barrier_dispatches};
pub use barrier_tracker::CheckpointBarrierTracker;
pub use batch_sql::{
    BatchSqlInlineTable, BatchSqlOutcome, BatchSqlTable, decode_inline_record_batches,
    execute_batch_sql_coordinated, submit_batch_sql_job,
};
pub use bounded_window::execute_bounded_window_coordinated;
pub use checkpoint::{CheckpointCoordinator, CheckpointCoordinatorState};
pub use cluster_control::{ClusterControlPlane, SingleNodeLeader};
pub use config::{CoordinatorConfig, JobSubmitter, TlsConfig};
pub use coordinator::{Coordinator, OrchestratorHandles, RestoreDirective, SharedCoordinator};
pub use coordinator_daemon::{
    CoordinatorDaemonConfig, CoordinatorSidecarFn, JobCoordinatorDaemonConfig,
    build_leader_election, build_shared_coordinator, coordinator_daemon_help,
    coordinator_http_router, job_coordinator_daemon_help, parse_coordinator_daemon_config,
    parse_job_coordinator_daemon_config, run_cluster_control_plane, run_clusterd_daemon,
    run_job_coordinator_daemon, run_standalone_coordinator, spawn_coordinator_sidecars,
};
pub use error::{SchedulerError, SchedulerResult, TaskUpdateOutcome};
#[cfg(feature = "etcd")]
pub use etcd_lease::{DEFAULT_CCP_LEADER_KEY, EtcdLeaseElection};
#[cfg(feature = "etcd")]
pub use etcd_metadata::EtcdMetadataStore;
pub(crate) mod rocksdb_metadata;
pub use continuous_stream_http::{
    ContinuousStreamError, drain_continuous_stream_coordinated, push_continuous_input_coordinated,
    register_continuous_stream_coordinated,
};
pub use grpc::{
    CoordinatorExecutorGrpcService, CoordinatorExecutorTonicService,
    CoordinatorManagementGrpcService, coordinator_executor_grpc_server,
    coordinator_management_grpc_server, serve_coordinator_executor_grpc_with_listener,
    serve_coordinator_executor_grpc_with_listener_and_tracker, server_tls_config_from_env,
};
pub use heartbeat::{
    ExecutorHealthSnapshot, ExecutorHeartbeatAge, ExecutorRecord, ExecutorRegistry,
};
pub use in_process::{
    IN_PROCESS_TASK_ENDPOINT, InProcessCoordinatorBridge, is_in_process_task_endpoint,
};
pub use job::{
    JobDetailSnapshot, JobRecord, JobSnapshot, NamespaceQuotaSnapshot, ResourceUsage,
    SlotAwareScheduler, StabilityMetrics, StageRecord, StageSnapshot, StaticScheduler,
    SubmitOutcome, TaskRecord, TaskSnapshot, job_spec_from_logical_plan,
    job_spec_from_physical_plan,
};
pub use job_coordinator::JobCoordinator;
pub use krishiv_common::DurabilityProfile;
pub use leadership::{LeaderElection, SingleNodeElection};
pub use metrics::{SchedulerMetrics, scheduler_metrics};
pub use queryable_state_http::{
    QueryStateResponse, decode_key_hex, encode_key_hex, queryable_state_router,
};
pub use rocksdb_metadata::RocksDbMetadataStore;
pub use store::{
    ContinuousSnapshot, EventLogEvent, InMemoryMetadataStore, MetadataStore, NonBlockingStoreHandle,
};
pub use transport::CoordinatorExecutorTransport;

#[cfg(test)]
mod tests;

pub(crate) use grpc::status_from_scheduler_error;
