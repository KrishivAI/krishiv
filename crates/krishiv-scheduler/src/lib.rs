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
pub mod leadership;
pub mod metrics;

pub mod barrier_client;
pub mod barrier_dispatch;
pub mod barrier_tracker;
pub mod batch_sql;
pub mod batch_sql_http;
pub mod checkpoint;
pub mod cluster_control;
pub mod coordinator_daemon;
pub mod federation_http;
pub mod heartbeat;
pub mod in_process;
pub mod job;
pub mod job_coordinator;
pub mod llm_quota;
pub mod store;
pub mod transport;

// Re-export the public API at the crate root for source compatibility.
pub use adaptive::{
    AdaptiveDecisionKind, AdaptiveDecisionLog, AdaptiveOverrideConfig, ExecutorHeartbeatEffects,
    ThrottleDecision,
};
pub use admission::{
    ConfigFileQueueManager, InMemoryQueueManager, QueueManager, QuotaPolicy, QuotaQueueManager,
};
pub use auth::{
    AuthContext, extract_auth_context, set_allow_anonymous, set_grpc_auth_provider,
    validate_grpc_auth,
};
pub use barrier_dispatch::{BarrierDispatchPlan, drive_barrier_dispatches};
pub use barrier_tracker::CheckpointBarrierTracker;
pub use batch_sql::{
    BatchSqlOutcome, BatchSqlTable, decode_inline_record_batches, execute_batch_sql_coordinated,
};
pub use checkpoint::{CheckpointCoordinator, CheckpointCoordinatorState};
pub use cluster_control::{ClusterControlPlane, SingleNodeLeader};
pub use config::{CoordinatorConfig, JobSubmitter, TlsConfig};
pub use coordinator::{Coordinator, SharedCoordinator};
pub use coordinator_daemon::{
    CoordinatorDaemonConfig, JobCoordinatorDaemonConfig, build_leader_election,
    build_shared_coordinator, coordinator_daemon_help, coordinator_http_router,
    job_coordinator_daemon_help, parse_coordinator_daemon_config,
    parse_job_coordinator_daemon_config, run_cluster_control_plane, run_clusterd_daemon,
    run_job_coordinator_daemon, run_standalone_coordinator, spawn_coordinator_sidecars,
};
pub use error::{SchedulerError, SchedulerResult, TaskUpdateOutcome};
#[cfg(feature = "etcd")]
pub use etcd_lease::{DEFAULT_CCP_LEADER_KEY, EtcdLeaseElection};
#[cfg(feature = "etcd")]
pub use etcd_metadata::EtcdMetadataStore;
pub use grpc::{
    CoordinatorExecutorGrpcService, CoordinatorExecutorTonicService,
    CoordinatorManagementGrpcService, coordinator_executor_grpc_server,
    coordinator_management_grpc_server, serve_coordinator_executor_grpc_with_listener,
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
pub use leadership::{LeaderElection, SingleNodeElection};
pub use metrics::{SchedulerMetrics, scheduler_metrics};
#[cfg(feature = "sqlite")]
pub use store::SqliteMetadataStore;
pub use store::{EventLogEvent, InMemoryMetadataStore, JsonFileMetadataStore, MetadataStore};
pub use transport::CoordinatorExecutorTransport;

#[cfg(test)]
mod tests;

pub(crate) use grpc::status_from_scheduler_error;

// #[cfg(test)]
// pub(crate) use krishiv_proto::{ExecutorDescriptor, ExecutorId};
// (removed to resolve duplicate re-export during test profile build; tests qualify via krishiv_proto directly)
