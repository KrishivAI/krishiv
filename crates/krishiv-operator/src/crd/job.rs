//! KrishivJob CRD types.

use std::collections::BTreeMap;

use krishiv_proto::{JobKind, JobState};
use serde::{Deserialize, Serialize};

use crate::constants::{API_GROUP, API_VERSION, FINALIZER, KIND};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    /// Resource name.
    pub name: String,
    /// Resource namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Kubernetes resource UID (populated from the API server).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// Kubernetes generation observed by the controller.
    #[serde(default)]
    pub generation: i64,
    /// Resource labels.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// Resource finalizers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub finalizers: Vec<String>,
    /// Non-null when the resource has been deleted but finalizers are still present.
    #[serde(
        default,
        rename = "deletionTimestamp",
        skip_serializing_if = "Option::is_none"
    )]
    pub deletion_timestamp: Option<String>,
}

impl ObjectMeta {
    /// Create metadata for a namespaced resource.
    pub fn namespaced(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: Some(namespace.into()),
            uid: None,
            generation: 1,
            labels: BTreeMap::new(),
            finalizers: Vec::new(),
            deletion_timestamp: None,
        }
    }

    /// Namespace used for scheduler identity.
    pub fn namespace_or_default(&self) -> &str {
        self.namespace.as_deref().unwrap_or("default")
    }

    /// URL-safe R2 scheduler job id for this namespaced resource.
    pub fn scheduler_job_id(&self) -> String {
        format!("{}.{}", self.namespace_or_default(), self.name)
    }

    /// Whether the Krishiv finalizer is present on this resource.
    pub fn has_finalizer(&self) -> bool {
        self.finalizers.iter().any(|f| f == FINALIZER)
    }

    /// Whether the resource has been deleted (deletion timestamp is set).
    pub fn is_being_deleted(&self) -> bool {
        self.deletion_timestamp.is_some()
    }
}

/// `KrishivJob` custom resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KrishivJobResource {
    /// Kubernetes API version.
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// Kubernetes resource kind.
    pub kind: String,
    /// Resource metadata.
    pub metadata: ObjectMeta,
    /// Desired job state.
    pub spec: KrishivJobSpec,
    /// Observed job status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<KrishivJobStatus>,
}

impl KrishivJobResource {
    /// Create a new R2 `KrishivJob` resource.
    pub fn new(metadata: ObjectMeta, spec: KrishivJobSpec) -> Self {
        Self {
            api_version: format!("{API_GROUP}/{API_VERSION}"),
            kind: KIND.to_owned(),
            metadata,
            spec,
            status: None,
        }
    }

    /// Scheduler job id derived from metadata.
    pub fn scheduler_job_id(&self) -> String {
        self.metadata.scheduler_job_id()
    }
}

/// Desired execution mode in a `KrishivJob`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KrishivJobMode {
    /// Bounded batch job.
    Batch,
    /// Early R2 streaming job with R1-level stream semantics.
    Streaming,
}

impl From<KrishivJobMode> for JobKind {
    fn from(value: KrishivJobMode) -> Self {
        match value {
            KrishivJobMode::Batch => Self::Batch,
            KrishivJobMode::Streaming => Self::Streaming,
        }
    }
}

/// Kubernetes restart policy accepted by the R2 CRD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RestartPolicy {
    /// Do not restart failed pods.
    #[default]
    Never,
    /// Restart on failure.
    OnFailure,
}

/// Desired `KrishivJob` spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KrishivJobSpec {
    /// Bounded batch or early R2 streaming execution.
    pub mode: KrishivJobMode,
    /// Container image used by executors for this job.
    pub image: String,
    /// Number of static tasks for the R2 scheduler.
    pub tasks: usize,
    /// Maximum executor task parallelism requested by the job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<usize>,
    /// Optional container entrypoint override.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entrypoint: Vec<String>,
    /// Optional container arguments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Restart policy.
    #[serde(default, rename = "restartPolicy")]
    pub restart_policy: RestartPolicy,
    /// Optional labels propagated to future runtime objects.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// When true, the operator runs a per-job orchestration loop (JCP) in addition
    /// to the cluster control plane tick loops (ADR-DIST-01).
    #[serde(default, rename = "dedicatedCoordinator")]
    pub dedicated_coordinator: bool,
}

impl KrishivJobSpec {
    /// Create a new job spec.
    pub fn new(mode: KrishivJobMode, image: impl Into<String>, tasks: usize) -> Self {
        Self {
            mode,
            image: image.into(),
            tasks,
            parallelism: None,
            entrypoint: Vec::new(),
            args: Vec::new(),
            restart_policy: RestartPolicy::Never,
            labels: BTreeMap::new(),
            dedicated_coordinator: false,
        }
    }

    /// Effective task parallelism requested by the job.
    pub fn effective_parallelism(&self) -> usize {
        self.parallelism.unwrap_or(self.tasks)
    }
}

/// Observed `KrishivJob` phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KrishivJobPhase {
    /// Resource was accepted by the controller.
    Accepted,
    /// Resource is being planned.
    Planning,
    /// Job is running.
    Running,
    /// Job succeeded.
    Succeeded,
    /// Job failed.
    Failed,
    /// Job was cancelled.
    Cancelled,
}

impl From<JobState> for KrishivJobPhase {
    fn from(value: JobState) -> Self {
        match value {
            JobState::Accepted => Self::Accepted,
            JobState::Planning => Self::Planning,
            JobState::Running => Self::Running,
            JobState::Succeeded => Self::Succeeded,
            JobState::Failed => Self::Failed,
            JobState::Cancelled => Self::Cancelled,
        }
    }
}

/// Task counters stored under `.status.tasks`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStatusCounters {
    /// Assigned task count.
    pub assigned: usize,
    /// Running task count.
    pub running: usize,
    /// Succeeded task count.
    pub succeeded: usize,
    /// Failed task count.
    pub failed: usize,
}

/// Kubernetes condition status values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionStatus {
    /// Condition is true.
    True,
    /// Condition is false.
    False,
    /// Condition is unknown.
    Unknown,
}

/// Kubernetes-style condition stored under `.status.conditions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCondition {
    /// Condition type.
    #[serde(rename = "type")]
    pub condition_type: String,
    /// Condition status.
    pub status: ConditionStatus,
    /// Machine-readable reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Human-readable message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Last transition timestamp. R2 tests keep this unset for determinism.
    #[serde(
        default,
        rename = "lastTransitionTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_transition_time: Option<String>,
}

impl JobCondition {
    pub fn new(
        condition_type: impl Into<String>,
        status: ConditionStatus,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            condition_type: condition_type.into(),
            status,
            reason: Some(reason.into()),
            message: Some(message.into()),
            last_transition_time: None,
        }
    }
}

/// Observed `KrishivJob` status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KrishivJobStatus {
    /// High-level job phase.
    pub phase: KrishivJobPhase,
    /// Active coordinator id that observed the status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator: Option<String>,
    /// Kubernetes generation observed by the controller.
    #[serde(rename = "observedGeneration")]
    pub observed_generation: i64,
    /// Number of scheduler stages.
    pub stages: usize,
    /// Task state counters.
    pub tasks: TaskStatusCounters,
    /// Kubernetes-style status conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<JobCondition>,
}
