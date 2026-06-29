//! Executor pod launch failure detection.

use krishiv_proto::ExecutorId;
use serde_json::Value;

use crate::constants::EXECUTOR_ID_LABEL;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorPodLaunchFailure {
    /// Scheduler executor id associated with the failed pod, when known.
    pub executor_id: Option<ExecutorId>,
    /// Machine-readable reason, suitable for a status condition.
    pub reason: String,
    /// Human-readable message from pod status/container waiting state.
    pub message: String,
}

impl ExecutorPodLaunchFailure {
    pub(crate) fn new(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            executor_id: None,
            reason: reason.into(),
            message: message.into(),
        }
    }

    pub(crate) fn with_executor_id(mut self, executor_id: ExecutorId) -> Self {
        self.executor_id = Some(executor_id);
        self
    }
}

/// Detect a classified executor pod launch failure from a Kubernetes Pod JSON value.
///
/// Used by the operator when watching executor pods spawned for a `KrishivJob`. Tests use JSON
/// fixtures so detection remains deterministic without a Kubernetes API server.
pub fn detect_executor_pod_launch_failure(pod: &Value) -> Option<ExecutorPodLaunchFailure> {
    let executor_id = executor_id_from_pod(pod);
    let status = pod.get("status")?;
    let phase = status.get("phase").and_then(Value::as_str);
    let reason = status.get("reason").and_then(Value::as_str);
    let message = status.get("message").and_then(Value::as_str);
    if phase == Some("Failed") {
        return Some(with_optional_executor_id(
            ExecutorPodLaunchFailure::new(
                reason.unwrap_or("PodFailed"),
                message.unwrap_or("executor pod failed before task launch"),
            ),
            executor_id,
        ));
    }
    if reason == Some("Unschedulable") {
        return Some(with_optional_executor_id(
            ExecutorPodLaunchFailure::new(
                "Unschedulable",
                message.unwrap_or("executor pod is unschedulable"),
            ),
            executor_id,
        ));
    }

    for status in status
        .get("containerStatuses")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let waiting = status.get("state").and_then(|state| state.get("waiting"));
        if let Some(waiting) = waiting {
            let reason = waiting
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("ContainerWaiting");
            if matches!(
                reason,
                "ImagePullBackOff"
                    | "ErrImagePull"
                    | "CrashLoopBackOff"
                    | "CreateContainerConfigError"
                    | "CreateContainerError"
            ) {
                return Some(with_optional_executor_id(
                    ExecutorPodLaunchFailure::new(
                        reason,
                        waiting
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("executor container failed to launch"),
                    ),
                    executor_id,
                ));
            }
        }
    }

    None
}

fn executor_id_from_pod(pod: &Value) -> Option<ExecutorId> {
    pod.get("metadata")
        .and_then(|metadata| metadata.get("labels"))
        .and_then(|labels| labels.get(EXECUTOR_ID_LABEL))
        .and_then(Value::as_str)
        .and_then(|value| ExecutorId::try_new(value).ok())
}

fn with_optional_executor_id(
    failure: ExecutorPodLaunchFailure,
    executor_id: Option<ExecutorId>,
) -> ExecutorPodLaunchFailure {
    match executor_id {
        Some(executor_id) => failure.with_executor_id(executor_id),
        None => failure,
    }
}
