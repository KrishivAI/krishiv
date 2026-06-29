//! KrishivJob status patching.

pub use crate::crd::job::{
    ConditionStatus, JobCondition, KrishivJobPhase, KrishivJobStatus, TaskStatusCounters,
};
#[cfg(feature = "k8s")]
pub use crate::dynamic::{patch_krishivjob_finalizer, patch_krishivjob_status, status_patch};
