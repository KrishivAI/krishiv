use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use krishiv_proto::JobSpec;
use serde::{Deserialize, Serialize};

use crate::{NamespaceQuotaSnapshot, SubmitOutcome};

/// Admission decision returned by a `QueueManager`.
///
/// Receives the static `JobSpec` and a live `NamespaceQuotaSnapshot` from the
/// coordinator. Implementations compare the spec's resource requests against
/// the snapshot's current reservations and their own configured limits.
pub trait QueueManager: Send + Sync + fmt::Debug {
    /// Return whether `spec` may enter the scheduler immediately.
    ///
    /// `quota` contains the live reservation totals for the job's namespace.
    fn admit(&self, spec: &JobSpec, quota: &NamespaceQuotaSnapshot) -> SubmitOutcome;

    /// Notify the queue manager when a job reaches a terminal state.
    ///
    /// `usage` carries the accumulated cost from `TaskRuntimeStats`. The
    /// default is a no-op; stateful implementations may use this for
    /// accounting or logging.
    fn on_job_complete(&self, _job_id: &krishiv_proto::JobId, _usage: &crate::ResourceUsage) {}
}

/// Always-admit queue manager for embedded and test contexts.
///
/// Every job is immediately accepted regardless of quota snapshot values. This
/// is the default; R7.1 `QuotaQueueManager` and `CrdQueueManager` replace it
/// for production deployments.
#[derive(Debug, Default, Clone)]
pub struct InMemoryQueueManager;

impl QueueManager for InMemoryQueueManager {
    fn admit(&self, _spec: &JobSpec, _quota: &NamespaceQuotaSnapshot) -> SubmitOutcome {
        SubmitOutcome::Accepted
    }
}

// ── QuotaQueueManager (process-mode quota enforcement) ───────────────────────

/// Static resource limits for one namespace (or the default namespace).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaPolicy {
    /// Maximum total CPU nanoseconds reserved simultaneously (`None` = unlimited).
    pub cpu_nanos_limit: Option<u64>,
    /// Maximum total memory bytes reserved simultaneously (`None` = unlimited).
    pub memory_bytes_limit: Option<u64>,
    /// Maximum number of concurrently active jobs (`None` = unlimited).
    pub max_concurrent_jobs: Option<usize>,
}

/// Quota-aware queue manager for process (non-Kubernetes) deployments.
///
/// Checks `cpu_limit_nanos`, `memory_limit_bytes`, and concurrent-job count
/// against per-namespace or default policies. A job that would exceed any
/// limit is returned as `Queued { position: 0 }` rather than rejected, so the
/// caller may retry admission after earlier jobs complete.
///
/// When constructed via [`QuotaQueueManager::with_state_path`], `namespace_policies`
/// are persisted to disk on every mutation and reloaded automatically on startup,
/// so they survive coordinator restarts.
#[derive(Debug)]
pub struct QuotaQueueManager {
    default_policy: QuotaPolicy,
    namespace_policies: HashMap<String, QuotaPolicy>,
    /// If `Some`, mutations to `namespace_policies` are atomically written to
    /// this path so they survive process restarts.
    state_path: Option<PathBuf>,
}

impl QuotaQueueManager {
    /// Create a quota manager with a default policy and optional per-namespace overrides.
    ///
    /// No persistence is configured; use [`Self::with_state_path`] for durable storage.
    pub fn new(
        default_policy: QuotaPolicy,
        namespace_policies: HashMap<String, QuotaPolicy>,
    ) -> Self {
        Self {
            default_policy,
            namespace_policies,
            state_path: None,
        }
    }

    /// Create a quota manager with a single default policy applied to all namespaces.
    pub fn with_default(default_policy: QuotaPolicy) -> Self {
        Self::new(default_policy, HashMap::new())
    }

    /// Create a quota manager that persists `namespace_policies` to `path`.
    ///
    /// If `path` already exists its contents are deserialized and used as the
    /// initial `namespace_policies` (log-warn on parse error). All subsequent
    /// calls to [`Self::register_policy`] and [`Self::remove_policy`] atomically
    /// flush the updated policies to disk so they survive coordinator restarts.
    pub fn with_state_path(path: PathBuf) -> Self {
        let mut mgr = Self::new(QuotaPolicy::default(), HashMap::new());
        if path.exists() {
            match fs::read_to_string(&path) {
                Ok(content) => match serde_json::from_str::<HashMap<String, QuotaPolicy>>(&content)
                {
                    Ok(policies) => {
                        mgr.namespace_policies = policies;
                    }
                    Err(e) => {
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "QuotaQueueManager: failed to deserialize state file; starting with empty policies"
                        );
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "QuotaQueueManager: failed to read state file; starting with empty policies"
                    );
                }
            }
        }
        mgr.state_path = Some(path);
        mgr
    }

    /// Register (or replace) a per-namespace `policy`.
    ///
    /// If a `state_path` is configured the updated policies are atomically
    /// flushed to disk.
    pub fn register_policy(&mut self, namespace: String, policy: QuotaPolicy) {
        self.namespace_policies.insert(namespace, policy);
        self.persist();
    }

    /// Remove the per-namespace policy for `namespace`.
    ///
    /// Returns the removed policy, if any. If a `state_path` is configured the
    /// updated policies are atomically flushed to disk.
    pub fn remove_policy(&mut self, namespace: &str) -> Option<QuotaPolicy> {
        let removed = self.namespace_policies.remove(namespace);
        if removed.is_some() {
            self.persist();
        }
        removed
    }

    /// Atomically persist `namespace_policies` to `state_path`.
    ///
    /// Serializes to JSON on the calling thread (fast), then spawns a detached
    /// thread for the actual disk write so the coordinator lock is not held
    /// across blocking I/O. Errors are logged at WARN — persistence is best-effort.
    fn persist(&self) {
        let Some(ref path) = self.state_path else {
            return;
        };

        let json = match serde_json::to_string(&self.namespace_policies) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "QuotaQueueManager: failed to serialize namespace policies"
                );
                return;
            }
        };

        let path = path.clone();
        std::thread::spawn(move || {
            let tmp_path = path.with_extension("tmp");
            if let Err(e) = fs::write(&tmp_path, &json) {
                tracing::warn!(
                    path = %tmp_path.display(),
                    error = %e,
                    "QuotaQueueManager: failed to write temporary state file"
                );
                return;
            }
            if let Err(e) = fs::rename(&tmp_path, &path) {
                tracing::warn!(
                    src = %tmp_path.display(),
                    dst = %path.display(),
                    error = %e,
                    "QuotaQueueManager: failed to rename temporary state file"
                );
            }
        });
    }

    fn policy_for(&self, namespace_id: Option<&str>) -> &QuotaPolicy {
        match namespace_id {
            Some(ns) => self
                .namespace_policies
                .get(ns)
                .unwrap_or(&self.default_policy),
            None => &self.default_policy,
        }
    }
}

impl QueueManager for QuotaQueueManager {
    fn admit(&self, spec: &JobSpec, quota: &NamespaceQuotaSnapshot) -> SubmitOutcome {
        let policy = self.policy_for(spec.namespace_id());

        if let Some(limit) = policy.max_concurrent_jobs
            && quota.active_job_count >= limit
        {
            return SubmitOutcome::Queued {
                position: quota.active_job_count - limit,
            };
        }
        if let Some(limit) = policy.cpu_nanos_limit {
            let requested = spec.cpu_limit_nanos().unwrap_or(0);
            if quota.cpu_nanos_reserved.saturating_add(requested) > limit {
                return SubmitOutcome::Queued { position: 0 };
            }
        }
        if let Some(limit) = policy.memory_bytes_limit {
            let requested = spec.memory_limit_bytes().unwrap_or(0);
            if quota.memory_bytes_reserved.saturating_add(requested) > limit {
                return SubmitOutcome::Queued { position: 0 };
            }
        }
        SubmitOutcome::Accepted
    }
}

// ── ConfigFileQueueManager ────────────────────────────────────────────────────

/// On-disk config format for `ConfigFileQueueManager`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct QueueConfig {
    #[serde(default)]
    default: QuotaPolicy,
    #[serde(default)]
    namespaces: HashMap<String, QuotaPolicy>,
}

/// File-backed queue manager that reads quota policies from a JSON config file.
///
/// Policies are loaded once at construction time. Re-load by creating a new
/// instance from the updated file. This keeps the implementation free of async
/// runtimes and background threads.
///
/// Config file format (JSON):
/// ```json
/// {
///   "default": { "max_concurrent_jobs": 10 },
///   "namespaces": {
///     "analytics": { "cpu_nanos_limit": 1000000000000, "memory_bytes_limit": 8589934592 }
///   }
/// }
/// ```
#[derive(Debug)]
pub struct ConfigFileQueueManager {
    inner: QuotaQueueManager,
}

impl ConfigFileQueueManager {
    /// Load queue policies from the JSON file at `path`.
    pub fn from_path(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: QueueConfig = serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Self {
            inner: QuotaQueueManager::new(config.default, config.namespaces),
        })
    }

    /// Construct directly from a `QueueConfig` (useful in tests).
    pub fn from_config(default: QuotaPolicy, namespaces: HashMap<String, QuotaPolicy>) -> Self {
        Self {
            inner: QuotaQueueManager::new(default, namespaces),
        }
    }
}

impl QueueManager for ConfigFileQueueManager {
    fn admit(&self, spec: &JobSpec, quota: &NamespaceQuotaSnapshot) -> SubmitOutcome {
        self.inner.admit(spec, quota)
    }
}

// ── CrdQueueManager ────────────────────────────────────────────────────────────

/// Queue manager that reloads admission policies from a JSON config file
/// periodically, suitable for Kubernetes ConfigMap-mounted deployments.
///
/// On each call to `admit()`, the manager checks whether the file has been
/// modified since the last load and reloads if needed. This avoids the need
/// for a background thread while still picking up operator- or CRD-driven
/// policy changes within one `reload_interval`.
#[derive(Debug)]
pub struct CrdQueueManager {
    inner: Arc<RwLock<QuotaQueueManager>>,
    config_path: PathBuf,
    last_modified: Arc<RwLock<Option<std::time::SystemTime>>>,
    reload_interval: Duration,
}

impl CrdQueueManager {
    /// Create a `CrdQueueManager` that loads policies from `path` and
    /// checks for updates at most once every `reload_interval`.
    pub fn new(path: impl Into<PathBuf>, reload_interval: Duration) -> std::io::Result<Self> {
        let path = path.into();
        let config = Self::load_config(&path)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(config)),
            config_path: path,
            last_modified: Arc::new(RwLock::new(None)),
            reload_interval,
        })
    }

    fn load_config(path: &Path) -> std::io::Result<QuotaQueueManager> {
        let content = std::fs::read_to_string(path)?;
        let config: QueueConfig = serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(QuotaQueueManager::new(config.default, config.namespaces))
    }

    fn maybe_reload(&self) {
        match std::fs::metadata(&self.config_path) {
            Ok(meta) => {
                let mtime = meta.modified().ok();
                let should_reload = {
                    let last = self.last_modified.read().unwrap_or_else(|p| p.into_inner());
                    mtime != *last
                };
                if should_reload {
                    // Check reload interval to avoid thundering re-reads.
                    let can_reload = {
                        let last = self.last_modified.read().unwrap_or_else(|p| p.into_inner());
                        last.map_or(true, |t| {
                            t.elapsed().unwrap_or(Duration::MAX) >= self.reload_interval
                        })
                    };
                    if can_reload {
                        match Self::load_config(&self.config_path) {
                            Ok(qm) => {
                                *self.inner.write().unwrap_or_else(|p| p.into_inner()) = qm;
                                *self
                                    .last_modified
                                    .write()
                                    .unwrap_or_else(|p| p.into_inner()) = mtime;
                                tracing::info!(
                                    path = %self.config_path.display(),
                                    "crd queue manager reloaded admission policies"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    path = %self.config_path.display(),
                                    error = %e,
                                    "crd queue manager failed to reload policies; using cached"
                                );
                            }
                        }
                    }
                }
            }
            Err(_) => {
                // File may not exist yet; keep current policies.
            }
        }
    }
}

impl QueueManager for CrdQueueManager {
    fn admit(&self, spec: &JobSpec, quota: &NamespaceQuotaSnapshot) -> SubmitOutcome {
        self.maybe_reload();
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .admit(spec, quota)
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;

    #[test]
    fn crd_queue_manager_reload_picks_up_new_policy() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("admission.json");

        // Write initial policy: max 1 concurrent job.
        let initial = r#"{"default": {"max_concurrent_jobs": 1}}"#;
        std::fs::write(&config_path, initial).unwrap();

        let mgr = CrdQueueManager::new(&config_path, Duration::from_secs(0)).unwrap();

        let spec = JobSpec::new(
            krishiv_proto::JobId::try_new("job-1").unwrap(),
            "test",
            krishiv_proto::JobKind::Batch,
        );
        let quota = NamespaceQuotaSnapshot::default();

        // First job admitted.
        let outcome = mgr.admit(&spec, &quota);
        assert!(
            matches!(outcome, SubmitOutcome::Accepted),
            "first job must be accepted: {:?}",
            outcome
        );

        // Update policy: max 0 concurrent jobs (block all).
        let updated = r#"{"default": {"max_concurrent_jobs": 0}}"#;
        // Sleep past mtime resolution (some FS have 1s granularity).
        std::thread::sleep(std::time::Duration::from_secs(1));
        std::fs::write(&config_path, updated).unwrap();

        // Next admission should block.
        let outcome = mgr.admit(&spec, &quota);
        assert!(
            !matches!(outcome, SubmitOutcome::Accepted),
            "job should be queued after policy update: {:?}",
            outcome
        );
    }
}
