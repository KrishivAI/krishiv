//! Kubernetes API constants.

/// Krishiv Kubernetes API group.
pub const API_GROUP: &str = "krishiv.io";

/// KrishivJob API version owned by R2.
pub const API_VERSION: &str = "v1alpha1";

/// KrishivJob Kubernetes kind.
pub const KIND: &str = "KrishivJob";

/// R2 finalizer name reserved for future cleanup.
pub const FINALIZER: &str = "krishiv.io/job-finalizer";

/// Pod label used to associate an executor pod with a scheduler executor id.
pub const EXECUTOR_ID_LABEL: &str = "krishiv.io/executor-id";
pub const FIELD_MANAGER: &str = "krishiv-operator";
