#![forbid(unsafe_code)]

//! Central registry of every `KRISHIV_*` runtime flag (Phase 51, audit §12).
//!
//! Every environment flag the engine reads is declared here exactly once with
//! its type, default, and documentation. Daemon startups call
//! [`log_env_issues`] so a typo'd flag (`…_LIMIT_BYTE`) produces a startup
//! warning instead of being silently ignored, and an invalid value for a
//! known flag is reported against its declared type.
//!
//! A registry test scans the workspace sources and fails when a `KRISHIV_*`
//! literal is read anywhere without being declared here — the registry cannot
//! silently rot.
//!
//! The reference documentation (`docs/reference/env-flags.md`) and the
//! `krishiv doctor` flag listing are both generated from this table via
//! [`reference_markdown`].

/// Value type of a flag, used for startup validation and doc generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlagKind {
    /// Boolean; recognized truthy values: `1`, `true`, `yes`, `on`
    /// (case-insensitive, trimmed). Everything else is false.
    Bool,
    /// Unsigned integer (`u64`).
    UInt,
    /// Signed integer (`i64`).
    Int,
    /// Floating-point number.
    Float,
    /// Free-form text.
    Text,
    /// Filesystem path (no existence check at validation time).
    Path,
    /// `host:port` socket address.
    SocketAddr,
    /// URL/URI (scheme-prefixed).
    Url,
    /// Comma-separated list.
    List,
    /// Credential material — never log the value.
    Secret,
    /// One of a closed set of values.
    Enum(&'static [&'static str]),
}

/// Where a flag is consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlagScope {
    /// Read by production binaries (daemons, CLI, libraries).
    Runtime,
    /// Read only by tests / e2e harnesses.
    Test,
    /// Read only by benchmark harnesses.
    Bench,
}

/// A single declared environment flag.
#[derive(Debug, Clone, Copy)]
pub struct FlagSpec {
    /// Full env-var name (`KRISHIV_…`).
    pub name: &'static str,
    /// Value type, used for validation + docs.
    pub kind: FlagKind,
    /// Human-readable default (`"unset"` when absence means disabled).
    pub default: &'static str,
    /// One-line description for generated docs and `doctor`.
    pub doc: &'static str,
    /// Consumer scope.
    pub scope: FlagScope,
}

const fn rt(
    name: &'static str,
    kind: FlagKind,
    default: &'static str,
    doc: &'static str,
) -> FlagSpec {
    FlagSpec {
        name,
        kind,
        default,
        doc,
        scope: FlagScope::Runtime,
    }
}

const fn test(
    name: &'static str,
    kind: FlagKind,
    default: &'static str,
    doc: &'static str,
) -> FlagSpec {
    FlagSpec {
        name,
        kind,
        default,
        doc,
        scope: FlagScope::Test,
    }
}

const fn bench(
    name: &'static str,
    kind: FlagKind,
    default: &'static str,
    doc: &'static str,
) -> FlagSpec {
    FlagSpec {
        name,
        kind,
        default,
        doc,
        scope: FlagScope::Bench,
    }
}

/// Dynamic flag prefixes: any var starting with one of these is a declared
/// pass-through namespace (e.g. Iceberg REST catalog properties).
pub const FLAG_PREFIXES: &[(&str, &str)] = &[(
    "KRISHIV_ICEBERG_REST_",
    "Pass-through namespace: `KRISHIV_ICEBERG_REST_<PROP>` becomes the Iceberg REST catalog property `<prop>` (lower-cased). Named vars (URI/NAME/TOKEN/WAREHOUSE) are declared individually.",
)];

/// Every `KRISHIV_*` flag the engine reads, alphabetical by name.
pub static FLAGS: &[FlagSpec] = &[
    rt(
        "KRISHIV_ALLOW_ANONYMOUS",
        FlagKind::Bool,
        "false",
        "Allow unauthenticated coordinator gRPC (operator + coordinator daemon). Production profiles refuse to start with this set unless explicitly overridden.",
    ),
    rt(
        "KRISHIV_ALLOW_ANONYMOUS_HTTP",
        FlagKind::Bool,
        "false",
        "Allow unauthenticated HTTP control-plane routes. Logs a warning when active in production mode.",
    ),
    rt(
        "KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS",
        FlagKind::Bool,
        "false",
        "Permit native (full-privilege) scalar UDF registration under restrictive durability profiles.",
    ),
    rt(
        "KRISHIV_ALLOW_LEGACY_FRAGMENTS",
        FlagKind::Bool,
        "false",
        "Permit untyped legacy task fragments (stream:*, raw SQL strings) outside dev-local.",
    ),
    rt(
        "KRISHIV_API_KEY",
        FlagKind::Secret,
        "unset",
        "Single Flight SQL API key presented by clients (fallback for KRISHIV_FLIGHT_API_KEY).",
    ),
    rt(
        "KRISHIV_AQE",
        FlagKind::Bool,
        "on",
        "Adaptive query execution master switch (Phase 54). `off` disables every stage-boundary rewrite (coalescing, skew split) and the placeholder-plan hint pass; per-mechanism flags refine it.",
    ),
    rt(
        "KRISHIV_AQE_COALESCE",
        FlagKind::Bool,
        "on",
        "AQE reduce-partition coalescing: merge small measured shuffle partitions into fewer reduce tasks (dfplan multi-partition bodies). Subordinate to KRISHIV_AQE.",
    ),
    rt(
        "KRISHIV_AQE_SKEW_FACTOR",
        FlagKind::Float,
        "4.0",
        "A reduce partition is skewed when its measured bytes exceed this factor x the median partition size (and KRISHIV_AQE_SKEW_MIN_BYTES).",
    ),
    rt(
        "KRISHIV_AQE_SKEW_MIN_BYTES",
        FlagKind::UInt,
        "134217728",
        "Absolute floor (bytes) below which a reduce partition is never treated as skewed (default 128 MiB).",
    ),
    rt(
        "KRISHIV_AQE_SKEW_SPLIT",
        FlagKind::Bool,
        "on",
        "AQE skew handling: split a skewed reduce partition into map-task-range sub-tasks (split-safe plans only). Subordinate to KRISHIV_AQE.",
    ),
    rt(
        "KRISHIV_AQE_TARGET_PARTITION_BYTES",
        FlagKind::UInt,
        "67108864",
        "Target upstream shuffle bytes per reduce task for AQE coalescing and skew-split sizing (default 64 MiB).",
    ),
    rt(
        "KRISHIV_API_KEYS",
        FlagKind::Secret,
        "unset",
        "Comma-separated set of accepted Flight SQL API keys (server side).",
    ),
    rt(
        "KRISHIV_BARRIER_GRPC_ADDR",
        FlagKind::SocketAddr,
        "unset",
        "Executor barrier-transport gRPC listen address (aligned window join / checkpoint barriers).",
    ),
    rt(
        "KRISHIV_BATCH_SIZE",
        FlagKind::UInt,
        "8192",
        "DataFusion execution batch size (rows per record batch).",
    ),
    rt(
        "KRISHIV_BENCH_IVM_MAX_ROWS",
        FlagKind::UInt,
        "unset",
        "Caps the IVM-vs-recompute benchmark row ladder; unset runs the full ladder.",
    ),
    rt(
        "KRISHIV_BATCH_SQL_TIMEOUT_SECS",
        FlagKind::UInt,
        "300",
        "Coordinator-mode batch SQL completion timeout in seconds.",
    ),
    rt(
        "KRISHIV_CA_CERT",
        FlagKind::Path,
        "unset",
        "CA certificate path used by gRPC clients to verify TLS server certs.",
    ),
    rt(
        "KRISHIV_CHECKPOINT_DIR",
        FlagKind::Path,
        "unset",
        "Local checkpoint directory for embedded/single-node sessions.",
    ),
    rt(
        "KRISHIV_CHECKPOINT_STORAGE",
        FlagKind::Url,
        "unset",
        "Checkpoint storage URI (memory://, file://…, s3://…). Durable profiles reject memory://.",
    ),
    rt(
        "KRISHIV_CLUSTER_DATA_DIR",
        FlagKind::Path,
        "~/.krishiv/cluster",
        "Data directory for `krishiv cluster` bare-metal deployments.",
    ),
    rt(
        "KRISHIV_CLUSTER_HTTP_ADDR",
        FlagKind::SocketAddr,
        "127.0.0.1:8080",
        "HTTP address for `krishiv cluster` status endpoints.",
    ),
    rt(
        "KRISHIV_COORDINATOR",
        FlagKind::Url,
        "unset",
        "Deprecated alias of KRISHIV_COORDINATOR_URL (CLI/query paths).",
    ),
    rt(
        "KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS",
        FlagKind::UInt,
        "30",
        "Interval for re-reading coordinator bearer-token files.",
    ),
    rt(
        "KRISHIV_COORDINATOR_AUTH_SECRET_KEY",
        FlagKind::Text,
        "token",
        "K8s Secret key holding the coordinator bearer token (operator-injected pods).",
    ),
    rt(
        "KRISHIV_COORDINATOR_AUTH_SECRET_NAME",
        FlagKind::Text,
        "unset",
        "K8s Secret name holding the coordinator bearer token (operator-injected pods).",
    ),
    rt(
        "KRISHIV_COORDINATOR_BEARER_TOKEN",
        FlagKind::Secret,
        "unset",
        "Bearer token clients present to the coordinator gRPC/HTTP APIs.",
    ),
    rt(
        "KRISHIV_COORDINATOR_BEARER_TOKENS",
        FlagKind::Secret,
        "unset",
        "Comma-separated set of accepted coordinator bearer tokens (server side).",
    ),
    rt(
        "KRISHIV_COORDINATOR_BEARER_TOKENS_FILE",
        FlagKind::Path,
        "unset",
        "File containing newline-separated accepted coordinator bearer tokens; hot-reloaded.",
    ),
    rt(
        "KRISHIV_COORDINATOR_BEARER_TOKEN_FILE",
        FlagKind::Path,
        "unset",
        "File containing a single accepted coordinator bearer token; hot-reloaded.",
    ),
    rt(
        "KRISHIV_COORDINATOR_ENDPOINT",
        FlagKind::Url,
        "unset",
        "Deprecated alias of KRISHIV_COORDINATOR_URL (executor/operator paths).",
    ),
    rt(
        "KRISHIV_COORDINATOR_HTTP",
        FlagKind::Url,
        "unset",
        "Coordinator HTTP base URL (control-plane REST), when it differs from the gRPC URL.",
    ),
    rt(
        "KRISHIV_COORDINATOR_ID",
        FlagKind::Text,
        "coordinator-1",
        "Stable identity of this coordinator instance (leader election, fencing).",
    ),
    rt(
        "KRISHIV_COORDINATOR_URL",
        FlagKind::Url,
        "unset",
        "Canonical coordinator gRPC URL clients and executors connect to.",
    ),
    rt(
        "KRISHIV_CTAS_TARGET_FILE_BYTES",
        FlagKind::UInt,
        "134217728",
        "Target data-file size for durable CTAS writes.",
    ),
    rt(
        "KRISHIV_DAEMON_RUNTIME_THREADS",
        FlagKind::UInt,
        "auto (min(cpu, 4) embedded; cpu for a full daemon)",
        "Tokio worker-thread count for a long-running coordinator/executor daemon runtime; 0 or unset auto-sizes from CPU count.",
    ),
    rt(
        "KRISHIV_DEPLOYMENT_TARGET",
        FlagKind::Text,
        "unknown",
        "Deployment label attached to telemetry (dev, staging, prod…).",
    ),
    rt(
        "KRISHIV_DURABILITY_PROFILE",
        FlagKind::Enum(&["dev-local", "single-node-durable", "distributed-durable"]),
        "dev-local",
        "Durability/safety profile; gates auth, state persistence, and connector requirements.",
    ),
    rt(
        "KRISHIV_ETCD_ENDPOINTS",
        FlagKind::List,
        "unset",
        "Comma-separated etcd endpoints for HA leader election (clusterd etcd feature).",
    ),
    rt(
        "KRISHIV_ETCD_LEADER_KEY",
        FlagKind::Text,
        "/krishiv/ccp/leader",
        "etcd key used for the coordinator leader lease.",
    ),
    rt(
        "KRISHIV_EXECUTOR_ID",
        FlagKind::Text,
        "unset",
        "Stable identity of this executor instance (assigned by operator/CLI).",
    ),
    rt(
        "KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES",
        FlagKind::UInt,
        "cgroup-derived",
        "Process-wide executor memory reservation layer; unset = unlimited.",
    ),
    rt(
        "KRISHIV_EXECUTOR_TASK_AUTH_SECRET_KEY",
        FlagKind::Text,
        "token",
        "K8s Secret key holding the executor task bearer token (operator-injected pods).",
    ),
    rt(
        "KRISHIV_EXECUTOR_TASK_AUTH_SECRET_NAME",
        FlagKind::Text,
        "unset",
        "K8s Secret name holding the executor task bearer token (operator-injected pods).",
    ),
    rt(
        "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN",
        FlagKind::Secret,
        "unset",
        "Bearer token the coordinator presents on executor task gRPC calls.",
    ),
    rt(
        "KRISHIV_FALLBACK_RUNTIME_THREADS",
        FlagKind::UInt,
        "2",
        "Worker threads for the shared fallback Tokio runtime used by sync-over-async bridges.",
    ),
    rt(
        "KRISHIV_FLIGHT_ADDR",
        FlagKind::SocketAddr,
        "127.0.0.1:50055",
        "Flight SQL service listen address.",
    ),
    rt(
        "KRISHIV_FLIGHT_ALLOW_ALL_AUTHENTICATED",
        FlagKind::Bool,
        "false",
        "Standalone Flight SQL: treat any authenticated subject as authorized \
         (AllowAllPolicyHook) instead of SEC-2 default-deny. For deployments \
         with no governance catalog; the API key is the authorization boundary.",
    ),
    rt(
        "KRISHIV_FLIGHT_API_KEY",
        FlagKind::Secret,
        "unset",
        "API key the Flight SQL client presents (takes precedence over KRISHIV_API_KEY).",
    ),
    rt(
        "KRISHIV_FLIGHT_MAX_CONCURRENT_QUERIES",
        FlagKind::UInt,
        "16",
        "Maximum concurrently executing Flight SQL queries.",
    ),
    rt(
        "KRISHIV_FLIGHT_MAX_RESULT_BYTES",
        FlagKind::UInt,
        "unset",
        "Per-query Flight SQL result-size cap; unset = unlimited.",
    ),
    rt(
        "KRISHIV_FLIGHT_PREPARED_STMT_CAPACITY",
        FlagKind::UInt,
        "128",
        "Maximum cached prepared statements per Flight SQL session.",
    ),
    rt(
        "KRISHIV_FLIGHT_REQUEST_TIMEOUT_SECS",
        FlagKind::UInt,
        "0",
        "Hard per-request deadline (seconds) on the client→coordinator Flight \
         channel; 0 (default) disables it so long-running distributed queries \
         are bounded by the coordinator's own statement timeout \
         (KRISHIV_BATCH_SQL_TIMEOUT_SECS) rather than a premature transport cap. \
         Dead peers are still detected via HTTP/2 keepalive.",
    ),
    rt(
        "KRISHIV_FULL_SNAPSHOT_EVERY",
        FlagKind::UInt,
        "8",
        "Every Nth checkpoint epoch takes a full portable snapshot in incremental mode (bounds the SST manifest chain).",
    ),
    rt(
        "KRISHIV_GLUE_CATALOG_ID",
        FlagKind::Text,
        "unset",
        "AWS Glue catalog ID (account) for the Glue catalog integration.",
    ),
    rt(
        "KRISHIV_GLUE_DATABASE",
        FlagKind::Text,
        "default",
        "AWS Glue database name for the Glue catalog integration.",
    ),
    rt(
        "KRISHIV_GRPC_ADDR",
        FlagKind::SocketAddr,
        "127.0.0.1:50051",
        "Coordinator gRPC listen address.",
    ),
    rt(
        "KRISHIV_GRPC_MAX_MESSAGE_BYTES",
        FlagKind::UInt,
        "268435456",
        "Maximum gRPC message size for coordinator/executor transports.",
    ),
    rt(
        "KRISHIV_HEALTH_PORT",
        FlagKind::UInt,
        "unset",
        "Standalone health-endpoint port for daemon deployments.",
    ),
    rt(
        "KRISHIV_HEARTBEAT_INTERVAL_SECS",
        FlagKind::UInt,
        "5",
        "Executor→coordinator heartbeat interval.",
    ),
    rt(
        "KRISHIV_HOT_KEY_BASE_ROWS_PER_SECOND",
        FlagKind::UInt,
        "10000",
        "Baseline per-key rate used by the adaptive hot-key detector.",
    ),
    rt(
        "KRISHIV_HTTP_ADDR",
        FlagKind::SocketAddr,
        "unset",
        "Executor HTTP listen address (control endpoints).",
    ),
    rt(
        "KRISHIV_ICEBERG_REST_NAME",
        FlagKind::Text,
        "main",
        "Catalog name to register the Iceberg REST catalog under.",
    ),
    rt(
        "KRISHIV_ICEBERG_REST_TOKEN",
        FlagKind::Secret,
        "unset",
        "Bearer token for the Iceberg REST catalog.",
    ),
    rt(
        "KRISHIV_ICEBERG_REST_URI",
        FlagKind::Url,
        "unset",
        "Iceberg REST catalog endpoint; presence activates the REST catalog.",
    ),
    rt(
        "KRISHIV_ICEBERG_REST_WAREHOUSE",
        FlagKind::Text,
        "empty",
        "Warehouse location/name passed to the Iceberg REST catalog.",
    ),
    rt(
        "KRISHIV_IDLE_TICK_MS",
        FlagKind::UInt,
        "engine default",
        "Continuous-engine idle tick interval in milliseconds.",
    ),
    rt(
        "KRISHIV_INCREMENTAL_CHECKPOINTS",
        FlagKind::Bool,
        "true",
        "RocksDB-backed window state checkpoints SST deltas instead of full snapshots (Phase 56).",
    ),
    rt(
        "KRISHIV_INLINE_IPC_MAX_BYTES",
        FlagKind::UInt,
        "4194304",
        "Maximum inline base64 Arrow IPC payload accepted in batch SQL requests.",
    ),
    rt(
        "KRISHIV_INLINE_RESULT_MAX_BYTES",
        FlagKind::UInt,
        "8388608",
        "Result size above which executor task output spools to disk instead of inlining.",
    ),
    rt(
        "KRISHIV_IVM_SHARDS",
        FlagKind::UInt,
        "1",
        "Shard count for coordinator-resident IVM flows.",
    ),
    rt(
        "KRISHIV_JCP_POLL_INTERVAL_SECS",
        FlagKind::UInt,
        "2",
        "Job-completion poll interval for job-mode coordinator runs.",
    ),
    rt(
        "KRISHIV_JOB_GC_GRACE_SECS",
        FlagKind::UInt,
        "30",
        "Grace window a terminal job stays queryable before the GC tick may \
         evict it, so a slow consumer still observes its outcome + result.",
    ),
    rt(
        "KRISHIV_JOB_ID",
        FlagKind::Text,
        "unset",
        "Job ID for single-job (job-mode) coordinator/executor pods.",
    ),
    rt(
        "KRISHIV_JOB_SPEC_JSON",
        FlagKind::Text,
        "unset",
        "Inline JSON job spec submitted at startup in job-mode.",
    ),
    rt(
        "KRISHIV_LEADER_BACKEND",
        FlagKind::Enum(&["single", "etcd"]),
        "single",
        "Coordinator leader-election backend.",
    ),
    rt(
        "KRISHIV_LEADER_LEASE_SECS",
        FlagKind::UInt,
        "15",
        "Leader lease TTL for etcd-backed election.",
    ),
    rt(
        "KRISHIV_LOG_FORMAT",
        FlagKind::Enum(&["json", "pretty", "compact"]),
        "json",
        "Log/stderr output format for the tracing subscriber (json = daemon default).",
    ),
    rt(
        "KRISHIV_LOCAL_DATA_DIR",
        FlagKind::Path,
        "~/.krishiv/local",
        "Data directory for `krishiv local` single-node deployments.",
    ),
    rt(
        "KRISHIV_LOCAL_HTTP_ADDR",
        FlagKind::SocketAddr,
        "127.0.0.1:8080",
        "HTTP address for `krishiv local` status endpoints.",
    ),
    rt(
        "KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT",
        FlagKind::UInt,
        "engine default",
        "Row cap for MATCH_RECOGNIZE evaluation over streaming inputs.",
    ),
    rt(
        "KRISHIV_MAX_CONCURRENT_ASSIGNMENT_RPCS",
        FlagKind::UInt,
        "16",
        "Coordinator-side concurrency cap for task assignment RPC fan-out.",
    ),
    rt(
        "KRISHIV_MAX_SHUFFLE_REGEN",
        FlagKind::UInt,
        "8",
        "Maximum times a lost shuffle partition may be regenerated before the \
         job fails terminally (consumer-driven FetchFailed recovery bound).",
    ),
    rt(
        "KRISHIV_MCP_ADDR",
        FlagKind::SocketAddr,
        "127.0.0.1:8811",
        "MCP server listen address (http transport).",
    ),
    rt(
        "KRISHIV_MCP_ALLOW_WRITE_SQL",
        FlagKind::Bool,
        "false",
        "Allow the MCP run_sql tool to execute write statements.",
    ),
    rt(
        "KRISHIV_MCP_MAX_ROWS",
        FlagKind::UInt,
        "1000",
        "Row cap on MCP query results.",
    ),
    rt(
        "KRISHIV_MCP_TIMEOUT_MS",
        FlagKind::UInt,
        "30000",
        "MCP tool execution timeout.",
    ),
    rt(
        "KRISHIV_MCP_TRANSPORT",
        FlagKind::Enum(&["stdio", "http"]),
        "stdio",
        "MCP server transport.",
    ),
    rt(
        "KRISHIV_METADATA_BACKEND",
        FlagKind::Enum(&["memory", "rocksdb", "redb"]),
        "rocksdb",
        "Coordinator metadata store backend.",
    ),
    rt(
        "KRISHIV_METADATA_PATH",
        FlagKind::Path,
        "unset",
        "Filesystem path for the persistent coordinator metadata store.",
    ),
    rt(
        "KRISHIV_MODE",
        FlagKind::Enum(&[
            "embedded",
            "single-node",
            "distributed",
            "bare-metal",
            "k8s",
        ]),
        "embedded",
        "Session execution mode selector.",
    ),
    rt(
        "KRISHIV_NAMESPACE",
        FlagKind::Text,
        "default",
        "Kubernetes namespace the operator manages.",
    ),
    rt(
        "KRISHIV_NAMESPACE_MAX_ACTIVE_JOBS",
        FlagKind::UInt,
        "unset",
        "Admission cap: maximum concurrently active jobs per namespace.",
    ),
    rt(
        "KRISHIV_NAMESPACE_MAX_CPU_NANOS",
        FlagKind::UInt,
        "unset",
        "Admission cap: maximum aggregate CPU (nanos) per namespace.",
    ),
    rt(
        "KRISHIV_NAMESPACE_MAX_MEMORY_BYTES",
        FlagKind::UInt,
        "unset",
        "Admission cap: maximum aggregate memory per namespace.",
    ),
    rt(
        "KRISHIV_OIDC_AUDIENCE",
        FlagKind::Text,
        "unset",
        "Expected audience claim for OIDC-authenticated coordinator requests.",
    ),
    rt(
        "KRISHIV_OIDC_JWKS_URI",
        FlagKind::Url,
        "unset",
        "JWKS endpoint for OIDC token verification; presence activates OIDC auth.",
    ),
    rt(
        "KRISHIV_PLAN_CACHE_MAX_ENTRIES",
        FlagKind::UInt,
        "128",
        "Logical-plan cache capacity per SQL session.",
    ),
    rt(
        "KRISHIV_PRODUCTION",
        FlagKind::Bool,
        "false",
        "Production mode: tightens defaults (fail-closed metadata, auth requirements, connector restrictions).",
    ),
    rt(
        "KRISHIV_PYTHON_UDF_TIMEOUT_MS",
        FlagKind::UInt,
        "30000",
        "Per-call timeout for sandboxed Python UDF execution.",
    ),
    rt(
        "KRISHIV_QUERY_MEMORY_LIMIT_BYTES",
        FlagKind::UInt,
        "cgroup-derived",
        "Per-query FairSpillPool budget for embedded/IVM sessions.",
    ),
    rt(
        "KRISHIV_RACK_ID",
        FlagKind::Text,
        "unset",
        "Rack identifier the executor advertises for RACK_LOCAL placement (Phase 53). Node identity is the executor host.",
    ),
    rt(
        "KRISHIV_REMOTE_EXEC",
        FlagKind::Bool,
        "mode-dependent",
        "Force remote (coordinator) execution on or off for API sessions.",
    ),
    rt(
        "KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH",
        FlagKind::Bool,
        "profile-dependent",
        "Require bearer auth on executor task gRPC even in dev profiles.",
    ),
    rt(
        "KRISHIV_RESULT_SPOOL_DIR",
        FlagKind::Path,
        "temp dir",
        "Directory for disk-spooled large query results.",
    ),
    rt(
        "KRISHIV_RESULT_SPOOL_MAX_BYTES",
        FlagKind::UInt,
        "1073741824",
        "Cap on total spooled result bytes per node.",
    ),
    rt(
        "KRISHIV_RESULT_SPOOL_SYNC_INTERVAL_BYTES",
        FlagKind::UInt,
        "67108864",
        "Bytes written between fsyncs of the disk-spooled result file; 0 or unset uses the 64 MiB default.",
    ),
    rt(
        "KRISHIV_ROCKSDB_MAX_OPEN_FILES",
        FlagKind::Int,
        "rocksdb default",
        "RocksDB max_open_files for state/metadata stores (-1 = unlimited).",
    ),
    rt(
        "KRISHIV_ROCKSDB_WRITE_BUFFER_MB",
        FlagKind::UInt,
        "rocksdb default",
        "RocksDB write-buffer (memtable) size in MiB.",
    ),
    rt(
        "KRISHIV_RUNTIME_FILTERS",
        FlagKind::Bool,
        "on",
        "DataFusion dynamic (runtime) filters: TopK / join / aggregate predicates pushed into probe-side file scans at execution time (Phase 54). `off` disables all three via the DataFusion master switch.",
    ),
    rt(
        "KRISHIV_SESSION_IDLE_TIMEOUT_SECS",
        FlagKind::UInt,
        "0",
        "Phase 59 session hardening: evict a Flight SQL session's per-session \
         bookkeeping after this many seconds with no active statements; 0 \
         (default) disables idle eviction.",
    ),
    rt(
        "KRISHIV_SESSION_MAX_CONCURRENT_STATEMENTS",
        FlagKind::UInt,
        "0",
        "Phase 59 session hardening: maximum statements a single Flight SQL \
         session (authenticated subject) may execute concurrently before \
         further statements are rejected with resource_exhausted; 0 (default) \
         disables the per-session cap. Complements the global \
         KRISHIV_FLIGHT_MAX_CONCURRENT_QUERIES.",
    ),
    rt(
        "KRISHIV_SHUFFLE_ADDR",
        FlagKind::SocketAddr,
        "127.0.0.1:50060",
        "Shuffle service HTTP listen address.",
    ),
    rt(
        "KRISHIV_SHUFFLE_DIR",
        FlagKind::Path,
        "temp dir",
        "Local-disk shuffle store directory.",
    ),
    rt(
        "KRISHIV_SHUFFLE_FETCH_CONCURRENCY",
        FlagKind::UInt,
        "4",
        "Reduce-side concurrent shuffle partition fetches.",
    ),
    rt(
        "KRISHIV_SHUFFLE_FETCH_RETRIES",
        FlagKind::UInt,
        "3",
        "Retry attempts per shuffle partition fetch.",
    ),
    rt(
        "KRISHIV_SHUFFLE_FETCH_RETRY_BASE_MS",
        FlagKind::UInt,
        "100",
        "Base backoff for shuffle fetch retries.",
    ),
    rt(
        "KRISHIV_SHUFFLE_FLIGHT_ADDR",
        FlagKind::SocketAddr,
        "unset",
        "Shuffle Flight transport listen address (executor).",
    ),
    rt(
        "KRISHIV_SHUFFLE_MEMORY_BYTES",
        FlagKind::UInt,
        "268435456",
        "In-memory shuffle store budget before spill/rejection.",
    ),
    rt(
        "KRISHIV_SHUFFLE_PARTITIONS",
        FlagKind::UInt,
        "target-parallelism",
        "Default shuffle partition count for distributed plans.",
    ),
    rt(
        "KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES",
        FlagKind::UInt,
        "67108864",
        "Sort-shuffle writer in-memory buffer threshold before spilling a run.",
    ),
    rt(
        "KRISHIV_SHUFFLE_TOKEN",
        FlagKind::Secret,
        "unset",
        "Bearer token protecting shuffle service endpoints.",
    ),
    rt(
        "KRISHIV_SHUFFLE_TOKEN_FILE",
        FlagKind::Path,
        "unset",
        "File containing the shuffle bearer token; hot-reloaded.",
    ),
    rt(
        "KRISHIV_SHUFFLE_TOKEN_RELOAD_SECS",
        FlagKind::UInt,
        "30",
        "Interval for re-reading the shuffle token file.",
    ),
    rt(
        "KRISHIV_SHUFFLE_URI",
        FlagKind::Url,
        "unset",
        "Shuffle backend URI (file://, s3://, tiered://local;s3://…).",
    ),
    rt(
        "KRISHIV_STAGE_SPLIT",
        FlagKind::Bool,
        "on",
        "Distributed batch stage splitting (Phase 52); off/0/false runs batch SQL single-task.",
    ),
    rt(
        "KRISHIV_STAGE_TARGET_PARTITIONS",
        FlagKind::UInt,
        "4",
        "Planning-time partition count for distributed batch stages (scan + shuffle fan-out).",
    ),
    rt(
        "KRISHIV_STATE_BACKEND",
        FlagKind::Enum(&["rocksdb", "disaggregated"]),
        "rocksdb",
        "Executor generic state backend; disaggregated = DFS-primary with local cache (requires KRISHIV_STATE_DFS_ROOT).",
    ),
    rt(
        "KRISHIV_STATE_DFS_ROOT",
        FlagKind::Path,
        "unset",
        "DFS/object-store root for the disaggregated state backend.",
    ),
    rt(
        "KRISHIV_STATE_DIR",
        FlagKind::Path,
        "unset",
        "Executor state-backend directory (RocksDB window/operator state).",
    ),
    rt(
        "KRISHIV_STREAMING_TASK_TIMEOUT_SECS",
        FlagKind::UInt,
        "unset",
        "Watchdog timeout for streaming task cycles; unset = disabled.",
    ),
    rt(
        "KRISHIV_STREAM_EARLY_FIRE_MS",
        FlagKind::UInt,
        "unset",
        "Speculative early-fire interval for open windows (embedded loop only — the distributed stream:rloop: run-loop does not read this flag).",
    ),
    rt(
        "KRISHIV_STREAM_LINGER_MS",
        FlagKind::UInt,
        "profile",
        "Run-loop batch/linger before each drain in ms; overrides the KRISHIV_STREAM_PROFILE default (0 low-latency, 5 throughput).",
    ),
    rt(
        "KRISHIV_STREAM_PROFILE",
        FlagKind::Enum(&["low-latency", "throughput"]),
        "low-latency",
        "Streaming loop profile: embedded checkpoint cadence and the distributed run-loop batch/linger dial (Phase 55).",
    ),
    rt(
        "KRISHIV_TARGET_PARALLELISM",
        FlagKind::UInt,
        "cores",
        "DataFusion target partition count for local execution.",
    ),
    rt(
        "KRISHIV_TASK_GRPC_ADDR",
        FlagKind::SocketAddr,
        "127.0.0.1:50052",
        "Executor task gRPC listen address.",
    ),
    rt(
        "KRISHIV_TASK_SLOTS",
        FlagKind::UInt,
        "CPU-derived",
        "Executor task slots; unset derives from available CPU cores.",
    ),
    rt(
        "KRISHIV_TASK_TARGET_PARALLELISM",
        FlagKind::UInt,
        "cores/slots",
        "DataFusion parallelism per executor task engine; unset = per-slot share of cores.",
    ),
    rt(
        "KRISHIV_TLS_CERT",
        FlagKind::Path,
        "unset",
        "TLS certificate path for coordinator/executor gRPC servers.",
    ),
    rt(
        "KRISHIV_TLS_KEY",
        FlagKind::Path,
        "unset",
        "TLS private-key path for coordinator/executor gRPC servers.",
    ),
    rt(
        "KRISHIV_UI",
        FlagKind::Bool,
        "on",
        "Embedded web-UI off-switch: KRISHIV_UI=off boots the daemon without the always-on embedded UI factory (certified platform profile sets off).",
    ),
    rt(
        "KRISHIV_UI_TOKEN",
        FlagKind::Secret,
        "unset",
        "Bearer token protecting the embedded web UI.",
    ),
    rt(
        "KRISHIV_UI_TOKEN_FILE",
        FlagKind::Path,
        "unset",
        "File containing the UI bearer token.",
    ),
    rt(
        "KRISHIV_UNITY_CATALOG_NAME",
        FlagKind::Text,
        "main",
        "Catalog name to register the Unity Catalog integration under.",
    ),
    rt(
        "KRISHIV_UNITY_HOST",
        FlagKind::Url,
        "unset",
        "Unity Catalog host URL; presence activates the integration.",
    ),
    rt(
        "KRISHIV_UNITY_TOKEN",
        FlagKind::Secret,
        "unset",
        "Bearer token for Unity Catalog.",
    ),
    rt(
        "KRISHIV_WATERMARK_IDLE_MS",
        FlagKind::UInt,
        "30000",
        "Run-loop per-split watermark idleness timeout: a silent split is excluded from the min-combine after this long (Phase 55 watermarks v2).",
    ),
    rt(
        "KRISHIV_WAREHOUSE_ROOT",
        FlagKind::Path,
        ".",
        "Root path for connector-table warehouse storage.",
    ),
    // ── Test-scope flags ────────────────────────────────────────────────
    test(
        "KRISHIV_KIND_CLUSTER",
        FlagKind::Text,
        "krishiv-e2e",
        "kind cluster name for operator e2e smoke tests.",
    ),
    test(
        "KRISHIV_KIND_E2E",
        FlagKind::Bool,
        "false",
        "Enable the kind-based operator e2e smoke tests.",
    ),
    test(
        "KRISHIV_KIND_IMAGE",
        FlagKind::Text,
        "unset",
        "Engine image to load into the kind cluster.",
    ),
    test(
        "KRISHIV_KIND_NAMESPACE",
        FlagKind::Text,
        "default",
        "Namespace used by kind e2e tests.",
    ),
    test(
        "KRISHIV_KIND_SKIP_CREATE",
        FlagKind::Bool,
        "false",
        "Reuse an existing kind cluster instead of creating one.",
    ),
    test(
        "KRISHIV_KIND_SKIP_LOAD_IMAGE",
        FlagKind::Bool,
        "false",
        "Skip loading the engine image into kind.",
    ),
    test(
        "KRISHIV_KIND_TIMEOUT_SECS",
        FlagKind::UInt,
        "300",
        "Timeout for kind e2e operations.",
    ),
    test(
        "KRISHIV_TEST_DATABASE_URL",
        FlagKind::Url,
        "unset",
        "Postgres URL for catalog integration tests.",
    ),
    test(
        "KRISHIV_TEST_S3_BUCKET",
        FlagKind::Text,
        "unset",
        "S3 bucket for object-store integration tests.",
    ),
    // ── Bench-scope flags ───────────────────────────────────────────────
    bench(
        "KRISHIV_TPCDS_DATA_DIR",
        FlagKind::Path,
        "unset",
        "TPC-DS dataset directory for the bench harness.",
    ),
    bench(
        "KRISHIV_TPCH_DATA_DIR",
        FlagKind::Path,
        "unset",
        "Legacy TPC-H SF10 dataset directory (prefer the _SF* variants).",
    ),
    bench(
        "KRISHIV_TPCH_DATA_DIR_SF1",
        FlagKind::Path,
        "unset",
        "TPC-H SF1 dataset directory.",
    ),
    bench(
        "KRISHIV_TPCH_DATA_DIR_SF10",
        FlagKind::Path,
        "unset",
        "TPC-H SF10 dataset directory.",
    ),
    bench(
        "KRISHIV_TPCH_DATA_DIR_SF100",
        FlagKind::Path,
        "unset",
        "TPC-H SF100 dataset directory.",
    ),
];

/// Look up a declared flag by exact name, falling back to prefix namespaces.
pub fn lookup(name: &str) -> Option<&'static FlagSpec> {
    FLAGS.iter().find(|f| f.name == name).or_else(|| {
        FLAG_PREFIXES
            .iter()
            .any(|(p, _)| name.starts_with(p) && name.len() > p.len())
            .then_some(&PREFIX_PASSTHROUGH)
    })
}

static PREFIX_PASSTHROUGH: FlagSpec = FlagSpec {
    name: "KRISHIV_ICEBERG_REST_*",
    kind: FlagKind::Text,
    default: "unset",
    doc: "Pass-through catalog property.",
    scope: FlagScope::Runtime,
};

// ── Shared parsers ──────────────────────────────────────────────────────

/// The single boolean-env parser for the workspace: `1`/`true`/`yes`/`on`
/// (case-insensitive, trimmed) are true; everything else (and unset) is false.
pub fn truthy_env(name: &str) -> bool {
    std::env::var(name).map(|v| is_truthy(&v)).unwrap_or(false)
}

/// Whether a raw string is in the recognized truthy set.
pub fn is_truthy(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Whether a raw string is in the recognized falsy set (for validation:
/// values outside both sets are reported as suspicious for Bool flags).
pub fn is_falsy(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "" | "0" | "false" | "no" | "off"
    )
}

/// Parse an env var as `u64`; `None` when unset or unparseable.
pub fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

/// Parse an env var as `usize`; `None` when unset or unparseable.
pub fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

// ── Coordinator endpoint aliasing ───────────────────────────────────────

/// Canonical coordinator URL variable.
pub const COORDINATOR_URL_ENV: &str = "KRISHIV_COORDINATOR_URL";
/// Deprecated aliases accepted for one release train with a startup warning.
pub const COORDINATOR_URL_ALIASES: &[&str] =
    &["KRISHIV_COORDINATOR", "KRISHIV_COORDINATOR_ENDPOINT"];

/// Resolve the coordinator URL from the canonical variable, falling back to
/// the deprecated aliases (warning once per process when an alias is used).
pub fn coordinator_url_env() -> Option<String> {
    if let Ok(v) = std::env::var(COORDINATOR_URL_ENV)
        && !v.trim().is_empty()
    {
        return Some(v);
    }
    for alias in COORDINATOR_URL_ALIASES {
        if let Ok(v) = std::env::var(alias)
            && !v.trim().is_empty()
        {
            warn_deprecated_alias(alias);
            return Some(v);
        }
    }
    None
}

fn warn_deprecated_alias(alias: &str) {
    use std::sync::OnceLock;
    static WARNED: OnceLock<()> = OnceLock::new();
    let mut first = false;
    WARNED.get_or_init(|| first = true);
    if first {
        tracing::warn!(
            alias,
            canonical = COORDINATOR_URL_ENV,
            "deprecated coordinator endpoint variable; use the canonical name"
        );
    }
}

// ── Startup validation ──────────────────────────────────────────────────

/// A problem detected while scanning the process environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvIssue {
    /// A `KRISHIV_*` var is set but not declared in the registry (typo?).
    Unknown { name: String },
    /// A declared var holds a value that does not parse as its kind.
    Invalid {
        name: String,
        kind: &'static str,
        value_hint: String,
    },
    /// A deprecated alias is set; the canonical name should be used.
    DeprecatedAlias {
        name: String,
        canonical: &'static str,
    },
}

impl std::fmt::Display for EnvIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown { name } => write!(
                f,
                "unrecognized environment flag {name} (not in the KRISHIV_* registry; typo?)"
            ),
            Self::Invalid {
                name,
                kind,
                value_hint,
            } => {
                write!(f, "{name} does not parse as {kind}: {value_hint}")
            }
            Self::DeprecatedAlias { name, canonical } => {
                write!(f, "{name} is deprecated; use {canonical}")
            }
        }
    }
}

/// Validate one raw value against a flag's declared kind (pure — no env
/// access), so callers holding values from another source (e.g. `doctor`'s
/// injected lookup) can reuse the exact validation rules.
pub fn validate_value(spec: &FlagSpec, value: &str) -> Option<EnvIssue> {
    let bad = |kind: &'static str, hint: String| EnvIssue::Invalid {
        name: spec.name.to_string(),
        kind,
        value_hint: hint,
    };
    let trimmed = value.trim();
    match spec.kind {
        FlagKind::Bool => {
            if !is_truthy(trimmed) && !is_falsy(trimmed) {
                return Some(bad(
                    "bool (1/true/yes/on or 0/false/no/off)",
                    format!("{trimmed:?} will be treated as false"),
                ));
            }
        }
        FlagKind::UInt => {
            if !trimmed.is_empty() && trimmed.parse::<u64>().is_err() {
                return Some(bad("unsigned integer", format!("{trimmed:?}")));
            }
        }
        FlagKind::Int => {
            if !trimmed.is_empty() && trimmed.parse::<i64>().is_err() {
                return Some(bad("integer", format!("{trimmed:?}")));
            }
        }
        FlagKind::Float => {
            if !trimmed.is_empty() && trimmed.parse::<f64>().is_err() {
                return Some(bad("number", format!("{trimmed:?}")));
            }
        }
        FlagKind::SocketAddr => {
            if !trimmed.is_empty() && trimmed.parse::<std::net::SocketAddr>().is_err() {
                return Some(bad("host:port socket address", format!("{trimmed:?}")));
            }
        }
        FlagKind::Enum(allowed) => {
            let norm = trimmed.to_ascii_lowercase();
            // Enum flags historically accept short/underscore aliases;
            // only report values that no reader would recognize.
            let recognized = allowed.iter().any(|a| {
                norm == *a || norm.replace('_', "-") == *a || a.starts_with(norm.as_str())
            });
            if !trimmed.is_empty() && !recognized {
                return Some(bad(
                    "one of the documented values",
                    format!("{trimmed:?} (expected one of {allowed:?})"),
                ));
            }
        }
        // Free-form kinds: nothing to validate without touching the
        // filesystem / network. Secrets are deliberately not inspected.
        FlagKind::Text | FlagKind::Path | FlagKind::Url | FlagKind::List | FlagKind::Secret => {}
    }
    None
}

/// Scan the process environment for `KRISHIV_*` issues.
pub fn validate_env() -> Vec<EnvIssue> {
    let mut issues = Vec::new();
    for (name, value) in std::env::vars() {
        if !name.starts_with("KRISHIV_") {
            continue;
        }
        let Some(spec) = lookup(&name) else {
            issues.push(EnvIssue::Unknown { name });
            continue;
        };
        if COORDINATOR_URL_ALIASES.contains(&name.as_str()) {
            issues.push(EnvIssue::DeprecatedAlias {
                name: name.clone(),
                canonical: COORDINATOR_URL_ENV,
            });
        }
        if let Some(issue) = validate_value(spec, &value) {
            issues.push(issue);
        }
    }
    issues.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    issues
}

/// Validate the environment and log every issue as a warning. Call once at
/// daemon startup (coordinator, executor, operator, flight host, MCP).
pub fn log_env_issues() {
    for issue in validate_env() {
        tracing::warn!(%issue, "environment flag issue");
    }
}

// ── Doc generation ──────────────────────────────────────────────────────

/// Render the registry as the committed reference document
/// (`docs/reference/env-flags.md`). A test asserts the committed file
/// matches this output, so the doc cannot drift from the code.
pub fn reference_markdown() -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32 * 1024);
    out.push_str(
        "# Environment flag reference\n\n\
         Generated from `krishiv-common::env_registry` — do not edit by hand.\n\
         Regenerate with:\n\
         `KRISHIV_BLESS_ENV_REFERENCE=1 cargo test -p krishiv-common env_registry`\n",
    );
    for (scope, title) in [
        (FlagScope::Runtime, "Runtime flags"),
        (FlagScope::Test, "Test-only flags"),
        (FlagScope::Bench, "Benchmark flags"),
    ] {
        let _ = writeln!(
            out,
            "\n## {title}\n\n| Name | Type | Default | Description |\n|---|---|---|---|"
        );
        for f in FLAGS.iter().filter(|f| f.scope == scope) {
            let kind = match f.kind {
                FlagKind::Bool => "bool".to_string(),
                FlagKind::UInt => "uint".to_string(),
                FlagKind::Int => "int".to_string(),
                FlagKind::Float => "float".to_string(),
                FlagKind::Text => "text".to_string(),
                FlagKind::Path => "path".to_string(),
                FlagKind::SocketAddr => "host:port".to_string(),
                FlagKind::Url => "url".to_string(),
                FlagKind::List => "list".to_string(),
                FlagKind::Secret => "secret".to_string(),
                FlagKind::Enum(vals) => vals.join(" \\| "),
            };
            let _ = writeln!(
                out,
                "| `{}` | {} | `{}` | {} |",
                f.name, kind, f.default, f.doc
            );
        }
    }
    out.push_str("\n## Dynamic namespaces\n\n");
    for (prefix, doc) in FLAG_PREFIXES {
        let _ = writeln!(out, "- `{prefix}<PROP>` — {doc}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::path::Path;

    fn scan_flags(dir: &Path, exclude_registry: bool, out: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).expect("read_dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "target" || name.starts_with('.') {
                    continue;
                }
                scan_flags(&path, exclude_registry, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                if exclude_registry && path.ends_with("krishiv-common/src/env_registry.rs") {
                    continue;
                }
                let src = std::fs::read_to_string(&path).expect("read source");
                let bytes = src.as_bytes();
                let mut i = 0;
                while let Some(pos) = src[i..].find("KRISHIV_") {
                    let start = i + pos;
                    let mut end = start + "KRISHIV_".len();
                    while end < bytes.len()
                        && (bytes[end].is_ascii_uppercase()
                            || bytes[end].is_ascii_digit()
                            || bytes[end] == b'_')
                    {
                        end += 1;
                    }
                    if end > start + "KRISHIV_".len() {
                        out.insert(src[start..end].to_string());
                    }
                    i = end;
                }
            }
        }
    }

    fn workspace_crates_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates dir")
            .to_path_buf()
    }

    /// Meta-flags of test harnesses, not engine configuration: doc/reference
    /// "bless" switches read only inside a test to regenerate a committed
    /// golden file (env reference, conformance corpus, connector-reachability
    /// doc, PySpark-parity doc, SQL-grammar doc). These are developer doc-gen
    /// toggles, never product configuration, so they are exempt from the
    /// declared-flag scan rather than surfaced in the env reference.
    const SCAN_ALLOWLIST: &[&str] = &[
        "KRISHIV_BLESS_ENV_REFERENCE",
        "KRISHIV_BLESS_CORPUS",
        "KRISHIV_BLESS_CONNECTOR_DOCS",
        "KRISHIV_BLESS_PYSPARK_PARITY",
        "KRISHIV_BLESS_SQL_DOCS",
    ];

    #[test]
    fn every_flag_read_in_source_is_declared() {
        let mut seen = BTreeSet::new();
        scan_flags(&workspace_crates_dir(), false, &mut seen);
        let undeclared: Vec<_> = seen
            .iter()
            .filter(|name| !SCAN_ALLOWLIST.contains(&name.as_str()))
            .filter(|name| {
                // trailing-underscore tokens are prefix literals; check as prefix ns
                lookup(name).is_none()
                    && !FLAG_PREFIXES.iter().any(|(p, _)| {
                        p.trim_end_matches('_') == name.trim_end_matches('_') || name.starts_with(p)
                    })
            })
            .collect();
        assert!(
            undeclared.is_empty(),
            "KRISHIV_* vars read in source but missing from env_registry::FLAGS \
             (declare them with type/default/doc): {undeclared:?}"
        );
    }

    #[test]
    fn every_declared_flag_still_exists_in_source() {
        let mut seen = BTreeSet::new();
        scan_flags(&workspace_crates_dir(), true, &mut seen);
        let stale: Vec<_> = FLAGS
            .iter()
            .map(|f| f.name)
            .filter(|name| !seen.contains(*name))
            .collect();
        assert!(
            stale.is_empty(),
            "flags declared in env_registry::FLAGS but no longer read anywhere \
             (remove the stale entries): {stale:?}"
        );
    }

    #[test]
    fn committed_reference_doc_matches_registry() {
        let doc_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .join("docs/reference/env-flags.md");
        let expected = reference_markdown();
        if std::env::var("KRISHIV_BLESS_ENV_REFERENCE").is_ok() {
            std::fs::write(&doc_path, &expected).expect("write reference doc");
            return;
        }
        let committed = std::fs::read_to_string(&doc_path).unwrap_or_default();
        assert_eq!(
            committed, expected,
            "docs/reference/env-flags.md is out of date; regenerate with \
             KRISHIV_BLESS_ENV_REFERENCE=1 cargo test -p krishiv-common env_registry"
        );
    }

    #[test]
    fn validate_env_flags_unknown_and_invalid() {
        // Not using set_var: validate_env reads the live process env, and
        // tests run multi-threaded. Exercise the pure paths instead.
        let not_real = format!("KRISHIV_{}", "NOT_A_REAL_FLAG");
        assert!(lookup(&not_real).is_none());
        assert!(lookup("KRISHIV_GRPC_ADDR").is_some());
        assert!(lookup("KRISHIV_ICEBERG_REST_CUSTOM_PROP").is_some());
        assert!(is_truthy(" TRUE "));
        assert!(is_truthy("on"));
        assert!(!is_truthy("enabled"));
        assert!(is_falsy("OFF"));
        assert!(!is_falsy("enabled"));
    }

    #[test]
    fn coordinator_alias_constants_are_declared() {
        assert!(lookup(COORDINATOR_URL_ENV).is_some());
        for alias in COORDINATOR_URL_ALIASES {
            assert!(lookup(alias).is_some(), "alias {alias} must be declared");
        }
    }

    /// FLAG-2 (audit §12): the security-relevant boolean flags
    /// (`KRISHIV_ALLOW_ANONYMOUS`, `KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH`,
    /// `KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS`) must resolve to the *same* boolean
    /// at every read site regardless of capitalization/spelling. The original
    /// finding was that one site parsed case-insensitively while another matched
    /// exact `"true"`/`"1"`, so a flag could silently take effect on one path
    /// and not another. Every site now routes through [`is_truthy`] /
    /// [`truthy_env`] (grpc.rs's `parse_bool_env` is a one-line wrapper over
    /// `truthy_env`, and `production.rs` uses `truthy_env` directly). This test
    /// locks the shared parser's behavior across the capitalization variants a
    /// deployment might realistically use, so a future divergent parser would
    /// have to break this assertion, not just a distant integration test.
    #[test]
    fn flag2_security_flags_parse_uniformly_across_capitalizations() {
        // Every documented truthy spelling, in the casings an operator might
        // plausibly write in Helm values / env, must be accepted.
        for truthy in [
            "1", "true", "TRUE", "True", "yes", "YES", "on", "ON", " on ", "  TrUe  ",
        ] {
            assert!(
                is_truthy(truthy),
                "{truthy:?} must be recognized as enabling a security flag"
            );
            assert!(
                !is_falsy(truthy),
                "{truthy:?} must not also be recognized as falsy"
            );
        }
        // Falsy / absent spellings must never enable a fail-closed flag.
        for falsy in ["0", "false", "FALSE", "no", "NO", "off", "OFF", "", "   "] {
            assert!(
                !is_truthy(falsy),
                "{falsy:?} must NOT enable a security flag"
            );
        }
        // A typo'd value ("enabled") is neither truthy nor falsy — it is
        // reported as suspicious by validate_env rather than silently enabling.
        assert!(!is_truthy("enabled"));
        assert!(!is_falsy("enabled"));
    }
}
