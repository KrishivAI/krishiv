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
        "KRISHIV_ANN_AUTO_REWRITE",
        FlagKind::Bool,
        "on",
        "ANN auto-rewrite of `ORDER BY <distance> LIMIT k` onto a staged vector index (Phase 36 G19). `off` disables the rewrite entirely and every such query runs as an exact scan. Single-node engine only.",
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
        "KRISHIV_COMPUTE_THREADS",
        FlagKind::UInt,
        "auto (max(1, cores - 1))",
        "Thread count for the global Rayon compute-kernel pool (shuffle/checkpoint/decode kernels); 0 or unset auto-sizes to cores minus one reserved for the async reactor.",
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
        "256",
        "Maximum concurrently executing Flight SQL queries.",
    ),
    rt(
        "KRISHIV_FLIGHT_MAX_RESULT_BYTES",
        FlagKind::UInt,
        "2147483648",
        "Per-query Flight SQL result-size cap. NOT unlimited when unset: the compiled-in default is 2 GiB.",
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
        // Real default is 10 (krishiv-executor cli.rs `unwrap_or(10)`); the
        // registry declared 5, so docs/env-flags.md and `krishiv doctor` lied.
        "10",
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
        "67108864",
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
        "min(available_parallelism, 8)",
        "Shard count for an auto-partitioned coordinator-resident IVM flow; 1 \
         disables auto-partitioning. Unset derives from CPU count, capped at 8 \
         (`krishiv_scheduler::ivm::default_ivm_shards`), not 1.",
    ),
    rt(
        "KRISHIV_IVM_SPILL_DIR",
        FlagKind::Text,
        "OS temp directory",
        "Directory an IVM tick's DataFusion spill files are written to.",
    ),
    rt(
        "KRISHIV_IVM_SPILL_MAX_DISK_BYTES",
        FlagKind::UInt,
        "10737418240",
        "Ceiling on bytes an IVM tick's spill directory may hold; 0/unparseable \
         falls back to the default.",
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
        "128",
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
        // Real default is 100 (krishiv-mcp `DEFAULT_MAX_ROWS`); was declared 1000.
        "100",
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
        // Real default is 256 (krishiv-sql `PLAN_CACHE_MAX_ENTRIES`); was declared 128.
        "256",
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
        "Total FairSpillPool budget SHARED by every engine in the process \
         (task slots, Flight SQL, IVM); 0 disables the limit.",
    ),
    rt(
        "KRISHIV_QUERY_SPILL_DIR",
        FlagKind::Text,
        "OS temp directory",
        "Directory batch SQL spill files are written to. The OS default is a \
         tmpfs on some hosts, where spilling consumes the memory it relieves.",
    ),
    rt(
        "KRISHIV_QUERY_SPILL_MAX_DISK_BYTES",
        FlagKind::UInt,
        "max(80% of spill filesystem free space, 100 GiB)",
        "Ceiling on the total size of the batch SQL spill directory.",
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
        "8589934592",
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
        "KRISHIV_CROSS_STAGE_RUNTIME_FILTER",
        FlagKind::Bool,
        "off",
        "Cross-stage runtime bloom filters: a distributed plan gains a filter stage over the join build side, and the probe stage drops non-matching rows BEFORE shuffling them. Distinct from KRISHIV_RUNTIME_FILTERS, which is DataFusion's in-plan mechanism and cannot see across a stage cut. Read on the coordinator, at planning time.",
    ),
    rt(
        "KRISHIV_STAGE_REUSE",
        FlagKind::Bool,
        "off",
        "Stage reuse (Spark's ReuseExchange): two leaf stages that compute the same rows with the same shuffle contract are collapsed into one, and both consumers read the same shuffle output. Measured on SF100 to remove a duplicate FULL lineitem scan from q18 and q21 and a duplicate partsupp scan from q2. Restricted to leaf stages, and refused when the plan text mentions a volatile function. Read on the coordinator, at planning time.",
    ),
    rt(
        "KRISHIV_SEMI_JOIN_PUSHDOWN",
        FlagKind::Bool,
        "on",
        "Push a semi-join through an inner join. Gated by KRISHIV_SEMI_JOIN_REDUCTION as well, so turning that off disables both.",
    ),
    rt(
        "KRISHIV_LATE_MATERIALIZATION",
        FlagKind::Bool,
        "on",
        "Late materialisation of a bounded top-N aggregate: group on the declared key alone, take the top N, then re-join the base tables to fetch the columns the key determines, so those columns never enter the joins or the shuffle. Requires a declared PRIMARY KEY (see ParquetTableSpec::with_primary_key) and an ORDER BY ... LIMIT of at most 10000. On TPC-H q10 at SF100 the seven grouping columns are 14.8x of the query. Read wherever a query is planned.",
    ),
    rt(
        "KRISHIV_SHUFFLE_FETCH_BUFFER",
        FlagKind::UInt,
        "1",
        "How many upstream map fragments a reduce partition opens concurrently. Raising this is NOT free: the shuffle server holds a do_get permit for the lifetime of each response. Measured on a 3-node SF100 cluster: 4 wedged it at 0% CPU, and 2 wedged TPC-H q10 for 40+ minutes moving ~2.5 KB of network in 45 s (1 moved 1.77 GB in the same window). Stays at 1 until the server stops holding a permit across the response.",
    ),
    rt(
        "KRISHIV_SHUFFLE_FETCH_TRANSPORT_GRACE_SECS",
        FlagKind::UInt,
        "90",
        "Wall-clock grace for retrying a shuffle fetch across a transport error, covering an executor restart. `0` disables the grace; NotFound still fails fast.",
    ),
    rt(
        "KRISHIV_BROADCAST_JOIN_BYTES",
        FlagKind::UInt,
        "33554432 (32 MiB), staged path only",
        "Build-side byte ceiling under which a join is BROADCAST rather than \
         hash-shuffled, on the distributed staged path only (embedded keeps \
         DataFusion's 1 MiB default, which is right for a single process where \
         a shuffle is a memcpy). Here a shuffle is the pod network, measured at \
         ~11 MiB/s across separate hosts: TPC-H q8/q9 hash-partition the raw \
         600M-row lineitem scan — ~36 GiB on the wire — because the filtered \
         dimension side lands just over 1 MiB and so is not eligible to \
         broadcast. Bounded by the per-task memory share, since the build side \
         is collected per task.",
    ),
    rt(
        "KRISHIV_SPILL_JOIN_BUILD_BYTES",
        FlagKind::UInt,
        "50% of the per-task memory share (disabled when uncapped)",
        "Hash-join build sides with a KNOWN size estimate above this many bytes \
         are planned as sort-merge joins, which can spill, instead of hash \
         joins, which cannot (TPC-H q18: 'Resources exhausted ... \
         HashJoinInput[0] ... 732.4 MB'). Unknown estimates and uncapped \
         engines keep hash join.",
    ),
    rt(
        "KRISHIV_SHUFFLE_GRPC_MAX_MESSAGE_BYTES",
        FlagKind::UInt,
        "268435456 (256 MiB)",
        "Maximum gRPC message the shuffle transport will send or decode. Must \
         exceed the shuffle writer's coalesce target with room to spare, or a \
         map task produces a partition the reduce side refuses to decode — \
         tonic reports OutOfRange, the consumer retries forever and finally \
         reports NotFound, which reads as a lost shuffle rather than an \
         oversized message. TPC-H q10 at SF100 died this way on every sweep.",
    ),
    rt(
        "KRISHIV_SHUFFLE_WIRE_COMPRESSION",
        FlagKind::Enum(&["lz4", "zstd", "none"]),
        "lz4",
        "Arrow IPC body compression for shuffle partitions on the wire: \
         lz4 | zstd | none. Reduce tasks fetch their input from other \
         executors over Flight, so most of every shuffle crosses the pod \
         network — measured at ~7.6 MB/s here against 150-286 MB/s for a \
         node-local read. tonic is built without a compression feature, so \
         gRPC-level compression is unavailable rather than merely off; this is \
         the layer that covers the transfer. Compression at rest is a separate \
         knob (KRISHIV_SHUFFLE_STORAGE_COMPRESSION). The codec travels in each \
         record-batch message, so a reader decompresses without negotiation and \
         a peer sending uncompressed data still works. LZ4 runs two orders of \
         magnitude faster than this link, so the default is one-sided here; set \
         `none` on a fast fabric, where the CPU cost becomes comparable to the \
         transfer it saves.",
    ),
    rt(
        "KRISHIV_SHUFFLE_STORAGE_COMPRESSION",
        FlagKind::Enum(&["lz4", "zstd", "none"]),
        "lz4",
        "Codec for shuffle partitions at rest — Parquet on local disk, Arrow \
         IPC in the object store: lz4 | zstd | none. Both stores previously \
         constructed themselves with None, and the only production caller that \
         ever set a codec was the shuffle HTTP service; every backend a \
         distributed query actually uses (local, object, tiered) took the \
         default, so partitions were written raw. An object-store partition \
         crosses the pod network twice, once written and once fetched. Both \
         formats are self-describing — Parquet records its codec in file \
         metadata, Arrow IPC in each record-batch message — so changing this \
         is safe under partitions already written, in both directions.",
    ),
    rt(
        "KRISHIV_GRACE_HASH_JOIN",
        FlagKind::Bool,
        "off",
        "Send an over-threshold hash join to the grace hash join — partition \
         both sides by key and join bucket by bucket, each bucket an ordinary \
         in-memory hash join — instead of converting it to a sort-merge join. \
         Sort-merge sorts BOTH inputs in full even when nearly all the data \
         would have fitted, which cost TPC-H q2 6.3x (208s -> 1317s); grace \
         sorts nothing and only the buckets that overflow reach disk. Off by \
         default: sort-merge is what the SF100 sweeps have been measured \
         against, and a newer operator earns the default by beating it on the \
         cluster. Falls back to sort-merge for shapes it refuses (today, a \
         broadcast join whose sides have different partition counts).",
    ),
    rt(
        "KRISHIV_GRACE_HASH_JOIN_BUCKETS",
        FlagKind::UInt,
        "32, or enough that a bucket lands near half the per-task share",
        "Hash buckets the grace hash join partitions each side into when the \
         build side overflows its budget. Clamped to [2, 256]. Larger is \
         safer: a bucket is one temp file and one small in-memory join, while \
         too few buckets means a bucket that still does not fit, which is the \
         failure the operator exists to prevent.",
    ),
    rt(
        "KRISHIV_UNSPILLABLE_HEADROOM_PERCENT",
        FlagKind::UInt,
        "25",
        "Percent of the query memory pool reserved for consumers that CANNOT \
         spill (a hash-join build side is the main one). FairSpillPool caps \
         spillable consumers at a share of the pool but lets unspillable ones \
         take only what remains after both classes, so N spillers each inside \
         their own share can together leave nothing for a small hash join — \
         which is how a 2.6 GB pool refused 877 bytes. 0 disables the guard.",
    ),
    rt(
        "KRISHIV_SHUFFLE_PAGE_CACHE_BYTES",
        FlagKind::UInt,
        "12.5% of the cgroup limit (512 MiB uncontained)",
        "Ceiling on page cache held by committed-but-unconsumed shuffle \
         partitions. Cached output serves same-node reduce reads from RAM \
         instead of disk; the bound stops it growing unreclaimed across stages, \
         which is what got executors OOM-killed at SF100.",
    ),
    rt(
        "KRISHIV_SEMI_JOIN_REDUCTION",
        FlagKind::Bool,
        "on",
        "Semi-join reduction through an aggregate: when a grouped aggregate is \
         inner-joined on one of its own grouping keys, filter the aggregate's \
         input to the surviving keys first. TPC-H q17 spends 88% of its compute \
         building ~20M groups when the join keeps ~2000. `off` disables it.",
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
        "134217728",
        "In-memory shuffle store budget before spill/rejection.",
    ),
    rt(
        "KRISHIV_SHUFFLE_PARTITIONS",
        FlagKind::UInt,
        "target-parallelism",
        "Default shuffle partition count for distributed plans.",
    ),
    rt(
        "KRISHIV_SHUFFLE_SERVE_CONCURRENCY",
        FlagKind::UInt,
        "cgroup-derived",
        "Concurrent shuffle Flight `do_get` responses one executor will serve; \
         bounds the aggregate bytes held for consumers across all peers. \
         Derived from the page-cache budget in units of the 32 MiB inline-read \
         limit, floor 2.",
    ),
    rt(
        "KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES",
        FlagKind::UInt,
        "67108864",
        "Sort-shuffle writer in-memory buffer threshold before spilling a run.",
    ),
    rt(
        "KRISHIV_SHUFFLE_STORE_BYTES",
        FlagKind::UInt,
        "cgroup-derived",
        "Push-shuffle store ceiling, carved from the same container budget as \
         the query pool so the two cannot together exceed the container; \
         0 disables the limit.",
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
        "cluster-derived",
        "Planning-time partition count for distributed batch stages (scan + \
         shuffle fan-out); unset derives 2 tasks per live cluster slot.",
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
        "KRISHIV_BATCH_TASK_TIMEOUT_SECS",
        FlagKind::UInt,
        "3600",
        "Watchdog timeout for batch task execution. A per-task task_timeout_secs still wins. Previously compile-time only, while the streaming watchdog was tunable — the asymmetry had no rationale and SF100 queries run within 2x of this ceiling.",
    ),
    rt(
        "KRISHIV_STREAMING_TASK_TIMEOUT_SECS",
        FlagKind::UInt,
        "300",
        "Watchdog timeout for streaming task cycles. NOT disabled when unset: the compiled-in default is 300 s. A per-task task_timeout_secs still wins.",
    ),
    rt(
        "KRISHIV_RLOOP_EGRESS_CAP",
        FlagKind::UInt,
        "512",
        "Run-loop egress buffer cap in batches. The buffer drops its OLDEST batch on overflow, so this bounds how much computed output a slow drain consumer may lose before catching up; it is a per-JOB budget shared by co-located subtasks. 0 and unparseable values fall back to the default (a 0 cap would discard every batch).",
    ),
    rt(
        "KRISHIV_RLOOP_EGRESS_BACKPRESSURE_MS",
        FlagKind::UInt,
        "30000",
        "How long a run-loop stalls when its egress ring is full AND the ring is the job's ONLY delivery path (no durable sink). ADR §73: dropping there is silent data loss, so the loop waits for a consumer instead; the wait is bounded because an unbounded one wedges a job nobody drains, and the job faults at the deadline. Ignored when a durable sink is attached — that buffer's overflow is not loss and is still trimmed. 0 is MEANINGFUL and preserved: never wait, fault immediately. Only unparseable values fall back to the default.",
    ),
    rt(
        "KRISHIV_RLOOP_INPUT_BUFFER_CAP",
        FlagKind::UInt,
        "64",
        "Per-buffer cap on pending pushed batches for a run-loop subtask input key. Pushes beyond it are refused with backpressure (HTTP 429 via the coordinator); throughput is bounded by drain rate x this cap.",
    ),
    rt(
        "KRISHIV_ALLOW_UNFLUSHED_BOUNDED",
        FlagKind::Bool,
        "false",
        "Accept a bounded streaming job whose runtime cannot flush its open windows at end-of-stream. Off by default: such a run omits one row per group with no other sign, so it now fails instead of reporting success. Set to 1 only when a partial answer is genuinely acceptable.",
    ),
    rt(
        "KRISHIV_BENCH_PARALLELISM",
        FlagKind::UInt,
        "3",
        "Run-loop subtask parallelism the distributed NEXMark harness (nexmark_distributed) requests per non-pipeline job. Pipelines are always registered at 1 (stage re-keying).",
    ),
    rt(
        "KRISHIV_BENCH_CHECKPOINT_INTERVAL_MS",
        FlagKind::UInt,
        "0",
        "Non-zero: the distributed NEXMark harness registers every job with barrier checkpointing at this interval (durable-mode benchmark). 0 (default): no checkpointing.",
    ),
    rt(
        "KRISHIV_BENCH_CHECKPOINT_PATH",
        FlagKind::Text,
        "file:///var/lib/krishiv/checkpoints",
        "Checkpoint storage path the distributed NEXMark harness passes at registration when KRISHIV_BENCH_CHECKPOINT_INTERVAL_MS is non-zero.",
    ),
    rt(
        "KRISHIV_IVM_MAX_PENDING_BYTES",
        FlagKind::UInt,
        "1073741824",
        "Per-IVM-job cap on queued input bytes across all sources. A /feed that would push the backlog past it is refused with HTTP 429 instead of growing an unbounded in-memory queue (audit INT-F11). 0 restores the old unbounded behaviour; a value that does not parse falls back to the default.",
    ),
    rt(
        "KRISHIV_IVM_LEGACY_TICK_WIRE",
        FlagKind::Bool,
        "false",
        "Forces the coordinator to send the pre-IVMD2 JSON payload on every resident IVM tick, even to an executor whose attach echo said it reads the binary one (audit INT-F19). The operator escape hatch for the tick-wire change: it gives back the old behaviour exactly, which means giving up the 25% wire saving AND per-view health on resident ticks (a JSON tick is answered on the v1 result wire, so /step reports view_health.reported=false).",
    ),
    rt(
        "KRISHIV_IVM_DISPATCH_TIMEOUT_SECS",
        FlagKind::UInt,
        "300",
        "How long the coordinator waits for an executor-resident IVM tick before falling back to central compute. Also bounds how long DELETE on a job can block behind an in-flight tick (audit DIST-H7). 0 or an unparseable value falls back to the default.",
    ),
    rt(
        "KRISHIV_BENCH_CASE_FILTER",
        FlagKind::Text,
        "",
        "Comma-separated NEXMark case names (e.g. q3_local_items,q8_monitor_new_users) that the streaming-terminal harness runs instead of the full sweep, for fast A/B iteration on one shape. Empty (default): run every case.",
    ),
    rt(
        "KRISHIV_FLIGHT_URL",
        FlagKind::Text,
        "http://127.0.0.1:27075",
        "Coordinator Flight endpoint the terminal NEXMark harness (nexmark_terminal) builds its distributed Session against (task #151).",
    ),
    rt(
        "KRISHIV_BENCH_DIRECT_PUSH",
        FlagKind::Bool,
        "unset",
        "Set to 1: the distributed NEXMark harness resolves executor ingest targets once per job (GET /api/v1/continuous/{job}/targets) and pushes Arrow IPC straight to executor task gRPC endpoints, bypassing the coordinator HTTP hop and base64/JSON re-encode (task #149 fix 7). Requires the producer to REACH executor endpoints: in-cluster or loopback single-node; pod IPs are unreachable through a coordinator-only tunnel.",
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
        "capacity-derived",
        "Executor task slots; unset derives from CPU cores and the cgroup \
         memory limit together. Also sizes per-task parallelism.",
    ),
    rt(
        "KRISHIV_TASK_TARGET_PARALLELISM",
        FlagKind::UInt,
        "cores/slots",
        "DataFusion parallelism per executor task engine; unset = per-slot \
         share of cores, using the RESOLVED slot count (incl. --slots).",
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
        "KRISHIV_TPCH_ONLY",
        FlagKind::Text,
        "unset",
        "Comma-separated TPC-H query names (e.g. `q10,q21`) restricting a verify/bench run; unset runs all 22.",
    ),
    test(
        "KRISHIV_TPCH_SCALE_FACTOR",
        FlagKind::Float,
        "1.0",
        "TPC-H scale factor the verifier assumes; q11's threshold is scale-dependent, so this must match the data being checked.",
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
    rt(
        "KRISHIV_DIMENSION_REDUCTION",
        FlagKind::Bool,
        "false",
        "Enable the broadcast-dimension reducer (physical). Opt-in: the logical form of this rule was cleared by a three-query A/B, shipped on, and cost q10 18x.",
    ),
    rt(
        "KRISHIV_CTE_MATERIALIZE",
        FlagKind::Bool,
        "true",
        "Materialise a CTE referenced more than once instead of letting DataFusion inline it per reference, unless its consumers filter it. Single-query process only. TPC-DS SF1: q36 2.2x, q27 1.9x, suite +2.6%, 99/99 identical. Set 0/false to disable.",
    ),
    rt(
        "KRISHIV_JOIN_REORDER",
        FlagKind::Bool,
        "true",
        "Reorder inner-join chains smallest-connected-first from the engine's row-count registry. DataFusion has no join reordering, so join order is FROM-clause order; TPC-DS SF1 q72 is 10.2x on this and the 99-query suite 15%. Set 0/false to disable.",
    ),
    rt(
        "KRISHIV_SEMI_JOIN_DIMENSION",
        FlagKind::Bool,
        "false",
        "Enable the selective-dimension semi-join reducer, which reduces a fact stream by a filtered dimension before the large joins (q7).",
    ),
    rt(
        "KRISHIV_ELASTIC_DF_SHARE",
        FlagKind::Bool,
        "true",
        "Let a task slot borrow idle DataFusion partitions from other slots. Set 0/false to disable; an explicit KRISHIV_TARGET_PARALLELISM pin disables it outright.",
    ),
    rt(
        "KRISHIV_FLIGHT_DRAIN_ACTION_MAX_BYTES",
        FlagKind::UInt,
        "50331648",
        "Response budget in bytes for the Flight SQL ContinuousDrain do_action. Must stay under the client's 64 MiB do_action cap; oversized responses error with the data put back.",
    ),
    rt(
        "KRISHIV_KAFKA_DECODE_BATCH",
        FlagKind::UInt,
        "512",
        "Maximum records decoded per Kafka source poll. Values <= 0 are ignored.",
    ),
    rt(
        "KRISHIV_SHUFFLE_FETCH_OPEN_TIMEOUT_SECS",
        FlagKind::UInt,
        "15",
        "Per-attempt timeout for opening a shuffle fetch stream, before the retry policy's transport grace applies.",
    ),
    bench(
        "KRISHIV_BENCH_IVM_ROWS",
        FlagKind::Text,
        "unset",
        "Comma-separated row counts replacing the IVM-vs-full-recompute ladder outright, for pinning a crossover between the fixed rungs.",
    ),
    bench(
        "KRISHIV_BENCH_CORPUS_SEED",
        FlagKind::UInt,
        "20000",
        "Rows per source seeded before the NEXMark corpus-tick benchmark starts timing. Scaling THIS with the delta pinned is what separates a genuine O(delta) tick from one carrying a state-proportional term.",
    ),
    bench(
        "KRISHIV_BENCH_CORPUS_DELTA",
        FlagKind::UInt,
        "5000",
        "Rows per source fed before each timed tick of the NEXMark corpus-tick benchmark. Scaling this instead of the seed exposes terms quadratic in the delta (how IVM-AUD-PERF-2 was found).",
    ),
    bench(
        "KRISHIV_BENCH_CORPUS_TICKS",
        FlagKind::UInt,
        "5",
        "Timed ticks per query per flow in the NEXMark corpus-tick benchmark; the median is reported.",
    ),
    bench(
        "KRISHIV_BENCH_CORPUS_ONLY",
        FlagKind::Text,
        "unset",
        "Restrict the NEXMark corpus-tick benchmark to queries whose name contains this substring.",
    ),
    bench(
        "KRISHIV_BENCH_PLANT_REGRESSION_MS",
        FlagKind::UInt,
        "unset",
        "Add this many milliseconds to every timed streaming-latency sample, to verify the regression gate actually fires.",
    ),
    test(
        "KRISHIV_SOAK_SECONDS",
        FlagKind::UInt,
        "86400",
        "Duration of the lateness soak harness run.",
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

/// The **declared** default for `name`, parsed as a number.
///
/// `None` when the flag is unknown or its documented default is prose
/// (`"unset"`, `"derived from the cgroup limit"`) rather than a value.
///
/// # Why this exists
///
/// This registry generates `docs/reference/env-flags.md` and the `krishiv
/// doctor` listing, so its `default` field is what an operator sizing a
/// cluster reads. But the value the engine actually uses lives in a
/// `DEFAULT_*` const next to the code that reads the variable, and **nothing
/// connected the two**. They drifted, and an audit found seven flags whose
/// published default was wrong — `KRISHIV_RESULT_SPOOL_MAX_BYTES` documented
/// as 1 GiB against a real 8 GiB, `KRISHIV_MAX_CONCURRENT_ASSIGNMENT_RPCS` as
/// 16 against 128 — plus two that misdescribed *behaviour*, promising
/// "unset = unlimited" and "unset = disabled" where the code enforces a 2 GiB
/// cap and a 300 s watchdog.
///
/// The registry test proves every flag that is *read* is *declared*. It could
/// not prove the declaration is true. This accessor is what lets each owning
/// crate assert that its compiled-in default matches what the docs promise,
/// at the exact spot a developer would change the number.
#[must_use]
pub fn declared_default_number(name: &str) -> Option<u64> {
    lookup(name)
        .map(|spec| spec.default)
        .and_then(|raw| raw.trim().replace('_', "").parse().ok())
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
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs")
                || path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".rs.inc"))
            {
                // `.rs.inc` sections are compiled via `include!` and hold real
                // `KRISHIV_*` reads (e.g. krishiv-executor/src/sections/*.rs.inc).
                // Their extension is `inc`, not `rs`, so the plain `== "rs"`
                // filter skipped them — the registry-rot guard had a hole a flag
                // read only from a `.rs.inc` would slip through.
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
        "KRISHIV_BLESS_CERT_MATRIX",
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
