# Environment flag reference

Generated from `krishiv-common::env_registry` — do not edit by hand.
Regenerate with:
`KRISHIV_BLESS_ENV_REFERENCE=1 cargo test -p krishiv-common env_registry`

## Runtime flags

| Name | Type | Default | Description |
|---|---|---|---|
| `KRISHIV_ALLOW_ANONYMOUS` | bool | `false` | Allow unauthenticated coordinator gRPC (operator + coordinator daemon). Production profiles refuse to start with this set unless explicitly overridden. |
| `KRISHIV_ALLOW_ANONYMOUS_HTTP` | bool | `false` | Allow unauthenticated HTTP control-plane routes. Logs a warning when active in production mode. |
| `KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS` | bool | `false` | Permit native (full-privilege) scalar UDF registration under restrictive durability profiles. |
| `KRISHIV_ALLOW_LEGACY_FRAGMENTS` | bool | `false` | Permit untyped legacy task fragments (stream:*, raw SQL strings) outside dev-local. |
| `KRISHIV_API_KEY` | secret | `unset` | Single Flight SQL API key presented by clients (fallback for KRISHIV_FLIGHT_API_KEY). |
| `KRISHIV_API_KEYS` | secret | `unset` | Comma-separated set of accepted Flight SQL API keys (server side). |
| `KRISHIV_BARRIER_GRPC_ADDR` | host:port | `unset` | Executor barrier-transport gRPC listen address (aligned window join / checkpoint barriers). |
| `KRISHIV_BATCH_SIZE` | uint | `8192` | DataFusion execution batch size (rows per record batch). |
| `KRISHIV_BATCH_SQL_TIMEOUT_SECS` | uint | `300` | Coordinator-mode batch SQL completion timeout in seconds. |
| `KRISHIV_CA_CERT` | path | `unset` | CA certificate path used by gRPC clients to verify TLS server certs. |
| `KRISHIV_CHECKPOINT_DIR` | path | `unset` | Local checkpoint directory for embedded/single-node sessions. |
| `KRISHIV_CHECKPOINT_STORAGE` | url | `unset` | Checkpoint storage URI (memory://, file://…, s3://…). Durable profiles reject memory://. |
| `KRISHIV_CLUSTER_DATA_DIR` | path | `~/.krishiv/cluster` | Data directory for `krishiv cluster` bare-metal deployments. |
| `KRISHIV_CLUSTER_HTTP_ADDR` | host:port | `127.0.0.1:8080` | HTTP address for `krishiv cluster` status endpoints. |
| `KRISHIV_COORDINATOR` | url | `unset` | Deprecated alias of KRISHIV_COORDINATOR_URL (CLI/query paths). |
| `KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS` | uint | `30` | Interval for re-reading coordinator bearer-token files. |
| `KRISHIV_COORDINATOR_AUTH_SECRET_KEY` | text | `token` | K8s Secret key holding the coordinator bearer token (operator-injected pods). |
| `KRISHIV_COORDINATOR_AUTH_SECRET_NAME` | text | `unset` | K8s Secret name holding the coordinator bearer token (operator-injected pods). |
| `KRISHIV_COORDINATOR_BEARER_TOKEN` | secret | `unset` | Bearer token clients present to the coordinator gRPC/HTTP APIs. |
| `KRISHIV_COORDINATOR_BEARER_TOKENS` | secret | `unset` | Comma-separated set of accepted coordinator bearer tokens (server side). |
| `KRISHIV_COORDINATOR_BEARER_TOKENS_FILE` | path | `unset` | File containing newline-separated accepted coordinator bearer tokens; hot-reloaded. |
| `KRISHIV_COORDINATOR_BEARER_TOKEN_FILE` | path | `unset` | File containing a single accepted coordinator bearer token; hot-reloaded. |
| `KRISHIV_COORDINATOR_ENDPOINT` | url | `unset` | Deprecated alias of KRISHIV_COORDINATOR_URL (executor/operator paths). |
| `KRISHIV_COORDINATOR_HTTP` | url | `unset` | Coordinator HTTP base URL (control-plane REST), when it differs from the gRPC URL. |
| `KRISHIV_COORDINATOR_ID` | text | `coordinator-1` | Stable identity of this coordinator instance (leader election, fencing). |
| `KRISHIV_COORDINATOR_URL` | url | `unset` | Canonical coordinator gRPC URL clients and executors connect to. |
| `KRISHIV_CTAS_TARGET_FILE_BYTES` | uint | `134217728` | Target data-file size for durable CTAS writes. |
| `KRISHIV_DEPLOYMENT_TARGET` | text | `unknown` | Deployment label attached to telemetry (dev, staging, prod…). |
| `KRISHIV_DURABILITY_PROFILE` | dev-local \| single-node-durable \| distributed-durable | `dev-local` | Durability/safety profile; gates auth, state persistence, and connector requirements. |
| `KRISHIV_ETCD_ENDPOINTS` | list | `unset` | Comma-separated etcd endpoints for HA leader election (clusterd etcd feature). |
| `KRISHIV_ETCD_LEADER_KEY` | text | `/krishiv/ccp/leader` | etcd key used for the coordinator leader lease. |
| `KRISHIV_EXECUTOR_ID` | text | `unset` | Stable identity of this executor instance (assigned by operator/CLI). |
| `KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES` | uint | `cgroup-derived` | Process-wide executor memory reservation layer; unset = unlimited. |
| `KRISHIV_EXECUTOR_TASK_AUTH_SECRET_KEY` | text | `token` | K8s Secret key holding the executor task bearer token (operator-injected pods). |
| `KRISHIV_EXECUTOR_TASK_AUTH_SECRET_NAME` | text | `unset` | K8s Secret name holding the executor task bearer token (operator-injected pods). |
| `KRISHIV_EXECUTOR_TASK_BEARER_TOKEN` | secret | `unset` | Bearer token the coordinator presents on executor task gRPC calls. |
| `KRISHIV_FALLBACK_RUNTIME_THREADS` | uint | `2` | Worker threads for the shared fallback Tokio runtime used by sync-over-async bridges. |
| `KRISHIV_FLIGHT_ADDR` | host:port | `127.0.0.1:50055` | Flight SQL service listen address. |
| `KRISHIV_FLIGHT_API_KEY` | secret | `unset` | API key the Flight SQL client presents (takes precedence over KRISHIV_API_KEY). |
| `KRISHIV_FLIGHT_MAX_CONCURRENT_QUERIES` | uint | `16` | Maximum concurrently executing Flight SQL queries. |
| `KRISHIV_FLIGHT_MAX_RESULT_BYTES` | uint | `unset` | Per-query Flight SQL result-size cap; unset = unlimited. |
| `KRISHIV_FLIGHT_PREPARED_STMT_CAPACITY` | uint | `128` | Maximum cached prepared statements per Flight SQL session. |
| `KRISHIV_GLUE_CATALOG_ID` | text | `unset` | AWS Glue catalog ID (account) for the Glue catalog integration. |
| `KRISHIV_GLUE_DATABASE` | text | `default` | AWS Glue database name for the Glue catalog integration. |
| `KRISHIV_GRPC_ADDR` | host:port | `127.0.0.1:50051` | Coordinator gRPC listen address. |
| `KRISHIV_GRPC_MAX_MESSAGE_BYTES` | uint | `268435456` | Maximum gRPC message size for coordinator/executor transports. |
| `KRISHIV_HEALTH_PORT` | uint | `unset` | Standalone health-endpoint port for daemon deployments. |
| `KRISHIV_HEARTBEAT_INTERVAL_SECS` | uint | `5` | Executor→coordinator heartbeat interval. |
| `KRISHIV_HOT_KEY_BASE_ROWS_PER_SECOND` | uint | `10000` | Baseline per-key rate used by the adaptive hot-key detector. |
| `KRISHIV_HTTP_ADDR` | host:port | `unset` | Executor HTTP listen address (control endpoints). |
| `KRISHIV_ICEBERG_REST_NAME` | text | `main` | Catalog name to register the Iceberg REST catalog under. |
| `KRISHIV_ICEBERG_REST_TOKEN` | secret | `unset` | Bearer token for the Iceberg REST catalog. |
| `KRISHIV_ICEBERG_REST_URI` | url | `unset` | Iceberg REST catalog endpoint; presence activates the REST catalog. |
| `KRISHIV_ICEBERG_REST_WAREHOUSE` | text | `empty` | Warehouse location/name passed to the Iceberg REST catalog. |
| `KRISHIV_IDLE_TICK_MS` | uint | `engine default` | Continuous-engine idle tick interval in milliseconds. |
| `KRISHIV_INLINE_IPC_MAX_BYTES` | uint | `4194304` | Maximum inline base64 Arrow IPC payload accepted in batch SQL requests. |
| `KRISHIV_INLINE_RESULT_MAX_BYTES` | uint | `8388608` | Result size above which executor task output spools to disk instead of inlining. |
| `KRISHIV_IVM_SHARDS` | uint | `1` | Shard count for coordinator-resident IVM flows. |
| `KRISHIV_JCP_POLL_INTERVAL_SECS` | uint | `2` | Job-completion poll interval for job-mode coordinator runs. |
| `KRISHIV_JOB_ID` | text | `unset` | Job ID for single-job (job-mode) coordinator/executor pods. |
| `KRISHIV_JOB_SPEC_JSON` | text | `unset` | Inline JSON job spec submitted at startup in job-mode. |
| `KRISHIV_LEADER_BACKEND` | single \| etcd | `single` | Coordinator leader-election backend. |
| `KRISHIV_LEADER_LEASE_SECS` | uint | `15` | Leader lease TTL for etcd-backed election. |
| `KRISHIV_LOG_FORMAT` | json \| pretty \| compact | `json` | Log/stderr output format for the tracing subscriber (json = daemon default). |
| `KRISHIV_LOCAL_DATA_DIR` | path | `~/.krishiv/local` | Data directory for `krishiv local` single-node deployments. |
| `KRISHIV_LOCAL_HTTP_ADDR` | host:port | `127.0.0.1:8080` | HTTP address for `krishiv local` status endpoints. |
| `KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT` | uint | `engine default` | Row cap for MATCH_RECOGNIZE evaluation over streaming inputs. |
| `KRISHIV_MAX_CONCURRENT_ASSIGNMENT_RPCS` | uint | `16` | Coordinator-side concurrency cap for task assignment RPC fan-out. |
| `KRISHIV_MCP_ADDR` | host:port | `127.0.0.1:8811` | MCP server listen address (http transport). |
| `KRISHIV_MCP_ALLOW_WRITE_SQL` | bool | `false` | Allow the MCP run_sql tool to execute write statements. |
| `KRISHIV_MCP_MAX_ROWS` | uint | `1000` | Row cap on MCP query results. |
| `KRISHIV_MCP_TIMEOUT_MS` | uint | `30000` | MCP tool execution timeout. |
| `KRISHIV_MCP_TRANSPORT` | stdio \| http | `stdio` | MCP server transport. |
| `KRISHIV_METADATA_BACKEND` | memory \| rocksdb \| redb | `rocksdb` | Coordinator metadata store backend. |
| `KRISHIV_METADATA_PATH` | path | `unset` | Filesystem path for the persistent coordinator metadata store. |
| `KRISHIV_MODE` | embedded \| single-node \| distributed \| bare-metal \| k8s | `embedded` | Session execution mode selector. |
| `KRISHIV_NAMESPACE` | text | `default` | Kubernetes namespace the operator manages. |
| `KRISHIV_NAMESPACE_MAX_ACTIVE_JOBS` | uint | `unset` | Admission cap: maximum concurrently active jobs per namespace. |
| `KRISHIV_NAMESPACE_MAX_CPU_NANOS` | uint | `unset` | Admission cap: maximum aggregate CPU (nanos) per namespace. |
| `KRISHIV_NAMESPACE_MAX_MEMORY_BYTES` | uint | `unset` | Admission cap: maximum aggregate memory per namespace. |
| `KRISHIV_OIDC_AUDIENCE` | text | `unset` | Expected audience claim for OIDC-authenticated coordinator requests. |
| `KRISHIV_OIDC_JWKS_URI` | url | `unset` | JWKS endpoint for OIDC token verification; presence activates OIDC auth. |
| `KRISHIV_PLAN_CACHE_MAX_ENTRIES` | uint | `128` | Logical-plan cache capacity per SQL session. |
| `KRISHIV_PRODUCTION` | bool | `false` | Production mode: tightens defaults (fail-closed metadata, auth requirements, connector restrictions). |
| `KRISHIV_PYTHON_UDF_TIMEOUT_MS` | uint | `30000` | Per-call timeout for sandboxed Python UDF execution. |
| `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` | uint | `cgroup-derived` | Per-query FairSpillPool budget for embedded/IVM sessions. |
| `KRISHIV_REMOTE_EXEC` | bool | `mode-dependent` | Force remote (coordinator) execution on or off for API sessions. |
| `KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH` | bool | `profile-dependent` | Require bearer auth on executor task gRPC even in dev profiles. |
| `KRISHIV_RESULT_SPOOL_DIR` | path | `temp dir` | Directory for disk-spooled large query results. |
| `KRISHIV_RESULT_SPOOL_MAX_BYTES` | uint | `1073741824` | Cap on total spooled result bytes per node. |
| `KRISHIV_ROCKSDB_MAX_OPEN_FILES` | int | `rocksdb default` | RocksDB max_open_files for state/metadata stores (-1 = unlimited). |
| `KRISHIV_ROCKSDB_WRITE_BUFFER_MB` | uint | `rocksdb default` | RocksDB write-buffer (memtable) size in MiB. |
| `KRISHIV_SHUFFLE_ADDR` | host:port | `127.0.0.1:50060` | Shuffle service HTTP listen address. |
| `KRISHIV_SHUFFLE_DIR` | path | `temp dir` | Local-disk shuffle store directory. |
| `KRISHIV_SHUFFLE_FETCH_CONCURRENCY` | uint | `4` | Reduce-side concurrent shuffle partition fetches. |
| `KRISHIV_SHUFFLE_FETCH_RETRIES` | uint | `3` | Retry attempts per shuffle partition fetch. |
| `KRISHIV_SHUFFLE_FETCH_RETRY_BASE_MS` | uint | `100` | Base backoff for shuffle fetch retries. |
| `KRISHIV_SHUFFLE_FLIGHT_ADDR` | host:port | `unset` | Shuffle Flight transport listen address (executor). |
| `KRISHIV_SHUFFLE_MEMORY_BYTES` | uint | `268435456` | In-memory shuffle store budget before spill/rejection. |
| `KRISHIV_SHUFFLE_PARTITIONS` | uint | `target-parallelism` | Default shuffle partition count for distributed plans. |
| `KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES` | uint | `67108864` | Sort-shuffle writer in-memory buffer threshold before spilling a run. |
| `KRISHIV_SHUFFLE_TOKEN` | secret | `unset` | Bearer token protecting shuffle service endpoints. |
| `KRISHIV_SHUFFLE_TOKEN_FILE` | path | `unset` | File containing the shuffle bearer token; hot-reloaded. |
| `KRISHIV_SHUFFLE_TOKEN_RELOAD_SECS` | uint | `30` | Interval for re-reading the shuffle token file. |
| `KRISHIV_SHUFFLE_URI` | url | `unset` | Shuffle backend URI (file://, s3://, tiered://local;s3://…). |
| `KRISHIV_STATE_DIR` | path | `unset` | Executor state-backend directory (RocksDB window/operator state). |
| `KRISHIV_STREAMING_TASK_TIMEOUT_SECS` | uint | `unset` | Watchdog timeout for streaming task cycles; unset = disabled. |
| `KRISHIV_STREAM_EARLY_FIRE_MS` | uint | `unset` | Speculative early-fire interval for open windows (embedded loop; distributed wiring is Phase 55). |
| `KRISHIV_STREAM_PROFILE` | low-latency \| throughput | `low-latency` | Embedded streaming loop profile (checkpoint cadence, batching). |
| `KRISHIV_TARGET_PARALLELISM` | uint | `cores` | DataFusion target partition count for local execution. |
| `KRISHIV_TASK_GRPC_ADDR` | host:port | `127.0.0.1:50052` | Executor task gRPC listen address. |
| `KRISHIV_TASK_SLOTS` | uint | `CPU-derived` | Executor task slots; unset derives from available CPU cores. |
| `KRISHIV_TLS_CERT` | path | `unset` | TLS certificate path for coordinator/executor gRPC servers. |
| `KRISHIV_TLS_KEY` | path | `unset` | TLS private-key path for coordinator/executor gRPC servers. |
| `KRISHIV_UI_TOKEN` | secret | `unset` | Bearer token protecting the embedded web UI. |
| `KRISHIV_UI_TOKEN_FILE` | path | `unset` | File containing the UI bearer token. |
| `KRISHIV_UNITY_CATALOG_NAME` | text | `main` | Catalog name to register the Unity Catalog integration under. |
| `KRISHIV_UNITY_HOST` | url | `unset` | Unity Catalog host URL; presence activates the integration. |
| `KRISHIV_UNITY_TOKEN` | secret | `unset` | Bearer token for Unity Catalog. |
| `KRISHIV_WAREHOUSE_ROOT` | path | `.` | Root path for connector-table warehouse storage. |

## Test-only flags

| Name | Type | Default | Description |
|---|---|---|---|
| `KRISHIV_KIND_CLUSTER` | text | `krishiv-e2e` | kind cluster name for operator e2e smoke tests. |
| `KRISHIV_KIND_E2E` | bool | `false` | Enable the kind-based operator e2e smoke tests. |
| `KRISHIV_KIND_IMAGE` | text | `unset` | Engine image to load into the kind cluster. |
| `KRISHIV_KIND_NAMESPACE` | text | `default` | Namespace used by kind e2e tests. |
| `KRISHIV_KIND_SKIP_CREATE` | bool | `false` | Reuse an existing kind cluster instead of creating one. |
| `KRISHIV_KIND_SKIP_LOAD_IMAGE` | bool | `false` | Skip loading the engine image into kind. |
| `KRISHIV_KIND_TIMEOUT_SECS` | uint | `300` | Timeout for kind e2e operations. |
| `KRISHIV_TEST_DATABASE_URL` | url | `unset` | Postgres URL for catalog integration tests. |
| `KRISHIV_TEST_S3_BUCKET` | text | `unset` | S3 bucket for object-store integration tests. |

## Benchmark flags

| Name | Type | Default | Description |
|---|---|---|---|
| `KRISHIV_TPCDS_DATA_DIR` | path | `unset` | TPC-DS dataset directory for the bench harness. |
| `KRISHIV_TPCH_DATA_DIR` | path | `unset` | Legacy TPC-H SF10 dataset directory (prefer the _SF* variants). |
| `KRISHIV_TPCH_DATA_DIR_SF1` | path | `unset` | TPC-H SF1 dataset directory. |
| `KRISHIV_TPCH_DATA_DIR_SF10` | path | `unset` | TPC-H SF10 dataset directory. |
| `KRISHIV_TPCH_DATA_DIR_SF100` | path | `unset` | TPC-H SF100 dataset directory. |

## Dynamic namespaces

- `KRISHIV_ICEBERG_REST_<PROP>` — Pass-through namespace: `KRISHIV_ICEBERG_REST_<PROP>` becomes the Iceberg REST catalog property `<prop>` (lower-cased). Named vars (URI/NAME/TOKEN/WAREHOUSE) are declared individually.
