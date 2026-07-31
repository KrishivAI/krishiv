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
| `KRISHIV_AQE` | bool | `on` | Adaptive query execution master switch (Phase 54). `off` disables every stage-boundary rewrite (coalescing, skew split) and the placeholder-plan hint pass; per-mechanism flags refine it. |
| `KRISHIV_AQE_COALESCE` | bool | `on` | AQE reduce-partition coalescing: merge small measured shuffle partitions into fewer reduce tasks (dfplan multi-partition bodies). Subordinate to KRISHIV_AQE. |
| `KRISHIV_AQE_SKEW_FACTOR` | float | `4.0` | A reduce partition is skewed when its measured bytes exceed this factor x the median partition size (and KRISHIV_AQE_SKEW_MIN_BYTES). |
| `KRISHIV_AQE_SKEW_MIN_BYTES` | uint | `134217728` | Absolute floor (bytes) below which a reduce partition is never treated as skewed (default 128 MiB). |
| `KRISHIV_AQE_SKEW_SPLIT` | bool | `on` | AQE skew handling: split a skewed reduce partition into map-task-range sub-tasks (split-safe plans only). Subordinate to KRISHIV_AQE. |
| `KRISHIV_AQE_TARGET_PARTITION_BYTES` | uint | `67108864` | Target upstream shuffle bytes per reduce task for AQE coalescing and skew-split sizing (default 64 MiB). |
| `KRISHIV_API_KEYS` | secret | `unset` | Comma-separated set of accepted Flight SQL API keys (server side). |
| `KRISHIV_BARRIER_GRPC_ADDR` | host:port | `unset` | Executor barrier-transport gRPC listen address (aligned window join / checkpoint barriers). |
| `KRISHIV_BATCH_SIZE` | uint | `8192` | DataFusion execution batch size (rows per record batch). |
| `KRISHIV_BENCH_IVM_MAX_ROWS` | uint | `unset` | Caps the IVM-vs-recompute benchmark row ladder; unset runs the full ladder. |
| `KRISHIV_BATCH_SQL_TIMEOUT_SECS` | uint | `300` | Coordinator-mode batch SQL completion timeout in seconds. |
| `KRISHIV_CA_CERT` | path | `unset` | CA certificate path used by gRPC clients to verify TLS server certs. |
| `KRISHIV_CHECKPOINT_DIR` | path | `unset` | Local checkpoint directory for embedded/single-node sessions. |
| `KRISHIV_CHECKPOINT_STORAGE` | url | `unset` | Checkpoint storage URI (memory://, file://…, s3://…). Durable profiles reject memory://. |
| `KRISHIV_CLUSTER_DATA_DIR` | path | `~/.krishiv/cluster` | Data directory for `krishiv cluster` bare-metal deployments. |
| `KRISHIV_CLUSTER_HTTP_ADDR` | host:port | `127.0.0.1:8080` | HTTP address for `krishiv cluster` status endpoints. |
| `KRISHIV_COMPUTE_THREADS` | uint | `auto (max(1, cores - 1))` | Thread count for the global Rayon compute-kernel pool (shuffle/checkpoint/decode kernels); 0 or unset auto-sizes to cores minus one reserved for the async reactor. |
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
| `KRISHIV_DAEMON_RUNTIME_THREADS` | uint | `auto (min(cpu, 4) embedded; cpu for a full daemon)` | Tokio worker-thread count for a long-running coordinator/executor daemon runtime; 0 or unset auto-sizes from CPU count. |
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
| `KRISHIV_FLIGHT_ALLOW_ALL_AUTHENTICATED` | bool | `false` | Standalone Flight SQL: treat any authenticated subject as authorized (AllowAllPolicyHook) instead of SEC-2 default-deny. For deployments with no governance catalog; the API key is the authorization boundary. |
| `KRISHIV_FLIGHT_API_KEY` | secret | `unset` | API key the Flight SQL client presents (takes precedence over KRISHIV_API_KEY). |
| `KRISHIV_FLIGHT_MAX_CONCURRENT_QUERIES` | uint | `256` | Maximum concurrently executing Flight SQL queries. |
| `KRISHIV_FLIGHT_MAX_RESULT_BYTES` | uint | `2147483648` | Per-query Flight SQL result-size cap. NOT unlimited when unset: the compiled-in default is 2 GiB. |
| `KRISHIV_FLIGHT_PREPARED_STMT_CAPACITY` | uint | `128` | Maximum cached prepared statements per Flight SQL session. |
| `KRISHIV_FLIGHT_REQUEST_TIMEOUT_SECS` | uint | `0` | Hard per-request deadline (seconds) on the client→coordinator Flight channel; 0 (default) disables it so long-running distributed queries are bounded by the coordinator's own statement timeout (KRISHIV_BATCH_SQL_TIMEOUT_SECS) rather than a premature transport cap. Dead peers are still detected via HTTP/2 keepalive. |
| `KRISHIV_FULL_SNAPSHOT_EVERY` | uint | `8` | Every Nth checkpoint epoch takes a full portable snapshot in incremental mode (bounds the SST manifest chain). |
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
| `KRISHIV_INCREMENTAL_CHECKPOINTS` | bool | `true` | RocksDB-backed window state checkpoints SST deltas instead of full snapshots (Phase 56). |
| `KRISHIV_INLINE_IPC_MAX_BYTES` | uint | `67108864` | Maximum inline base64 Arrow IPC payload accepted in batch SQL requests. |
| `KRISHIV_INLINE_RESULT_MAX_BYTES` | uint | `8388608` | Result size above which executor task output spools to disk instead of inlining. |
| `KRISHIV_IVM_SHARDS` | uint | `1` | Shard count for coordinator-resident IVM flows. |
| `KRISHIV_JCP_POLL_INTERVAL_SECS` | uint | `2` | Job-completion poll interval for job-mode coordinator runs. |
| `KRISHIV_JOB_GC_GRACE_SECS` | uint | `30` | Grace window a terminal job stays queryable before the GC tick may evict it, so a slow consumer still observes its outcome + result. |
| `KRISHIV_JOB_ID` | text | `unset` | Job ID for single-job (job-mode) coordinator/executor pods. |
| `KRISHIV_JOB_SPEC_JSON` | text | `unset` | Inline JSON job spec submitted at startup in job-mode. |
| `KRISHIV_LEADER_BACKEND` | single \| etcd | `single` | Coordinator leader-election backend. |
| `KRISHIV_LEADER_LEASE_SECS` | uint | `15` | Leader lease TTL for etcd-backed election. |
| `KRISHIV_LOG_FORMAT` | json \| pretty \| compact | `json` | Log/stderr output format for the tracing subscriber (json = daemon default). |
| `KRISHIV_LOCAL_DATA_DIR` | path | `~/.krishiv/local` | Data directory for `krishiv local` single-node deployments. |
| `KRISHIV_LOCAL_HTTP_ADDR` | host:port | `127.0.0.1:8080` | HTTP address for `krishiv local` status endpoints. |
| `KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT` | uint | `engine default` | Row cap for MATCH_RECOGNIZE evaluation over streaming inputs. |
| `KRISHIV_MAX_CONCURRENT_ASSIGNMENT_RPCS` | uint | `128` | Coordinator-side concurrency cap for task assignment RPC fan-out. |
| `KRISHIV_MAX_SHUFFLE_REGEN` | uint | `8` | Maximum times a lost shuffle partition may be regenerated before the job fails terminally (consumer-driven FetchFailed recovery bound). |
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
| `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` | uint | `cgroup-derived` | Total FairSpillPool budget SHARED by every engine in the process (task slots, Flight SQL, IVM); 0 disables the limit. |
| `KRISHIV_RACK_ID` | text | `unset` | Rack identifier the executor advertises for RACK_LOCAL placement (Phase 53). Node identity is the executor host. |
| `KRISHIV_REMOTE_EXEC` | bool | `mode-dependent` | Force remote (coordinator) execution on or off for API sessions. |
| `KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH` | bool | `profile-dependent` | Require bearer auth on executor task gRPC even in dev profiles. |
| `KRISHIV_RESULT_SPOOL_DIR` | path | `temp dir` | Directory for disk-spooled large query results. |
| `KRISHIV_RESULT_SPOOL_MAX_BYTES` | uint | `8589934592` | Cap on total spooled result bytes per node. |
| `KRISHIV_RESULT_SPOOL_SYNC_INTERVAL_BYTES` | uint | `67108864` | Bytes written between fsyncs of the disk-spooled result file; 0 or unset uses the 64 MiB default. |
| `KRISHIV_ROCKSDB_MAX_OPEN_FILES` | int | `rocksdb default` | RocksDB max_open_files for state/metadata stores (-1 = unlimited). |
| `KRISHIV_ROCKSDB_WRITE_BUFFER_MB` | uint | `rocksdb default` | RocksDB write-buffer (memtable) size in MiB. |
| `KRISHIV_RUNTIME_FILTERS` | bool | `on` | DataFusion dynamic (runtime) filters: TopK / join / aggregate predicates pushed into probe-side file scans at execution time (Phase 54). `off` disables all three via the DataFusion master switch. |
| `KRISHIV_CROSS_STAGE_RUNTIME_FILTER` | bool | `off` | Cross-stage runtime bloom filters: a distributed plan gains a filter stage over the join build side, and the probe stage drops non-matching rows BEFORE shuffling them. Distinct from KRISHIV_RUNTIME_FILTERS, which is DataFusion's in-plan mechanism and cannot see across a stage cut. Read on the coordinator, at planning time. |
| `KRISHIV_STAGE_REUSE` | bool | `off` | Stage reuse (Spark's ReuseExchange): two leaf stages that compute the same rows with the same shuffle contract are collapsed into one, and both consumers read the same shuffle output. Measured on SF100 to remove a duplicate FULL lineitem scan from q18 and q21 and a duplicate partsupp scan from q2. Restricted to leaf stages, and refused when the plan text mentions a volatile function. Read on the coordinator, at planning time. |
| `KRISHIV_SEMI_JOIN_PUSHDOWN` | bool | `on` | Push a semi-join through an inner join. Gated by KRISHIV_SEMI_JOIN_REDUCTION as well, so turning that off disables both. |
| `KRISHIV_LATE_MATERIALIZATION` | bool | `on` | Late materialisation of a bounded top-N aggregate: group on the declared key alone, take the top N, then re-join the base tables to fetch the columns the key determines, so those columns never enter the joins or the shuffle. Requires a declared PRIMARY KEY (see ParquetTableSpec::with_primary_key) and an ORDER BY ... LIMIT of at most 10000. On TPC-H q10 at SF100 the seven grouping columns are 14.8x of the query. Read wherever a query is planned. |
| `KRISHIV_SHUFFLE_FETCH_BUFFER` | uint | `1` | How many upstream map fragments a reduce partition opens concurrently. Raising this is NOT free: the shuffle server holds a do_get permit for the lifetime of each response. Measured on a 3-node SF100 cluster: 4 wedged it at 0% CPU, and 2 wedged TPC-H q10 for 40+ minutes moving ~2.5 KB of network in 45 s (1 moved 1.77 GB in the same window). Stays at 1 until the server stops holding a permit across the response. |
| `KRISHIV_SHUFFLE_FETCH_TRANSPORT_GRACE_SECS` | uint | `90` | Wall-clock grace for retrying a shuffle fetch across a transport error, covering an executor restart. `0` disables the grace; NotFound still fails fast. |
| `KRISHIV_BROADCAST_JOIN_BYTES` | uint | `33554432 (32 MiB), staged path only` | Build-side byte ceiling under which a join is BROADCAST rather than hash-shuffled, on the distributed staged path only (embedded keeps DataFusion's 1 MiB default, which is right for a single process where a shuffle is a memcpy). Here a shuffle is the pod network, measured at ~11 MiB/s across separate hosts: TPC-H q8/q9 hash-partition the raw 600M-row lineitem scan — ~36 GiB on the wire — because the filtered dimension side lands just over 1 MiB and so is not eligible to broadcast. Bounded by the per-task memory share, since the build side is collected per task. |
| `KRISHIV_SPILL_JOIN_BUILD_BYTES` | uint | `50% of the per-task memory share (disabled when uncapped)` | Hash-join build sides with a KNOWN size estimate above this many bytes are planned as sort-merge joins, which can spill, instead of hash joins, which cannot (TPC-H q18: 'Resources exhausted ... HashJoinInput[0] ... 732.4 MB'). Unknown estimates and uncapped engines keep hash join. |
| `KRISHIV_SHUFFLE_GRPC_MAX_MESSAGE_BYTES` | uint | `268435456 (256 MiB)` | Maximum gRPC message the shuffle transport will send or decode. Must exceed the shuffle writer's coalesce target with room to spare, or a map task produces a partition the reduce side refuses to decode — tonic reports OutOfRange, the consumer retries forever and finally reports NotFound, which reads as a lost shuffle rather than an oversized message. TPC-H q10 at SF100 died this way on every sweep. |
| `KRISHIV_SHUFFLE_WIRE_COMPRESSION` | lz4 \| zstd \| none | `lz4` | Arrow IPC body compression for shuffle partitions on the wire: lz4 | zstd | none. Reduce tasks fetch their input from other executors over Flight, so most of every shuffle crosses the pod network — measured at ~7.6 MB/s here against 150-286 MB/s for a node-local read. tonic is built without a compression feature, so gRPC-level compression is unavailable rather than merely off; this is the layer that covers the transfer. Compression at rest is a separate knob (KRISHIV_SHUFFLE_STORAGE_COMPRESSION). The codec travels in each record-batch message, so a reader decompresses without negotiation and a peer sending uncompressed data still works. LZ4 runs two orders of magnitude faster than this link, so the default is one-sided here; set `none` on a fast fabric, where the CPU cost becomes comparable to the transfer it saves. |
| `KRISHIV_SHUFFLE_STORAGE_COMPRESSION` | lz4 \| zstd \| none | `lz4` | Codec for shuffle partitions at rest — Parquet on local disk, Arrow IPC in the object store: lz4 | zstd | none. Both stores previously constructed themselves with None, and the only production caller that ever set a codec was the shuffle HTTP service; every backend a distributed query actually uses (local, object, tiered) took the default, so partitions were written raw. An object-store partition crosses the pod network twice, once written and once fetched. Both formats are self-describing — Parquet records its codec in file metadata, Arrow IPC in each record-batch message — so changing this is safe under partitions already written, in both directions. |
| `KRISHIV_GRACE_HASH_JOIN` | bool | `off` | Send an over-threshold hash join to the grace hash join — partition both sides by key and join bucket by bucket, each bucket an ordinary in-memory hash join — instead of converting it to a sort-merge join. Sort-merge sorts BOTH inputs in full even when nearly all the data would have fitted, which cost TPC-H q2 6.3x (208s -> 1317s); grace sorts nothing and only the buckets that overflow reach disk. Off by default: sort-merge is what the SF100 sweeps have been measured against, and a newer operator earns the default by beating it on the cluster. Falls back to sort-merge for shapes it refuses (today, a broadcast join whose sides have different partition counts). |
| `KRISHIV_GRACE_HASH_JOIN_BUCKETS` | uint | `32, or enough that a bucket lands near half the per-task share` | Hash buckets the grace hash join partitions each side into when the build side overflows its budget. Clamped to [2, 256]. Larger is safer: a bucket is one temp file and one small in-memory join, while too few buckets means a bucket that still does not fit, which is the failure the operator exists to prevent. |
| `KRISHIV_UNSPILLABLE_HEADROOM_PERCENT` | uint | `25` | Percent of the query memory pool reserved for consumers that CANNOT spill (a hash-join build side is the main one). FairSpillPool caps spillable consumers at a share of the pool but lets unspillable ones take only what remains after both classes, so N spillers each inside their own share can together leave nothing for a small hash join — which is how a 2.6 GB pool refused 877 bytes. 0 disables the guard. |
| `KRISHIV_SHUFFLE_PAGE_CACHE_BYTES` | uint | `12.5% of the cgroup limit (512 MiB uncontained)` | Ceiling on page cache held by committed-but-unconsumed shuffle partitions. Cached output serves same-node reduce reads from RAM instead of disk; the bound stops it growing unreclaimed across stages, which is what got executors OOM-killed at SF100. |
| `KRISHIV_SEMI_JOIN_REDUCTION` | bool | `on` | Semi-join reduction through an aggregate: when a grouped aggregate is inner-joined on one of its own grouping keys, filter the aggregate's input to the surviving keys first. TPC-H q17 spends 88% of its compute building ~20M groups when the join keeps ~2000. `off` disables it. |
| `KRISHIV_SESSION_IDLE_TIMEOUT_SECS` | uint | `0` | Phase 59 session hardening: evict a Flight SQL session's per-session bookkeeping after this many seconds with no active statements; 0 (default) disables idle eviction. |
| `KRISHIV_SESSION_MAX_CONCURRENT_STATEMENTS` | uint | `0` | Phase 59 session hardening: maximum statements a single Flight SQL session (authenticated subject) may execute concurrently before further statements are rejected with resource_exhausted; 0 (default) disables the per-session cap. Complements the global KRISHIV_FLIGHT_MAX_CONCURRENT_QUERIES. |
| `KRISHIV_SHUFFLE_ADDR` | host:port | `127.0.0.1:50060` | Shuffle service HTTP listen address. |
| `KRISHIV_SHUFFLE_DIR` | path | `temp dir` | Local-disk shuffle store directory. |
| `KRISHIV_SHUFFLE_FETCH_CONCURRENCY` | uint | `4` | Reduce-side concurrent shuffle partition fetches. |
| `KRISHIV_SHUFFLE_FETCH_RETRIES` | uint | `3` | Retry attempts per shuffle partition fetch. |
| `KRISHIV_SHUFFLE_FETCH_RETRY_BASE_MS` | uint | `100` | Base backoff for shuffle fetch retries. |
| `KRISHIV_SHUFFLE_FLIGHT_ADDR` | host:port | `unset` | Shuffle Flight transport listen address (executor). |
| `KRISHIV_SHUFFLE_MEMORY_BYTES` | uint | `134217728` | In-memory shuffle store budget before spill/rejection. |
| `KRISHIV_SHUFFLE_PARTITIONS` | uint | `target-parallelism` | Default shuffle partition count for distributed plans. |
| `KRISHIV_SHUFFLE_SERVE_CONCURRENCY` | uint | `cgroup-derived` | Concurrent shuffle Flight `do_get` responses one executor will serve; bounds the aggregate bytes held for consumers across all peers. Derived from the page-cache budget in units of the 32 MiB inline-read limit, floor 2. |
| `KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES` | uint | `67108864` | Sort-shuffle writer in-memory buffer threshold before spilling a run. |
| `KRISHIV_SHUFFLE_STORE_BYTES` | uint | `cgroup-derived` | Push-shuffle store ceiling, carved from the same container budget as the query pool so the two cannot together exceed the container; 0 disables the limit. |
| `KRISHIV_SHUFFLE_TOKEN` | secret | `unset` | Bearer token protecting shuffle service endpoints. |
| `KRISHIV_SHUFFLE_TOKEN_FILE` | path | `unset` | File containing the shuffle bearer token; hot-reloaded. |
| `KRISHIV_SHUFFLE_TOKEN_RELOAD_SECS` | uint | `30` | Interval for re-reading the shuffle token file. |
| `KRISHIV_SHUFFLE_URI` | url | `unset` | Shuffle backend URI (file://, s3://, tiered://local;s3://…). |
| `KRISHIV_STAGE_SPLIT` | bool | `on` | Distributed batch stage splitting (Phase 52); off/0/false runs batch SQL single-task. |
| `KRISHIV_STAGE_TARGET_PARTITIONS` | uint | `cluster-derived` | Planning-time partition count for distributed batch stages (scan + shuffle fan-out); unset derives 2 tasks per live cluster slot. |
| `KRISHIV_STATE_BACKEND` | rocksdb \| disaggregated | `rocksdb` | Executor generic state backend; disaggregated = DFS-primary with local cache (requires KRISHIV_STATE_DFS_ROOT). |
| `KRISHIV_STATE_DFS_ROOT` | path | `unset` | DFS/object-store root for the disaggregated state backend. |
| `KRISHIV_STATE_DIR` | path | `unset` | Executor state-backend directory (RocksDB window/operator state). |
| `KRISHIV_BATCH_TASK_TIMEOUT_SECS` | uint | `3600` | Watchdog timeout for batch task execution. A per-task task_timeout_secs still wins. Previously compile-time only, while the streaming watchdog was tunable — the asymmetry had no rationale and SF100 queries run within 2x of this ceiling. |
| `KRISHIV_STREAMING_TASK_TIMEOUT_SECS` | uint | `300` | Watchdog timeout for streaming task cycles. NOT disabled when unset: the compiled-in default is 300 s. A per-task task_timeout_secs still wins. |
| `KRISHIV_STREAM_EARLY_FIRE_MS` | uint | `unset` | Speculative early-fire interval for open windows (embedded loop only — the distributed stream:rloop: run-loop does not read this flag). |
| `KRISHIV_STREAM_LINGER_MS` | uint | `profile` | Run-loop batch/linger before each drain in ms; overrides the KRISHIV_STREAM_PROFILE default (0 low-latency, 5 throughput). |
| `KRISHIV_STREAM_PROFILE` | low-latency \| throughput | `low-latency` | Streaming loop profile: embedded checkpoint cadence and the distributed run-loop batch/linger dial (Phase 55). |
| `KRISHIV_TARGET_PARALLELISM` | uint | `cores` | DataFusion target partition count for local execution. |
| `KRISHIV_TASK_GRPC_ADDR` | host:port | `127.0.0.1:50052` | Executor task gRPC listen address. |
| `KRISHIV_TASK_SLOTS` | uint | `capacity-derived` | Executor task slots; unset derives from CPU cores and the cgroup memory limit together. Also sizes per-task parallelism. |
| `KRISHIV_TASK_TARGET_PARALLELISM` | uint | `cores/slots` | DataFusion parallelism per executor task engine; unset = per-slot share of cores, using the RESOLVED slot count (incl. --slots). |
| `KRISHIV_TLS_CERT` | path | `unset` | TLS certificate path for coordinator/executor gRPC servers. |
| `KRISHIV_TLS_KEY` | path | `unset` | TLS private-key path for coordinator/executor gRPC servers. |
| `KRISHIV_UI` | bool | `on` | Embedded web-UI off-switch: KRISHIV_UI=off boots the daemon without the always-on embedded UI factory (certified platform profile sets off). |
| `KRISHIV_UI_TOKEN` | secret | `unset` | Bearer token protecting the embedded web UI. |
| `KRISHIV_UI_TOKEN_FILE` | path | `unset` | File containing the UI bearer token. |
| `KRISHIV_UNITY_CATALOG_NAME` | text | `main` | Catalog name to register the Unity Catalog integration under. |
| `KRISHIV_UNITY_HOST` | url | `unset` | Unity Catalog host URL; presence activates the integration. |
| `KRISHIV_UNITY_TOKEN` | secret | `unset` | Bearer token for Unity Catalog. |
| `KRISHIV_WATERMARK_IDLE_MS` | uint | `30000` | Run-loop per-split watermark idleness timeout: a silent split is excluded from the min-combine after this long (Phase 55 watermarks v2). |
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
| `KRISHIV_TPCH_ONLY` | text | `unset` | Comma-separated TPC-H query names (e.g. `q10,q21`) restricting a verify/bench run; unset runs all 22. |
| `KRISHIV_TPCH_SCALE_FACTOR` | float | `1.0` | TPC-H scale factor the verifier assumes; q11's threshold is scale-dependent, so this must match the data being checked. |
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
