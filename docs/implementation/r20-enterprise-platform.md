# R20 Enterprise Platform & Ecosystem Implementation Tracker

## Goal

Deliver a complete enterprise data platform experience that allows operations
teams to run and govern Krishiv without deep knowledge of its internals.
R20 ships a React-based self-serve portal with a live DAG view and cost
dashboard, automated data lineage capture via a DataFusion plan hook, GDPR/CCPA
right-to-erasure pipelines and data classification scans, tamper-evident
SOC2/HIPAA audit logs with hash chaining, SLA management with PagerDuty
alerting, dbt-native incremental materialisation, OpenMetadata and DataHub
integration, multi-tenant PROCESS and NETWORK isolation on Kubernetes, and
production-grade managed service packaging (Helm, Terraform, AWS Marketplace,
Docker Compose, `krishiv-cloud` CLI). R20 is the enterprise graduation release;
it must close the gap from "does it work?" to "can operations teams run and
govern it?".

## Scope

In scope:

- New `krishiv-portal` crate: React web application embedded in the Rust binary via `include_bytes!` per ADR-20.4.
- New `krishiv-lineage` crate: lineage graph engine, DataFusion `PhysicalOptimizerRule` for capturing source/sink tables per ADR-20.1.
- New `krishiv-compliance` crate: GDPR/CCPA retention policies, right-to-erasure pipeline, data classification scanner.
- SOC2/HIPAA immutable audit log: single-writer hash-chaining service backed by S3, batched writes per ADR-20.3.
- `krishiv-governance` extensions: compliance-level masking, PII/PHI/PCI classifiers.
- `krishiv-metrics` extensions: SLA breach detection, `BreachAction.alert` (PagerDuty), `BreachAction.restart_pipeline`.
- dbt `krishiv_incremental` materialisation type using the R14 live table engine.
- OpenMetadata and DataHub push integration via `CatalogSync`.
- Multi-tenant PROCESS isolation (separate executor Deployments per namespace) and NETWORK isolation (Kubernetes `NetworkPolicy`) per ADR-20.2.
- Helm chart with production defaults (HA, TLS, RBAC, resource limits).
- Terraform modules for AWS EKS, GCP GKE, and Azure AKS.
- AWS Marketplace AMI and Docker Compose quick-start.
- `krishiv-cloud` CLI for managed service provisioning.

Out of scope:

- CGROUP isolation level (requires privileged containers — not acceptable in a managed service per ADR-20.2).
- VM-level isolation (separate Kubernetes clusters per tenant) — deferred beyond R20.
- Full unified DataFusion `MERGE INTO` plan node (the R18 format-specific implementation remains; unified plan node is a future ADR target).
- Self-hosted schema registry (Confluent compatibility layer is R18; this release focuses on portal and governance).
- Real-time collaborative portal features (shared dashboards, comments) — deferred.

## Dependencies

- R12–R17 complete: all P0 bugs resolved; Python API, incremental computation, Spark compatibility, advanced streaming, and AI pipeline features are stable. Lineage capture (ADR-20.1) depends on DataFusion query hooks that are stabilised across R14–R16.
- R18 complete: time travel and MERGE INTO are prerequisites for the erasure pipeline (which must merge-delete rows from Delta and Iceberg tables).
- R19 complete: multi-tenant namespace isolation builds on R19's regional coordinator federation; cost dashboard requires R19's cost-aware placement metrics; the portal's live DAG view requires R19's multi-region job routing to resolve which coordinator a job is running on.
- `krishiv-governance` crate exists from R9 with masking and RBAC foundations.
- `krishiv-metrics` crate exists from R7 with Prometheus integration and quota accounting.
- `krishiv-ui` crate exists from R10 as the HTTP API server; R20 extends it to serve the React portal.
- CI pipeline has Node.js ≥ 20 available for the React build step (added in Sprint 2).

## Architectural Decisions Required

### ADR-20.1: Lineage Capture Strategy

**Problem**

To record data lineage, every query that reads from a source and writes to a
sink must be intercepted. Lineage must capture input tables, output tables, and
the transformation applied — without requiring users to annotate their jobs
manually. The interception point must be reliable across SQL queries, Python
DataFrame operations, and programmatic write calls.

**Options**

- A. DataFusion `PhysicalOptimizerRule` that extracts source and sink table
  names from the `ExecutionPlan` before execution. The rule receives the full
  physical plan, walks all `TableScan` nodes to collect source tables, and
  inspects `InsertExec`/`FileSinkExec` nodes to collect sink tables. Emits a
  `LineageEvent` to `krishiv-lineage` before the plan is executed. Accurate:
  the physical plan fully resolves all table references including CTEs, subqueries,
  and view expansions. Requires DataFusion's physical optimizer extension point
  which is stable as of DataFusion 37.
- B. Parse SQL text for `FROM`, `INSERT INTO`, and `CREATE TABLE AS SELECT`
  clauses using a regex or secondary AST walk before DataFusion planning.
  Does not cover programmatic DataFrame writes (e.g., `df.write_delta(...)`)
  that never produce SQL text. Misses tables referenced through CTEs, views,
  and macro expansions that are resolved during planning. Brittle.
- C. Require explicit lineage annotations in job spec: users declare
  `LineageSpec(inputs=["orders"], outputs=["daily_revenue"])`. Complete user
  control; no inference. High user burden; lineage is incomplete for jobs not
  annotated; defeats the "automated" lineage claim.

**Recommendation**

Option A. The DataFusion `PhysicalOptimizerRule` (or `PhysicalPlanVisitor`)
is the correct extension point. Implement `LineageCaptureRule` in `krishiv-lineage`
that walks the `ExecutionPlan` tree, collects source table URIs from
`ParquetExec`, `IcebergScan`, `DeltaScan`, `HudiScan` nodes, and sink table
URIs from `FileSinkExec` and custom sink operators. Emit the collected event
asynchronously to the lineage graph store. This approach also captures lineage
for programmatic writes because DataFusion physical plans are generated for all
DataFrame operations before execution.

**Risk if deferred**

Without automated lineage capture, the data catalog is incomplete and the
`lineage.for_table().upstream()` API returns empty results. GDPR compliance
(right-to-erasure) also requires lineage to identify which downstream tables
contain data derived from a given source — if lineage is missing, erasure jobs
cannot be verified as complete.

---

### ADR-20.2: Multi-Tenant Isolation Level Scope

**Problem**

Multi-tenant isolation prevents one tenant's workload from affecting another's
in terms of resource consumption, data access, and network communication.
Three isolation levels are defined: PROCESS (separate executor processes via
Kubernetes Pods), NETWORK (Kubernetes `NetworkPolicy`), and CGROUP (CPU/memory
hard limits via cgroups, requiring privileged containers). The R20 scope must
be decided before the operator reconciliation loop is extended for multi-tenancy.

**Options**

- A. PROCESS isolation only: each tenant namespace gets its own Kubernetes
  `Namespace` and executor `Deployment`. Access control between namespaces is
  via RBAC only. Network traffic between tenant executors is allowed (no
  `NetworkPolicy`). Simple to implement; does not prevent data exfiltration via
  network if an executor is compromised.
- B. PROCESS + NETWORK isolation: PROCESS isolation as above, plus
  Kubernetes `NetworkPolicy` resources that prevent cross-namespace executor
  communication. `NetworkPolicy` is supported by all major CNI plugins (Calico,
  Cilium, AWS VPC CNI). Does not require privileged containers. Covers the
  primary threat model for enterprise multi-tenant SaaS (data isolation between
  teams/organisations).
- C. PROCESS + NETWORK + CGROUP isolation: all of the above, plus CPU and
  memory cgroup hard limits via `resources.limits` in executor pod specs (this
  is standard Kubernetes) and optional Linux cgroup namespace isolation requiring
  privileged containers. The privileged container requirement introduces a
  significant security risk in a managed service — a compromised executor
  could escape to the host.

**Recommendation**

Option B. PROCESS + NETWORK isolation covers the primary enterprise threat
model without the security risks of privileged containers. Standard Kubernetes
resource limits (`resources.limits.cpu`, `resources.limits.memory`) in executor
pod specs provide the CGROUP-level resource enforcement without requiring
privileged mode. Document CGROUP namespace isolation as not supported in the
managed service and recommend VM-level isolation (separate clusters) for
workloads requiring hardware-level tenant separation.

**Risk if deferred**

Implementing PROCESS isolation without NETWORK isolation means a compromised
or buggy executor in one tenant namespace can open TCP connections to executors
in other tenant namespaces — violating data isolation guarantees. This is
unacceptable for SOC2 certification.

---

### ADR-20.3: Tamper-Evident Audit Log Architecture

**Problem**

SOC2 and HIPAA require that audit logs be tamper-evident: it must be possible
to detect after the fact whether any audit record was modified or deleted.
Hash chaining (each entry includes a hash of the previous entry) creates a
cryptographic chain of custody. The implementation must decide on the writer
model, since hash chaining creates a sequential dependency between writes.

**Options**

- A. Single-writer audit log service: all audit events are routed through a
  single `AuditLogWriter` goroutine/task in `krishiv-compliance`. The writer
  maintains the current chain head hash in memory and appends to S3 in batches
  (buffer 100ms of events, write as one S3 object). The chain is linear and
  verifiable by re-hashing from the genesis entry. Single point of failure —
  if the writer crashes, in-flight events are lost until restart. Mitigation:
  buffer events in a small in-process ring buffer; on restart, recover the last
  chain head from S3.
- B. Per-coordinator audit log with cross-log Merkle tree: each coordinator
  (and each global coordinator in a multi-region deployment) maintains its own
  hash chain. A Merkle tree is computed periodically across all chains to
  produce a cross-chain integrity proof. Parallel writes; complex verification;
  requires a separate Merkle tree service.
- C. Use an existing immutable audit log service: AWS CloudTrail, Azure Monitor,
  or Google Cloud Audit Logs. No custom implementation; dependent on cloud
  provider; not available for bare-metal deployments; log format is not
  Krishiv-specific.

**Recommendation**

Option A for R20. Single-writer is correct and simple. The 100ms batch write
window limits S3 API calls while keeping audit latency low. For multi-region
deployments, each region's global coordinator runs its own single-writer with
a region-prefixed S3 key space; cross-region integrity is verified by an
optional offline audit tool that walks all regional chains. Scale via batching,
not parallelism. Option C can be offered as an alternative backend in a future
release for cloud-only managed service customers.

**Risk if deferred**

Without a tamper-evident audit log, Krishiv cannot claim SOC2 or HIPAA
compliance. Enterprises requiring compliance certification will not adopt the
managed service. This is a hard gate for the enterprise market.

---

### ADR-20.4: React Portal Deployment in Rust Binary

**Problem**

The `krishiv-ui` crate currently serves server-rendered HTML via `askama`.
A React frontend requires a separate build step (npm/Node.js) that produces
static assets (`index.html`, JavaScript bundles, CSS). These assets must be
served by the Rust HTTP server. The deployment model for the static assets
must be decided before the CI pipeline is extended.

**Options**

- A. Embed React build artifacts in the Rust binary via `include_bytes!`: the CI
  pipeline runs `npm run build` in `crates/krishiv-portal/ui/`, then `cargo build`
  embeds the output directory using `include_bytes!` or the `rust-embed` crate.
  Single binary deployment — operators deploy one binary and the portal is
  available at `/` with no CDN required. Dev iteration is slow (rebuild Rust
  after every React change). Mitigated by a dev mode that reads assets from
  disk when `KRISHIV_DEV_ASSETS_DIR` is set.
- B. Serve static files from a separate S3 bucket or CDN: the CI pipeline
  uploads React build output to S3; the Rust server redirects portal requests
  to the CDN URL. Decoupled deployment; fast dev iteration; requires a CDN/S3
  bucket in every deployment environment (including bare-metal and Docker
  Compose quick-start). Adds operational complexity for users who just want a
  quick-start.
- C. Implement the portal in Rust WebAssembly (WASM) using a framework like
  Leptos or Yew. No npm or Node.js build toolchain. Experimental; the WASM
  frontend ecosystem is less mature than React; the available component
  libraries (charts, tables, DAG visualisation) are significantly less
  comprehensive than the React ecosystem. Not feasible for R20's portal scope.

**Recommendation**

Option A (embed in binary). Distribution simplicity is the correct priority for
a managed service that targets operators who want a one-command deployment.
Use the `rust-embed` crate to embed the `out/` directory. Add a
`KRISHIV_DEV_ASSETS_DIR` environment variable that, when set, reads assets from
disk instead of the embedded copy — this enables hot-reloading during portal
development without a full Rust rebuild.

**Risk if deferred**

Without a deployment decision, the CI pipeline cannot be extended to include
the npm build step. This blocks all portal development work from Sprint 2
onward. The decision must be recorded as DECIDED before Sprint 2 begins.

## Sprint 1 — Data Catalog & Automated Lineage

### S1.1: krishiv-lineage crate — new crate

- [ ] Create `crates/krishiv-lineage/Cargo.toml` with dependencies: `datafusion`, `krishiv-catalog`, `serde`, `serde_json`, `tokio`, `tracing`, `petgraph`.
- [ ] Define `LineageNode` enum: `Table(TableUri)`, `View(ViewName)`, `Transform(JobId)`.
- [ ] Define `LineageEdge` struct: `from: LineageNode`, `to: LineageNode`, `job_id: JobId`, `recorded_at: DateTime<Utc>`.
- [ ] Implement `LineageGraph` backed by `petgraph::DiGraph<LineageNode, LineageEdge>` with `upstream(table, depth)` and `downstream(column, depth)` traversal methods.
- [ ] Implement `LineageStore` trait with `async fn record(event: LineageEvent)` and `async fn for_table(uri: &TableUri) -> LineageGraph`.

**Validation**: `cargo build -p krishiv-lineage`

### S1.2: LineageCaptureRule in DataFusion — krishiv-lineage, krishiv-sql

- [ ] Implement `LineageCaptureRule: PhysicalOptimizerRule` per ADR-20.1 that walks the `ExecutionPlan` tree.
- [ ] Collect source table URIs from `ParquetExec`, `IcebergScan`, `DeltaScan`, and `HudiScan` nodes.
- [ ] Collect sink table URIs from `FileSinkExec` and Krishiv custom sink operators.
- [ ] Emit `LineageEvent { job_id, sources: Vec<TableUri>, sinks: Vec<TableUri>, plan_hash: u64, recorded_at }` to `LineageStore` asynchronously (do not block query execution).
- [ ] Register `LineageCaptureRule` in `krishiv-sql`'s physical optimizer pipeline.
- [ ] Add a unit test: construct a physical plan with known source and sink nodes; assert the rule emits the correct `LineageEvent`.

**Validation**: `cargo test -p krishiv-lineage && cargo test -p krishiv-sql`

### S1.3: Lineage query API — krishiv-lineage, krishiv-api

- [ ] Implement `session.catalog().lineage()` returning a `LineageClient`.
- [ ] Implement `lineage.for_table(name).upstream(depth: u32)` returning a `LineageGraph`.
- [ ] Implement `lineage.for_column(qualified_column).downstream()` returning column-level lineage where available (table-level fallback for R20).
- [ ] Implement `lineage.export(format: LineageExportFormat, target: &str)` supporting `OpenMetadata` and `DataHub` formats.
- [ ] Add integration tests that run a pipeline, then query lineage and assert the upstream table list matches the pipeline's sources.

**Validation**: `cargo test -p krishiv-lineage && cargo test -p krishiv-api`

### S1.4: OpenMetadata and DataHub push integration — krishiv-lineage

- [ ] Implement `OpenMetadataLineageExporter` that converts `LineageGraph` events to the OpenMetadata API format and POSTs to the configured `openmetadata_url`.
- [ ] Implement `DataHubLineageExporter` that converts to DataHub's `DataFlowInfo` and `DataJobInfo` aspect format.
- [ ] Support `CatalogSync(sync_interval)` that schedules periodic pushes via `tokio::time::interval`.
- [ ] Add unit tests with mock HTTP servers for both exporters.

**Validation**: `cargo test -p krishiv-lineage`

## Sprint 2 — Self-Serve Portal (React)

### S2.1: Node.js build pipeline in CI — krishiv-portal

- [ ] Create `crates/krishiv-portal/ui/` as a Vite + React + TypeScript project with dependencies: `react`, `@tanstack/react-query`, `recharts` (charts), `dagre-d3` (DAG visualisation), `@radix-ui/react-*` (accessible components).
- [ ] Add a `build.rs` in `crates/krishiv-portal/` that invokes `npm run build` via `std::process::Command` if `KRISHIV_DEV_ASSETS_DIR` is not set.
- [ ] Update CI workflow to install Node.js 20 and run `npm ci` before `cargo build`.
- [ ] Embed the `ui/out/` directory in the Rust binary using `rust-embed` per ADR-20.4.

**Validation**: `cargo build -p krishiv-portal`

### S2.2: Portal backend API — krishiv-ui

- [ ] Extend `krishiv-ui`'s Axum HTTP server with portal API routes under `/api/v1/portal/`:
  - `GET /api/v1/portal/catalog/tables` — list tables from `krishiv-catalog`.
  - `GET /api/v1/portal/catalog/tables/{name}/lineage` — table lineage from `krishiv-lineage`.
  - `GET /api/v1/portal/jobs` — active and recent jobs from the scheduler.
  - `GET /api/v1/portal/jobs/{id}/dag` — physical plan DAG as a JSON graph.
  - `GET /api/v1/portal/jobs/{id}/operators` — per-operator throughput and lag metrics.
  - `GET /api/v1/portal/cost` — cost attribution by job, namespace, and team from `krishiv-metrics`.
  - `GET /api/v1/portal/policies` — RBAC roles and masking rules from `krishiv-governance`.
- [ ] Serve the embedded React app for all non-API routes (SPA routing).
- [ ] Add integration tests for each API endpoint using `axum-test`.

**Validation**: `cargo test -p krishiv-ui`

### S2.3: Catalog browser and job management UI — krishiv-portal (React)

- [ ] Implement `CatalogBrowser` React component: list tables, click to view schema, column statistics, and lineage graph.
- [ ] Implement `JobList` React component: show active/recent jobs, status, duration, and a cancel button.
- [ ] Implement `JobDetail` view: show physical plan DAG using `dagre-d3`; colour nodes by throughput (green/yellow/red).
- [ ] Implement `PolicyManager` view: list RBAC roles and masking rules; button to add/edit (POSTs to governance API).
- [ ] Implement `CostDashboard` view: per-job and per-namespace cost bar chart using `recharts`.
- [ ] Add Playwright end-to-end tests for the catalog browser and job list (gated behind `#[cfg(feature = "e2e-tests")]`).

**Validation**: `npm test` in `crates/krishiv-portal/ui/` passes all unit tests.

### S2.4: Live DAG view with streaming updates — krishiv-ui, krishiv-portal

- [ ] Add `GET /api/v1/portal/jobs/{id}/operators/stream` as a Server-Sent Events (SSE) endpoint in `krishiv-ui` that pushes operator metrics at 1-second intervals.
- [ ] Implement `LiveDagView` React component that subscribes to the SSE stream via `EventSource` and updates operator throughput/lag numbers in real time without full page reload.
- [ ] Add a test that connects to the SSE stream and asserts at least 3 events are received within 5 seconds.

**Validation**: `cargo test -p krishiv-ui`

## Sprint 3 — GDPR/CCPA & SOC2/HIPAA Compliance

### S3.1: krishiv-compliance crate — new crate

- [ ] Create `crates/krishiv-compliance/Cargo.toml` with dependencies: `krishiv-governance`, `krishiv-lineage`, `krishiv-catalog`, `sha2`, `serde`, `tokio`, `tracing`.
- [ ] Define `RetentionPolicy { table: TableUri, ttl_days: u32, delete_strategy: DeletionStrategy, compliance: ComplianceFramework }`.
- [ ] Implement `RetentionPolicyEngine` that runs on a configurable schedule and deletes/anonymises rows older than `ttl_days` from the target table.
- [ ] Implement `ErasureJob { user_id: String, tables: Vec<TableUri>, verify_completeness: bool }` that issues `MERGE INTO` delete clauses for matching rows across all listed tables.
- [ ] Add a test that populates a fixture table, runs an erasure job, and asserts all matching rows are deleted and an audit record is produced.

**Validation**: `cargo test -p krishiv-compliance`

### S3.2: Data classification scanner — krishiv-compliance

- [ ] Implement `DataClassifier` that samples up to 1000 rows from a table and applies regex and ML-heuristic classifiers.
- [ ] Implement `PiiClassifier`: detect names, emails, phone numbers, SSNs, IP addresses using regex patterns from the `fancy-regex` crate.
- [ ] Implement `PhiClassifier`: detect medical record numbers, ICD codes, diagnosis text patterns.
- [ ] Implement `PciClassifier`: detect credit card numbers (Luhn check), CVV patterns, bank routing numbers.
- [ ] Return `ClassificationResult { table: TableUri, columns: Vec<ColumnClassification> }` where `ColumnClassification` lists detected sensitivity labels.
- [ ] Add unit tests with fixture data covering each classifier.

**Validation**: `cargo test -p krishiv-compliance`

### S3.3: Tamper-evident audit log — krishiv-compliance

- [ ] Implement `AuditLogWriter` per ADR-20.3: a single Tokio task that receives `AuditEvent` via an `mpsc::channel`, buffers events for 100ms, then writes a batch as one S3 JSON Lines object with the key `audit/{region}/{date}/{timestamp}.jsonl`.
- [ ] Implement hash chaining: each `AuditEvent` includes `prev_hash: String` (SHA-256 of the previous batch's serialised JSON); the first event of a new log uses a known genesis hash.
- [ ] Implement `AuditLogVerifier::verify_chain(bucket, prefix)` that reads all batch objects in order, re-hashes each, and asserts continuity.
- [ ] Add a unit test that writes 500 events, verifies the chain, then corrupts one event and asserts the verifier detects the corruption.

**Validation**: `cargo test -p krishiv-compliance`

### S3.4: Python compliance API

- [ ] Expose `session.set_retention_policy(table, ttl_days, delete_strategy, compliance)`.
- [ ] Expose `session.submit_erasure_job(user_id, tables, verify_completeness)` returning an `ErasureJobHandle` with `.await_completion()`.
- [ ] Expose `session.scan_classification(table, classifiers)` returning a `ClassificationResult`.
- [ ] Expose `ks.classifiers.PII`, `ks.classifiers.PHI`, `ks.classifiers.PCI`.
- [ ] Expose `ks.AuditConfig(backend, log_query_results, compliance)` and `ks.audit_backends.immutable_s3(bucket, encrypt, hash_chaining)`.
- [ ] Add `.pyi` stub entries for all new symbols.

**Validation**: `cargo test -p krishiv-python`

## Sprint 4 — SLA Management & Alerting

### S4.1: SLA definition and monitoring — krishiv-metrics

- [ ] Define `SLA { max_processing_lag: Duration, max_checkpoint_age: Duration, breach_action: BreachAction, critical_breach_action: Option<BreachAction> }` in `krishiv-api`.
- [ ] Implement `SlaMonitor` in `krishiv-metrics` as a Tokio task that polls each running job's `current_lag` and `last_checkpoint_age` from the scheduler's metrics store every 10 seconds.
- [ ] Compare against the job's configured `SLA` thresholds; emit `SlaBreachEvent { job_id, metric, current_value, threshold, severity }` when a threshold is crossed.
- [ ] Track breach count and health status per job: `HEALTHY`, `WARNING` (soft breach), `CRITICAL` (hard breach).

**Validation**: `cargo test -p krishiv-metrics`

### S4.2: BreachAction implementations — krishiv-metrics

- [ ] Implement `BreachAction::Alert(AlertChannel)` where `AlertChannel` supports `PagerDuty { integration_key }` and `Webhook { url }`.
- [ ] Implement `PagerDutyAlerter` using the PagerDuty Events API v2 (`reqwest`-based HTTP POST).
- [ ] Implement `WebhookAlerter` that POSTs a JSON payload with the `SlaBreachEvent` to the configured URL.
- [ ] Implement `BreachAction::RestartPipeline` that calls `scheduler.cancel_job(job_id)` and `scheduler.submit_job(job_spec)` to trigger a clean restart.
- [ ] Add unit tests for each breach action using mock HTTP servers.

**Validation**: `cargo test -p krishiv-metrics`

### S4.3: SLA status API — krishiv-api, krishiv-ui

- [ ] Implement `session.jobs.sla_status(job_id)` returning `SlaStatus { current_lag, last_checkpoint_age, breaches_24h, health }`.
- [ ] Expose `GET /api/v1/portal/jobs/{id}/sla` in `krishiv-ui` that returns the `SlaStatus` as JSON.
- [ ] Add the SLA status badge to the `JobList` React component in the portal (green HEALTHY, yellow WARNING, red CRITICAL).
- [ ] Add a unit test that injects a lag spike via a mock scheduler, triggers `SlaMonitor`, and asserts a `BreachAction::Alert` is fired.

**Validation**: `cargo test -p krishiv-api && cargo test -p krishiv-ui`

### S4.4: Python SLA API

- [ ] Expose `ks.SLA(max_processing_lag, max_checkpoint_age, breach_action, critical_breach_action)`.
- [ ] Expose `ks.BreachAction.alert(channel)` and `ks.BreachAction.restart_pipeline()`.
- [ ] Expose `ks.AlertChannel.pagerduty(integration_key)` and `ks.AlertChannel.webhook(url)`.
- [ ] Wire `SLA` into `session.submit_job(pipeline, sla=...)`.
- [ ] Add `.pyi` stub entries.

**Validation**: `cargo test -p krishiv-python`

## Sprint 5 — Enterprise Packaging (Helm, Terraform, Marketplace)

### S5.1: Helm chart — deploy/helm/krishiv/

- [ ] Create `deploy/helm/krishiv/Chart.yaml`, `values.yaml`, and templates for: coordinator `Deployment`, executor `DaemonSet`/`Deployment`, `Service`, `Ingress`, `ServiceAccount`, `ClusterRole`, `ClusterRoleBinding`, `PodDisruptionBudget`.
- [ ] Add production defaults in `values.yaml`: `replicaCount: 3` for coordinator (requires `--ha-mode etcd`), resource limits, `securityContext.runAsNonRoot: true`, TLS via cert-manager annotation.
- [ ] Add a `_helpers.tpl` that generates RBAC rules scoped to the release namespace.
- [ ] Validate with `helm lint deploy/helm/krishiv/` and `helm template krishiv deploy/helm/krishiv/ --debug`.

**Validation**: `helm lint deploy/helm/krishiv/ && helm unittest deploy/helm/krishiv/`

### S5.2: Terraform modules — deploy/terraform/

- [ ] Create `deploy/terraform/aws/` module: EKS cluster, node groups (on-demand + spot), IAM roles, S3 buckets for checkpoints and audit logs, RDS Postgres for global coordinator.
- [ ] Create `deploy/terraform/gcp/` module: GKE cluster, Workload Identity, Cloud Storage buckets, Cloud SQL Postgres.
- [ ] Create `deploy/terraform/azure/` module: AKS cluster, Managed Identity, Azure Blob Storage, Azure Database for PostgreSQL.
- [ ] Add `README.md` in each module directory documenting required variables and outputs.
- [ ] Validate with `terraform validate` in each module directory.

**Validation**: `terraform validate deploy/terraform/aws/ && terraform validate deploy/terraform/gcp/ && terraform validate deploy/terraform/azure/`

### S5.3: Docker Compose quick-start — deploy/docker-compose/

- [ ] Create `deploy/docker-compose/docker-compose.yml` with services: `krishiv-coordinator`, `krishiv-executor` (2 replicas), `minio` (S3-compatible), `postgres` (coordinator metadata), `etcd` (HA mode), `schema-registry` (Confluent OSS).
- [ ] Add `deploy/docker-compose/.env.example` with all required environment variables.
- [ ] Add a smoke test script `deploy/docker-compose/smoke-test.sh` that submits a test SQL query and asserts a non-empty result.

**Validation**: `bash deploy/docker-compose/smoke-test.sh` in CI with Docker Compose v2.

### S5.4: krishiv-cloud CLI — new binary crate

- [ ] Create `crates/krishiv-cloud/` as a binary crate with `clap` argument parsing.
- [ ] Implement `krishiv-cloud cluster create --name --region --cloud --coordinator-size --executor-count --executor-size`.
- [ ] Implement `krishiv-cloud cluster list` and `krishiv-cloud cluster delete`.
- [ ] Implement `krishiv-cloud cluster status --name` returning coordinator URL and executor count.
- [ ] For R20, the managed service backend is a local mock (returns fixture URLs); real cloud API integration is deferred to a managed service launch.
- [ ] Add unit tests for all subcommands against the mock backend.

**Validation**: `cargo test -p krishiv-cloud`

## Sprint 6 — Multi-Tenant Isolation & dbt-Native

### S6.1: Namespace isolation — krishiv-operator

- [ ] Extend `KrishivJob` CRD spec with `namespace: String` and `isolation: IsolationLevel` (PROCESS, NETWORK) fields.
- [ ] Implement PROCESS isolation: create a dedicated Kubernetes `Namespace` per tenant `namespace` value; deploy executor pods in that namespace with a `serviceAccountName` scoped to the tenant.
- [ ] Implement NETWORK isolation: generate `NetworkPolicy` resources in each tenant namespace that deny ingress/egress to all other tenant namespaces. Allow only coordinator → executor and executor → S3/object storage traffic.
- [ ] Add a controller test that submits two jobs with different namespaces, asserts two Kubernetes `Namespace` objects are created, and that `NetworkPolicy` denies cross-namespace traffic (verified by inspecting the policy spec, not requiring a live CNI).

**Validation**: `cargo test -p krishiv-operator`

### S6.2: Python isolation API

- [ ] Expose `ks.IsolationLevel.PROCESS` and `ks.IsolationLevel.NETWORK`.
- [ ] Extend `ks.Session.connect(namespace="team-analytics", isolation=ks.IsolationLevel.NETWORK)`.
- [ ] Add `.pyi` stub entries.
- [ ] Add a Python integration test that connects two sessions with different namespaces and asserts they cannot read each other's job state.

**Validation**: `cargo test -p krishiv-python`

### S6.3: dbt krishiv_incremental materialisation — krishiv-dbt-adapter

- [ ] Add `krishiv_incremental` materialisation to the `krishiv` dbt adapter (from R15) as a custom materialisation macro.
- [ ] The macro detects whether the target table exists: if not, runs a full `CREATE TABLE AS SELECT`; if yes, runs an incremental `MERGE INTO` (using the R18 format-specific path) keyed on `unique_key`.
- [ ] Support `on_schema_change: fail | append_new_columns | sync_all_columns`.
- [ ] Add `+krishiv_trigger: on_change` model config that wires into the R14 live table engine for CDC-driven incremental runs.
- [ ] Add dbt integration tests: `dbt run --full-refresh` and `dbt run` (incremental) both succeed; a second `dbt run` with an added column succeeds when `on_schema_change: append_new_columns`.

**Validation**: `dbt test` in the krishiv dbt adapter test project.

### S6.4: Final compliance audit — all crates

- [ ] Run `cargo clippy --workspace -- -D warnings` and fix all new warnings introduced in R20 sprints.
- [ ] Run `cargo test --workspace` and verify zero failures.
- [ ] Verify `AuditLogVerifier::verify_chain` passes on a chain produced by a 1-hour soak test (1000 events/minute × 60 minutes).
- [ ] Verify `helm install krishiv deploy/helm/krishiv/` on a fresh `kind` cluster: all pods reach `Running` within 5 minutes.

**Validation**: `cargo test --workspace && cargo clippy --workspace -- -D warnings`

## Acceptance Gate

R20 is complete when:

- [ ] Data portal: a non-engineer can discover a table by name, view its column lineage graph, and submit a SQL query through the UI without CLI access — verified by a Playwright end-to-end test.
- [ ] Lineage capture: run a pipeline that reads from two sources and writes to one sink; `lineage.for_table(sink).upstream(depth=2)` returns both source tables.
- [ ] GDPR erasure: an erasure job deletes all rows for a given `user_id` from three configured tables and produces a verifiable audit record containing the table names, row counts deleted, and the deletion timestamp.
- [ ] Data classification: `session.scan_classification("customer_data", [PII, PHI])` correctly identifies an email column as PII and a diagnosis column as PHI in the fixture table.
- [ ] SOC2 audit trail: `AuditLogVerifier::verify_chain` passes on a 10,000-event chain written by the `AuditLogWriter`; after artificially corrupting event 5000, the verifier reports a chain break at that position.
- [ ] SLA breach: inject an artificial lag spike into a running job; assert a PagerDuty alert fires within 2 minutes (verified by a mock PagerDuty webhook in the integration test).
- [ ] dbt `dbt run --full-refresh` followed by `dbt run` (incremental) both complete successfully with the `krishiv_incremental` materialisation type against a test Iceberg table.
- [ ] Helm install: `helm install krishiv deploy/helm/krishiv/` on a fresh `kind` cluster; all components healthy (coordinator, executors, etcd) within 5 minutes.
- [ ] Multi-tenant isolation: two jobs submitted to different namespaces produce `NetworkPolicy` objects that deny cross-namespace traffic per the Kubernetes policy spec.
- [ ] `cargo test --workspace` passes with zero failures.
- [ ] `cargo clippy --workspace -- -D warnings` passes.
- [ ] The ADR-20.4 decision is recorded as DECIDED in `docs/architecture/architectural-decisions-r12-r20.md` before Sprint 2 begins.
