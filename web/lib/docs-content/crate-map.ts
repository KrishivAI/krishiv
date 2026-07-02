export type CrateInfo = {
  name: string;
  responsibility: string;
  maturity: 'Available' | 'Preview' | 'Experimental' | 'In Progress' | 'Planned';
  keyApis?: string[];
  docsLink?: string;
  relatedExamples?: string[];
};

export const CRATE_MAP: CrateInfo[] = [
  { name: 'krishiv', responsibility: 'User-facing facade and CLI binary.', maturity: 'Available', keyApis: ['sql', 'explain', 'jobs', 'local start'], docsLink: '/docs/latest/cli/overview' },
  { name: 'krishiv-api', responsibility: 'Session, DataFrame, Stream, IncrementalFlow, and all public Rust API types.', maturity: 'Available', keyApis: ['Session', 'DataFrame', 'Stream', 'IncrementalFlow', 'PipelineBuilder'], docsLink: '/docs/latest/rust' },
  { name: 'krishiv-sql', responsibility: 'DataFusion integration, SQL execution, catalog and table-provider abstractions.', maturity: 'Available', docsLink: '/docs/latest/sql' },
  { name: 'krishiv-plan', responsibility: 'Logical/physical plans, expression AST, UDF contracts, governance/policy, CEP.', maturity: 'Available' },
  { name: 'krishiv-runtime', responsibility: 'Embedded, single-node, and remote runtime routing.', maturity: 'Available', docsLink: '/docs/latest/concepts/execution-model' },
  { name: 'krishiv-dataflow', responsibility: 'Arrow operator runtime, queues, barriers, windows, joins, stateful ops.', maturity: 'Available', docsLink: '/docs/latest/streaming/overview' },
  { name: 'krishiv-delta', responsibility: 'DeltaBatch, operators, IncrementalView, IntegrateOp.', maturity: 'Experimental', docsLink: '/docs/latest/concepts/incremental-flow' },
  { name: 'krishiv-ivm', responsibility: 'Incremental view maintenance engine.', maturity: 'Experimental', docsLink: '/docs/latest/concepts/incremental-flow' },
  { name: 'krishiv-scheduler', responsibility: 'Coordinator, job/task lifecycle, metadata stores, leadership, gRPC server.', maturity: 'Available', docsLink: '/docs/latest/operations/scheduler' },
  { name: 'krishiv-executor', responsibility: 'Executor process, task runner, task assignment receiver, shuffle/checkpoint hooks.', maturity: 'Available', docsLink: '/docs/latest/concepts/distributed-mode' },
  { name: 'krishiv-state', responsibility: 'In-memory and RocksDB-backed keyed state, TTL, migration, checkpoint/savepoint.', maturity: 'Preview', docsLink: '/docs/latest/state/overview' },
  { name: 'krishiv-shuffle', responsibility: 'In-memory, local disk, object-store, and Flight-oriented shuffle support.', maturity: 'Preview', docsLink: '/docs/latest/operations/shuffle' },
  { name: 'krishiv-connectors', responsibility: 'Source/sink contracts, Parquet/Kafka/S3 paths, Iceberg-first lakehouse helpers.', maturity: 'Preview', docsLink: '/docs/latest/connectors' },
  { name: 'krishiv-proto', responsibility: 'Typed IDs and coordinator/executor wire contracts.', maturity: 'Available' },
  { name: 'krishiv-common', responsibility: 'Shared utilities used across runtime and engine crates.', maturity: 'Available' },
  { name: 'krishiv-python', responsibility: 'PyO3 Python bindings.', maturity: 'Available', docsLink: '/docs/latest/python' },
  { name: 'krishiv-flight-sql', responsibility: 'Arrow Flight SQL server.', maturity: 'Preview', docsLink: '/docs/latest/cli/overview' },
  { name: 'krishiv-sql-gateway', responsibility: 'Separately versioned JDBC/ODBC SQL gateway facade.', maturity: 'Preview' },
  { name: 'krishiv-operator', responsibility: 'Kubernetes CRD and operator integration.', maturity: 'Preview', docsLink: '/docs/latest/operations/deployment' },
  { name: 'krishiv-ui', responsibility: 'Status API and web UI assets.', maturity: 'Preview' },
  { name: 'krishiv-metrics', responsibility: 'Metrics, tracing, and debug report structures.', maturity: 'Available', docsLink: '/docs/latest/observability/overview' },
  { name: 'krishiv-engine-core', responsibility: 'Core engine abstractions shared across runtime modes.', maturity: 'Available' },
  { name: 'krishiv-bench', responsibility: 'Benchmarks (on-demand; excluded from default workspace builds).', maturity: 'Available' },
  { name: 'krishiv-chaos', responsibility: 'Cross-crate chaos and fault-injection integration tests.', maturity: 'Available' },
];
