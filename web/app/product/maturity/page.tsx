import type { Metadata } from 'next';
import { Badge, SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Feature Maturity',
  description: 'Codebase-verified capability maturity for Krishiv — Stable, Experimental, Preview, and Planned features.',
};

type MaturityTier = {
  tone: 'green' | 'violet' | 'blue' | 'gray';
  label: string;
  description: string;
  items: Array<{ name: string; note: string }>;
};

const tiers: MaturityTier[] = [
  {
    tone: 'green',
    label: 'Available',
    description: 'Implemented, tested, and used in core workflows. APIs are stable within minor versions.',
    items: [
      { name: 'Batch SQL', note: 'DataFusion-backed SQL over Apache Arrow RecordBatches and registered sources.' },
      { name: 'Apache Arrow data model', note: 'RecordBatch is the internal and IPC columnar format across all runtime paths.' },
      { name: 'Rust Session / DataFrame API', note: 'Session, DataFrame, and Stream types are the primary Rust-facing API surface.' },
      { name: 'DataFusion SQL planning', note: 'SQL parsing, logical planning, expression evaluation, and local execution via DataFusion.' },
      { name: 'Embedded runtime mode', note: 'Runs all components in-process; no network endpoints required. Used in tests and local API calls.' },
      { name: 'Single-node runtime mode', note: 'Runs coordinator, executor, and Flight/gRPC endpoints on one host with local filesystem and RocksDB.' },
      { name: 'Python bindings (core)', note: 'PyO3 bindings expose Session, DataFrame, and streaming APIs. Optional connector features are feature-gated.' },
      { name: 'Explicit durability profiles', note: 'dev-local, single-node-durable, and distributed-durable profiles control metadata, shuffle, state, and checkpoint storage.' },
    ],
  },
  {
    tone: 'violet',
    label: 'Experimental',
    description: 'Implemented and functional. APIs and semantics may change. Not certified for production use.',
    items: [
      { name: 'Delta Batch / IVM', note: 'DeltaBatch (weighted Arrow rows) and IncrementalFlow (view maintenance across ticks) are implemented with partitioning, snapshots, and checkpoint hooks. Distributed executor-side IVM execution is deferred.' },
      { name: 'Python connector features', note: 'Kafka, Iceberg, and vector sink bindings exist as optional Cargo features. API surface is not yet stable.' },
    ],
  },
  {
    tone: 'blue',
    label: 'Preview',
    description: 'Scaffolding and initial implementation exist. End-to-end certification work is ongoing. Use with caution.',
    items: [
      { name: 'Distributed runtime mode', note: 'Remote coordinator and executor transport with bearer-token auth. Requires explicit Flight endpoint; no silent local fallback.' },
      { name: 'Iceberg catalog integration', note: 'REST, Hive, and Glue catalog paths. Iceberg is the primary lakehouse target; certification work continues.' },
      { name: 'Kafka connector', note: 'Source and transactional sink via rdkafka. End-to-end exactly-once depends on certified checkpoint combinations.' },
      { name: 'Parquet / S3 / ADLS connectors', note: 'Connector contracts and implementations exist; end-to-end guarantees depend on certified combinations.' },
      { name: 'Shuffle service', note: 'In-memory, local disk, object-store, and Flight-oriented shuffle paths behind the krishiv-shuffle crate API.' },
      { name: 'Checkpoint storage', note: 'Async checkpoint primitives with sync compatibility wrappers. Scheduler gRPC checkpoint acks use the async path.' },
      { name: 'State management', note: 'In-memory and RocksDB-backed keyed state, TTL, migration, and incremental state behind the krishiv-state crate API.' },
      { name: 'Kubernetes operator / CRD', note: 'CRD and operator integration in the krishiv-operator crate. Manifests live in k8s/.' },
      { name: 'Scheduler fault tolerance', note: 'Job/task lifecycle, metadata stores, and leadership coordination via krishiv-scheduler. Failure handling foundations are in place.' },
    ],
  },
  {
    tone: 'gray',
    label: 'Planned',
    description: 'On the roadmap but not yet implemented. Do not rely on these without maintainer confirmation.',
    items: [
      { name: 'Distributed IVM', note: 'Executor-side incremental view maintenance across a distributed cluster. Requires distributed IVM protocol design.' },
      { name: 'Full exactly-once guarantees', note: 'End-to-end exactly-once across arbitrary source/sink/checkpoint combinations. Currently scoped to certified combinations only.' },
      { name: 'Krishiv Cloud', note: 'Managed compute offering. Not yet implemented.' },
    ],
  },
];

export default function Maturity() {
  return (
    <SiteShell>
      <main className="container">
        <section className="page-hero">
          <Badge tone="blue">Feature Maturity</Badge>
          <h1 className="gradient-text">Capability status, backed by codebase evidence.</h1>
          <p className="lead">
            Each status below is derived from inspecting Rust sources, tests, examples, and public APIs in the Krishiv workspace. Statuses reflect what is implemented, not what is intended.
          </p>
        </section>

        {tiers.map((tier) => (
          <section className="section" key={tier.label}>
            <div className="maturity-section">
              <h2>
                <Badge tone={tier.tone}>{tier.label}</Badge>
              </h2>
              <p className="lead" style={{ fontSize: 16, marginBottom: 20 }}>{tier.description}</p>
              <div className="maturity-grid">
                {tier.items.map((item) => (
                  <div className="maturity-card" key={item.name}>
                    <h3>{item.name}</h3>
                    <p>{item.note}</p>
                  </div>
                ))}
              </div>
            </div>
          </section>
        ))}

        <div className="maturity-note">
          <strong>Maintainer note:</strong> Statuses marked <em>Preview</em> or <em>Planned</em> require maintainer confirmation before use in documentation or marketing materials. Capability descriptions are based on codebase inspection and may not reflect in-flight work on development branches.
        </div>
      </main>
    </SiteShell>
  );
}
