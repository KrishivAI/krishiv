import type { Metadata } from 'next';
import { Badge, SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Engine Feature Maturity',
  description:
    'Repository-backed maturity labels for Krishiv Engine batch SQL, streaming, connectors, distributed execution, and incremental view maintenance.',
  openGraph: {
    title: 'Krishiv Engine Feature Maturity',
    description:
      'Repository-backed capability status for Krishiv Engine, including Available, Preview, Experimental, and In progress work.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/product/maturity',
  },
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
    description: 'Implemented in the source workspace. The project is still a developer preview, so this is not a production-readiness promise.',
    items: [
      { name: 'Batch SQL', note: 'DataFusion-backed SQL over Apache Arrow RecordBatches and registered sources.' },
      { name: 'Apache Arrow data model', note: 'RecordBatch is the primary in-memory and IPC columnar format.' },
      { name: 'Rust Session / DataFrame API', note: 'Session, DataFrame, and Stream types are the primary Rust-facing API surface.' },
      { name: 'DataFusion SQL planning', note: 'SQL parsing, logical planning, expression evaluation, and local execution via DataFusion.' },
      { name: 'Embedded runtime mode', note: 'Runs all components in-process; no network endpoints required. Used in tests and local API calls.' },
      { name: 'Single-node runtime mode', note: 'Places core Engine components on one host. Durability depends on the configured state, checkpoint, and storage backends.' },
    ],
  },
  {
    tone: 'violet',
    label: 'Experimental',
    description: 'Implemented and functional. APIs and semantics may change. Not certified for production use.',
    items: [
      { name: 'Delta Batch / IVM', note: 'DeltaBatch (weighted Arrow rows) and IncrementalFlow (view maintenance across ticks) are implemented with partitioning, snapshots, and checkpoint hooks. Distributed executor-side IVM execution is deferred.' },
      { name: 'Python connector surface', note: 'The current extension compiles several connector families by default; additional Cargo features add more. Packaging and API boundaries are not yet stable.' },
    ],
  },
  {
    tone: 'blue',
    label: 'Preview',
    description: 'Implementation paths exist, but APIs, recovery behavior, or end-to-end combinations still need certification.',
    items: [
      { name: 'Stateful streaming', note: 'Streaming sessions, windows, state, and job APIs exist. Recovery and connector guarantees remain combination-specific.' },
      { name: 'Python bindings (source)', note: 'PyO3 bindings expose core Session and DataFrame APIs when built from the repository. Names and signatures can change before 1.0.' },
      { name: 'Distributed runtime mode', note: 'Remote coordinator and executor transport with bearer-token auth. Requires explicit Flight endpoint; no silent local fallback.' },
      { name: 'Iceberg integration', note: 'Iceberg-oriented catalog and table paths exist; backend and commit-protocol certification continues.' },
      { name: 'Kafka connector', note: 'Source and sink paths exist. Delivery guarantees depend on the exact source, sink, and checkpoint combination.' },
      { name: 'Parquet and object-store paths', note: 'Connector paths exist. The named S3 registry driver is currently local-backed; remote cloud construction is separately feature-gated.' },
      { name: 'Shuffle service', note: 'In-memory, local disk, object-store, and Flight-oriented shuffle paths behind the krishiv-shuffle crate API.' },
      { name: 'Checkpoint storage', note: 'Async checkpoint primitives with sync compatibility wrappers. Scheduler gRPC checkpoint acks use the async path.' },
      { name: 'State management', note: 'In-memory and RocksDB-backed keyed state, TTL, migration, and incremental state behind the krishiv-state crate API.' },
      { name: 'Kubernetes operator / CRD', note: 'CRD and operator integration in the krishiv-operator crate. Manifests live in k8s/.' },
      { name: 'Scheduler foundations', note: 'Job/task lifecycle, metadata-store, and leadership-coordination code exists; operational fault-tolerance certification is still in progress.' },
    ],
  },
  {
    tone: 'gray',
    label: 'Planned',
    description: 'On the roadmap but not yet implemented. Do not rely on these without maintainer confirmation.',
    items: [
      { name: 'Distributed IVM', note: 'Executor-side incremental view maintenance across a distributed cluster. Requires distributed IVM protocol design.' },
      { name: 'Broad exactly-once certification', note: 'Engine does not claim exactly-once across arbitrary source, sink, state, and checkpoint combinations.' },
    ],
  },
];

export default function Maturity() {
  return (
    <SiteShell>
      <main className="container">
        <section className="page-hero">
          <Badge tone="blue">Engine maturity</Badge>
          <h1 className="gradient-text">Capability status, backed by codebase evidence.</h1>
          <p className="lead">
            Each status is derived from Engine sources, tests, examples, and public APIs. It describes repository maturity—not production readiness or a hosted-service SLA.
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
          <strong>Maintainer note:</strong> Verify Preview, Experimental, and Planned claims against the current source tree before publishing them elsewhere. Krishiv Platform has its own coming-soon status and is not included in this Engine matrix.
        </div>
      </main>
    </SiteShell>
  );
}
