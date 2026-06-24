import Link from 'next/link';
import type { JSX } from 'react';
import { Badge, Section, SiteShell } from '@/components/Shell';

type ArchIcon = 'interfaces' | 'runtime' | 'foundation' | 'primitives' | 'ecosystem';

const layers: Array<{ title: string; labels: string; icon: ArchIcon }> = [
  { title: 'Interfaces', labels: 'SQL · Rust · Python', icon: 'interfaces' },
  { title: 'Unified Runtime', labels: 'Batch · Streaming · Incremental Processing', icon: 'runtime' },
  { title: 'Execution Foundation', labels: 'DataFusion · Apache Arrow', icon: 'foundation' },
  { title: 'Distributed Primitives', labels: 'Scheduling · Shuffle · State · Checkpoints', icon: 'primitives' },
  { title: 'Data Ecosystem', labels: 'Iceberg · Kafka · Parquet · Object Storage · Catalogs', icon: 'ecosystem' },
];

const layerIcons: Record<ArchIcon, JSX.Element> = {
  interfaces: (
    <svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M5 3L1 8l4 5M11 3l4 5-4 5"/>
    </svg>
  ),
  runtime: (
    <svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor" aria-hidden="true">
      <circle cx="8" cy="8" r="7" opacity=".2"/>
      <circle cx="8" cy="8" r="4" opacity=".55"/>
      <circle cx="8" cy="8" r="1.75"/>
    </svg>
  ),
  foundation: (
    <svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor" aria-hidden="true">
      <path d="M9.5 2L5.5 9H9l-1.5 5.5 6.5-8H10.5z"/>
    </svg>
  ),
  primitives: (
    <svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor" aria-hidden="true">
      <circle cx="4" cy="4" r="2"/><circle cx="12" cy="4" r="2"/>
      <circle cx="4" cy="12" r="2"/><circle cx="12" cy="12" r="2"/>
      <rect x="6.25" y="3.25" width="3.5" height="1.5"/>
      <rect x="6.25" y="11.25" width="3.5" height="1.5"/>
      <rect x="3.25" y="6.25" width="1.5" height="3.5"/>
      <rect x="11.25" y="6.25" width="1.5" height="3.5"/>
    </svg>
  ),
  ecosystem: (
    <svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor" aria-hidden="true">
      <rect x="2" y="2" width="12" height="3" rx="1.5"/>
      <rect x="2" y="6.5" width="12" height="3" rx="1.5"/>
      <rect x="2" y="11" width="12" height="3" rx="1.5"/>
    </svg>
  ),
};

const requestFlow = [
  'APIs accept SQL, Rust, or Python calls and create sessions or dataframes.',
  'DataFusion parses and plans SQL; Krishiv plan and policy modules add typed runtime contracts.',
  'ExecutionRuntime selects embedded, single-node, or remote placement without silent distributed fallback.',
  'Coordinators own job/task lifecycle; executors run replaceable data-plane work.',
  'State, checkpoints, shuffle, and connectors use durability profiles and explicit capabilities.',
  'Results return as Arrow RecordBatch values or streaming batches depending on the API.',
];

export default function Architecture() {
  return (
    <SiteShell>
      <main className="container">
        <section className="page-hero">
          <Badge tone="blue">Architecture</Badge>
          <h1 className="gradient-text">A unified engine boundary for local and distributed data work.</h1>
          <p className="lead">
            Krishiv routes APIs through one planning and runtime model, with scheduler, executor, state, shuffle, checkpoint, metadata, and connector behavior kept behind explicit crate APIs.
          </p>
        </section>

        <Section title="System layers">
          <div className="arch-diagram" aria-label="Krishiv architecture layers" style={{ maxWidth: 560 }}>
            {layers.map((layer, i) => (
              <div key={layer.title}>
                {i > 0 && (
                  <div
                    className="arch-connector"
                    style={{
                      '--delay-left': `${i * 0.22}s`,
                      '--delay-right': `${i * 0.22 + 0.6}s`,
                    } as React.CSSProperties}
                  >
                    <span className="arch-line arch-line-left"/>
                    <span className="arch-line arch-line-right"/>
                  </div>
                )}
                <div className="arch-layer">
                  <span className="arch-icon">{layerIcons[layer.icon]}</span>
                  <div>
                    <p className="arch-layer-title">{layer.title}</p>
                    <p className="arch-layer-labels">{layer.labels}</p>
                  </div>
                </div>
              </div>
            ))}
            <p className="arch-maturity-link">
              <a href="/product/maturity">Explore feature maturity →</a>
            </p>
          </div>
        </Section>

        <Section title="Request flow">
          <div className="grid">
            {requestFlow.map((item) => (
              <div className="card" key={item}>
                <p>{item}</p>
              </div>
            ))}
          </div>
        </Section>

        <Section title="Batch, streaming, and delta / IVM">
          <p className="lead">
            Batch SQL, streaming windows, and delta-oriented IVM share Arrow and DataFusion foundations. IVM is experimental: IncrementalFlow exists, but distributed executor-side IVM is in progress.
          </p>
        </Section>

        <Section title="State, checkpoints, scheduling, and shuffle">
          <p className="lead">
            Krishiv exposes dev-local, single-node-durable, and distributed-durable profiles. These profiles select metadata, shuffle, state, and checkpoint storage choices instead of implying universal exactly-once behavior.
          </p>
        </Section>

        <Section title="Storage, catalogs, and topology">
          <div className="split">
            <div className="card">
              <h3>Storage and catalogs</h3>
              <p>
                Iceberg is the primary lakehouse platform. REST catalog compatible paths, Hive, Glue, Parquet, Kafka, S3/object store, and ADLS are documented with preview or feature-gated maturity.
              </p>
            </div>
            <div className="card">
              <h3>Local versus distributed</h3>
              <p>
                Embedded mode runs in process. Single-node runs components on one host. Distributed mode requires explicit remote endpoints, bearer-token production control-plane paths, and replaceable executors.
              </p>
            </div>
          </div>
          <div className="actions">
            <Link className="btn btn-primary" href="/docs/latest/concepts/architecture">
              Read architecture docs
            </Link>
            <Link className="btn btn-secondary" href="/docs/latest/concepts/distributed-mode">
              Distributed mode
            </Link>
          </div>
        </Section>
      </main>
    </SiteShell>
  );
}
