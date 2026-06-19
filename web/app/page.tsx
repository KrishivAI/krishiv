import Link from 'next/link';
import type { JSX } from 'react';
import { Badge, Section, SiteShell } from '@/components/Shell';
import { githubUrl, publicFacts } from '@/lib/site';

type ArchIcon = 'interfaces' | 'runtime' | 'foundation' | 'primitives' | 'ecosystem';

const archLayers: Array<{ title: string; labels: string; icon: ArchIcon }> = [
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

const features = [
  [
    'Unified Batch + Streaming',
    'Batch SQL and streaming jobs share Arrow batches, DataFusion planning, runtime routing, and scheduler/executor boundaries.',
    'Available',
  ],
  [
    'Delta Batch / IVM',
    'DeltaBatch carries weighted Arrow rows and IncrementalFlow maintains views across ticks. Distributed executor-side IVM remains in progress.',
    'Experimental',
  ],
  [
    'Rust-native runtime',
    'Rust 2024, Tokio, typed IDs, typed plans, and explicit durability profiles form the runtime foundation.',
    'Available',
  ],
  [
    'DataFusion + Arrow',
    'SQL parsing, planning, local execution, and the internal columnar model are built around DataFusion and Apache Arrow RecordBatch.',
    'Available',
  ],
  [
    'SQL, Rust, Python',
    'Rust Session/DataFrame/Stream APIs and PyO3 bindings expose SQL and streaming-oriented workflows. Some Python connector features are feature-gated.',
    'Available',
  ],
  [
    'Iceberg and catalogs',
    'Iceberg is the primary lakehouse target with REST/Hive/Glue catalog paths and ongoing certification work.',
    'Preview',
  ],
  [
    'Local to distributed',
    'Embedded, single-node, and distributed placements are explicit; distributed sessions require endpoints instead of silent fallback.',
    'Available',
  ],
  [
    'State, checkpoints, shuffle',
    'State, checkpoint, metadata, shuffle, and connector behavior sit behind crate APIs with explicit durability profiles.',
    'Preview',
  ],
] as const;

function statusTone(status: string): 'blue' | 'green' | 'violet' {
  if (status.includes('Available')) return 'green';
  if (status.includes('Experimental')) return 'violet';
  return 'blue';
}

function ArchDiagram() {
  return (
    <div className="arch-diagram" aria-label="Krishiv architecture layers">
      {archLayers.map((layer, i) => (
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
        <Link href="/product/maturity">Explore feature maturity →</Link>
      </p>
    </div>
  );
}

export default function Home() {
  return (
    <SiteShell>
      <main className="container">
        <section className="hero">
          <div>
            <Badge tone="orange">Open source · Built in Rust</Badge>
            <h1>
              One Engine for{' '}
              <span className="gradient-text">Batch, Streaming, and Incremental Processing</span>
            </h1>
            <p className="lead">
              Krishiv is a Rust-native compute engine that brings batch, streaming, and incremental workloads into one execution model—from local development to distributed systems.
            </p>
            <div className="actions">
              <Link className="btn btn-primary" href="/docs/latest/getting-started">
                Get Started →
              </Link>
              <a className="btn btn-secondary" href={githubUrl}>
                View on GitHub
              </a>
            </div>
          </div>

          <ArchDiagram />
        </section>

        <div className="cap-strip">
          {[
            'Unified execution',
            'Rust native',
            'Iceberg first',
            'Delta batch / IVM',
            'Local to distributed',
            'Fault-tolerant foundations',
          ].map((item) => (
            <div className="card" key={item}>
              <strong>{item}</strong>
            </div>
          ))}
        </div>

        <Section eyebrow="Problem / solution" title="Stop maintaining separate systems for related workloads.">
          <div className="split">
            <div className="card">
              <h3>Before Krishiv</h3>
              <ul className="muted">
                <li>Separate batch jobs</li>
                <li>Streaming-only systems</li>
                <li>Different APIs</li>
                <li>Duplicated operational complexity</li>
              </ul>
            </div>
            <div className="card">
              <h3>With Krishiv</h3>
              <ul className="muted">
                <li>Shared execution model</li>
                <li>Unified APIs</li>
                <li>Incremental / delta-oriented processing</li>
                <li>One ecosystem of connectors and runtime primitives</li>
              </ul>
            </div>
          </div>
        </Section>

        <Section eyebrow="Capabilities" title="A factual, codebase-backed feature set.">
          <div className="grid">
            {features.map(([title, text, status]) => (
              <div className="card" key={title}>
                <Badge tone={statusTone(status)}>{status}</Badge>
                <h3>{title}</h3>
                <p>{text}</p>
              </div>
            ))}
          </div>
        </Section>

        <Section eyebrow="Examples" title="SQL, Rust, and Python surfaces.">
          <div className="code-tabs">
            <div className="tabbar">
              <span>SQL</span>
              <span>Rust</span>
              <span>Python</span>
            </div>
            <pre>{`-- Available: DataFusion-backed SQL over registered sources
SELECT customer_id, SUM(amount) AS total_spend
FROM orders
GROUP BY customer_id;

-- Conceptual incremental view shape; use docs for current APIs
CREATE INCREMENTAL VIEW order_totals AS
SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;`}</pre>
          </div>
        </Section>

        <Section eyebrow="Architecture" title="One runtime model across embedded, single-node, and distributed modes.">
          <div className="split">
            <div className="diagram">
              {[
                'APIs and SQL',
                'Planning and policy',
                'Execution runtime',
                'Coordinator and scheduler',
                'Executors and dataflow',
                'State, shuffle, checkpoints, connectors',
              ].map((item, index) => (
                <div key={item}>
                  {index > 0 && <div className="flow-arrow">↓</div>}
                  <div className="layer">
                    <strong>{item}</strong>
                  </div>
                </div>
              ))}
            </div>
            <div>
              <p className="lead">
                Krishiv keeps scheduler, executor, state, shuffle, checkpoint, and connector behavior behind crate APIs. That lets the same product model run in embedded local workflows and remote clusters while keeping job ownership fenced to one active coordinator per job.
              </p>
              <Link className="btn btn-secondary" href="/architecture">
                Explore architecture
              </Link>
            </div>
          </div>
        </Section>

        <Section eyebrow="Ecosystem" title="Connectors and storage are represented by maturity, not hype.">
          <div className="ecosystem">
            {publicFacts.map((fact) => (
              <div className="card" key={fact.name}>
                <Badge tone={statusTone(fact.status)}>{fact.status}</Badge>
                <h3>{fact.name}</h3>
                <p>{fact.text}</p>
              </div>
            ))}
          </div>
        </Section>

        <section className="section card centered-cta">
          <h2>Build the next generation of data workloads with Krishiv.</h2>
          <p className="lead">Krishiv Cloud — managed compute, planned for the future.</p>
          <div className="actions">
            <Link className="btn btn-primary" href="/docs/latest">
              Read the Docs
            </Link>
            <a className="btn btn-secondary" href={githubUrl}>
              View on GitHub
            </a>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
