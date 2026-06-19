import Link from 'next/link';
import { Badge, Section, SiteShell } from '@/components/Shell';
import { githubUrl, publicFacts } from '@/lib/site';

const layers = [
  ['SQL · Rust · Python', 'Available'],
  ['Unified planning layer', 'Available'],
  ['DataFusion + Arrow', 'Available'],
  ['Batch · Streaming · Delta Batch / IVM', 'Experimental IVM'],
  ['Scheduler · Shuffle · Checkpointing · State', 'Preview foundations'],
  ['Iceberg · Kafka · Parquet · S3 · ADLS · Catalogs', 'Preview / gated'],
] as const;

const features = [
  [
    'Unified Batch + Streaming',
    'Batch SQL and streaming jobs share Arrow batches, DataFusion planning, runtime routing, and scheduler/executor boundaries.',
    'Available',
  ],
  [
    'Delta Batch / IVM',
    'DeltaBatch carries weighted Arrow rows and IncrementalFlow maintains views across ticks. Distributed executor-side IVM remains a separate project.',
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

export default function Home() {
  return (
    <SiteShell>
      <main className="container">
        <section className="hero">
          <div>
            <Badge tone="orange">Open source · Built in Rust</Badge>
            <h1>
              One Engine for <span className="gradient-text">Batch and Streaming</span>
            </h1>
            <p className="lead">
              Krishiv is a Rust-native compute engine for unified batch, streaming, and incremental data processing—from local development to distributed clusters.
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

          <div className="diagram" aria-label="Krishiv architecture layers">
            {layers.map(([label, status], index) => (
              <div key={label}>
                {index > 0 && <div className="flow-arrow">↓</div>}
                <div className="layer">
                  <strong>{label}</strong>
                  <Badge tone={statusTone(status)}>{status}</Badge>
                </div>
              </div>
            ))}
          </div>
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
