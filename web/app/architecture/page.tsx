import Link from 'next/link';
import { Badge, Section, SiteShell } from '@/components/Shell';

const rows = [
  'SQL, Rust, Python APIs',
  'Session, catalog, and policy hooks',
  'DataFusion logical planning + Arrow RecordBatch',
  'ExecutionRuntime placement',
  'Coordinator, scheduler, metadata, leadership',
  'Executors, task runner, dataflow operators',
  'Shuffle, state, checkpoints, connectors',
];

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

        <Section title="High-level system diagram">
          <div className="diagram">
            {rows.map((row, index) => (
              <div key={row}>
                {index > 0 && <div className="flow-arrow">↓</div>}
                <div className="layer">
                  <strong>{row}</strong>
                  {index > 2 && <Badge tone="blue">runtime boundary</Badge>}
                </div>
              </div>
            ))}
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
            Batch SQL, streaming windows, and delta-oriented IVM share Arrow and DataFusion foundations. IVM is experimental: IncrementalFlow exists, but distributed executor-side IVM should be described as in progress.
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
            <Link className="btn btn-primary" href="/docs/latest/architecture">
              Read architecture docs
            </Link>
            <Link className="btn btn-secondary" href="/docs/latest/execution/distributed-mode">
              Distributed mode
            </Link>
          </div>
        </Section>
      </main>
    </SiteShell>
  );
}
