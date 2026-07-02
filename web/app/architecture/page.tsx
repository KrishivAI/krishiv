import type { Metadata } from 'next';
import Link from 'next/link';
import { Badge, Section, SiteShell } from '@/components/Shell';
import {
  TopologyDiagram,
  RequestFlowDiagram,
  DataPlaneDiagram,
  LifecycleDiagram,
} from '@/components/ArchitectureDiagrams';

export const metadata: Metadata = {
  title: 'Architecture',
  description:
    'Krishiv architecture: one Rust-native compute engine running as embedded library, single-node daemon, or distributed cluster with coordinator and executors.',
  openGraph: {
    title: 'Krishiv Architecture — One Engine, Three Shapes',
    description:
      'How Krishiv runs the same batch SQL, streaming, and incremental processing code in embedded, single-node, and distributed modes.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/architecture',
  },
};

export default function Architecture() {
  return (
    <SiteShell>
      <main className="container">
        <section className="page-hero">
          <Badge tone="blue">Architecture</Badge>
          <h1 className="gradient-text">One engine, three shapes, the same APIs.</h1>
          <p className="lead">
            Krishiv runs the same SQL, Python, and Rust APIs in three forms: as a library
            inside your process, as a daemon on a single host, and as a coordinator-plus-executors
            cluster. The same plan flows through all three. The same code that runs locally
            runs at scale.
          </p>
          <p className="lead" style={{ color: 'var(--muted)', fontSize: 15 }}>
            This page is the mental model. For crate boundaries and design invariants, see
            the <a href="/docs/latest/concepts/architecture">architecture reference</a> in
            the docs.
          </p>
        </section>

        <Section
          eyebrow="The three shapes"
          title="Run Krishiv the way that fits the workload"
        >
          <p style={{ color: 'var(--muted-strong)', maxWidth: 720 }}>
            You pick the shape at startup. There is no silent fall-through from local to
            distributed, and no surprise network calls when you only want a library.
            The same code paths run in all three.
          </p>
          <div className="diagram">
            <TopologyDiagram />
          </div>
          <div className="grid" style={{ marginTop: 20 }}>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Embedded</h3>
              <p>
                <code>Session.embedded()</code> runs Krishiv in your process. No daemon,
                no cluster. Ideal for notebooks, scripts, tests, and libraries that need
                SQL or DataFrame ops inline. Results are returned as in-memory Arrow buffers.
              </p>
            </div>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Single-node</h3>
              <p>
                A local Krishiv daemon owns durable state, checkpoints, and one or more
                task slots. Use it when you want a long-running pipeline with restarts but
                do not need to scale out across hosts. Connects over Arrow Flight.
              </p>
            </div>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Distributed</h3>
              <p>
                A coordinator schedules jobs across <em>N</em> executors. Each executor is
                a replaceable worker; the coordinator is the single source of truth for
                job state. Shuffle, state, and checkpoints live on a shared object store.
              </p>
            </div>
          </div>
        </Section>

        <Section
          eyebrow="What happens when you press run"
          title="A query, from your code to a result"
        >
          <p style={{ color: 'var(--muted-strong)', maxWidth: 720 }}>
            The same flow runs whether you are calling from a Jupyter notebook or a
            coordinator handling a thousand tasks. Stages you can ignore most of the
            time, and the stages you can hook into when you need to.
          </p>
          <div className="diagram">
            <RequestFlowDiagram />
          </div>
          <ol className="prose" style={{ color: 'var(--muted-strong)' }}>
            <li>
              <strong style={{ color: 'var(--text)' }}>Parse and bind.</strong> Your SQL or
              DataFrame call enters a session. The session resolves table and UDF names
              against its catalog and binds the expression tree to types.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Plan and optimize.</strong> The logical
              plan is rewritten with cost-based rules, then fragmented into a physical plan
              that can be split across executors when running distributed.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Execute.</strong> The plan is run as a
              graph of Arrow operators. State, shuffle, and checkpoint hooks are wired in
              here. Streaming pipelines add windows, watermarks, and timers.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Return.</strong> Batch results come
              back as one or more <code>RecordBatch</code>es. Streaming results come as a
              <code> RecordBatchStream</code> you iterate.
            </li>
          </ol>
        </Section>

        <Section
          eyebrow="How a cluster is laid out"
          title="Control plane above, data plane below"
        >
          <p style={{ color: 'var(--muted-strong)', maxWidth: 720 }}>
            The coordinator is the only component that owns job state. Executors are
            replaceable workers — losing one means restarting the tasks it was running,
            not losing the job. State and checkpoints are pulled out of the workers
            into a shared store so executors can be added or removed freely.
          </p>
          <div className="diagram">
            <DataPlaneDiagram />
          </div>
          <div className="split" style={{ marginTop: 20 }}>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Coordinator</h3>
              <p>
                Owns the job catalog, schedules tasks, holds leadership for exactly one
                active coordinator per job, and applies committed state changes from
                executors. It does not run data.
              </p>
            </div>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Executors</h3>
              <p>
                Run tasks, hold local state, and report progress. Each executor registers
                with the coordinator and runs whatever tasks it is offered. They do not
                talk to each other directly — all communication routes through the
                coordinator or shared shuffle.
              </p>
            </div>
          </div>
        </Section>

        <Section
          eyebrow="From submit to result"
          title="The pipeline lifecycle"
        >
          <p style={{ color: 'var(--muted-strong)', maxWidth: 720 }}>
            Whether you submit a SQL batch query or a streaming pipeline, the lifecycle
            is the same five steps. The differences (continuous vs one-shot, recovery
            vs restart) show up in the last two stages.
          </p>
          <div className="diagram">
            <LifecycleDiagram />
          </div>
          <ul className="prose" style={{ color: 'var(--muted-strong)' }}>
            <li>
              <strong style={{ color: 'var(--text)' }}>Validate</strong> catches missing
              columns, type mismatches, and unresolved UDFs at submit time — before any
              executor is asked to do work.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Plan</strong> builds a fragment graph
              the coordinator can split across executors, with cost estimates used to
              place joins and aggregations.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Run</strong> streams or batches data
              through the plan. On failure, executors restart from the last committed
              checkpoint — they do not re-run from the source.
            </li>
          </ul>
        </Section>

        <Section
          eyebrow="Where to read more"
          title="Honest boundaries"
        >
          <p style={{ color: 'var(--muted-strong)', maxWidth: 720 }}>
            The architecture is one engine, but its maturity is not uniform.
            <a href="/product/maturity"> Feature maturity</a> is the source of truth for
            what is ready to depend on. The short version:
          </p>
          <ul className="prose" style={{ color: 'var(--muted-strong)' }}>
            <li>
              <strong style={{ color: 'var(--text)' }}>Available:</strong> in-process batch
              and streaming SQL, the Python and Rust APIs, single-node deployment, the
              Iceberg and Parquet connectors.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Preview:</strong> distributed
              execution and end-to-end pipeline exactly-once. The shape is right; the
              tuning and certification are still in progress.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Experimental:</strong> incremental
              view maintenance and the IncrementalFlow API.
            </li>
          </ul>
          <div className="actions">
            <Link className="btn btn-primary" href="/docs/latest/concepts/architecture">
              Architecture reference
            </Link>
            <Link className="btn btn-secondary" href="/docs/latest/concepts/distributed-mode">
              Distributed mode
            </Link>
            <Link className="btn btn-secondary" href="/product/maturity">
              Feature maturity
            </Link>
          </div>
        </Section>
      </main>
    </SiteShell>
  );
}
