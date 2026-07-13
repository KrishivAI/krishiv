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
  title: 'Engine Architecture',
  description:
    'How Krishiv Engine is organized across available embedded and single-node placements and a Preview distributed path.',
  openGraph: {
    title: 'Krishiv Engine Architecture',
    description:
      'The Rust, Arrow, and DataFusion foundations behind Krishiv Engine execution modes.',
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
          <h1 className="gradient-text">Engine boundaries, from one process to a preview cluster.</h1>
          <p className="lead">
            Krishiv Engine has explicit embedded and single-node placements. The repository
            also contains coordinator, scheduler, executor, and transport foundations for a
            distributed placement, which remains Preview. Each path builds on Rust, Arrow,
            and DataFusion, but their operational maturity is not the same.
          </p>
          <p className="lead" style={{ color: 'var(--muted)', fontSize: 15 }}>
            This page is the mental model. For crate boundaries and design invariants, see
            the <a href="/docs/engine/concepts/architecture">architecture reference</a> in
            the docs.
          </p>
        </section>

        <Section
          eyebrow="The three shapes"
          title="Choose a placement, then check its maturity"
        >
          <p style={{ color: 'var(--muted-strong)', maxWidth: 720 }}>
            Execution mode is an explicit configuration choice. Embedded and single-node
            operation are available in the source workspace; distributed operation is a
            Preview path that still needs end-to-end operational certification.
          </p>
          <div className="diagram">
            <TopologyDiagram />
          </div>
          <div className="grid" style={{ marginTop: 20 }}>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Embedded</h3>
              <p>
                Runs Engine in your process without a remote coordinator. It is the clearest
                starting point for local SQL, DataFrame work, tests, and API evaluation.
                Batch results use Arrow <code>RecordBatch</code> values.
              </p>
            </div>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Single-node</h3>
              <p>
                Places the Engine control and data-plane components on one host. State,
                checkpoint, and restart behavior depend on the configured backends and
                durability profile; they are not implied by the placement alone.
              </p>
            </div>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Distributed</h3>
              <p>
                Coordinator and executor code establishes a remote execution path across
                workers. This mode is Preview: use it to evaluate the architecture, not as
                a promise of high availability, elastic scale, or production readiness.
              </p>
            </div>
          </div>
        </Section>

        <Section
          eyebrow="What happens when you press run"
          title="A query, from your code to a result"
        >
          <p style={{ color: 'var(--muted-strong)', maxWidth: 720 }}>
            A batch request enters a session, is planned through DataFusion, and runs over
            Arrow data. Streaming and distributed paths extend that foundation, but remain
            subject to their own Preview maturity and connector constraints.
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
              plan is rewritten and lowered to physical execution. Preview distributed
              paths add runtime placement and task boundaries.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Execute.</strong> The plan is run as a
              graph of Arrow operators. State, shuffle, and checkpoint abstractions connect
              here; streaming paths add their own stateful operators.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Return.</strong> Batch results come
              back as one or more <code>RecordBatch</code> values. Streaming APIs expose
              incremental results through their job and stream interfaces.
            </li>
          </ol>
        </Section>

        <Section
          eyebrow="How a cluster is laid out"
          title="Control plane above, data plane below"
        >
          <p style={{ color: 'var(--muted-strong)', maxWidth: 720 }}>
            The Preview distributed design separates coordination from execution. The
            repository includes scheduling, metadata, shuffle, state, checkpoint, and
            transport abstractions. Recovery and durability depend on the exact configured
            stores and connector combination; this diagram is a boundary map, not an SLA.
          </p>
          <div className="diagram">
            <DataPlaneDiagram />
          </div>
          <div className="split" style={{ marginTop: 20 }}>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Coordinator</h3>
              <p>
                Scheduler and coordinator modules expose job/task lifecycle, metadata,
                leadership, and remote-control paths. Their presence does not by itself
                imply a certified highly available control plane.
              </p>
            </div>
            <div className="card">
              <h3 style={{ color: 'var(--text)', margin: '0 0 8px' }}>Executors</h3>
              <p>
                Executor modules run assigned work and connect to state, shuffle, and
                checkpoint interfaces. Transport and failure-recovery behavior remain part
                of the distributed Preview surface.
              </p>
            </div>
          </div>
        </Section>

        <Section
          eyebrow="From submit to result"
          title="The pipeline lifecycle"
        >
          <p style={{ color: 'var(--muted-strong)', maxWidth: 720 }}>
            The repository represents validation, planning, scheduling, execution, and
            completion as distinct concerns. Batch and streaming do not have identical
            recovery semantics, and connector guarantees must be evaluated end to end.
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
              through the plan. On failure, behavior depends on the selected execution
              mode, state backend, checkpoint store, source, and sink.
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
              <strong style={{ color: 'var(--text)' }}>Available:</strong> batch SQL,
              DataFusion planning, Arrow data, embedded and single-node execution, and
              source-built Rust and core Python APIs.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Preview:</strong> stateful streaming,
              distributed execution, checkpoint/state integrations, and Kafka, Parquet,
              S3, and Iceberg paths. Guarantees are combination-specific.
            </li>
            <li>
              <strong style={{ color: 'var(--text)' }}>Experimental:</strong> incremental
              view maintenance and the IncrementalFlow API.
            </li>
          </ul>
          <div className="actions">
            <Link className="btn btn-primary" href="/docs/engine/concepts/architecture">
              Architecture reference
            </Link>
            <Link className="btn btn-secondary" href="/docs/engine/operations/distributed">
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
