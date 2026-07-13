import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowIcon, InteriorHero } from '@/components/InteriorPage';
import { SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Introducing Krishiv: One Engine for Batch, Streaming, and Incremental Data Processing',
  description:
    'A codebase-grounded overview of the Krishiv unified compute engine — why it exists, what it does today, and how it runs batch SQL, streaming, and incremental view maintenance under one Apache Arrow / DataFusion runtime.',
  openGraph: {
    title: 'Introducing Krishiv — One Engine for Batch, Streaming, and Incremental Processing',
    description:
      'A codebase-grounded overview of the Krishiv unified compute engine vision.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/blog/introducing-krishiv',
  },
};

export default function Post() {
  return (
    <SiteShell>
      <main className="ip-page">
        <InteriorHero
          compact
          eyebrow="Engineering note"
          title="Introducing Krishiv: One Engine for Batch, Streaming, and Incremental Data Processing"
          beforeTitle={
            <nav className="ip-breadcrumb" aria-label="Breadcrumb">
              <Link href="/blog">Blog</Link><i aria-hidden="true" /><span aria-current="page">Introducing Krishiv</span>
            </nav>
          }
          description={
            <p>
              Why Krishiv keeps batch, streaming, and incremental computation on one
              Rust-native foundation—and where the implementation stands today.
            </p>
          }
        >
          <div className="ip-article-meta">
            <span>Krishiv Engine</span>
            <span>Source-grounded overview</span>
            <span>Developer preview</span>
          </div>
        </InteriorHero>

        <div className="mk-wrap ip-article-layout">
          <article className="ip-article-body">
            <p className="ip-article-lede">
              Data teams often split closely related work across batch jobs, streaming systems,
              and separate incremental pipelines. Krishiv is being built around a different
              shape: one compute framework with a shared data model and explicit runtime seams.
            </p>

            <section id="what-exists">
              <h2>What exists now</h2>
              <p>
                Krishiv has implemented batch SQL foundations, streaming APIs and windowing
                examples, explicit embedded, single-node, and distributed runtime modes,
                scheduler and executor crates, shuffle, state, and checkpoint abstractions, and
                Python bindings.
              </p>
              <p>
                Iceberg is the primary lakehouse target. Kafka, Parquet, S3 and object-store,
                and catalog integrations retain Preview maturity where end-to-end certification
                is still pending.
              </p>
            </section>

            <section id="foundation">
              <h2>Why Rust, Arrow, and DataFusion</h2>
              <p>
                Rust and Tokio provide the runtime foundation. Apache Arrow
                <code> RecordBatch</code> is the columnar data model and IPC shape. DataFusion
                provides SQL parsing, planning, expressions, and local execution.
              </p>
              <p>
                That lets Krishiv concentrate its Engine work on placement, dataflow, state,
                shuffle, checkpoints, and connector boundaries instead of rebuilding the entire
                query stack.
              </p>
            </section>

            <section id="workload-shapes">
              <h2>Batch, streaming, and incremental processing</h2>
              <p>
                Batch SQL is available in the source workspace. Stateful streaming remains
                Preview. <code>DeltaBatch</code> and <code>IncrementalFlow</code> provide
                Experimental incremental view maintenance based on weighted Arrow rows.
              </p>
              <div className="ip-callout">
                <strong>Maturity is not uniform</strong>
                <p>
                  Distributed execution and connector guarantees depend on the exact runtime,
                  storage, source, sink, state, and checkpoint combination being evaluated.
                </p>
              </div>
            </section>

            <section id="next-steps">
              <h2>Where to go next</h2>
              <p>
                Read the <Link href="/docs/engine">Engine docs</Link> for current APIs, the{' '}
                <Link href="/architecture">architecture page</Link> for system boundaries, and
                the <Link href="/product/maturity">maturity matrix</Link> before depending on a
                Preview or Experimental path.
              </p>
              <p>
                The public copy stays deliberately conservative: no invented benchmarks,
                competitor comparisons, availability dates, or unsupported guarantees.
              </p>
            </section>
          </article>

          <aside className="ip-article-aside" aria-label="Article navigation and context">
            <div className="ip-aside-block">
              <strong>On this page</strong>
              <nav aria-label="Article sections">
                <a href="#what-exists">What exists now</a>
                <a href="#foundation">Rust, Arrow, and DataFusion</a>
                <a href="#workload-shapes">Workload shapes</a>
                <a href="#next-steps">Where to go next</a>
              </nav>
            </div>
            <div className="ip-aside-block">
              <strong>Publishing standard</strong>
              <p>
                Capability statements are tied to repository evidence and retain their current
                maturity labels.
              </p>
            </div>
          </aside>
        </div>

        <section className="ip-cta">
          <div className="mk-wrap ip-cta-inner">
            <div>
              <h2>Evaluate the Engine from the source of truth.</h2>
              <p>Start with the source-based quickstart, then verify the paths you plan to use.</p>
            </div>
            <div className="ip-actions">
              <Link className="mk-button mk-button-primary" href="/docs/engine/getting-started">
                Get started <ArrowIcon />
              </Link>
              <Link className="mk-button mk-button-secondary" href="/docs/engine/maturity">
                Review maturity
              </Link>
            </div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
