import type { Metadata } from 'next';
import Link from 'next/link';
import { SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

export const metadata: Metadata = {
  title: 'Krishiv Engine',
  description:
    'Apache-2.0, Rust-native compute for batch SQL and Preview stateful streaming, with Experimental incremental view maintenance.',
  alternates: { canonical: 'https://krishiv.ai/engine' },
};

function Arrow() {
  return <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M4 10h12m-5-5 5 5-5 5" /></svg>;
}

const workloads = [
  {
    index: '01',
    title: 'Batch SQL',
    status: 'Available',
    copy: 'Run DataFusion-backed SQL and DataFrame plans over registered Arrow and Parquet-style sources.',
    tags: ['Finite inputs', 'Arrow results', 'SQL + DataFrame'],
  },
  {
    index: '02',
    title: 'Stateful streaming',
    status: 'Preview',
    copy: 'Build event-time windows, watermarks, stateful operators, checkpoints, and continuous joins.',
    tags: ['Event time', 'Keyed state', 'Long-lived jobs'],
  },
  {
    index: '03',
    title: 'Incremental views',
    status: 'Experimental',
    copy: 'Propagate weighted inserts and retractions to maintain changing results, with visible fallback paths.',
    tags: ['DeltaBatch', 'IncrementalFlow', 'Local-first'],
  },
];

export default function EnginePage() {
  return (
    <SiteShell>
      <main className="pd-page pd-engine">
        <section className="pd-hero">
          <div className="mk-wrap pd-hero-grid">
            <div className="pd-hero-copy">
              <div className="mk-eyebrow"><i /> Developer preview · Apache 2.0</div>
              <p className="pd-overline">Krishiv Engine</p>
              <h1>Compute for data that doesn’t stand still.</h1>
              <p className="pd-lead">
                A Rust-native compute framework for batch SQL and stateful streaming,
                with experimental incremental view maintenance on one Arrow data model.
              </p>
              <div className="mk-actions">
                <Link className="mk-button mk-button-primary" href="/docs/engine/getting-started">Build from source <Arrow /></Link>
                <Link className="mk-button mk-button-secondary" href="/docs/engine">Read the docs</Link>
                <a className="mk-text-link" href={githubUrl}>View on GitHub</a>
              </div>
              <p className="pd-disclaimer">Pre-release and not for production use. Pin a commit when evaluating.</p>
            </div>
            <div className="pd-code-card" aria-label="Krishiv Python query example">
              <div className="pd-code-top"><span>query.py</span><span>Python API</span></div>
              <pre><code><span className="code-blue">import</span> krishiv <span className="code-blue">as</span> ks{`\n\n`}session = ks.Session(){`\n`}result = session.sql({`\n`}  <span className="code-green">&quot;SELECT 42 AS answer&quot;</span>{`\n`}).collect(){`\n\n`}print(result.pretty())</code></pre>
              <div className="pd-runtime-strip"><span>SQL</span><i /><span>DataFusion plan</span><i /><span>Arrow batches</span></div>
            </div>
          </div>
        </section>

        <section className="pd-section">
          <div className="mk-wrap">
            <div className="mk-section-heading">
              <div><p className="mk-kicker">Workload model</p><h2>Three shapes. One execution spine.</h2></div>
              <p>Shared primitives reduce the seams between finite queries, long-lived pipelines, and maintained results.</p>
            </div>
            <div className="pd-workload-grid">
              {workloads.map((item) => (
                <article key={item.title} className="pd-workload-card">
                  <div><span>{item.index}</span><b>{item.status}</b></div>
                  <h3>{item.title}</h3>
                  <p>{item.copy}</p>
                  <ul>{item.tags.map((tag) => <li key={tag}>{tag}</li>)}</ul>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section className="pd-section pd-section-contrast">
          <div className="mk-wrap pd-architecture-grid">
            <div>
              <p className="mk-kicker">Architecture</p>
              <h2>Move the work without changing the front door.</h2>
              <p className="mk-section-copy">
                SQL, Rust, Python, Flight SQL, and MCP enter through public session and
                runtime seams. Placement remains explicit at every topology.
              </p>
              <Link className="mk-card-link" href="/docs/engine/concepts/architecture">Read the architecture <Arrow /></Link>
            </div>
            <div className="pd-stack">
              <div className="pd-stack-row pd-stack-interfaces"><span>SQL</span><span>Rust</span><span>Python</span><span>Flight SQL</span><span>MCP</span></div>
              <div className="pd-stack-arrow">↓</div>
              <div className="pd-stack-row pd-stack-core"><strong>Session + catalog</strong><span>DataFusion + Krishiv plans</span></div>
              <div className="pd-stack-arrow">↓</div>
              <div className="pd-stack-row pd-stack-modes"><span>Embedded</span><span>Single node</span><span>Distributed <small>preview</small></span></div>
              <div className="pd-stack-arrow">↓</div>
              <div className="pd-stack-row pd-stack-services"><span>Operators</span><span>Shuffle</span><span>State</span><span>Connectors</span></div>
            </div>
          </div>
        </section>

        <section className="pd-section">
          <div className="mk-wrap pd-split">
            <div>
              <p className="mk-kicker">Built for ownership</p>
              <h2>The engine is a product on its own.</h2>
              <p className="mk-section-copy">
                Run it inside an application, on one host, or against an explicit remote
                coordinator. Platform is not required and never gets a private engine path.
              </p>
            </div>
            <div className="pd-boundary-grid">
              <article>
                <span>Engine owns</span>
                <ul>
                  <li>Planning and execution</li>
                  <li>Streaming operators and state</li>
                  <li>Shuffle and checkpoint primitives</li>
                  <li>Connector contracts and data movement</li>
                </ul>
              </article>
              <article>
                <span>Engine does not own</span>
                <ul>
                  <li>Cross-job workflow orchestration</li>
                  <li>Managed warehouses or billing</li>
                  <li>Enterprise catalog administration</li>
                  <li>Notebooks or model serving</li>
                </ul>
              </article>
            </div>
          </div>
        </section>

        <section className="mk-cta">
          <div className="mk-wrap mk-cta-inner">
            <div><p className="mk-kicker">Developer preview</p><h2>Start with a verified, source-based quickstart.</h2><p>No fictional package commands and no hidden production claims.</p></div>
            <div className="mk-actions"><Link className="mk-button mk-button-primary" href="/docs/engine/getting-started">Run a query <Arrow /></Link><Link className="mk-button mk-button-secondary" href="/docs/engine/maturity">Review maturity</Link></div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
