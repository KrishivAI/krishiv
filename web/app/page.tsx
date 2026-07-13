import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowIcon } from '@/components/InteriorPage';
import { LandingCodeDemo } from '@/components/LandingCodeDemo';
import { SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

export const metadata: Metadata = {
  title: 'Krishiv — Open compute for data that moves',
  description:
    'Build with the Apache-2.0 Krishiv Engine for batch SQL and Preview stateful streaming. Krishiv Platform, an integrated self-hosted control plane, is coming soon.',
  alternates: { canonical: 'https://krishiv.ai' },
  openGraph: {
    title: 'Krishiv — Open compute for data that moves',
    description:
      'Start with an inspectable Rust-native compute engine. Add the upcoming self-hosted control plane when your team needs it.',
  },
};

const evidence = [
  { label: 'License', value: 'Apache-2.0 Engine' },
  { label: 'Runtime', value: 'Rust + Tokio' },
  { label: 'Data model', value: 'Apache Arrow' },
  { label: 'Planning', value: 'DataFusion' },
  { label: 'Current path', value: 'Source-built preview' },
];

const workloads = [
  {
    index: '01',
    status: 'Available',
    title: 'Batch SQL',
    copy: 'Plan finite SQL and DataFrame work over Arrow data, then collect RecordBatch results.',
    detail: 'Finite input · bounded result',
  },
  {
    index: '02',
    status: 'Preview',
    title: 'Stateful streaming',
    copy: 'Model event-time windows, watermarks, keyed state, checkpoints, and continuous joins.',
    detail: 'Long-lived · stateful',
  },
  {
    index: '03',
    status: 'Experimental',
    title: 'Incremental views',
    copy: 'Propagate weighted inserts and retractions through local-first maintained computations.',
    detail: 'Changing input · delta-driven',
  },
];

const docsRoutes = [
  {
    index: '01',
    title: 'Run your first query',
    copy: 'Build the CLI and execute embedded SQL from a source checkout.',
    href: '/docs/engine/getting-started',
  },
  {
    index: '02',
    title: 'Choose a placement',
    copy: 'Compare embedded, single-node, and distributed runtime boundaries.',
    href: '/docs/engine/concepts/execution-modes',
  },
  {
    index: '03',
    title: 'Configure the runtime',
    copy: 'Resolve placement, state, checkpoints, authentication, and tuning.',
    href: '/docs/engine/reference/configuration',
  },
  {
    index: '04',
    title: 'Verify feature maturity',
    copy: 'Separate available paths from Preview and Experimental surfaces.',
    href: '/docs/engine/maturity',
  },
];

const maturity = [
  { label: 'Available', value: 'Batch and local placement', tone: 'available' },
  { label: 'Preview', value: 'Streaming and distributed paths', tone: 'preview' },
  { label: 'Experimental', value: 'Incremental view maintenance', tone: 'experimental' },
  { label: 'Coming soon', value: 'Krishiv Platform', tone: 'planned' },
];

function HeroRuntime() {
  return (
    <figure className="lp-runtime">
      <figcaption className="sr-only">
        A query enters Krishiv Engine through a public API, is planned with DataFusion,
        executes over Arrow, and returns a RecordBatch result.
      </figcaption>
      <div className="lp-runtime-head">
        <div><i /><i /><i /></div>
        <span>engine / execution trace</span>
        <b><i /> source-built</b>
      </div>
      <div className="lp-runtime-inputs">
        <span className="is-active">SQL</span><span>Rust</span><span>Python</span><span>Flight</span>
      </div>
      <div className="lp-runtime-path">
        <div className="lp-runtime-node">
          <span>01 / Entry</span>
          <strong>Session + catalog</strong>
          <small>Public API boundary</small>
        </div>
        <div className="lp-runtime-connector"><i /><span>parse · bind</span></div>
        <div className="lp-runtime-node is-accent">
          <span>02 / Plan</span>
          <strong>DataFusion + Krishiv</strong>
          <small>Logical → physical plan</small>
        </div>
        <div className="lp-runtime-connector"><i /><span>execute</span></div>
        <div className="lp-runtime-node">
          <span>03 / Compute</span>
          <strong>Arrow operators</strong>
          <small>Explicit placement</small>
        </div>
      </div>
      <div className="lp-runtime-output">
        <div><span>answer</span><small>Int64</small></div>
        <strong>42</strong>
        <b>1 RecordBatch</b>
      </div>
      <div className="lp-runtime-foot">
        <span>Rust</span><i />
        <span>Arrow</span><i />
        <span>DataFusion</span><i />
        <span>Tokio</span>
      </div>
    </figure>
  );
}

function PlacementMap() {
  return (
    <ol className="lp-placement-map" role="list">
      <li>
        <div className="lp-placement-marker"><span>01</span><i /></div>
        <div className="lp-placement-card">
          <div><span>Available</span><b>In process</b></div>
          <h3>Embedded</h3>
          <p>Planner and execution run inside your Rust or Python application.</p>
          <small>No network boundary required</small>
        </div>
      </li>
      <li>
        <div className="lp-placement-marker"><span>02</span><i /></div>
        <div className="lp-placement-card">
          <div><span>Available</span><b>One host</b></div>
          <h3>Single node</h3>
          <p>Coordinator, executor, HTTP, and Flight boundaries share one machine.</p>
          <small>Service seams without a cluster</small>
        </div>
      </li>
      <li>
        <div className="lp-placement-marker is-muted"><span>03</span><i /></div>
        <div className="lp-placement-card is-preview">
          <div><span>Preview</span><b>Remote workers</b></div>
          <h3>Distributed</h3>
          <p>Work is assigned through an explicit coordinator and executor topology.</p>
          <small>Evaluation path · not an HA claim</small>
        </div>
      </li>
    </ol>
  );
}

function FoundationVisual() {
  return (
    <div className="lp-foundation" role="img" aria-label="Shared Engine foundation for batch, streaming, and incremental workloads">
      <div className="lp-foundation-label"><span>Shared execution spine</span><b>Engine</b></div>
      <div className="lp-foundation-interfaces"><span>SQL</span><span>Rust</span><span>Python</span><span>Flight SQL</span></div>
      <div className="lp-foundation-line"><i /><i /><i /></div>
      <div className="lp-foundation-core">
        <small>Open compute layer</small>
        <strong>Krishiv Engine</strong>
        <span>Developer preview</span>
      </div>
      <div className="lp-foundation-modules">
        <span>Planner</span><span>Operators</span><span>Shuffle</span><span>State</span><span>Checkpoints</span><span>Connectors</span>
      </div>
      <div className="lp-foundation-base"><span>Apache Arrow</span><i>+</i><span>DataFusion</span><i>+</i><span>Tokio</span></div>
    </div>
  );
}

export default function HomePage() {
  return (
    <SiteShell>
      <main className="lp-page">
        <section className="lp-hero">
          <div className="mk-wrap lp-hero-grid">
            <div className="lp-hero-copy">
              <p className="lp-eyebrow"><i /> Open compute / explicit control</p>
              <h1>Run data workloads on an engine <span>you can inspect.</span></h1>
              <p className="lp-hero-lead">
                Krishiv Engine brings batch SQL and Preview stateful streaming onto one
                Rust-native foundation. Start from source today; add the upcoming Platform
                control plane only when your team needs it.
              </p>
              <div className="lp-actions">
                <Link className="mk-button mk-button-primary" href="/docs/engine/getting-started">
                  Build from source <ArrowIcon />
                </Link>
                <Link className="mk-button mk-button-secondary" href="/architecture">
                  Explore architecture
                </Link>
                <a className="lp-text-link" href={githubUrl}>View on GitHub <ArrowIcon /></a>
              </div>
              <div className="lp-hero-status">
                <span><i /> Engine · Apache-2.0 developer preview</span>
                <span className="is-muted"><i /> Platform · coming soon</span>
              </div>
            </div>
            <HeroRuntime />
          </div>
        </section>

        <section className="lp-evidence" aria-label="Engine foundations">
          <div className="mk-wrap lp-evidence-grid">
            {evidence.map((item, index) => (
              <div key={item.label}>
                <span>0{index + 1} / {item.label}</span>
                <strong>{item.value}</strong>
              </div>
            ))}
          </div>
        </section>

        <section className="lp-section lp-products" id="products">
          <div className="mk-wrap">
            <div className="lp-section-head">
              <div>
                <p className="lp-kicker">01 / Product system</p>
                <h2>Open compute first. An integrated control plane later.</h2>
              </div>
              <p>
                Two products with a clean public boundary. Engine stands alone; Platform
                will consume the same interfaces available to every Engine user.
              </p>
            </div>

            <div className="lp-product-grid">
              <article className="lp-product-card lp-engine-card">
                <div className="lp-product-top">
                  <span>Krishiv Engine</span>
                  <b><i /> Developer preview</b>
                </div>
                <div className="lp-product-title">
                  <p>Apache-2.0 compute layer</p>
                  <h3>Own the runtime boundary.</h3>
                  <p>Plan, execute, move, and maintain data through public Engine contracts.</p>
                </div>
                <div className="lp-engine-surface">
                  <div><span>Batch SQL</span><b>Available</b></div>
                  <div><span>Stateful streaming</span><b>Preview</b></div>
                  <div><span>Incremental views</span><b>Experimental</b></div>
                  <div><span>Embedded</span><b>Available</b></div>
                  <div><span>Single node</span><b>Available</b></div>
                  <div><span>Distributed</span><b>Preview</b></div>
                </div>
                <div className="lp-product-actions">
                  <Link href="/engine">Explore Engine <ArrowIcon /></Link>
                  <Link href="/docs/engine">Read Engine docs</Link>
                </div>
              </article>

              <article className="lp-product-card lp-platform-card">
                <div className="lp-product-top">
                  <span>Krishiv Platform</span>
                  <b className="is-muted"><i /> Coming soon</b>
                </div>
                <div className="lp-product-title">
                  <p>Self-hosted control plane</p>
                  <h3>Bring the team around the Engine.</h3>
                  <p>A planned workspace for SQL, catalog administration, jobs, governance, and operations.</p>
                </div>
                <ul className="lp-platform-list">
                  <li><span>Workspace</span><b>Console · API · CLI · MCP</b></li>
                  <li><span>Data</span><b>SQL · catalog · pipelines</b></li>
                  <li><span>Operations</span><b>Jobs · governance · audit</b></li>
                </ul>
                <p className="lp-platform-note">No download, public preview, or availability date is being announced.</p>
                <Link className="lp-product-link" href="/platform">See the product direction <ArrowIcon /></Link>
              </article>
            </div>
          </div>
        </section>

        <section className="lp-section lp-placement" id="placement">
          <div className="mk-wrap">
            <div className="lp-section-head">
              <div>
                <p className="lp-kicker">02 / Placement</p>
                <h2>Change the topology, not the front door.</h2>
              </div>
              <p>
                Start with an in-process session. Exercise service boundaries on one host.
                Move remote only when the Preview distributed path fits the evaluation.
              </p>
            </div>
            <PlacementMap />
            <div className="lp-placement-foot">
              <span>Same public session model</span>
              <div><i /><i /><i /></div>
              <Link href="/docs/engine/concepts/execution-modes">Compare execution modes <ArrowIcon /></Link>
            </div>
          </div>
        </section>

        <section className="lp-section lp-workloads" id="workloads">
          <div className="mk-wrap">
            <div className="lp-section-head">
              <div>
                <p className="lp-kicker">03 / Workload model</p>
                <h2>Three shapes. One execution spine.</h2>
              </div>
              <p>
                Workloads share Arrow data, planning, runtime, state, and connector seams
                while retaining explicit maturity boundaries.
              </p>
            </div>
            <div className="lp-workload-layout">
              <div className="lp-workload-list">
                {workloads.map((workload) => (
                  <article key={workload.title}>
                    <div><span>{workload.index}</span><b>{workload.status}</b></div>
                    <h3>{workload.title}</h3>
                    <p>{workload.copy}</p>
                    <small>{workload.detail}</small>
                  </article>
                ))}
              </div>
              <FoundationVisual />
            </div>
          </div>
        </section>

        <section className="lp-section lp-developer" id="developer">
          <div className="mk-wrap">
            <div className="lp-section-head">
              <div>
                <p className="lp-kicker">04 / Developer proof</p>
                <h2>A real query, through documented APIs.</h2>
              </div>
              <p>
                These examples use the current source-built CLI and public Rust and Python
                facades. No fictional package command or hosted endpoint is implied.
              </p>
            </div>
            <LandingCodeDemo />

            <div className="lp-docs-head">
              <div><span>Continue in the docs</span><p>Choose the contract you need next.</p></div>
              <Link href="/docs/engine">View all Engine docs <ArrowIcon /></Link>
            </div>
            <div className="lp-docs-grid">
              {docsRoutes.map((route) => (
                <Link href={route.href} key={route.title}>
                  <span>{route.index}</span>
                  <h3>{route.title}</h3>
                  <p>{route.copy}</p>
                  <b>Open guide <ArrowIcon /></b>
                </Link>
              ))}
            </div>
          </div>
        </section>

        <section className="lp-section lp-maturity" id="maturity">
          <div className="mk-wrap lp-maturity-layout">
            <div className="lp-maturity-copy">
              <p className="lp-kicker">05 / Evidence over claims</p>
              <h2>Maturity stays visible.</h2>
              <p>
                A capability can exist without being a stable operating contract. Every
                public surface keeps its current status attached.
              </p>
              <Link className="mk-button mk-button-secondary" href="/product/maturity">Review the full maturity map <ArrowIcon /></Link>
            </div>
            <div className="lp-maturity-grid">
              {maturity.map((item, index) => (
                <div className={`is-${item.tone}`} key={item.label}>
                  <span>0{index + 1}</span>
                  <i />
                  <strong>{item.label}</strong>
                  <p>{item.value}</p>
                </div>
              ))}
            </div>
          </div>
        </section>

        <section className="lp-final">
          <div className="mk-wrap lp-final-inner">
            <div>
              <p className="lp-kicker">Start with what exists</p>
              <h2>Build the Engine from source.</h2>
              <p>Pin a revision, run an embedded query, and expand placement only when the workload requires it.</p>
            </div>
            <div className="lp-actions">
              <Link className="mk-button mk-button-primary" href="/docs/engine/getting-started">Run the quickstart <ArrowIcon /></Link>
              <a className="mk-button mk-button-secondary" href={githubUrl}>Inspect the repository</a>
            </div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
