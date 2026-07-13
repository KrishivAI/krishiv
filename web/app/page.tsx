import type { Metadata } from 'next';
import Link from 'next/link';
import type { ReactNode } from 'react';
import { SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

export const metadata: Metadata = {
  title: 'Krishiv — Engine developer preview. Platform coming soon.',
  description:
    'Krishiv Engine is an Apache-2.0 Rust compute framework with available batch SQL, Preview streaming, and Experimental incremental processing. Platform is coming soon.',
  alternates: { canonical: 'https://krishiv.ai' },
  openGraph: {
    title: 'Krishiv — Engine developer preview. Platform coming soon.',
    description:
      'Start with the Apache-2.0 Krishiv Engine. Grow into the upcoming self-hosted Krishiv Platform.',
  },
};

function Arrow({ diagonal = false }: { diagonal?: boolean }) {
  return (
    <svg viewBox="0 0 20 20" aria-hidden="true">
      {diagonal ? <path d="M5 15 15 5m-7 0h7v7" /> : <path d="M4 10h12m-5-5 5 5-5 5" />}
    </svg>
  );
}

function Github() {
  return (
    <svg viewBox="0 0 20 20" aria-hidden="true" fill="currentColor">
      <path d="M10 .9a9.1 9.1 0 0 0-2.9 17.7c.46.08.63-.2.63-.44v-1.6c-2.57.56-3.11-1.1-3.11-1.1-.42-1.07-1.03-1.35-1.03-1.35-.84-.58.06-.57.06-.57.93.07 1.42.96 1.42.96.83 1.41 2.18 1 2.71.77.08-.6.32-1 .59-1.23-2.05-.23-4.2-1.02-4.2-4.55 0-1 .36-1.83.95-2.47-.1-.24-.41-1.18.09-2.44 0 0 .78-.25 2.5.94A8.7 8.7 0 0 1 10 5.2c.77 0 1.54.1 2.27.3 1.72-1.19 2.5-.94 2.5-.94.5 1.26.19 2.2.1 2.44.59.64.94 1.46.94 2.47 0 3.54-2.16 4.31-4.21 4.54.33.29.63.85.63 1.72v2.55c0 .25.17.53.64.44A9.1 9.1 0 0 0 10 .9Z" />
    </svg>
  );
}

function StatusPill({ children, muted = false }: { children: ReactNode; muted?: boolean }) {
  return <span className={`mk-status${muted ? ' mk-status-muted' : ''}`}><i />{children}</span>;
}

function SystemMap() {
  return (
    <div className="mk-system" role="img" aria-label="Krishiv product architecture">
      <div className="mk-system-topline">
        <span>One data system</span>
        <span className="mk-live"><i /> Engine preview</span>
      </div>
      <div className="mk-inputs" aria-label="Engine interfaces">
        <span>SQL</span><span>Rust</span><span>Python</span><span>Flight</span>
      </div>
      <div className="mk-flow-line" aria-hidden="true"><i /><i /><i /></div>
      <div className="mk-engine-node">
        <div>
          <small>Apache-2.0 compute layer</small>
          <strong>Krishiv Engine</strong>
        </div>
        <span>Developer preview</span>
      </div>
      <div className="mk-primitives">
        <span>Batch SQL</span>
        <span>Streaming</span>
        <span>Incremental</span>
      </div>
      <div className="mk-foundation">
        <span>Arrow</span><b>+</b><span>DataFusion</span><b>+</b><span>Tokio</span>
      </div>
      <div className="mk-platform-node">
        <div>
          <small>Workspace + control plane</small>
          <strong>Krishiv Platform</strong>
        </div>
        <span>Coming soon</span>
      </div>
    </div>
  );
}

const workloadItems = [
  {
    number: '01',
    title: 'Batch',
    text: 'Plan DataFusion SQL over Arrow data in process or on one host, with an explicit distributed Preview path.',
    detail: 'Finite input · Arrow results',
  },
  {
    number: '02',
    title: 'Streaming',
    text: 'Build event-time pipelines with windows, watermarks, stateful operators, and checkpoints.',
    detail: 'Preview · Stateful',
  },
  {
    number: '03',
    title: 'Incremental',
    text: 'Maintain changing results with weighted inserts and retractions instead of recomputing everything.',
    detail: 'Experimental · Delta-driven',
  },
];

export default function HomePage() {
  return (
    <SiteShell>
      <main className="mk-page">
        <section className="mk-hero">
          <div className="mk-wrap mk-hero-grid">
            <div className="mk-hero-copy">
              <div className="mk-eyebrow"><i /> Rust-native data infrastructure</div>
              <h1>One foundation for data that moves.</h1>
              <p>
                Krishiv Engine brings batch, streaming, and incremental compute into one
                Rust-native runtime. Krishiv Platform will add the workspace around it.
              </p>
              <div className="mk-actions">
                <Link className="mk-button mk-button-primary" href="/engine">
                  Explore Engine <Arrow />
                </Link>
                <Link className="mk-button mk-button-secondary" href="/docs/engine">
                  Read the docs
                </Link>
                <a className="mk-text-link" href={githubUrl}>
                  <Github /> GitHub
                </a>
              </div>
              <div className="mk-hero-note">
                <StatusPill>Engine: open-source preview</StatusPill>
                <StatusPill muted>Platform: coming soon</StatusPill>
              </div>
            </div>
            <SystemMap />
          </div>
        </section>

        <section className="mk-products" id="products">
          <div className="mk-wrap">
            <div className="mk-section-heading">
              <div>
                <p className="mk-kicker">The product family</p>
                <h2>Start with compute. Add the control plane when you need it.</h2>
              </div>
              <p>
                Engine stays useful on its own. Platform is a separate product built on
                public Engine interfaces—not a requirement or a fork.
              </p>
            </div>
            <div className="mk-product-grid">
              <article className="mk-product-card mk-product-card-engine">
                <div className="mk-product-card-top">
                  <span className="mk-product-index">01 / Engine</span>
                  <StatusPill>Open-source preview</StatusPill>
                </div>
                <h3>Own the compute layer.</h3>
                <p>
                  An Apache-2.0 Rust engine for teams that want one runtime boundary
                  across batch SQL, streaming pipelines, and experimental incremental views.
                </p>
                <ul className="mk-check-list">
                  <li>Apache Arrow data model</li>
                  <li>DataFusion SQL planning</li>
                  <li>Embedded and single-node paths</li>
                  <li>Rust, Python, CLI, and service interfaces</li>
                </ul>
                <Link className="mk-card-link" href="/engine">Explore Krishiv Engine <Arrow diagonal /></Link>
              </article>
              <article className="mk-product-card mk-product-card-platform">
                <div className="mk-product-card-top">
                  <span className="mk-product-index">02 / Platform</span>
                  <StatusPill muted>Coming soon</StatusPill>
                </div>
                <h3>Bring the team around it.</h3>
                <p>
                  An upcoming self-hosted, source-available workspace for SQL,
                  Iceberg, pipelines, jobs, governance, operations, and governed MCP.
                </p>
                <ul className="mk-check-list">
                  <li>Integrated SQL workspace</li>
                  <li>Catalog and governance</li>
                  <li>Pipeline and job orchestration</li>
                  <li>One console, API, CLI, and MCP boundary</li>
                </ul>
                <Link className="mk-card-link" href="/platform">Preview the direction <Arrow diagonal /></Link>
              </article>
            </div>
          </div>
        </section>

        <section className="mk-workloads">
          <div className="mk-wrap">
            <div className="mk-section-heading mk-section-heading-compact">
              <div>
                <p className="mk-kicker">One execution spine</p>
                <h2>Different workload shapes. Shared primitives.</h2>
              </div>
            </div>
            <div className="mk-workload-list">
              {workloadItems.map((item) => (
                <article key={item.title} className="mk-workload-row">
                  <span>{item.number}</span>
                  <h3>{item.title}</h3>
                  <p>{item.text}</p>
                  <small>{item.detail}</small>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section className="mk-continuity">
          <div className="mk-wrap mk-continuity-grid">
            <div>
              <p className="mk-kicker">Local-to-remote continuity</p>
              <h2>Change placement, not your mental model.</h2>
              <p className="mk-section-copy">
                Start in process. Exercise service boundaries on one node. Move to an
                explicit remote coordinator when the distributed path fits your evaluation.
              </p>
              <Link className="mk-button mk-button-secondary" href="/docs/engine/concepts/execution-modes">
                Compare execution modes <Arrow />
              </Link>
            </div>
            <ol className="mk-mode-list">
              <li><span>01</span><div><strong>Embedded</strong><p>Planner and execution live inside your process.</p></div><b>Available</b></li>
              <li><span>02</span><div><strong>Single node</strong><p>Coordinator, executor, and services on one host.</p></div><b>Available</b></li>
              <li><span>03</span><div><strong>Distributed</strong><p>Explicit remote coordination and workers.</p></div><b>Preview</b></li>
            </ol>
          </div>
        </section>

        <section className="mk-cta">
          <div className="mk-wrap mk-cta-inner">
            <div>
              <p className="mk-kicker">Build from the source of truth</p>
              <h2>Evaluate the Engine with honest maturity labels.</h2>
              <p>Every public doc distinguishes available paths, previews, experiments, and work still in progress.</p>
            </div>
            <div className="mk-actions">
              <Link className="mk-button mk-button-primary" href="/docs/engine/getting-started">Get started <Arrow /></Link>
              <Link className="mk-button mk-button-secondary" href="/docs/engine/maturity">Feature maturity</Link>
            </div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
