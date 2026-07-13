import type { Metadata } from 'next';
import Link from 'next/link';
import { SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Krishiv Platform — Coming soon',
  description:
    'The upcoming self-hosted, source-available control plane and workspace for teams building on Krishiv Engine.',
  alternates: { canonical: 'https://krishiv.ai/platform' },
};

function Arrow() {
  return <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M4 10h12m-5-5 5 5-5 5" /></svg>;
}

const plannedAreas = [
  { index: '01', title: 'SQL workspace', text: 'Query sessions, results, and history over Engine Flight SQL.' },
  { index: '02', title: 'Catalog + governance', text: 'Iceberg catalog administration, roles, grants, and audit trails.' },
  { index: '03', title: 'Pipelines + jobs', text: 'Declarative SQL pipelines and scheduled SQL, Rust, and Python work.' },
  { index: '04', title: 'One operating surface', text: 'A console, API, CLI, and governed MCP boundary for the workspace.' },
];

export default function PlatformPage() {
  return (
    <SiteShell>
      <main className="pd-page pd-platform">
        <section className="pd-hero pd-platform-hero">
          <div className="mk-wrap pd-hero-grid">
            <div className="pd-hero-copy">
              <div className="mk-eyebrow mk-eyebrow-muted"><i /> Coming soon · Not yet available</div>
              <p className="pd-overline">Krishiv Platform</p>
              <h1>The workspace around the engine.</h1>
              <p className="pd-lead">
                An upcoming self-hosted control plane for teams building on Krishiv
                Engine—bringing SQL, Iceberg, pipelines, jobs, governance, and operations
                into one place.
              </p>
              <div className="mk-actions">
                <Link className="mk-button mk-button-primary" href="/engine">Explore the Engine <Arrow /></Link>
                <Link className="mk-button mk-button-secondary" href="/docs/platform">Platform docs status</Link>
              </div>
              <p className="pd-disclaimer">No download, public preview, or availability date is being announced yet.</p>
            </div>
            <div className="pd-platform-map" role="img" aria-label="Planned relationship between Platform and Engine">
              <div className="pd-map-header"><span>Krishiv Platform</span><b>Coming soon</b></div>
              <div className="pd-map-surface"><span>Console</span><span>API</span><span>CLI</span><span>MCP</span></div>
              <div className="pd-map-capabilities"><div>SQL workspace</div><div>Catalog</div><div>Pipelines</div><div>Jobs</div><div>Governance</div><div>Operations</div></div>
              <div className="pd-map-contract"><span>Public contracts</span><i /><i /><i /></div>
              <div className="pd-map-engine"><small>Data plane</small><strong>Krishiv Engine</strong><span>Apache 2.0</span></div>
            </div>
          </div>
        </section>

        <section className="pd-section">
          <div className="mk-wrap">
            <div className="mk-section-heading">
              <div><p className="mk-kicker">Planned product surface</p><h2>One place to build, govern, and operate.</h2></div>
              <p>These areas describe the direction of the first Platform surface. They are not availability or production-readiness claims.</p>
            </div>
            <div className="pd-planned-grid">
              {plannedAreas.map((area) => <article key={area.title}><span>{area.index}</span><h3>{area.title}</h3><p>{area.text}</p></article>)}
            </div>
          </div>
        </section>

        <section className="pd-section pd-section-contrast">
          <div className="mk-wrap pd-relationship">
            <div>
              <p className="mk-kicker">A clean product boundary</p>
              <h2>Control plane above. Compute plane below.</h2>
              <p className="mk-section-copy">Platform will consume Engine through Flight SQL, coordinator APIs, and operator contracts. It will not fork Engine or depend on private internals.</p>
            </div>
            <div className="pd-relationship-cards">
              <article><small>Upcoming control plane</small><strong>Krishiv Platform</strong><p>Workspace, catalog administration, orchestration, governance, and team operations.</p></article>
              <div className="pd-contract-line"><span>Flight SQL</span><span>Coordinator API</span><span>Operator CRDs</span></div>
              <article><small>Independent data plane</small><strong>Krishiv Engine</strong><p>Planning, execution, operators, state, shuffle, checkpoints, and connectors.</p></article>
            </div>
          </div>
        </section>

        <section className="pd-section">
          <div className="mk-wrap pd-availability">
            <div><p className="mk-kicker">Availability</p><h2>Honest now. Expand when the contract is real.</h2></div>
            <div>
              <p>Platform is not available today. Its public docs contain only an availability notice and the Engine relationship; setup and API documentation will appear when a public preview exists.</p>
              <p>The planned self-hosted product is source-available. It should not be described as open source; Krishiv Engine remains Apache-2.0.</p>
              <Link className="mk-card-link" href="/docs/platform">Read the availability notice <Arrow /></Link>
            </div>
          </div>
        </section>

        <section className="mk-cta">
          <div className="mk-wrap mk-cta-inner">
            <div><p className="mk-kicker">Developer preview</p><h2>Evaluate the open compute layer first.</h2><p>The Engine stands alone and gives Platform a public boundary to build on.</p></div>
            <div className="mk-actions"><Link className="mk-button mk-button-primary" href="/engine">Explore Engine <Arrow /></Link><Link className="mk-button mk-button-secondary" href="/docs/engine/getting-started">Build from source</Link></div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
