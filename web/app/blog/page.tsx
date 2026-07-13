import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowIcon, InteriorHero, SectionIntro } from '@/components/InteriorPage';
import { SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Blog',
  description:
    'Technical notes from the Krishiv engine — engineering updates on batch SQL, streaming pipelines, and incremental view maintenance.',
  openGraph: {
    title: 'Krishiv Blog — Engine Engineering Notes',
    description:
      'Technical notes from the Krishiv engine — engineering updates on batch SQL, streaming, and incremental processing.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/blog',
  },
};

function EditorialPanel() {
  return (
    <div className="ip-panel">
      <div className="ip-panel-top">
        <span className="ip-panel-label">Editorial standard</span>
        <span className="ip-panel-badge"><i />Source grounded</span>
      </div>
      <h2 className="ip-panel-title">Claims follow the code.</h2>
      <ul className="ip-panel-list">
        <li className="ip-panel-row"><span>Capabilities</span><strong>Repository backed</strong></li>
        <li className="ip-panel-row"><span>Maturity</span><strong>Explicitly labeled</strong></li>
        <li className="ip-panel-row"><span>Scope</span><strong>Engine engineering</strong></li>
      </ul>
      <p className="ip-panel-note">
        Notes distinguish available paths from Preview, Experimental, and planned work.
      </p>
    </div>
  );
}

export default function Blog() {
  return (
    <SiteShell>
      <main className="ip-page">
        <InteriorHero
          eyebrow="Engineering journal"
          title="Technical notes from the Krishiv Engine."
          description={
            <p>
              Codebase-grounded explanations of the runtime, its workload models, and the
              boundaries that still carry Preview or Experimental labels.
            </p>
          }
          aside={<EditorialPanel />}
        />

        <section className="ip-section">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="Latest note"
              title="One runtime, three workload shapes."
              description={
                <p>
                  Start with the architecture and the current implementation—not a promise
                  about work that has not shipped.
                </p>
              }
            />
            <Link className="ip-feature-card" href="/blog/introducing-krishiv">
              <div className="ip-feature-copy">
                <span className="ip-tag ip-tag-accent">Engineering</span>
                <h3>
                  Introducing Krishiv: One Engine for Batch, Streaming, and Incremental Data
                  Processing
                </h3>
                <p>
                  A codebase-grounded overview of why Krishiv shares Arrow, DataFusion, runtime,
                  state, shuffle, and checkpoint primitives across different workload shapes.
                </p>
                <div className="ip-feature-meta">
                  <span>Krishiv Engine</span>
                  <span>Architecture overview</span>
                  <span>Developer preview</span>
                </div>
                <span className="ip-card-link">Read the note <ArrowIcon /></span>
              </div>
              <div className="ip-feature-mark" aria-hidden="true"><span>01</span></div>
            </Link>
          </div>
        </section>

        <section className="ip-cta">
          <div className="mk-wrap ip-cta-inner">
            <div>
              <h2>Prefer the implementation details?</h2>
              <p>The Engine docs track current APIs, execution modes, and maturity boundaries.</p>
            </div>
            <div className="ip-actions">
              <Link className="mk-button mk-button-primary" href="/docs/engine">
                Read the docs <ArrowIcon />
              </Link>
              <Link className="mk-button mk-button-secondary" href="/architecture">
                Explore architecture
              </Link>
            </div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
