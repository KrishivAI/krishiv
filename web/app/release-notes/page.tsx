import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowIcon, InteriorHero, SectionIntro } from '@/components/InteriorPage';
import { SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

export const metadata: Metadata = {
  title: 'Development Releases',
  description: 'Tagged Krishiv Engine development releases and release candidates.',
  openGraph: {
    title: 'Krishiv Engine Development Releases',
    description: 'Tagged development releases and release candidates for Krishiv Engine.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/release-notes',
  },
};

function ReleasePolicy() {
  return (
    <div className="ip-panel">
      <div className="ip-panel-top">
        <span className="ip-panel-label">Release policy</span>
        <span className="ip-panel-badge"><i />Pre-release</span>
      </div>
      <h2 className="ip-panel-title">Tags, without inflated claims.</h2>
      <ul className="ip-panel-list">
        <li className="ip-panel-row"><span>Stable releases listed</span><strong>None</strong></li>
        <li className="ip-panel-row"><span>Tagged candidates</span><strong>v0.1.0-rc.1</strong></li>
        <li className="ip-panel-row"><span>Maturity source</span><strong>Current docs</strong></li>
      </ul>
      <p className="ip-panel-note">
        A release candidate is not a stable release or a production-readiness signal.
      </p>
    </div>
  );
}

export default function Releases() {
  return (
    <SiteShell>
      <main className="ip-page">
        <InteriorHero
          eyebrow="Engine releases"
          title="Development releases, clearly labeled."
          description={
            <p>
              This index lists tags that exist in the Engine repository and preserves their
              pre-release status. Current maturity documentation remains the evaluation source.
            </p>
          }
          aside={<ReleasePolicy />}
        />

        <section className="ip-section">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="Tagged artifacts"
              title="One repository tag is listed."
              description={
                <p>
                  The notice records what the tag is without constructing a changelog from the
                  current development branch.
                </p>
              }
            />
            <ol className="ip-rows" role="list">
              <li className="ip-row">
                <span>01</span>
                <h3>v0.1.0-rc.1</h3>
                <p>Release candidate · pre-release tag · no production-readiness claim.</p>
                <Link href="/release-notes/v0.1.0-rc.1">Read the notice</Link>
              </li>
            </ol>
          </div>
        </section>

        <section className="ip-section ip-section-contrast">
          <div className="mk-wrap">
            <SectionIntro
              compact
              eyebrow="How to read this index"
              title="A tag is evidence, not a guarantee."
            />
            <ol className="ip-step-grid">
              <li className="ip-step">
                <h3>Confirm the tag</h3>
                <p>Inspect the exact repository artifact and pin it for an evaluation.</p>
              </li>
              <li className="ip-step">
                <h3>Keep the channel label</h3>
                <p>Release candidates remain pre-release software, not stable releases.</p>
              </li>
              <li className="ip-step">
                <h3>Verify current maturity</h3>
                <p>Capabilities and guarantees must be checked against current source and docs.</p>
              </li>
            </ol>
          </div>
        </section>

        <section className="ip-cta">
          <div className="mk-wrap ip-cta-inner">
            <div>
              <h2>Inspect the artifact and its current boundaries.</h2>
              <p>Use the repository tag together with the current Engine maturity matrix.</p>
            </div>
            <div className="ip-actions">
              <a className="mk-button mk-button-primary" href={`${githubUrl}/releases/tag/v0.1.0-rc.1`}>
                Inspect the tag <ArrowIcon />
              </a>
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
