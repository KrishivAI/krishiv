import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowIcon, InteriorHero } from '@/components/InteriorPage';
import { SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

export const metadata: Metadata = {
  title: 'Krishiv Engine v0.1.0-rc.1',
  description: 'Notice for the tagged Krishiv Engine v0.1.0-rc.1 development release.',
  alternates: {
    canonical: 'https://krishiv.ai/release-notes/v0.1.0-rc.1',
  },
};

function ArtifactFacts() {
  return (
    <div className="ip-panel">
      <div className="ip-panel-top">
        <span className="ip-panel-label">Artifact facts</span>
        <span className="ip-panel-badge"><i />Tagged</span>
      </div>
      <h2 className="ip-panel-title">v0.1.0-rc.1</h2>
      <ul className="ip-panel-list">
        <li className="ip-panel-row"><span>Channel</span><strong>Release candidate</strong></li>
        <li className="ip-panel-row"><span>Stability</span><strong>Pre-release</strong></li>
        <li className="ip-panel-row"><span>Changelog</span><strong>Not inferred</strong></li>
      </ul>
      <p className="ip-panel-note">
        Current branch history is not presented as a changelog for this tag.
      </p>
    </div>
  );
}

export default function ReleaseCandidate() {
  return (
    <SiteShell>
      <main className="ip-page">
        <InteriorHero
          compact
          eyebrow="Release candidate"
          title="Krishiv Engine v0.1.0-rc.1"
          beforeTitle={
            <nav className="ip-breadcrumb" aria-label="Breadcrumb">
              <Link href="/release-notes">Development releases</Link>
              <i aria-hidden="true" />
              <span aria-current="page">v0.1.0-rc.1</span>
            </nav>
          }
          description={
            <p>
              A repository tag notice that preserves the artifact&apos;s pre-release status and
              avoids turning current development work into an invented changelog.
            </p>
          }
          aside={<ArtifactFacts />}
        >
          <div className="ip-article-meta">
            <span>Tagged artifact</span>
            <span>Pre-release</span>
            <span>Not a stable release</span>
          </div>
        </InteriorHero>

        <div className="mk-wrap ip-article-layout">
          <article className="ip-article-body">
            <p className="ip-article-lede">
              This tag exists in the Engine repository, but it is a release candidate—not a
              stable release or a production-readiness claim.
            </p>

            <section id="confirmed">
              <h2>What this page confirms</h2>
              <p>
                The Engine repository has a tag named <code>v0.1.0-rc.1</code>. Evaluators can
                inspect that exact artifact on GitHub and pin it rather than assuming the current
                development branch is equivalent.
              </p>
              <p>
                The release-candidate label is part of the artifact&apos;s meaning and should remain
                visible anywhere the tag is referenced.
              </p>
            </section>

            <section id="not-claimed">
              <h2>What it does not claim</h2>
              <p>
                This notice does not describe the tag as stable, production ready, generally
                available, or covered by a hosted-service support commitment.
              </p>
              <div className="ip-callout">
                <strong>No invented changelog</strong>
                <p>
                  The page intentionally does not attribute changes from the current development
                  branch to this earlier tag without tag-specific release notes.
                </p>
              </div>
            </section>

            <section id="evaluate">
              <h2>How to evaluate it</h2>
              <p>
                Inspect the tagged source, pin the artifact used for testing, and review the
                current <Link href="/docs/engine/maturity">Engine maturity documentation</Link>.
                Available, Preview, and Experimental labels describe different confidence levels
                and are not interchangeable.
              </p>
              <p>
                Runtime, recovery, and connector guarantees must be evaluated for the exact
                combination being used.
              </p>
            </section>
          </article>

          <aside className="ip-article-aside" aria-label="Release navigation and context">
            <div className="ip-aside-block">
              <strong>On this page</strong>
              <nav aria-label="Release notice sections">
                <a href="#confirmed">What is confirmed</a>
                <a href="#not-claimed">What is not claimed</a>
                <a href="#evaluate">How to evaluate it</a>
              </nav>
            </div>
            <div className="ip-aside-block">
              <strong>Current status</strong>
              <p>
                The project remains a developer preview. Use current source and docs for the
                latest capability boundaries.
              </p>
            </div>
          </aside>
        </div>

        <section className="ip-cta">
          <div className="mk-wrap ip-cta-inner">
            <div>
              <h2>Review the exact tag before evaluating.</h2>
              <p>Pair the artifact with the current maturity documentation.</p>
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
