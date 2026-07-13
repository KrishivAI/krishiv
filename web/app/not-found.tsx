import Link from 'next/link';
import { ArrowIcon, InteriorHero, SectionIntro } from '@/components/InteriorPage';
import { SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

const usefulPaths = [
  {
    label: 'Product',
    title: 'Krishiv Engine',
    description: 'Explore the open-source compute layer and its current boundaries.',
    href: '/engine',
  },
  {
    label: 'Reference',
    title: 'Engine documentation',
    description: 'Find source-based setup, concepts, operations, and API references.',
    href: '/docs/engine',
  },
  {
    label: 'Engineering',
    title: 'Architecture',
    description: 'See how embedded, single-node, and Preview distributed paths fit together.',
    href: '/architecture',
  },
];

function RoutePanel() {
  return (
    <div className="ip-panel">
      <div className="ip-panel-top">
        <span className="ip-panel-label">Known destinations</span>
        <span className="ip-panel-badge"><i />Available</span>
      </div>
      <h2 className="ip-panel-title">Try a public entry point.</h2>
      <ul className="ip-panel-list">
        <li className="ip-panel-row"><span>Engine</span><strong>/engine</strong></li>
        <li className="ip-panel-row"><span>Docs</span><strong>/docs/engine</strong></li>
        <li className="ip-panel-row"><span>Blog</span><strong>/blog</strong></li>
      </ul>
      <p className="ip-panel-note">The requested path does not match a published page.</p>
    </div>
  );
}

export default function NotFound() {
  return (
    <SiteShell>
      <main className="ip-page">
        <InteriorHero
          compact
          eyebrow="404 · Page not found"
          title="This page isn’t here."
          description={
            <p>
              The address may be outdated, incomplete, or moved. Choose a current public entry
              point below.
            </p>
          }
          aside={<RoutePanel />}
        >
          <div className="ip-actions">
            <Link className="mk-button mk-button-primary" href="/">
              Go home <ArrowIcon />
            </Link>
            <Link className="mk-button mk-button-secondary" href="/docs/engine">
              Open documentation
            </Link>
          </div>
        </InteriorHero>

        <section className="ip-section">
          <div className="mk-wrap">
            <SectionIntro
              compact
              eyebrow="Useful paths"
              title="Continue from a known page."
            />
            <div className="ip-card-grid">
              {usefulPaths.map((item, index) => (
                <Link className="ip-card" href={item.href} key={item.href}>
                  <div className="ip-card-top">
                    <span>0{index + 1}</span><span>{item.label}</span>
                  </div>
                  <h3>{item.title}</h3>
                  <p>{item.description}</p>
                  <span className="ip-card-link">Open page <ArrowIcon /></span>
                </Link>
              ))}
            </div>
          </div>
        </section>

        <section className="ip-cta">
          <div className="mk-wrap ip-cta-inner">
            <div>
              <h2>Looking for the source repository?</h2>
              <p>Engine code, issues, and discussions are available on GitHub.</p>
            </div>
            <div className="ip-actions">
              <a className="mk-button mk-button-primary" href={githubUrl}>
                Open GitHub <ArrowIcon />
              </a>
              <Link className="mk-button mk-button-secondary" href="/community">
                Community links
              </Link>
            </div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
