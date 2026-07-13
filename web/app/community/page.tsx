import type { Metadata } from 'next';
import { ArrowIcon, InteriorHero, SectionIntro } from '@/components/InteriorPage';
import { SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

export const metadata: Metadata = {
  title: 'Community',
  description:
    'Krishiv community — contribute, discuss, and get support for the Rust-native compute engine.',
  openGraph: {
    title: 'Krishiv Community',
    description: 'Contribute, discuss, and get support for the Krishiv compute engine.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/community',
  },
};

function CommunityStatus() {
  return (
    <div className="ip-panel">
      <div className="ip-panel-top">
        <span className="ip-panel-label">Community status</span>
        <span className="ip-panel-badge"><i />Public repository</span>
      </div>
      <h2 className="ip-panel-title">Open Engine development.</h2>
      <ul className="ip-panel-list">
        <li className="ip-panel-row"><span>Engine license</span><strong>Apache-2.0</strong></li>
        <li className="ip-panel-row"><span>Issues</span><strong>Public on GitHub</strong></li>
        <li className="ip-panel-row"><span>Discussions</span><strong>Public on GitHub</strong></li>
        <li className="ip-panel-row"><span>Formal governance</span><strong>Not published</strong></li>
      </ul>
      <p className="ip-panel-note">
        Public repository channels are available now; no formal support program is implied.
      </p>
    </div>
  );
}

const communityLinks = [
  {
    label: 'Source',
    title: 'Explore the repository',
    description: 'Inspect the Engine crates, examples, tests, and current development history.',
    href: githubUrl,
    action: 'View the source',
  },
  {
    label: 'Feedback',
    title: 'Report an issue',
    description: 'Share a reproducible bug, documentation gap, or focused feature request.',
    href: `${githubUrl}/issues`,
    action: 'Open GitHub Issues',
  },
  {
    label: 'Conversation',
    title: 'Start a discussion',
    description: 'Ask a technical question or discuss the project direction in public.',
    href: `${githubUrl}/discussions`,
    action: 'Visit Discussions',
  },
];

export default function Community() {
  return (
    <SiteShell>
      <main className="ip-page">
        <InteriorHero
          eyebrow="Open-source community"
          title="Build with the Engine in the open."
          description={
            <p>
              Krishiv Engine is developed in a public Apache-2.0 repository. Inspect the code,
              report focused issues, and discuss the project through its public GitHub channels.
            </p>
          }
          aside={<CommunityStatus />}
        >
          <div className="ip-actions">
            <a className="mk-button mk-button-primary" href={githubUrl}>
              View on GitHub <ArrowIcon />
            </a>
            <a className="mk-button mk-button-secondary" href={`${githubUrl}/blob/main/CONTRIBUTING.md`}>
              Contribution guide
            </a>
          </div>
        </InteriorHero>

        <section className="ip-section">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="Public channels"
              title="Start with the channel that fits the work."
              description={
                <p>
                  These links lead to public project spaces. Response times and support levels
                  are not guaranteed.
                </p>
              }
            />
            <div className="ip-card-grid">
              {communityLinks.map((item, index) => (
                <a className="ip-card" href={item.href} key={item.title}>
                  <div className="ip-card-top">
                    <span>0{index + 1}</span><span>{item.label}</span>
                  </div>
                  <h3>{item.title}</h3>
                  <p>{item.description}</p>
                  <span className="ip-card-link">{item.action} <ArrowIcon /></span>
                </a>
              ))}
            </div>
          </div>
        </section>

        <section className="ip-section ip-section-contrast">
          <div className="mk-wrap">
            <SectionIntro
              compact
              eyebrow="A useful contribution path"
              title="Evidence first, then a focused change."
            />
            <ol className="ip-step-grid">
              <li className="ip-step">
                <h3>Check the current source</h3>
                <p>Confirm the behavior against the current branch, tests, and maturity docs.</p>
              </li>
              <li className="ip-step">
                <h3>Describe the boundary</h3>
                <p>Include the runtime mode, connector combination, and a minimal reproduction.</p>
              </li>
              <li className="ip-step">
                <h3>Keep the change reviewable</h3>
                <p>Follow the repository contribution guidance and keep claims source-backed.</p>
              </li>
            </ol>
          </div>
        </section>

        <section className="ip-cta">
          <div className="mk-wrap ip-cta-inner">
            <div>
              <h2>Ready to inspect the Engine?</h2>
              <p>Start with the source and the contributor guidance maintained beside it.</p>
            </div>
            <div className="ip-actions">
              <a className="mk-button mk-button-primary" href={githubUrl}>
                Open the repository <ArrowIcon />
              </a>
              <a className="mk-button mk-button-secondary" href={`${githubUrl}/blob/main/CONTRIBUTING.md`}>
                Read contributing
              </a>
            </div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
