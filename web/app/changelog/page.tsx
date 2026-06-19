import { SiteHeader } from '@/components/SiteHeader';

const entries = [
  {
    version: 'Unreleased',
    text: 'Public Fumadocs website scaffold with landing page, docs, blog, examples, changelog, roadmap, and version metadata.',
  },
  {
    version: '0.1.0',
    text: 'Initial pre-1.0 development release line. The full canonical changelog remains at the repository root.',
  },
];

export const metadata = {
  title: 'Changelog',
  description: 'Release notes and public changes for Krishiv.',
};

export default function ChangelogPage() {
  return (
    <main className="home-shell">
      <div className="home-container list-page">
        <SiteHeader />
        <span className="eyebrow">Changelog</span>
        <h1>Krishiv release notes.</h1>
        <p className="section-lead">This page is ready to be generated from the root CHANGELOG.md in a follow-up phase.</p>
        <div className="grid two">
          {entries.map((entry) => (
            <article className="card" key={entry.version}>
              <h3>{entry.version}</h3>
              <p>{entry.text}</p>
            </article>
          ))}
        </div>
      </div>
    </main>
  );
}
