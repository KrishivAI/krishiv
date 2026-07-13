import type { Metadata } from 'next';
import Link from 'next/link';
import { Badge, SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

export const metadata: Metadata = {
  title: 'Krishiv Engine v0.1.0-rc.1',
  description:
    'Notice for the tagged Krishiv Engine v0.1.0-rc.1 development release.',
  alternates: {
    canonical: 'https://krishiv.ai/release-notes/v0.1.0-rc.1',
  },
};

export default function ReleaseCandidate() {
  return (
    <SiteShell>
      <main className="article">
        <Badge tone="orange">Release candidate</Badge>
        <h1>Krishiv Engine v0.1.0-rc.1</h1>
        <p>
          This tag exists in the Engine repository, but it is a pre-release—not a stable
          release or a production-readiness claim. This page intentionally does not invent
          a changelog from the current development branch.
        </p>
        <div className="actions">
          <a className="btn btn-primary" href={`${githubUrl}/releases/tag/v0.1.0-rc.1`}>
            Inspect the tag on GitHub
          </a>
          <Link className="btn btn-secondary" href="/docs/engine/maturity">
            Review current maturity
          </Link>
        </div>
      </main>
    </SiteShell>
  );
}
