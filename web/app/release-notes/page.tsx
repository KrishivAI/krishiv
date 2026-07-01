import type { Metadata } from 'next';
import Link from 'next/link';
import { Badge, SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Release Notes',
  description:
    'Krishiv release notes — versioned changelog with codebase-verified facts for each release.',
  openGraph: {
    title: 'Krishiv Release Notes',
    description: 'Versioned changelog with codebase-verified facts for each release.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/release-notes',
  },
};

export default function Releases(){return <SiteShell><main className="container"><section className="page-hero"><Badge tone="blue">Release notes</Badge><h1>Release notes</h1><p className="lead">Codebase-verified facts only; unknown changelog entries remain maintainer placeholders.</p></section><Link href="/release-notes/v0.1.0" className="card" style={{display:'block'}}><Badge tone="orange">v0.1.0</Badge><h2>Krishiv v0.1.0</h2><p className="muted">Initial release-note template populated with verified engine facts.</p></Link></main></SiteShell>}
