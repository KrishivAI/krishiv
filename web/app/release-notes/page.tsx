import type { Metadata } from 'next';
import Link from 'next/link';
import { Badge, SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Development Releases',
  description:
    'Tagged Krishiv Engine development releases and release candidates.',
  openGraph: {
    title: 'Krishiv Engine Development Releases',
    description: 'Tagged development releases and release candidates for Krishiv Engine.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/release-notes',
  },
};

export default function Releases(){return <SiteShell><main className="container"><section className="page-hero"><Badge tone="blue">Development releases</Badge><h1>Development releases</h1><p className="lead">Only tags that exist in the Engine repository are listed here. A release candidate is not a stable release or a production-readiness signal.</p></section><Link href="/release-notes/v0.1.0-rc.1" className="card" style={{display:'block'}}><Badge tone="orange">Release candidate</Badge><h2>Krishiv Engine v0.1.0-rc.1</h2><p className="muted">Pre-release tag. Use the current maturity docs when evaluating capabilities.</p></Link></main></SiteShell>}
