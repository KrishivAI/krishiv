import type { Metadata } from 'next';
import Link from 'next/link';
import { Badge, SiteShell } from '@/components/Shell';

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

export default function Blog(){return <SiteShell><main className="container"><section className="page-hero"><Badge tone="orange">Blog</Badge><h1>Technical notes from the Krishiv engine.</h1><p className="lead">Editorial updates separate implemented capabilities from in-progress work.</p></section><Link className="card" style={{display:'block'}} href="/blog/introducing-krishiv"><Badge tone="blue">Engineering</Badge><h2>Introducing Krishiv: One Engine for Batch, Streaming, and Incremental Data Processing</h2><p className="muted">A codebase-grounded overview of the unified engine vision.</p></Link></main></SiteShell>}
