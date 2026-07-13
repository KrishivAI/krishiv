import type { Metadata } from 'next';
import Link from 'next/link';
import { SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Products',
  description: 'Krishiv Engine is available in developer preview. Krishiv Platform is coming soon.',
  alternates: {
    canonical: 'https://krishiv.ai/product',
  },
};

export default function Product() {
  return (
    <SiteShell>
      <main className="pd-page">
        <section className="pd-hero">
          <div className="mk-wrap">
            <div className="mk-eyebrow"><i /> Krishiv products</div>
            <h1>Open compute. Integrated control plane.</h1>
            <p className="pd-lead">Two products with a clean public boundary: use Engine independently today, and add Platform when it becomes available.</p>
          </div>
        </section>
        <section className="pd-section">
          <div className="mk-wrap mk-product-grid">
            <article className="mk-product-card mk-product-card-engine">
              <div className="mk-product-card-top"><span className="mk-product-index">01 / Engine</span><span className="mk-status"><i />Developer preview</span></div>
              <h3>Krishiv Engine</h3><p>Apache-2.0 Rust-native compute for batch SQL and preview stateful streaming, with experimental incremental processing.</p>
              <Link className="mk-card-link" href="/engine">Explore Engine →</Link>
            </article>
            <article className="mk-product-card mk-product-card-platform">
              <div className="mk-product-card-top"><span className="mk-product-index">02 / Platform</span><span className="mk-status mk-status-muted"><i />Coming soon</span></div>
              <h3>Krishiv Platform</h3><p>An upcoming self-hosted, source-available workspace and control plane built on public Engine interfaces.</p>
              <Link className="mk-card-link" href="/platform">Preview the direction →</Link>
            </article>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
