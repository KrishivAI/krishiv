import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowIcon, InteriorHero, SectionIntro } from '@/components/InteriorPage';
import { SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

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
      <main className="ip-page">
        <InteriorHero
          eyebrow="Product system / 01"
          title="Open compute. An integrated control plane when you need it."
          description={
            <p>
              Krishiv has two deliberately separate products: use the Apache-2.0 Engine
              from source today, and evaluate Platform only when a public preview exists.
            </p>
          }
          aside={
            <div className="ip-panel">
              <div className="ip-panel-top">
                <span className="ip-panel-label">Product boundary</span>
                <span className="ip-panel-badge"><i />Explicit</span>
              </div>
              <h2 className="ip-panel-title">Independent by design.</h2>
              <ul className="ip-panel-list">
                <li className="ip-panel-row"><span>Engine</span><strong>Apache 2.0 · developer preview</strong></li>
                <li className="ip-panel-row"><span>Platform</span><strong>BSL 1.1 · coming soon</strong></li>
                <li className="ip-panel-row"><span>Integration</span><strong>Public Engine contracts</strong></li>
              </ul>
              <p className="ip-panel-note">Platform is not required to use Engine and has no private compute path.</p>
            </div>
          }
        >
          <div className="ip-actions">
            <Link className="mk-button mk-button-primary" href="/engine">Explore Engine <ArrowIcon /></Link>
            <Link className="mk-button mk-button-secondary" href="/platform">Platform direction</Link>
          </div>
        </InteriorHero>

        <section className="ip-section">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="Two products"
              title="A clean line between compute and control."
              description={<p>Each product has its own availability, license, responsibilities, and documentation root.</p>}
            />
            <div className="mk-product-grid">
            <article className="mk-product-card mk-product-card-engine">
              <div className="mk-product-card-top"><span className="mk-product-index">01 / Engine</span><span className="mk-status"><i />Developer preview</span></div>
              <h3>Krishiv Engine</h3><p>Apache-2.0 Rust-native compute for batch SQL and preview stateful streaming, with experimental incremental processing.</p>
              <ul className="mk-check-list">
                <li>Embedded and single-node entry points</li>
                <li>Rust, Python, SQL, Flight SQL, and MCP surfaces</li>
                <li>Explicit maturity for every workload shape</li>
              </ul>
              <Link className="mk-card-link" href="/engine">Explore Engine <ArrowIcon /></Link>
            </article>
            <article className="mk-product-card mk-product-card-platform">
              <div className="mk-product-card-top"><span className="mk-product-index">02 / Platform</span><span className="mk-status mk-status-muted"><i />Coming soon</span></div>
              <h3>Krishiv Platform</h3><p>An upcoming self-hosted, source-available workspace and control plane built on public Engine interfaces.</p>
              <ul className="mk-check-list">
                <li>Planned workspace and operational console</li>
                <li>Planned catalog, pipeline, and job coordination</li>
                <li>No install path or availability date yet</li>
              </ul>
              <Link className="mk-card-link" href="/platform">Preview the direction <ArrowIcon /></Link>
            </article>
            </div>
          </div>
        </section>

        <section className="ip-section ip-section-contrast">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="Where to begin"
              title="Start at the layer you actually need."
              description={<p>The current public path always begins with Engine. Platform remains a future option, not a prerequisite.</p>}
            />
            <ol className="ip-rows" role="list">
              <li className="ip-row">
                <span>01</span><h3>Embed compute</h3>
                <p>Use Engine in-process for local SQL, DataFrames, application integration, and evaluation.</p>
                <Link href="/docs/engine/getting-started">Get started →</Link>
              </li>
              <li className="ip-row">
                <span>02</span><h3>Exercise service boundaries</h3>
                <p>Run the single-node topology when you need coordinator, executor, HTTP, and Flight interfaces on one host.</p>
                <Link href="/docs/engine/guides/single-node">Single-node guide →</Link>
              </li>
              <li className="ip-row">
                <span>03</span><h3>Track the workspace</h3>
                <p>Read the Platform boundary now; setup and API docs will arrive only with a real public preview.</p>
                <Link href="/docs/platform">Platform notice →</Link>
              </li>
            </ol>
          </div>
        </section>

        <section className="ip-cta">
          <div className="mk-wrap ip-cta-inner">
            <div>
              <p className="ip-kicker">Available path</p>
              <h2>Build with Engine from the source you can inspect.</h2>
              <p>Pin a commit, start embedded, and expand topology only when the workload requires it.</p>
            </div>
            <div className="ip-actions">
              <Link className="mk-button mk-button-primary" href="/docs/engine/getting-started">Read the quickstart <ArrowIcon /></Link>
              <a className="mk-button mk-button-secondary" href={githubUrl}>View source</a>
            </div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
