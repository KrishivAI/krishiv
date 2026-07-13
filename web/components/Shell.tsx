import Image from 'next/image';
import Link from 'next/link';
import type { ReactNode } from 'react';
import { githubUrl } from '@/lib/site';

export const navItems = [
  { label: 'Engine', href: '/engine' },
  { label: 'Platform', href: '/platform' },
  { label: 'Docs', href: '/docs/engine' },
  { label: 'Blog', href: '/blog' },
];

export function BrandLogo({ compact = false }: { compact?: boolean }) {
  return (
    <span className="brand-logo" aria-label="Krishiv">
      <Image src="/brand/logo-mark.svg" alt="" width={38} height={38} priority />
      {!compact && <span>Krishiv</span>}
    </span>
  );
}

export function SiteShell({ children }: { children: ReactNode }) {
  return (
    <div className="site-shell">
      <Header />
      {children}
      <Footer />
    </div>
  );
}

function GithubIcon() {
  return <svg viewBox="0 0 20 20" width="20" height="20" fill="currentColor" aria-hidden="true"><path d="M10 .9a9.1 9.1 0 0 0-2.9 17.7c.46.08.63-.2.63-.44v-1.6c-2.57.56-3.11-1.1-3.11-1.1-.42-1.07-1.03-1.35-1.03-1.35-.84-.58.06-.57.06-.57.93.07 1.42.96 1.42.96.83 1.41 2.18 1 2.71.77.08-.6.32-1 .59-1.23-2.05-.23-4.2-1.02-4.2-4.55 0-1 .36-1.83.95-2.47-.1-.24-.41-1.18.09-2.44 0 0 .78-.25 2.5.94A8.7 8.7 0 0 1 10 5.2c.77 0 1.54.1 2.27.3 1.72-1.19 2.5-.94 2.5-.94.5 1.26.19 2.2.1 2.44.59.64.94 1.46.94 2.47 0 3.54-2.16 4.31-4.21 4.54.33.29.63.85.63 1.72v2.55c0 .25.17.53.64.44A9.1 9.1 0 0 0 10 .9Z"/></svg>;
}

export function Header() {
  return (
    <header className="header">
      <Link className="brand" href="/"><BrandLogo /></Link>
      <nav className="nav center-nav" aria-label="Primary navigation">
        {navItems.map((item) => <Link key={item.href} href={item.href}>{item.label}</Link>)}
      </nav>
      <div className="nav-actions">
        <a className="icon-link" href={githubUrl} aria-label="GitHub"><GithubIcon/></a>
        <Link className="btn btn-primary small" href="/docs/engine/getting-started">Get started</Link>
      </div>
      <details className="mobile-menu">
        <summary aria-label="Open menu"><span/><span/><span/></summary>
        <div>
          {navItems.map((item) => <Link key={item.href} href={item.href}>{item.label}</Link>)}
          <a href={githubUrl}>GitHub</a>
          <Link className="btn btn-primary small" href="/docs/engine/getting-started">Get started</Link>
        </div>
      </details>
    </header>
  );
}

export function Footer() {
  return (
    <footer className="footer">
      <div className="footer-grid">
        <div className="footer-brand">
          <strong>Krishiv</strong>
          <p>Data systems with one path from open compute to an integrated workspace.</p>
          <p className="footer-facts">Engine: Apache 2.0 · Platform: coming soon</p>
        </div>
        <div className="footer-col">
          <h4>Products</h4>
          <Link href="/engine">Krishiv Engine</Link>
          <Link href="/platform">Krishiv Platform</Link>
          <Link href="/architecture">Architecture</Link>
          <Link href="/product/maturity">Feature Maturity</Link>
        </div>
        <div className="footer-col">
          <h4>Developers</h4>
          <Link href="/docs/engine">Engine docs</Link>
          <Link href="/docs/engine/getting-started">Getting started</Link>
          <Link href="/docs/engine/reference/python">Python API</Link>
          <Link href="/docs/engine/reference/rust">Rust API</Link>
          <Link href="/docs/engine/reference/sql">SQL reference</Link>
        </div>
        <div className="footer-col">
          <h4>Community</h4>
          <a href={githubUrl}>GitHub</a>
          <a href={`${githubUrl}/issues`}>Issues</a>
          <a href={`${githubUrl}/discussions`}>Discussions</a>
          <Link href="/blog">Blog</Link>
          <a href={`${githubUrl}/blob/main/CONTRIBUTING.md`}>Contributing</a>
        </div>
        <div className="footer-col">
          <h4>Resources</h4>
          <Link href="/release-notes">Development releases</Link>
          <a href={`${githubUrl}/blob/main/LICENSE`}>License</a>
          <a href={`${githubUrl}/blob/main/SECURITY.md`}>Security</a>
          <a href="/feed.xml" type="application/rss+xml">RSS</a>
          <a href="https://krishiv.ai">Website</a>
        </div>
      </div>
      <div className="footer-bottom">
        <span>&copy; {new Date().getFullYear()} KrishivAI.</span>
        <div className="footer-links-bottom">
          <Link href="/docs/engine">Docs</Link>
          <Link href="/architecture">Architecture</Link>
          <a href={githubUrl}>GitHub</a>
        </div>
      </div>
    </footer>
  );
}

export function Badge({ children, tone = 'gray' }: { children: ReactNode; tone?: string }) { return <span className={`badge badge-${tone}`}>{children}</span>; }
export function Section({ eyebrow, title, children, id }: { eyebrow?: string; title: string; children: ReactNode; id?: string }) { return <section className="section" id={id}>{eyebrow && <p className="section-eyebrow">{eyebrow}</p>}<h2>{title}</h2>{children}</section>; }
