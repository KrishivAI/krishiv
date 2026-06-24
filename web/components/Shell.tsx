import Image from 'next/image';
import Link from 'next/link';
import type { ReactNode } from 'react';
import { githubUrl, navItems } from '@/lib/site';
import { SearchButton } from '@/components/Search';

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
        <SearchButton/>
        <a className="icon-link" href={githubUrl} aria-label="GitHub"><GithubIcon/></a>
        <Link className="btn btn-primary small" href="/docs/latest/getting-started">Get Started</Link>
      </div>
      <details className="mobile-menu">
        <summary aria-label="Open menu"><span/><span/><span/></summary>
        <div>
          {navItems.map((item) => <Link key={item.href} href={item.href}>{item.label}</Link>)}
          <a href={githubUrl}>GitHub</a>
          <Link className="btn btn-primary small" href="/docs/latest/getting-started">Get Started</Link>
        </div>
      </details>
    </header>
  );
}

export function Footer() {
  return (
    <footer className="footer">
      <div><strong>Krishiv</strong><p>Rust-native compute for batch SQL, streaming pipelines, and incremental processing. Apache&nbsp;Arrow · DataFusion · Tokio.</p></div>
      <div className="footer-links">
        <Link href="/docs/latest">Docs</Link>
        <Link href="/docs/latest/getting-started">Quickstart</Link>
        <Link href="/docs/latest/tutorial">Tutorial</Link>
        <Link href="/docs/latest/recipes">Recipes</Link>
        <Link href="/product/maturity">Maturity</Link>
        <Link href="/architecture">Architecture</Link>
        <Link href="/release-notes">Release notes</Link>
        <a href="https://github.com/krishiv-data/krishiv/blob/main/LICENSE">License</a>
        <a href={githubUrl}>GitHub</a>
      </div>
    </footer>
  );
}

export function Badge({ children, tone = 'gray' }: { children: ReactNode; tone?: string }) { return <span className={`badge badge-${tone}`}>{children}</span>; }
export function Section({ eyebrow, title, children }: { eyebrow?: string; title: string; children: ReactNode }) { return <section className="section">{eyebrow && <p className="section-eyebrow">{eyebrow}</p>}<h2>{title}</h2>{children}</section>; }
