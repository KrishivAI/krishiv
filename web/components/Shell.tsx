import Link from 'next/link';
import type { ReactNode } from 'react';
import { githubUrl, navItems } from '@/lib/site';

type BadgeTone = 'blue' | 'orange' | 'green' | 'violet' | 'gray';

export function SiteShell({ children }: { children: ReactNode }) {
  return (
    <div className="site-shell">
      <div className="ambient ambient-a" />
      <div className="ambient ambient-b" />
      <Header />
      {children}
      <Footer />
    </div>
  );
}

export function Header() {
  return (
    <header className="header">
      <Link className="brand" href="/">
        <span className="mark" aria-hidden="true">
          <svg viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg" width="26" height="26">
            <g fill="#F59E0B">
              <rect x="4"  y="10" width="5" height="12" rx="2"/>
              <rect x="11" y="6"  width="5" height="16" rx="2"/>
              <rect x="18" y="10" width="5" height="12" rx="2"/>
              <rect x="8"  y="22" width="9" height="20" rx="1"/>
            </g>
            <line x1="17" y1="32" x2="42" y2="20" stroke="#F59E0B" strokeWidth="7" strokeLinecap="round"/>
            <line x1="17" y1="32" x2="42" y2="44" stroke="#F59E0B" strokeWidth="7" strokeLinecap="round"/>
          </svg>
        </span>
        <span>Krishiv</span>
      </Link>
      <nav className="nav" aria-label="Primary navigation">
        {navItems.map((item) => (
          <Link key={item.href} href={item.href}>
            {item.label}
          </Link>
        ))}
        <Link className="btn btn-primary small" href="/docs/latest/getting-started">
          Get Started
        </Link>
      </nav>
    </header>
  );
}

export function Footer() {
  return (
    <footer className="footer">
      <div>
        <strong>Krishiv</strong>
        <p>Rust-native compute for batch SQL, streaming pipelines, and incremental processing.</p>
      </div>
      <div className="footer-links">
        <Link href="/docs/latest">Docs</Link>
        <Link href="/architecture">Architecture</Link>
        <Link href="/release-notes">Release notes</Link>
        <a href={githubUrl}>GitHub</a>
      </div>
    </footer>
  );
}

export function Badge({ children, tone = 'blue' }: { children: ReactNode; tone?: BadgeTone }) {
  return <span className={`badge badge-${tone}`}>{children}</span>;
}

export function Section({ eyebrow, title, children }: { eyebrow?: string; title: string; children: ReactNode }) {
  return (
    <section className="section">
      {eyebrow && <p className="eyebrow">{eyebrow}</p>}
      <h2>{title}</h2>
      {children}
    </section>
  );
}
