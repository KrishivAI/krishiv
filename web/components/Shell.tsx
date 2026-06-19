import Link from 'next/link';
import type { ReactNode } from 'react';
import { githubUrl, navItems } from '@/lib/site';

export function SiteShell({ children }: { children: ReactNode }) {
  return <div className="site-shell"><div className="ambient ambient-a"/><div className="ambient ambient-b"/><Header />{children}<Footer /></div>;
}

export function Header() {
  return <header className="header"><Link className="brand" href="/"><span className="mark">K</span><span>Krishiv</span></Link><nav className="nav" aria-label="Primary navigation">{navItems.map((item)=><Link key={item.href} href={item.href}>{item.label}</Link>)}<Link className="btn btn-primary small" href="/docs/latest/getting-started">Get Started</Link></nav></header>;
}

export function Footer() { return <footer className="footer"><div><strong>Krishiv</strong><p>Rust-native compute for batch SQL, streaming pipelines, and incremental processing.</p></div><div className="footer-links"><Link href="/docs/latest">Docs</Link><Link href="/architecture">Architecture</Link><Link href="/release-notes">Release notes</Link><a href={githubUrl}>GitHub</a></div></footer>; }

export function Badge({ children, tone='blue' }: { children: ReactNode; tone?: 'blue'|'orange'|'green'|'violet'|'gray' }) { return <span className={`badge badge-${tone}`}>{children}</span>; }
export function Section({ eyebrow, title, children }: { eyebrow?: string; title: string; children: ReactNode }) { return <section className="section">{eyebrow&&<p className="eyebrow">{eyebrow}</p>}<h2>{title}</h2>{children}</section>; }
