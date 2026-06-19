import Link from 'next/link';
import { basePath } from '@/lib/base-path';

const links = [
  ['Docs', '/docs'],
  ['Blog', '/blog'],
  ['Examples', '/examples'],
  ['Changelog', '/changelog'],
  ['Roadmap', '/roadmap'],
];

export function SiteHeader() {
  return (
    <header className="home-nav">
      <Link href={`${basePath}/`} style={{ display: 'inline-flex', alignItems: 'center', gap: 10, fontWeight: 800, textDecoration: 'none' }}>
        <img src={`${basePath}/krishiv-mark.svg`} alt="Krishiv" width={30} height={30} />
        <span>Krishiv</span>
      </Link>
      <nav className="home-nav-links" aria-label="Main navigation">
        {links.map(([label, href]) => (
          <Link href={`${basePath}${href}`} key={href} style={{ textDecoration: 'none' }}>
            {label}
          </Link>
        ))}
        <a href="https://github.com/KrishivAI/krishiv" rel="noreferrer" target="_blank" style={{ textDecoration: 'none' }}>
          GitHub
        </a>
      </nav>
    </header>
  );
}
