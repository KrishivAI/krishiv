import Link from 'next/link';
import { basePath } from '@/lib/base-path';

const columns = [
  {
    title: 'Product',
    links: [
      ['Docs', '/docs'],
      ['Examples', '/examples'],
      ['Blog', '/blog'],
      ['Changelog', '/changelog'],
      ['Roadmap', '/roadmap'],
    ],
  },
  {
    title: 'Resources',
    links: [
      ['Architecture', '/docs/runtime-modes'],
      ['SQL Reference', '/docs/batch-sql'],
      ['Rust API', '/docs/rust'],
      ['Python API', '/docs/python'],
      ['Release Notes', '/changelog'],
    ],
  },
  {
    title: 'Community',
    links: [
      ['GitHub', 'https://github.com/KrishivAI/krishiv'],
      ['Issues', 'https://github.com/KrishivAI/krishiv/issues'],
      ['Discussions', 'https://github.com/KrishivAI/krishiv/discussions'],
    ],
  },
];

export function Footer() {
  return (
    <footer className="landing-footer">
      <div className="landing-footer-grid">
        <div className="landing-footer-brand">
          <Link
            href={`${basePath}/`}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 10,
              fontWeight: 800,
              fontSize: 18,
              textDecoration: 'none',
            }}
          >
            <img src={`${basePath}/krishiv-mark.svg`} alt="Krishiv" width={28} height={28} />
            Krishiv
          </Link>
          <p>
            Unified batch + streaming compute engine.
            Built with Rust and Apache DataFusion.
          </p>
        </div>

        {columns.map((col) => (
          <div className="landing-footer-col" key={col.title}>
            <h4>{col.title}</h4>
            <ul>
              {col.links.map(([label, href]) => (
                <li key={href}>
                  {href.startsWith('http') ? (
                    <a href={href} target="_blank" rel="noreferrer">
                      {label}
                    </a>
                  ) : (
                    <Link href={`${basePath}${href}`}>{label}</Link>
                  )}
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>

      <div className="landing-footer-bottom">
        <span>© 2026 Krishiv Contributors. Apache 2.0 License.</span>
        <span>Powered by Rust + Apache DataFusion</span>
      </div>
    </footer>
  );
}
