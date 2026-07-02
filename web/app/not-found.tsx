import Link from 'next/link';
import { SiteShell } from '@/components/Shell';

export default function NotFound() {
  return (
    <SiteShell>
      <main className="container placeholder">
        <div>
          <p className="eyebrow">404</p>
          <h1>Page not found</h1>
          <p className="lead" style={{ maxWidth: 480 }}>
            The page you are looking for does not exist or has been moved.
          </p>
          <div style={{ marginTop: 24, display: 'flex', gap: 12, flexWrap: 'wrap' }}>
            <Link className="btn btn-primary" href="/">
              Go Home
            </Link>
            <Link className="btn btn-secondary" href="/docs/latest">
              Documentation
            </Link>
            <a className="btn btn-secondary" href="https://github.com/KrishivAI/krishiv">
              GitHub
            </a>
          </div>
        </div>
      </main>
    </SiteShell>
  );
}
