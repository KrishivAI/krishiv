import Link from 'next/link';
import { SiteHeader } from '@/components/SiteHeader';
import { blog } from '@/lib/source';

export const metadata = {
  title: 'Blog',
  description: 'Engineering updates, release notes, and architecture notes from Krishiv.',
};

export default function BlogIndex() {
  const posts = blog.getPages().sort((a, b) => String(b.data.date ?? '').localeCompare(String(a.data.date ?? '')));

  return (
    <main className="home-shell">
      <div className="home-container list-page">
        <SiteHeader />
        <span className="eyebrow">Blog</span>
        <h1>Engineering notes from Krishiv.</h1>
        <p className="section-lead">Release updates, architecture decisions, benchmark notes, and docs changes.</p>
        <div className="grid two">
          {posts.map((post) => (
            <Link className="card list-item" href={post.url} key={post.url}>
              <h3>{post.data.title}</h3>
              <p>{post.data.description}</p>
            </Link>
          ))}
        </div>
      </div>
    </main>
  );
}
