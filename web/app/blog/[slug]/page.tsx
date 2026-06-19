import { notFound } from 'next/navigation';
import { getMDXComponents } from '@/components/mdx';
import { blog } from '@/lib/source';
import { SiteHeader } from '@/components/SiteHeader';

export default async function BlogPost({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const page = blog.getPage([slug]);

  if (!page) notFound();

  const MDX = page.data.body;

  return (
    <main className="home-shell">
      <div className="home-container">
        <SiteHeader />
        <article className="list-page">
          <h1>{page.data.title}</h1>
          {page.data.description && <p className="section-lead">{page.data.description}</p>}
          <MDX components={getMDXComponents()} />
        </article>
      </div>
    </main>
  );
}

export function generateStaticParams() {
  return blog.getPages().map((page) => ({ slug: page.slugs[0] }));
}
