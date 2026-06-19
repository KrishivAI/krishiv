import { notFound } from 'next/navigation';
import { DocsBody, DocsPage, DocsTitle, DocsDescription } from 'fumadocs-ui/page';
import { getMDXComponents } from '@/components/mdx';
import { blog } from '@/lib/source';

export default async function BlogPost({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const page = blog.getPage([slug]);

  if (!page) notFound();

  const MDX = page.data.body;

  return (
    <main className="home-container list-page">
      <DocsPage toc={page.data.toc}>
        <DocsTitle>{page.data.title}</DocsTitle>
        <DocsDescription>{page.data.description}</DocsDescription>
        <DocsBody>
          <MDX components={getMDXComponents()} />
        </DocsBody>
      </DocsPage>
    </main>
  );
}

export function generateStaticParams() {
  return blog.getPages().map((page) => ({ slug: page.slugs[0] }));
}
