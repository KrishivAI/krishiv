import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { createRelativeLink } from 'fumadocs-ui/mdx';
import {
  DocsBody,
  DocsDescription,
  DocsPage,
  DocsTitle,
  ViewOptionsPopover,
} from 'fumadocs-ui/layouts/docs/page';
import { getMDXComponents } from '@/components/mdx';
import { DocsStatus, type DocsStatusName } from '@/components/DocsStatus';
import { githubUrl } from '@/lib/site';
import { resolveDocsSlug, source } from '@/lib/source';

type PageProps = {
  params: Promise<{ slug?: string[] }>;
};

export default async function Page({ params }: PageProps) {
  const { slug } = await params;
  const page = source.getPage(resolveDocsSlug(slug));
  if (!page) notFound();

  const data = page.data;
  const MDX = data.body;

  return (
    <DocsPage id="main-content" role="main" tabIndex={-1} toc={data.toc}>
      <header className="docs-page-header">
        <div className="docs-heading-copy">
          <div className="docs-page-kicker">
            <DocsStatus status={data.status as DocsStatusName | undefined} />
          </div>
          <DocsTitle>{data.title}</DocsTitle>
          <DocsDescription>{data.description}</DocsDescription>
        </div>
        <div className="docs-page-options">
          <ViewOptionsPopover
            githubUrl={`${githubUrl}/blob/main/web/content/docs/${page.path}`}
          />
        </div>
      </header>
      <DocsBody>
        <MDX
          components={getMDXComponents({
            a: createRelativeLink(source, page),
          })}
        />
      </DocsBody>
    </DocsPage>
  );
}

export function generateStaticParams() {
  const params = source.generateParams();
  return [{ slug: [] }, ...params];
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
  const { slug } = await params;
  const page = source.getPage(resolveDocsSlug(slug));
  if (!page) return {};

  const data = page.data;
  return {
    title: data.title,
    description: data.description,
    alternates: { canonical: page.url },
  };
}
