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
    <DocsPage toc={data.toc}>
      <div className="docs-page-kicker">
        <DocsStatus status={data.status as DocsStatusName | undefined} />
      </div>
      <DocsTitle>{data.title}</DocsTitle>
      <DocsDescription>{data.description}</DocsDescription>
      <ViewOptionsPopover
        githubUrl={`${githubUrl}/blob/main/web/content/docs/${page.path}`}
      />
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
  const engineAliases = source
    .getPages()
    .filter((page) => page.slugs[0] === 'engine')
    .flatMap((page) => {
      const rest = page.slugs.slice(1);
      return [{ slug: ['latest', ...rest] }, { slug: ['v0.1', ...rest] }];
    });

  return [{ slug: [] }, ...params, ...engineAliases];
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
