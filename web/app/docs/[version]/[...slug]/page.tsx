import Link from 'next/link';
import { notFound } from 'next/navigation';
import { Badge, SiteShell } from '@/components/Shell';
import { DocsMobileControls } from '@/components/DocsMobile';
import { source, type DocPageData } from '@/lib/source';
import { docsVersions } from '@/lib/versions';
import { getMDXComponents } from '@/components/mdx-components';
import { buildDocParams } from '@/lib/doc-params';
import { DocsSidebar } from '@/components/DocsSidebar';
import type { ComponentType } from 'react';
import type { MDXComponents } from 'mdx/types';

export default async function DocPage({params}:{params:Promise<{version:string;slug:string[]}>}){
  const {version,slug}=await params;
  const pageSlugs = [version, ...slug];
  const page = source.getPage(pageSlugs);
  if(!page) notFound();

  const data = page.data as unknown as DocPageData & { body?: ComponentType<{ components?: MDXComponents }>; toc?: Array<{ url: string; text: string; depth: number }> };
  const Body = data.body;
  const toc = data.toc ?? [];
  const status = data.status ?? 'Available';
  const description = data.description ?? '';
  const title = data.title;
  const pagePath = `/docs/${version}/${slug.join('/')}`;
  const activeSlug = slug.join('/');
  const headings = toc.filter((h) => h.depth === 2 || h.depth === 3).map((h) => ({ id: h.url.replace('#', ''), text: h.text }));
  const tree = source.pageTree;

  return (
    <SiteShell>
      <DocsMobileControls title={title} version={version} versions={docsVersions} groups={[]} activeSlug={activeSlug} headings={headings} versionPathTemplate={pagePath} />
      <main className="container docs-layout">
        <DocsSidebar tree={tree} version={version} activeSlug={activeSlug} />
        <article className="docs-main">
          <Badge tone={status==='Available'?'green':status==='Experimental'?'violet':status==='Planned'?'gray':'blue'}>
            {status}
          </Badge>
          <h1>{title}</h1>
          {description && <p className="lead">{description}</p>}
          {Body ? (
            <Body components={getMDXComponents()}/>
          ) : (
            <p>Body missing.</p>
          )}
        </article>
        <aside className="toc">
          <strong>On this page</strong>
          {toc.map(h => <a key={h.url} href={h.url}>{h.text}</a>)}
        </aside>
      </main>
    </SiteShell>
  );
}

export async function generateStaticParams(): Promise<Array<{ version: string; slug: string[] }>> {
  const all = await buildDocParams();
  return all.filter((p) => p.slug.length > 0);
}

export const dynamicParams = false;

export async function generateMetadata({params}:{params:Promise<{version:string;slug:string[]}>}){
  const {version,slug}=await params;
  const pageSlugs = [version, ...slug];
  const p=source.getPage(pageSlugs);
  if (!p) return {};
  const d = p.data as unknown as DocPageData;
  return {title:d.title,description:d.description};
}
