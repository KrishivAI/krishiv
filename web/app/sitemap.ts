import type { MetadataRoute } from 'next';

const baseUrl = 'https://krishiv.ai';
const lastModified = new Date('2026-07-13T00:00:00.000Z');

export const dynamic = 'force-static';

// Keep the sitemap independent from the generated MDX module graph. Importing the
// Fumadocs source here makes Next load every page component while collecting this
// metadata route, which is unnecessary and breaks static export workers.
const docPaths = [
  '/docs/engine',
  '/docs/engine/getting-started',
  '/docs/engine/installation',
  '/docs/engine/maturity',
  '/docs/engine/contributing',
  '/docs/engine/concepts/architecture',
  '/docs/engine/concepts/execution-modes',
  '/docs/engine/concepts/batch',
  '/docs/engine/concepts/streaming',
  '/docs/engine/concepts/incremental',
  '/docs/engine/guides/batch-sql',
  '/docs/engine/guides/streaming',
  '/docs/engine/guides/incremental',
  '/docs/engine/guides/single-node',
  '/docs/engine/operations/observability',
  '/docs/engine/operations/checkpointing',
  '/docs/engine/operations/security',
  '/docs/engine/operations/distributed',
  '/docs/engine/reference/cli',
  '/docs/engine/reference/sql',
  '/docs/engine/reference/rust',
  '/docs/engine/reference/python',
  '/docs/engine/reference/connectors',
  '/docs/engine/reference/configuration',
  '/docs/platform',
] as const;

export default function sitemap(): MetadataRoute.Sitemap {
  const pages: MetadataRoute.Sitemap = [
    { url: `${baseUrl}/`, lastModified, changeFrequency: 'weekly', priority: 1 },
    { url: `${baseUrl}/engine`, lastModified, changeFrequency: 'weekly', priority: 0.9 },
    { url: `${baseUrl}/platform`, lastModified, changeFrequency: 'monthly', priority: 0.8 },
    { url: `${baseUrl}/architecture`, lastModified, changeFrequency: 'monthly', priority: 0.7 },
    { url: `${baseUrl}/product/maturity`, lastModified, changeFrequency: 'weekly', priority: 0.7 },
    { url: `${baseUrl}/blog`, lastModified, changeFrequency: 'monthly', priority: 0.6 },
    { url: `${baseUrl}/blog/introducing-krishiv`, lastModified, changeFrequency: 'monthly', priority: 0.5 },
    { url: `${baseUrl}/release-notes`, lastModified, changeFrequency: 'monthly', priority: 0.5 },
    { url: `${baseUrl}/release-notes/v0.1.0-rc.1`, lastModified, changeFrequency: 'monthly', priority: 0.4 },
  ];

  const docs: MetadataRoute.Sitemap = docPaths.map((path) => ({
    url: `${baseUrl}${path}`,
    lastModified,
    changeFrequency: 'monthly',
    priority: path.split('/').length === 3 ? 0.8 : 0.65,
  }));

  return [...pages, ...docs];
}
