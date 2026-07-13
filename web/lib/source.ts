import { docs } from 'collections/server';
import { loader } from 'fumadocs-core/source';

export const source = loader({
  baseUrl: '/docs',
  source: docs.toFumadocsSource(),
});

export function resolveDocsSlug(slug?: string[]) {
  if (!slug || slug.length === 0) return ['engine'];
  if (slug[0] === 'latest' || slug[0] === 'v0.1') {
    return ['engine', ...slug.slice(1)];
  }
  return slug;
}
