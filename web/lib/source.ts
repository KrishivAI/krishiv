import { docs } from 'collections/server';
import { loader } from 'fumadocs-core/source';

export const source = loader({
  baseUrl: '/docs',
  source: docs.toFumadocsSource(),
});

export function resolveDocsSlug(slug?: string[]) {
  if (!slug || slug.length === 0) return ['engine'];
  return slug;
}
