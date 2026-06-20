import { docsVersions } from './versions';
import { gettingStartedPages } from './docs-content/getting-started';
import { sqlPages } from './docs-content/sql';
import { rustPages } from './docs-content/rust';
import { pythonPages } from './docs-content/python';
import { connectorsPages } from './docs-content/connectors';

export type DocStatus = 'Available' | 'Experimental' | 'In Progress' | 'Preview';

export type DocPage = {
  slug: string;
  title: string;
  description: string;
  status: DocStatus;
  group: string;
  body: string; // HTML content
};

// Ordered group list – determines sidebar order.
export const GROUP_ORDER = [
  'Getting Started',
  'Concepts',
  'SQL Reference',
  'Rust API',
  'Python API',
  'Connectors',
  'Operations',
] as const;

export const docPages: DocPage[] = [
  ...gettingStartedPages,
  ...sqlPages,
  ...rustPages,
  ...pythonPages,
  ...connectorsPages,
];

export type GroupedPages = { group: string; pages: DocPage[] };

export function getGroupedPages(): GroupedPages[] {
  const map = new Map<string, DocPage[]>();
  for (const g of GROUP_ORDER) map.set(g, []);
  for (const p of docPages) {
    const g = p.group;
    if (!map.has(g)) map.set(g, []);
    map.get(g)!.push(p);
  }
  return [...map.entries()]
    .filter(([, pages]) => pages.length > 0)
    .map(([group, pages]) => ({ group, pages }));
}

export function getDoc(version: string, slugParts?: string[]): DocPage | null {
  if (!docsVersions.some((v) => v.slug === version)) return null;
  const slug = (slugParts ?? []).join('/');
  return docPages.find((p) => p.slug === slug) ?? null;
}

export function getAllDocParams() {
  return docsVersions.flatMap((v) =>
    docPages.map((p) => ({ version: v.slug, slug: p.slug ? p.slug.split('/') : [] }))
  );
}
