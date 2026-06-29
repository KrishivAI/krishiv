export const docsVersions = [
  { label: 'Latest', slug: 'latest' },
  { label: 'v0.1', slug: 'v0.1' },
] as const;

export type DocsVersion = (typeof docsVersions)[number]['slug'];
export const defaultDocsVersion: DocsVersion = 'latest';
