import versions from '../versions.json';

export type DocsVersion = {
  label: string;
  slug: string;
  branch: string;
  version: string;
  default?: boolean;
};

export const docsVersions = versions as DocsVersion[];
export const latestDocsVersion = docsVersions.find((version) => version.default) ?? docsVersions[0];
