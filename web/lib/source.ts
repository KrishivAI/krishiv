import { docs } from 'collections/server';
import { loader } from 'fumadocs-core/source';

export const source = loader({
  baseUrl: '/docs',
  source: docs.toFumadocsSource(),
});

export type DocFrontmatter = {
  title: string;
  description?: string;
  status?: 'Available' | 'Experimental' | 'In Progress' | 'Preview' | 'Planned';
  full?: boolean;
};

export type DocPageData = DocFrontmatter & {
  body?: (props: { components?: Record<string, unknown> }) => React.JSX.Element;
  toc?: Array<{ url: string; text: string; depth: number }>;
};
