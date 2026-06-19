import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { basePath } from '@/lib/base-path';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
          <img src={`${basePath}/krishiv-mark.svg`} alt="" width={24} height={24} />
          Krishiv
        </span>
      ),
    },
    links: [
      { text: 'Docs', url: `${basePath}/docs` },
      { text: 'Blog', url: `${basePath}/blog` },
      { text: 'Examples', url: `${basePath}/examples` },
      { text: 'Changelog', url: `${basePath}/changelog` },
      { text: 'GitHub', url: 'https://github.com/KrishivAI/krishiv', external: true },
    ],
  };
}
