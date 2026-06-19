import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
          <img src="/krishiv-mark.svg" alt="" width={24} height={24} />
          Krishiv
        </span>
      ),
    },
    links: [
      { text: 'Docs', url: '/docs' },
      { text: 'Blog', url: '/blog' },
      { text: 'Examples', url: '/examples' },
      { text: 'Changelog', url: '/changelog' },
      { text: 'GitHub', url: 'https://github.com/KrishivAI/krishiv', external: true },
    ],
  };
}
