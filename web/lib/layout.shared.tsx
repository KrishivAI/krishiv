import Image from 'next/image';
import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { githubUrl } from '@/lib/site';

function DocsBrand() {
  return (
    <span className="docs-brand">
      <Image src="/brand/logo-mark.svg" alt="" width={28} height={28} />
      <span>Krishiv</span>
      <span className="docs-brand-label">Docs</span>
    </span>
  );
}

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: <DocsBrand />,
      url: '/',
    },
    links: [
      { text: 'Engine', url: '/engine', active: 'nested-url' },
      { text: 'Platform', url: '/platform', active: 'nested-url' },
      { text: 'Docs', url: '/docs/engine', active: 'nested-url' },
    ],
    githubUrl,
    themeSwitch: { enabled: false },
  };
}
