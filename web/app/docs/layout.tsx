import type { ReactNode } from 'react';
import { DocsLayout } from 'fumadocs-ui/layouts/docs';
import { baseOptions } from '@/lib/layout.shared';
import { source } from '@/lib/source';

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <>
      <a className="skip-link" href="#main-content">Skip to main content</a>
      <DocsLayout
        {...baseOptions()}
        tree={source.getPageTree()}
        tabMode="auto"
        sidebar={{ defaultOpenLevel: 1, prefetch: false }}
      >
        {children}
      </DocsLayout>
    </>
  );
}
