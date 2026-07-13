'use client';

import type { ReactNode } from 'react';
import { RootProvider } from 'fumadocs-ui/provider/next';
import DocsSearch from '@/components/DocsSearch';

export function Providers({ children }: { children: ReactNode }) {
  return (
    <RootProvider
      search={{ SearchDialog: DocsSearch }}
      theme={{ enabled: false, defaultTheme: 'dark', forcedTheme: 'dark' }}
    >
      {children}
    </RootProvider>
  );
}
