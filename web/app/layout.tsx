import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import { RootProvider } from 'fumadocs-ui/provider/next';
import './global.css';

export const metadata: Metadata = {
  title: {
    default: 'Krishiv — Unified batch SQL, streaming, and IVM',
    template: '%s | Krishiv',
  },
  description:
    'Krishiv is a Rust-native hybrid compute framework for batch SQL, streaming pipelines, and lakehouse-oriented data work.',
};

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="dark" suppressHydrationWarning>
      <body>
        <RootProvider>{children}</RootProvider>
      </body>
    </html>
  );
}
