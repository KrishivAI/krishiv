import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import './global.css';

export const metadata: Metadata = {
  title: { default: 'Krishiv — Rust-native batch, streaming & incremental compute', template: '%s | Krishiv' },
  description: 'Krishiv is a Rust-native compute framework for batch SQL, streaming pipelines, and incremental view maintenance. Apache Arrow data model, DataFusion SQL, embedded to distributed.',
  icons: { icon: '/brand/favicon.svg' },
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return <html lang="en"><body>{children}</body></html>;
}
