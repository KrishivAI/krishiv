import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import './global.css';

export const metadata: Metadata = {
  title: { default: 'Krishiv — One Engine for Batch and Streaming', template: '%s | Krishiv' },
  description: 'Krishiv is a Rust-native compute engine for unified batch, streaming, and incremental data processing.',
  icons: { icon: '/brand/favicon.svg' },
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return <html lang="en"><body>{children}</body></html>;
}
