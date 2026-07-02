import type { Metadata } from 'next';
import { SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Community',
  description:
    'Krishiv community — contribute, discuss, and get support for the Rust-native compute engine.',
  openGraph: {
    title: 'Krishiv Community',
    description: 'Contribute, discuss, and get support for the Krishiv compute engine.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/community',
  },
};

export default function Community(){return <SiteShell><main className="container placeholder"><div><p className="eyebrow">Community</p><h1>Community resources are planned.</h1><p className="lead">Contribution, governance, and support links will live here.</p></div></main></SiteShell>}
