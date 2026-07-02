import type { Metadata } from 'next';
import { SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Product',
  description:
    'Krishiv product overview — Rust-native compute engine for batch SQL, streaming pipelines, and incremental view maintenance.',
  alternates: {
    canonical: 'https://krishiv.ai/product',
  },
};

export default function Product(){return <SiteShell><main className="container placeholder"><div><p className="eyebrow">Product</p><h1>Product pages are planned.</h1><p className="lead">This placeholder keeps navigation stable while product-specific pages are developed from codebase-backed facts.</p></div></main></SiteShell>}
