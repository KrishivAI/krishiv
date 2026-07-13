import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import './global.css';
import './docs.css';
import { Providers } from '@/components/Providers';

const siteUrl = 'https://krishiv.ai';
const siteName = 'Krishiv';
const defaultDescription =
  'Krishiv builds Rust-native data infrastructure: an Apache-2.0 compute engine available in developer preview, with an integrated self-hosted platform coming soon.';

export const metadata: Metadata = {
  metadataBase: new URL(siteUrl),
  title: {
    default: `${siteName} — Engine developer preview. Platform coming soon.`,
    template: `%s | ${siteName}`,
  },
  description: defaultDescription,
  keywords: [
    'Rust compute engine',
    'batch SQL',
    'streaming engine',
    'incremental processing',
    'Apache Arrow',
    'DataFusion',
    'stateful streaming',
    'incremental view maintenance',
    'stream processing',
    'Rust DataFrame',
    'Python DataFrame',
    'data pipeline engine',
    'self-hosted data platform',
  ],
  authors: [{ name: 'KrishivAI', url: siteUrl }],
  creator: 'KrishivAI',
  publisher: 'KrishivAI',
  formatDetection: {
    email: false,
    address: false,
    telephone: false,
  },
  openGraph: {
    type: 'website',
    locale: 'en_US',
    url: siteUrl,
    siteName,
    title: `${siteName} — Engine developer preview. Platform coming soon.`,
    description: defaultDescription,
  },
  twitter: {
    card: 'summary',
    title: `${siteName} — Engine developer preview. Platform coming soon.`,
    description: defaultDescription,
    creator: '@krishivai',
  },
  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      'max-video-preview': -1,
      'max-image-preview': 'large',
      'max-snippet': -1,
    },
  },
  alternates: {
    canonical: siteUrl,
  },
  icons: {
    icon: '/brand/favicon.svg',
    shortcut: '/brand/favicon.svg',
    apple: '/brand/logo-mark.svg',
  },
  manifest: '/manifest.json',
  other: {
    'theme-color': '#000000',
    'msapplication-TileColor': '#000000',
  },
};

const organizationJsonLd = {
  '@context': 'https://schema.org',
  '@type': 'Organization',
  name: siteName,
  url: siteUrl,
  logo: `${siteUrl}/brand/logo-mark.svg`,
  description: defaultDescription,
  sameAs: ['https://github.com/KrishivAI/krishiv'],
  foundingDate: '2026',
  contactPoint: {
    '@type': 'ContactPoint',
    contactType: 'customer support',
    url: 'https://github.com/KrishivAI/krishiv/issues',
  },
};

const websiteJsonLd = {
  '@context': 'https://schema.org',
  '@type': 'WebSite',
  name: siteName,
  url: siteUrl,
  description: defaultDescription,
  publisher: {
    '@type': 'Organization',
    name: siteName,
    logo: `${siteUrl}/brand/logo-mark.svg`,
  },
};

const softwareJsonLd = {
  '@context': 'https://schema.org',
  '@type': 'SoftwareApplication',
  name: 'Krishiv Engine',
  description:
    'Apache-2.0, Rust-native compute for batch SQL and preview stateful streaming, with experimental incremental view maintenance.',
  url: `${siteUrl}/engine`,
  applicationCategory: 'DeveloperApplication',
  operatingSystem: 'Linux, macOS',
  offers: {
    '@type': 'Offer',
    price: '0',
    priceCurrency: 'USD',
  },
  softwareVersion: 'Developer preview',
  programmingLanguage: ['Rust', 'Python'],
  downloadUrl: 'https://github.com/KrishivAI/krishiv',
  installUrl: `${siteUrl}/docs/engine/getting-started`,
  featureList: [
    'Batch SQL execution',
    'Preview stateful streaming',
    'Experimental incremental view maintenance',
    'Apache Arrow data model',
    'DataFusion SQL planning and local execution',
    'Embedded and single-node execution',
    'Preview distributed execution',
    'Python and Rust APIs',
  ],
  keywords: 'Rust, compute engine, SQL, streaming, batch, Arrow, DataFusion',
  license: 'https://github.com/KrishivAI/krishiv/blob/main/LICENSE',
  codeRepository: 'https://github.com/KrishivAI/krishiv',
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="dark" suppressHydrationWarning>
      <head>
        <link rel="preconnect" href="https://github.com" crossOrigin="anonymous" />
        <link rel="dns-prefetch" href="https://github.com" />
        <link rel="alternate" type="application/rss+xml" title="Krishiv Blog" href="/feed.xml" />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(organizationJsonLd) }}
        />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(websiteJsonLd) }}
        />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(softwareJsonLd) }}
        />
      </head>
      <body className="flex min-h-screen flex-col">
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
