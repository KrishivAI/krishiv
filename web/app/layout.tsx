import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import './global.css';

const siteUrl = 'https://krishiv.ai';
const siteName = 'Krishiv';
const defaultDescription =
  'Krishiv is a Rust-native compute engine for batch SQL, streaming pipelines, and incremental view maintenance. Apache Arrow data model, DataFusion SQL, embedded to distributed.';

export const metadata: Metadata = {
  metadataBase: new URL(siteUrl),
  title: {
    default: `${siteName} — Rust-native batch, streaming & incremental compute`,
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
    'lakehouse engine',
    'Iceberg compute',
    'distributed SQL',
    'stream processing',
    'Rust DataFrame',
    'Python DataFrame',
    'data pipeline engine',
    'analytics engine',
    'Spark alternative',
    'Flink alternative',
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
    title: `${siteName} — Rust-native batch, streaming & incremental compute`,
    description: defaultDescription,
    images: [
      {
        url: '/brand/og-image.png',
        width: 1200,
        height: 630,
        alt: `${siteName} — Rust-native compute engine`,
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: `${siteName} — Rust-native batch, streaming & incremental compute`,
    description: defaultDescription,
    images: ['/brand/og-image.png'],
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
    'theme-color': '#050505',
    'msapplication-TileColor': '#050505',
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
  potentialAction: {
    '@type': 'SearchAction',
    target: {
      '@type': 'EntryPoint',
      urlTemplate: `${siteUrl}/docs/latest?q={search_term_string}`,
    },
    'query-input': 'required name=search_term_string',
  },
};

const softwareJsonLd = {
  '@context': 'https://schema.org',
  '@type': 'SoftwareApplication',
  name: siteName,
  description: defaultDescription,
  url: siteUrl,
  applicationCategory: 'DeveloperApplication',
  operatingSystem: 'Linux, macOS, Windows',
  offers: {
    '@type': 'Offer',
    price: '0',
    priceCurrency: 'USD',
  },
  softwareVersion: '0.1.0',
  programmingLanguage: ['Rust', 'Python'],
  runtimePlatform: 'Linux, macOS, Windows',
  downloadUrl: 'https://github.com/KrishivAI/krishiv/releases',
  installUrl: `${siteUrl}/docs/latest/getting-started`,
  screenshot: `${siteUrl}/brand/og-image.png`,
  featureList: [
    'Batch SQL execution',
    'Streaming pipelines',
    'Incremental view maintenance',
    'Apache Arrow columnar memory',
    'DataFusion SQL engine',
    'Apache Iceberg lakehouse',
    'Embedded, single-node, and distributed modes',
    'Python and Rust APIs',
    'Kafka, Parquet, S3 connectors',
  ],
  keywords: 'Rust, compute engine, SQL, streaming, batch, Arrow, DataFusion, Iceberg, lakehouse',
  license: 'https://github.com/KrishivAI/krishiv/blob/main/LICENSE',
  codeRepository: 'https://github.com/KrishivAI/krishiv',
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
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
      <body>{children}</body>
    </html>
  );
}
