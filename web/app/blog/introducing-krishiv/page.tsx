import type { Metadata } from 'next';
import Link from 'next/link';
import { Badge, SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Introducing Krishiv: One Engine for Batch, Streaming, and Incremental Data Processing',
  description:
    'A codebase-grounded overview of the Krishiv unified compute engine — why it exists, what it does today, and how it runs batch SQL, streaming, and incremental view maintenance under one Apache Arrow / DataFusion runtime.',
  openGraph: {
    title: 'Introducing Krishiv — One Engine for Batch, Streaming, and Incremental Processing',
    description:
      'A codebase-grounded overview of the Krishiv unified compute engine vision.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/blog/introducing-krishiv',
  },
};

export default function Post(){return <SiteShell><main className="article"><Badge tone="orange">Introducing Krishiv</Badge><h1>Introducing Krishiv: One Engine for Batch, Streaming, and Incremental Data Processing</h1><p>Data teams often split closely related work across batch jobs, streaming systems, and separate incremental pipelines. Krishiv is being built around a different shape: one Rust-native compute framework that keeps Arrow data, DataFusion planning, runtime routing, scheduler/executor behavior, state, shuffle, checkpoints, and connectors in one coherent architecture.</p><h2>What exists now</h2><p>Krishiv has implemented batch SQL foundations, streaming APIs and windowing examples, explicit embedded/single-node/distributed runtime modes, scheduler and executor crates, shuffle/state/checkpoint abstractions, and Python bindings. Iceberg is the primary lakehouse target, while Kafka, Parquet, S3/object-store, and catalog integrations are represented with preview maturity where certification is still pending.</p><h2>Why Rust, Arrow, and DataFusion</h2><p>Rust and Tokio provide the runtime foundation. Apache Arrow RecordBatch is the columnar data model and IPC shape. DataFusion gives Krishiv SQL parsing, planning, expressions, and local execution so the project can focus its engine work on runtime placement, dataflow, state, shuffle, checkpoints, and connector boundaries.</p><h2>Batch, streaming, and delta batch / IVM</h2><p>Batch SQL is available in the source workspace. Streaming is Preview, while DeltaBatch and IncrementalFlow provide experimental incremental view maintenance based on weighted Arrow rows. Distributed execution and connector guarantees remain subject to explicit maturity labels.</p><h2>Where to go next</h2><p>Read the <Link href="/docs/engine">Engine docs</Link> for current APIs and the <Link href="/architecture">architecture page</Link> for the system boundaries. The website copy is deliberately conservative: it avoids benchmarks, competitor comparisons, and unsupported guarantees.</p></main></SiteShell>}
