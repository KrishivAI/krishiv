import type { Metadata } from 'next';
import { Badge, SiteShell } from '@/components/Shell';

export const metadata: Metadata = {
  title: 'Krishiv v0.1.0',
  description:
    'Krishiv v0.1.0 release notes — initial release with batch SQL, streaming, embedded/single-node/distributed modes, and Python/Rust APIs.',
  openGraph: {
    title: 'Krishiv v0.1.0 Release Notes',
    description:
      'Initial release with batch SQL, streaming, embedded/single-node/distributed modes, and Python/Rust APIs.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/release-notes/v0.1.0',
  },
};
const entries=[['New','Rust workspace facade, API, SQL, runtime, scheduler, executor, shuffle, state, connectors, metrics, UI, operator, Flight SQL, SQL gateway, and Python crates are present.'],['New','Batch SQL uses DataFusion and Arrow RecordBatch foundations.'],['New','Embedded, single-node, and distributed runtime modes are documented as explicit placements.'],['Experimental','DeltaBatch and IncrementalFlow support incremental view maintenance workflows; distributed executor-side IVM remains in progress.'],['Experimental','Kafka exactly-once candidates and Iceberg/two-phase paths require certification before stronger claims.'],['Improved','Checkpoint storage exposes async scheduler/executor paths plus sync compatibility wrappers.'],['Fixed','Maintainer placeholder: populate from tagged release commits before publishing.'],['Breaking change','Maintainer placeholder: do not publish until verified against API reports.']];
export default function Release(){return <SiteShell><main className="article"><Badge tone="orange">v0.1.0</Badge><h1>Krishiv v0.1.0</h1><p>This template intentionally avoids a fabricated changelog. It lists only repository-verified capabilities and reserves unknown sections for maintainers.</p>{entries.map(([kind,text])=><div className="card" key={kind+text} style={{margin:'14px 0'}}><Badge tone={kind==='Experimental'?'violet':kind==='New'?'green':'blue'}>{kind}</Badge><p>{text}</p></div>)}</main></SiteShell>}
