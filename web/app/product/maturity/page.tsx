import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowIcon, InteriorHero, SectionIntro } from '@/components/InteriorPage';
import { Badge, SiteShell } from '@/components/Shell';
import { githubUrl } from '@/lib/site';

export const metadata: Metadata = {
  title: 'Engine Feature Maturity',
  description:
    'Repository-backed maturity labels for Krishiv Engine batch SQL, streaming, connectors, distributed execution, and incremental view maintenance.',
  openGraph: {
    title: 'Krishiv Engine Feature Maturity',
    description:
      'Repository-backed capability status for Krishiv Engine, including Available, Preview, Experimental, and In progress work.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/product/maturity',
  },
};

type MaturityTier = {
  tone: 'green' | 'violet' | 'blue' | 'gray';
  label: string;
  description: string;
  items: Array<{ name: string; note: string }>;
};

const tiers: MaturityTier[] = [
  {
    tone: 'green',
    label: 'Available',
    description: 'Implemented in the source workspace. The project is still a developer preview, so this is not a production-readiness promise.',
    items: [
      { name: 'Batch SQL', note: 'DataFusion-backed SQL over Apache Arrow RecordBatches and registered sources.' },
      { name: 'Apache Arrow data model', note: 'RecordBatch is the primary in-memory and IPC columnar format.' },
      { name: 'Rust Session / DataFrame API', note: 'Session, DataFrame, and Stream types are the primary Rust-facing API surface.' },
      { name: 'DataFusion SQL planning', note: 'SQL parsing, logical planning, expression evaluation, and local execution via DataFusion.' },
      { name: 'Embedded runtime mode', note: 'Runs all components in-process; no network endpoints required. Used in tests and local API calls.' },
      { name: 'Single-node runtime mode', note: 'Places core Engine components on one host. Durability depends on the configured state, checkpoint, and storage backends.' },
    ],
  },
  {
    tone: 'violet',
    label: 'Experimental',
    description: 'Implemented and functional. APIs and semantics may change. Not certified for production use.',
    items: [
      { name: 'Delta Batch / IVM', note: 'DeltaBatch (weighted Arrow rows) and IncrementalFlow (view maintenance across ticks) are implemented with partitioning, snapshots, and checkpoint hooks. Distributed executor-side IVM execution is deferred.' },
      { name: 'Python connector surface', note: 'The current extension compiles several connector families by default; additional Cargo features add more. Packaging and API boundaries are not yet stable.' },
    ],
  },
  {
    tone: 'blue',
    label: 'Preview',
    description: 'Implementation paths exist, but APIs, recovery behavior, or end-to-end combinations still need certification.',
    items: [
      { name: 'Stateful streaming', note: 'Streaming sessions, windows, state, and job APIs exist. Recovery and connector guarantees remain combination-specific.' },
      { name: 'Python bindings (source)', note: 'PyO3 bindings expose core Session and DataFrame APIs when built from the repository. Names and signatures can change before 1.0.' },
      { name: 'Distributed runtime mode', note: 'Remote coordinator and executor transport with bearer-token auth. Requires explicit Flight endpoint; no silent local fallback.' },
      { name: 'Iceberg integration', note: 'Iceberg-oriented catalog and table paths exist; backend and commit-protocol certification continues.' },
      { name: 'Kafka connector', note: 'Source and sink paths exist. Delivery guarantees depend on the exact source, sink, and checkpoint combination.' },
      { name: 'Parquet and object-store paths', note: 'Connector paths exist. The named S3 registry driver is currently local-backed; remote cloud construction is separately feature-gated.' },
      { name: 'Shuffle service', note: 'In-memory, local disk, object-store, and Flight-oriented shuffle paths behind the krishiv-shuffle crate API.' },
      { name: 'Checkpoint storage', note: 'Async checkpoint primitives with sync compatibility wrappers. Scheduler gRPC checkpoint acks use the async path.' },
      { name: 'State management', note: 'In-memory and RocksDB-backed keyed state, TTL, migration, and incremental state behind the krishiv-state crate API.' },
      { name: 'Kubernetes operator / CRD', note: 'CRD and operator integration in the krishiv-operator crate. Manifests live in k8s/.' },
      { name: 'Scheduler foundations', note: 'Job/task lifecycle, metadata-store, and leadership-coordination code exists; operational fault-tolerance certification is still in progress.' },
    ],
  },
  {
    tone: 'gray',
    label: 'Planned',
    description: 'On the roadmap but not yet implemented. Do not rely on these without maintainer confirmation.',
    items: [
      { name: 'Distributed IVM', note: 'Executor-side incremental view maintenance across a distributed cluster. Requires distributed IVM protocol design.' },
      { name: 'Broad exactly-once certification', note: 'Engine does not claim exactly-once across arbitrary source, sink, state, and checkpoint combinations.' },
    ],
  },
];

const orderedTiers = [tiers[0], tiers[2], tiers[1], tiers[3]];

export default function Maturity() {
  return (
    <SiteShell>
      <main className="ip-page">
        <InteriorHero
          eyebrow="Engine maturity / Evidence map"
          title="Capability status, backed by the source tree."
          description={
            <p>
              Labels describe implementation maturity in the current Engine repository.
              They are not production-readiness promises, support tiers, or hosted-service SLAs.
            </p>
          }
          aside={
            <div className="ip-panel">
              <div className="ip-panel-top">
                <span className="ip-panel-label">Current map</span>
                <span className="ip-panel-badge"><i />Pre-release</span>
              </div>
              <h2 className="ip-panel-title">Four labels. One rule: verify the exact path.</h2>
              <ul className="ip-panel-list">
                {orderedTiers.map((tier) => (
                  <li className="ip-panel-row" key={tier.label}>
                    <span>{tier.label}</span><strong>{tier.items.length} capability groups</strong>
                  </li>
                ))}
              </ul>
              <p className="ip-panel-note">Counts organize this page; they are not a quality score.</p>
            </div>
          }
        >
          <div className="ip-actions">
            <Link className="mk-button mk-button-primary" href="/docs/engine/maturity">Read the policy <ArrowIcon /></Link>
            <a className="mk-button mk-button-secondary" href={githubUrl}>Inspect the source</a>
          </div>
        </InteriorHero>

        <section className="ip-section">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="Current Engine map"
              title="Presence and stability are different things."
              description={<p>A capability can be implemented while its compatibility, recovery behavior, or operating envelope remains unsettled.</p>}
            />

            {orderedTiers.map((tier, index) => (
              <section className="ip-maturity-tier" key={tier.label}>
                <div className="ip-tier-head">
                  <div>
                    <span className="ip-tag">0{index + 1} / Status</span>
                    <h2><Badge tone={tier.tone}>{tier.label}</Badge></h2>
                  </div>
                  <p>{tier.description}</p>
                </div>
                <div className="maturity-grid">
                  {tier.items.map((item) => (
                    <article className="maturity-card" key={item.name}>
                      <h3>{item.name}</h3>
                      <p>{item.note}</p>
                    </article>
                  ))}
                </div>
              </section>
            ))}
          </div>
        </section>

        <section className="ip-section ip-section-contrast">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="How to use this page"
              title="Evaluate a combination, not a badge."
              description={<p>Runtime mode, connector, checkpoint storage, sink semantics, and the pinned commit all influence the real boundary.</p>}
            />
            <div className="ip-card-grid">
              <article className="ip-card">
                <div className="ip-card-top"><span>01</span><span>Scope</span></div>
                <h3>Read the exact surface.</h3>
                <p>A label for Engine does not automatically promote every connector, deployment mode, or language binding.</p>
              </article>
              <article className="ip-card">
                <div className="ip-card-top"><span>02</span><span>Evidence</span></div>
                <h3>Reproduce the path.</h3>
                <p>Prefer public APIs, CLI behavior, checked-in contracts, examples, and tests over dependency presence or roadmap copy.</p>
              </article>
              <article className="ip-card">
                <div className="ip-card-top"><span>03</span><span>Change</span></div>
                <h3>Pin the commit.</h3>
                <p>Pre-release APIs and protocols can change. Record the revision and build features used by an evaluation.</p>
              </article>
            </div>
          </div>
        </section>

        <section className="ip-cta">
          <div className="mk-wrap ip-cta-inner">
            <div>
              <p className="ip-kicker">Separate product status</p>
              <h2>Platform is coming soon and is not part of this matrix.</h2>
              <p>Its documentation remains an availability notice until a public preview creates real setup and API contracts.</p>
            </div>
            <div className="ip-actions">
              <Link className="mk-button mk-button-primary" href="/platform">Platform direction <ArrowIcon /></Link>
              <Link className="mk-button mk-button-secondary" href="/docs/platform">Platform docs</Link>
            </div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
