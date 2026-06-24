import Link from 'next/link';
import type { JSX, ReactNode } from 'react';
import { BrandLogo, SiteShell } from '@/components/Shell';
import { CodeBlock, CodeBlockTabs, CodeBlockTabsList, CodeBlockTabsTrigger, CodeBlockTab, Pre } from 'fumadocs-ui/components/codeblock';
import { githubUrl } from '@/lib/site';

type IconName =
  | 'terminal'
  | 'rust'
  | 'python'
  | 'runtime'
  | 'arrow'
  | 'scheduler'
  | 'database'
  | 'bolt'
  | 'delta'
  | 'cube'
  | 'snowflake'
  | 'server'
  | 'shield'
  | 'laptop'
  | 'cluster'
  | 'kafka'
  | 'parquet'
  | 'cloud'
  | 'catalog'
  | 'check'
  | 'flask'
  | 'eye';

const iconPaths: Record<IconName, JSX.Element> = {
  terminal: <><path d="M4 6.5 7 9l-3 2.5"/><path d="M9 12h4"/><rect x="2.5" y="3" width="15" height="14" rx="2"/></>,
  rust: <><path d="M10 3.2v2"/><path d="M10 14.8v2"/><path d="m5.2 5.2 1.4 1.4"/><path d="m13.4 13.4 1.4 1.4"/><path d="M3.2 10h2"/><path d="M14.8 10h2"/><path d="m5.2 14.8 1.4-1.4"/><path d="m13.4 6.6 1.4-1.4"/><circle cx="10" cy="10" r="4.2"/><path d="M8.5 12.1V7.9h2.1a1.2 1.2 0 0 1 0 2.4H8.5"/><path d="m10.7 10.3 1.5 1.8"/></>,
  python: <><path d="M10 3.2H7.4A2.4 2.4 0 0 0 5 5.6v2.1h5.9A2.1 2.1 0 0 1 13 9.8v4.6a2.4 2.4 0 0 1-2.4 2.4H8"/><path d="M10 16.8h2.6a2.4 2.4 0 0 0 2.4-2.4v-2.1H9.1A2.1 2.1 0 0 1 7 10.2V5.6a2.4 2.4 0 0 1 2.4-2.4H12"/><path d="M8.2 5.3h.1"/><path d="M11.8 14.7h.1"/></>,
  runtime: <><path d="M10 2.8v4.4"/><path d="M10 12.8v4.4"/><path d="M2.8 10h4.4"/><path d="M12.8 10h4.4"/><path d="M10 7.2 12.8 10 10 12.8 7.2 10Z"/></>,
  arrow: <><path d="m4 11 6-8"/><path d="M8 11h8l-6 6"/><path d="m8.6 7.5 2.8 2.5-2.8 2.5"/></>,
  scheduler: <><circle cx="5" cy="5" r="1"/><circle cx="10" cy="5" r="1"/><circle cx="15" cy="5" r="1"/><circle cx="5" cy="10" r="1"/><circle cx="10" cy="10" r="1"/><circle cx="15" cy="10" r="1"/><circle cx="5" cy="15" r="1"/><circle cx="10" cy="15" r="1"/><circle cx="15" cy="15" r="1"/></>,
  database: <><ellipse cx="10" cy="4.5" rx="6" ry="2.5"/><path d="M4 4.5v11c0 1.4 2.7 2.5 6 2.5s6-1.1 6-2.5v-11"/><path d="M4 10c0 1.4 2.7 2.5 6 2.5s6-1.1 6-2.5"/></>,
  bolt: <path d="m11 2-7 9h5l-1 7 8-10h-5l0-6Z"/>,
  delta: <><path d="M15 5 5 15"/><path d="M5 5h10v10"/><circle cx="5" cy="15" r="2"/><circle cx="15" cy="5" r="2"/></>,
  cube: <><path d="m10 2.8 6 3.4v7.2l-6 3.8-6-3.8V6.2Z"/><path d="m4 6.2 6 3.5 6-3.5"/><path d="M10 9.7v7.5"/></>,
  snowflake: <><path d="M10 2v16"/><path d="m4 5 12 10"/><path d="m16 5-12 10"/><path d="m7.5 3.8 2.5 2.1 2.5-2.1"/><path d="m7.5 16.2 2.5-2.1 2.5 2.1"/></>,
  server: <><rect x="3" y="3" width="14" height="5" rx="1.4"/><rect x="3" y="12" width="14" height="5" rx="1.4"/><path d="M6 5.5h.1"/><path d="M6 14.5h.1"/><path d="M8.5 5.5H14"/><path d="M8.5 14.5H14"/></>,
  shield: <path d="M10 2.5 16 5v4.7c0 3.7-2.4 6.5-6 7.8-3.6-1.3-6-4.1-6-7.8V5Z"/>,
  laptop: <><rect x="4" y="4" width="12" height="8" rx="1"/><path d="M2.8 15h14.4"/></>,
  cluster: <><rect x="4" y="3" width="5" height="4" rx="1"/><rect x="11" y="3" width="5" height="4" rx="1"/><rect x="7.5" y="13" width="5" height="4" rx="1"/><path d="M6.5 7v3h7V7"/><path d="M10 10v3"/></>,
  kafka: <><circle cx="10" cy="10" r="1.5"/><circle cx="5" cy="5" r="1.5"/><circle cx="15" cy="5" r="1.5"/><circle cx="5" cy="15" r="1.5"/><circle cx="15" cy="15" r="1.5"/><path d="M8.9 8.9 6.1 6.1"/><path d="m11.1 8.9 2.8-2.8"/><path d="m8.9 11.1-2.8 2.8"/><path d="m11.1 11.1 2.8 2.8"/></>,
  parquet: <><path d="m4 12 8-8"/><path d="m7 15 8-8"/><path d="M5.5 5.5h8v8h-8Z"/></>,
  cloud: <path d="M6.5 16h8a3.5 3.5 0 0 0 .4-7 5 5 0 0 0-9.6-1.5A4.3 4.3 0 0 0 6.5 16Z"/>,
  catalog: <><path d="M5 4h10v13H5z"/><path d="M8 4v13"/><path d="M10.5 7H13"/><path d="M10.5 10H13"/></>,
  check: <><path d="m4 10 4 4 8-9"/></>,
  flask: <><path d="M7 3v5L3 16a2 2 0 0 0 1.7 3h10.6A2 2 0 0 0 17 16L13 8V3"/><path d="M7 3h6"/><path d="M5 13h10"/></>,
  eye: <><path d="M2 10s2.7-5 8-5 8 5 8 5-2.7 5-8 5-8-5-8-5Z"/><circle cx="10" cy="10" r="2.5"/></>,
};

function LineIcon({ name, className = '' }: { name: IconName; className?: string }) {
  return <svg className={className} viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.65" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{iconPaths[name]}</svg>;
}

function HeroContent() {
  return (
    <div className="hero-copy">
      <div className="hero-badge">Rust-native <span/> Unified Compute Engine</div>
      <h1>One engine for<br/><em>Batch</em>, Streaming,<br/>and <em>Incremental</em><br/>Processing</h1>
      <p className="hero-lead">
        Krishiv is a Rust-native compute framework for batch SQL, streaming pipelines, and incremental
        view maintenance. Apache&nbsp;Arrow is the in-memory data model; DataFusion drives SQL.
      </p>
      <p className="hero-lead" style={{ fontSize: 14, color: 'var(--muted-strong)', marginTop: -8 }}>
        See <Link href="/product/maturity" style={{ textDecoration: 'underline' }}>feature maturity</Link> for what is currently production-ready vs. preview.
      </p>
      <div className="hero-actions">
        <Link className="btn btn-primary" href="/docs/latest/getting-started">Get Started <span aria-hidden="true">→</span></Link>
        <a className="btn btn-secondary" href={githubUrl}><GithubMark/> Star on GitHub</a>
      </div>
    </div>
  );
}

function RuntimeArchitectureDiagram() {
  const interfaces: Array<[IconName, string]> = [['terminal', 'SQL'], ['rust', 'Rust'], ['python', 'Python']];
  const layers: Array<[IconName, string, string]> = [
    ['runtime', 'Krishiv Runtime', 'Batch · Streaming · Incremental Processing'],
    ['arrow', 'DataFusion · Apache Arrow', ''],
    ['scheduler', 'Scheduling · Shuffle · State · Checkpoints', ''],
    ['database', 'Iceberg · Kafka · Parquet · Object Storage · Catalogs', ''],
  ];
  return (
    <div className="architecture-stage" aria-label="Krishiv runtime architecture diagram">
      <div className="interface-row">{interfaces.map(([icon, label]) => <div className="interface-item" key={label}><LineIcon name={icon}/><span>{label}</span></div>)}</div>
      <div className="interface-lines" aria-hidden="true"><span/><span/><span/></div>
      <div className="runtime-stack">
        {layers.map(([icon, title, sub], index) => <div className="runtime-layer" key={title}><LineIcon name={icon}/><div><strong>{title}</strong>{sub && <p>{sub}</p>}</div>{index === 0 && <span className="layer-glow"/>}</div>)}
      </div>
    </div>
  );
}

type Tier = 'green' | 'violet' | 'blue' | 'gray';
const tierLabel: Record<Tier, string> = { green: 'Available', violet: 'Experimental', blue: 'Preview', gray: 'Planned' };

const capabilities: Array<{ icon: IconName; title: string; tier: Tier; text: string }> = [
  { icon: 'bolt', title: 'Unified Engine', tier: 'green', text: 'One runtime for batch, streaming, and incremental processing — shared Arrow batches, shared planning.' },
  { icon: 'delta', title: 'Incremental Processing', tier: 'violet', text: 'DeltaBatch (weighted Arrow rows) and IncrementalFlow for view maintenance. Distributed executor IVM is in progress.' },
  { icon: 'cube', title: 'Rust-Native Performance', tier: 'green', text: 'Rust 2024 + Tokio. Typed IDs, typed plans, typed errors, explicit capability flags.' },
  { icon: 'snowflake', title: 'Iceberg Lakehouse', tier: 'blue', text: 'Apache Iceberg is the primary lakehouse target. REST, Hive, and Glue catalog paths; certification work continues.' },
  { icon: 'server', title: 'Embedded & Single-Node', tier: 'green', text: 'Run in-process for tests and apps, or as a local daemon with RocksDB and local shuffle.' },
  { icon: 'cluster', title: 'Distributed Mode', tier: 'blue', text: 'Remote coordinator + executor transport. Requires an explicit endpoint; no silent local fallback.' },
];

function CapabilityStrip() {
  return (
    <section className="capability-strip" aria-label="Krishiv capabilities">
      {capabilities.map((c) => (
        <article key={c.title} className="capability-item">
          <LineIcon name={c.icon}/>
          <div>
            <h3>{c.title} <span className={`badge badge-${c.tier}`} style={{ marginLeft: 8, verticalAlign: 'middle', fontSize: 10 }}>{tierLabel[c.tier]}</span></h3>
            <p>{c.text}</p>
          </div>
        </article>
      ))}
      <p className="muted" style={{ width: '100%', textAlign: 'center', fontSize: 13, marginTop: 8 }}>
        Status reflects what is implemented today. See <Link href="/product/maturity" style={{ textDecoration: 'underline' }}>Feature Maturity</Link> for the full matrix.
      </p>
    </section>
  );
}

function ExecutionJourney() {
  const cards: Array<[IconName, string, string, Tier]> = [
    ['laptop', 'Embedded', 'Run and debug in-process. No daemon, no cluster.', 'green'],
    ['server', 'Single Node', 'Local daemon with RocksDB state and local shuffle.', 'green'],
    ['cluster', 'Distributed', 'Remote coordinator + executors. Explicit endpoint, no silent fallback.', 'blue'],
  ];
  return <div className="journey-cards">{cards.map(([icon, title, text, tier], index) => <article className={`journey-card ${index === 0 ? 'active' : ''}`} key={title}><LineIcon name={icon}/><h3>{title} <span className={`badge badge-${tier}`} style={{ marginLeft: 6, fontSize: 10 }}>{tierLabel[tier]}</span></h3><p>{text}</p>{index < cards.length - 1 && <span className="journey-connector"/>}</article>)}</div>;
}

function DeveloperSection() {
  return (
    <section className="developer-section">
      <div className="developer-copy">
        <p className="section-eyebrow">Developer Experience</p>
        <h2>Start locally.<br/>Scale to single-node.<br/>Move to distributed when ready.</h2>
        <p>Same APIs. Same engine. Krishiv grows with your workload — and the engine is honest about what is production-ready vs. preview.</p>
        <ExecutionJourney/>
      </div>
      <CodeExamplePanel/>
    </section>
  );
}

function CodeExamplePanel() {
  return (
    <div className="code-panel">
      <CodeBlockTabs defaultValue="sql">
        <CodeBlockTabsList>
          <CodeBlockTabsTrigger value="sql">SQL</CodeBlockTabsTrigger>
          <CodeBlockTabsTrigger value="rust">Rust</CodeBlockTabsTrigger>
          <CodeBlockTabsTrigger value="python">Python</CodeBlockTabsTrigger>
        </CodeBlockTabsList>
        <CodeBlockTab value="sql">
          <CodeBlock>
            <Pre>
{`SELECT customer_id, SUM(amount) AS total_spend
FROM orders
WHERE event_time >= NOW() - INTERVAL '1' DAY
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 10;`}
            </Pre>
          </CodeBlock>
        </CodeBlockTab>
        <CodeBlockTab value="rust">
          <CodeBlock>
            <Pre>
{`use krishiv_api::{Session, col, lit, sum, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let session = Session::embedded().await?;
    let df = session
        .read_parquet("data/orders.parquet").await?
        .filter(col("amount").gt(lit(100)))?
        .group_by(vec![col("customer_id")])?
        .agg(vec![sum(col("amount")).alias("total_spend")])?
        .sort(vec![col("total_spend").desc()])?
        .limit(10);
    df.show().await?;
    Ok(())
}`}
            </Pre>
          </CodeBlock>
        </CodeBlockTab>
        <CodeBlockTab value="python">
          <CodeBlock>
            <Pre>
{`import krishiv as ks
from krishiv.functions import col, lit, sum

session = ks.Session.embedded()

df = (session.read_parquet("data/orders.parquet")
        .filter(col("amount") > lit(100))
        .group_by(["customer_id"])
        .agg([sum(col("amount")).alias("total_spend")])
        .order_by(["total_spend"], ascending=False)
        .limit(10))
df.show()`}
            </Pre>
          </CodeBlock>
        </CodeBlockTab>
      </CodeBlockTabs>
      <Link className="docs-link" href="/docs/latest/tutorial">→ A full end-to-end tutorial</Link>
    </div>
  );
}

function EcosystemRow() {
  const items: Array<[IconName, string, Tier]> = [
    ['snowflake', 'Apache Iceberg', 'blue'],
    ['kafka', 'Apache Kafka', 'blue'],
    ['parquet', 'Parquet', 'green'],
    ['cloud', 'S3 / ADLS / GCS', 'blue'],
    ['catalog', 'Hive / Glue / REST catalogs', 'blue'],
  ];
  return (
    <section className="ecosystem-row" aria-label="Connector ecosystem">
      {items.map(([icon, label, tier]) => <span key={label}><LineIcon name={icon}/>{label} <span className={`badge badge-${tier}`} style={{ marginLeft: 4, fontSize: 9 }}>{tierLabel[tier]}</span></span>)}
      <span>and more…</span>
    </section>
  );
}

function WhyKrishivSection() {
  const cols: Array<{ icon: IconName; title: string; body: ReactNode }> = [
    { icon: 'check', title: 'Honest maturity labels', body: <>Every page is tagged with <Link href="/product/maturity" style={{ textDecoration: 'underline' }}>Available / Experimental / Preview / Planned</Link>. The codebase backs the labels, not the other way around.</> },
    { icon: 'flask', title: 'Recipes, not just reference', body: <>Task-oriented <Link href="/docs/latest/recipes" style={{ textDecoration: 'underline' }}>recipes</Link> for the things you actually do: tumbling windows, Iceberg upserts, Kafka→Parquet, exactly-once pipelines.</> },
    { icon: 'eye', title: 'See how a query runs', body: <>A dedicated <Link href="/docs/latest/concepts/how-it-executes" style={{ textDecoration: 'underline' }}>execution walkthrough</Link> follows a SQL query from <code>session.sql(...)</code> through DataFusion, the coordinator, executors, and Arrow batches.</> },
  ];
  return (
    <section className="developer-section" aria-label="Why Krishiv">
      <div className="developer-copy">
        <p className="section-eyebrow">Documentation that respects your time</p>
        <h2>Why read these docs.</h2>
        <p>Most engine sites are either encyclopedic reference or aspirational marketing. We try to be neither: the docs lead with what the engine actually does today, and how to do the next thing on your list.</p>
      </div>
      <div className="grid" style={{ gridTemplateColumns: '1fr', gap: 12 }}>
        {cols.map((c) => (
          <div className="card" key={c.title}>
            <h3 style={{ display: 'flex', alignItems: 'center', gap: 8 }}><LineIcon name={c.icon}/> {c.title}</h3>
            <p>{c.body}</p>
          </div>
        ))}
      </div>
    </section>
  );
}

function GithubMark() { return <svg viewBox="0 0 20 20" width="18" height="18" fill="currentColor" aria-hidden="true"><path d="M10 .9a9.1 9.1 0 0 0-2.9 17.7c.46.08.63-.2.63-.44v-1.6c-2.57.56-3.11-1.1-3.11-1.1-.42-1.07-1.03-1.03-1.03-1.03-.84-.58.06-.57.06-.57.93.07 1.42.96 1.42.96.83 1.41 2.18 1 2.71.77.08-.6.32-1 .59-1.23-2.05-.23-4.2-1.02-4.2-4.55 0-1 .36-1.83.95-2.47-.1-.24-.41-1.18.09-2.44 0 0 .78-.25 2.5.94A8.7 8.7 0 0 1 10 5.2c.77 0 1.54.1 2.27.3 1.72-1.19 2.5-.94 2.5-.94.5 1.26.19 2.2.1 2.44.59.64.94 1.46.94 2.47 0 3.54-2.16 4.31-4.21 4.54.33.29.63.85.63 1.72v2.55c0 .25.17.53.64.44A9.1 9.1 0 0 0 10 .9Z"/></svg>; }

export default function Home() {
  return (
    <SiteShell>
      <main className="landing">
        <section className="hero"><HeroContent/><RuntimeArchitectureDiagram/></section>
        <CapabilityStrip/>
        <DeveloperSection/>
        <WhyKrishivSection/>
        <EcosystemRow/>
      </main>
    </SiteShell>
  );
}
