import type { Metadata } from 'next';
import Link from 'next/link';
import type { JSX, ReactNode } from 'react';
import { SiteShell } from '@/components/Shell';
import { CodeTabs } from '@/components/CodeTabs';
import { githubUrl } from '@/lib/site';

export const metadata: Metadata = {
  title: {
    default: 'Krishiv — Rust-native batch, streaming & incremental compute',
    template: '%s | Krishiv',
  },
  description:
    'Krishiv is a Rust-native compute engine for batch SQL, streaming pipelines, and incremental view maintenance. Apache Arrow data model, DataFusion SQL, embedded to distributed.',
  openGraph: {
    title: 'Krishiv — Rust-native batch, streaming & incremental compute',
    description:
      'One Rust-native engine for batch SQL, streaming pipelines, and incremental view maintenance. Apache Arrow + DataFusion. Embedded, single-node, or distributed.',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Krishiv — Rust-native batch, streaming & incremental compute',
    description:
      'One Rust-native engine for batch SQL, streaming pipelines, and incremental view maintenance. Apache Arrow + DataFusion.',
  },
  alternates: {
    canonical: 'https://krishiv.ai',
  },
};

/* ── Icons ────────────────────────────────────────────────────────────────── */

type IconName =
  | 'bolt' | 'delta' | 'cube' | 'server' | 'cluster' | 'arrow-right'
  | 'check' | 'x' | 'minus' | 'sql' | 'rust' | 'python'
  | 'iceberg' | 'kafka' | 'parquet' | 's3' | 'arrow-icon'
  | 'datafusion' | 'tokio' | 'shield' | 'code' | 'terminal'
  | 'users' | 'cpu' | 'database' | 'cloud' | 'clock' | 'zap';

const iconPaths: Record<IconName, JSX.Element> = {
  bolt: <path d="m11 2-7 9h5l-1 7 8-10h-5l0-6Z"/>,
  delta: <><path d="M15 5 5 15"/><path d="M5 5h10v10"/><circle cx="5" cy="15" r="2"/><circle cx="15" cy="5" r="2"/></>,
  cube: <><path d="m10 2.8 6 3.4v7.2l-6 3.8-6-3.8V6.2Z"/><path d="m4 6.2 6 3.5 6-3.5"/><path d="M10 9.7v7.5"/></>,
  server: <><rect x="3" y="3" width="14" height="5" rx="1.4"/><rect x="3" y="12" width="14" height="5" rx="1.4"/><path d="M6 5.5h.1"/><path d="M6 14.5h.1"/><path d="M8.5 5.5H14"/><path d="M8.5 14.5H14"/></>,
  cluster: <><rect x="4" y="3" width="5" height="4" rx="1"/><rect x="11" y="3" width="5" height="4" rx="1"/><rect x="7.5" y="13" width="5" height="4" rx="1"/><path d="M6.5 7v3h7V7"/><path d="M10 10v3"/></>,
  'arrow-right': <path d="M5 12h14m-7-7 7 7-7 7"/>,
  check: <path d="m4 10 4 4 8-9"/>,
  x: <><path d="m15 9-6 6"/><path d="m9 9 6 6"/></>,
  minus: <path d="M5 12h14"/>,
  sql: <><path d="M4 6.5 7 9l-3 2.5"/><path d="M9 12h4"/><rect x="2.5" y="3" width="15" height="14" rx="2"/></>,
  rust: <><path d="M10 3.2v2"/><path d="M10 14.8v2"/><path d="m5.2 5.2 1.4 1.4"/><path d="m13.4 13.4 1.4 1.4"/><path d="M3.2 10h2"/><path d="M14.8 10h2"/><path d="m5.2 14.8 1.4-1.4"/><path d="m13.4 6.6 1.4-1.4"/><circle cx="10" cy="10" r="4.2"/><path d="M8.5 12.1V7.9h2.1a1.2 1.2 0 0 1 0 2.4H8.5"/><path d="m10.7 10.3 1.5 1.8"/></>,
  python: <><path d="M10 3.2H7.4A2.4 2.4 0 0 0 5 5.6v2.1h5.9A2.1 2.1 0 0 1 13 9.8v4.6a2.4 2.4 0 0 1-2.4 2.4H8"/><path d="M10 16.8h2.6a2.4 2.4 0 0 0 2.4-2.4v-2.1H9.1A2.1 2.1 0 0 1 7 10.2V5.6a2.4 2.4 0 0 1 2.4-2.4H12"/><path d="M8.2 5.3h.1"/><path d="M11.8 14.7h.1"/></>,
  iceberg: <><path d="M10 2 3 14h14L10 2Z"/><path d="M6 14l4-6 4 6"/></>,
  kafka: <><circle cx="10" cy="10" r="1.5"/><circle cx="5" cy="5" r="1.5"/><circle cx="15" cy="5" r="1.5"/><circle cx="5" cy="15" r="1.5"/><circle cx="15" cy="15" r="1.5"/><path d="M8.9 8.9 6.1 6.1"/><path d="m11.1 8.9 2.8-2.8"/><path d="m8.9 11.1-2.8 2.8"/><path d="m11.1 11.1 2.8 2.8"/></>,
  parquet: <><path d="m4 12 8-8"/><path d="m7 15 8-8"/><path d="M5.5 5.5h8v8h-8Z"/></>,
  s3: <path d="M6.5 16h8a3.5 3.5 0 0 0 .4-7 5 5 0 0 0-9.6-1.5A4.3 4.3 0 0 0 6.5 16Z"/>,
  'arrow-icon': <><path d="m4 11 6-8"/><path d="M8 11h8l-6 6"/><path d="m8.6 7.5 2.8 2.5-2.8 2.5"/></>,
  datafusion: <><circle cx="10" cy="10" r="6"/><path d="M10 4v12"/><path d="M4 10h12"/></>,
  tokio: <><circle cx="10" cy="10" r="8"/><path d="M10 6v4l3 3"/></>,
  shield: <path d="M10 2.5 16 5v4.7c0 3.7-2.4 6.5-6 7.8-3.6-1.3-6-4.1-6-7.8V5Z"/>,
  code: <><path d="m16 18 6-6-6-6"/><path d="m8 6-6 6 6 6"/></>,
  terminal: <><path d="M4 6.5 7 9l-3 2.5"/><path d="M9 12h4"/><rect x="2.5" y="3" width="15" height="14" rx="2"/></>,
  users: <><circle cx="9" cy="7" r="3"/><path d="M3 21v-2a4 4 0 0 1 4-4h4a4 4 0 0 1 4 4v2"/><circle cx="15" cy="7" r="3"/><path d="M19 21v-1.5a3 3 0 0 0-3-3h-1"/></>,
  cpu: <><rect x="4" y="4" width="12" height="12" rx="2"/><rect x="8" y="8" width="4" height="4" rx="1"/><path d="M8 2v2"/><path d="M12 2v2"/><path d="M8 16v2"/><path d="M12 16v2"/><path d="M2 8h2"/><path d="M2 12h2"/><path d="M16 8h2"/><path d="M16 12h2"/></>,
  database: <><ellipse cx="10" cy="4.5" rx="6" ry="2.5"/><path d="M4 4.5v11c0 1.4 2.7 2.5 6 2.5s6-1.1 6-2.5v-11"/><path d="M4 10c0 1.4 2.7 2.5 6 2.5s6-1.1 6-2.5"/></>,
  cloud: <path d="M6.5 16h8a3.5 3.5 0 0 0 .4-7 5 5 0 0 0-9.6-1.5A4.3 4.3 0 0 0 6.5 16Z"/>,
  clock: <><circle cx="10" cy="10" r="8"/><path d="M10 6v4l3 3"/></>,
  zap: <path d="m13 2-7 9h5l-1 7 8-10h-5l0-6Z"/>,
};

function Icon({ name, className = '' }: { name: IconName; className?: string }) {
  return <svg className={className} viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.65" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{iconPaths[name]}</svg>;
}

/* ── Hero ─────────────────────────────────────────────────────────────────── */

function Hero() {
  return (
    <section className="hero-new" id="product">
      <div className="hero-new-inner">
        <div className="hero-new-copy">
          <div className="hero-badge">Rust-native Unified Compute Engine</div>
          <h1>One compute engine for batch, streaming, and AI&nbsp;pipelines</h1>
          <p className="hero-lead">
            Krishiv unifies batch SQL, streaming pipelines, and incremental processing
            under one Apache Arrow / DataFusion runtime. Write once in SQL, Rust, or Python.
            Run anywhere from embedded to distributed.
          </p>
          <div className="hero-actions">
            <Link className="btn btn-primary" href="/docs/latest/getting-started">
              Get Started <Icon name="arrow-right" />
            </Link>
            <Link className="btn btn-secondary" href="/docs/latest">
              Documentation
            </Link>
            <a className="btn btn-secondary" href={githubUrl}>
              <GithubIcon /> GitHub
            </a>
          </div>
        </div>
        <div className="hero-new-viz">
          <ExecutionGraphViz />
        </div>
      </div>
    </section>
  );
}

function GithubIcon() {
  return <svg viewBox="0 0 20 20" width="18" height="18" fill="currentColor" aria-hidden="true"><path d="M10 .9a9.1 9.1 0 0 0-2.9 17.7c.46.08.63-.2.63-.44v-1.6c-2.57.56-3.11-1.1-3.11-1.1-.42-1.07-1.03-1.03-1.03-1.03-.84-.58.06-.57.06-.57.93.07 1.42.96 1.42.96.83 1.41 2.18 1 2.71.77.08-.6.32-1 .59-1.23-2.05-.23-4.2-1.02-4.2-4.55 0-1 .36-1.83.95-2.47-.1-.24-.41-1.18.09-2.44 0 0 .78-.25 2.5.94A8.7 8.7 0 0 1 10 5.2c.77 0 1.54.1 2.27.3 1.72-1.19 2.5-.94 2.5-.94.5 1.26.19 2.2.1 2.44.59.64.94 1.46.94 2.47 0 3.54-2.16 4.31-4.21 4.54.33.29.63.85.63 1.72v2.55c0 .25.17.53.64.44A9.1 9.1 0 0 0 10 .9Z"/></svg>;
}

/* ── Execution Graph Visualization ────────────────────────────────────────── */

function ExecutionGraphViz() {
  const nodes = [
    { x: 50, y: 16, w: 100, h: 32, label: 'SQL', type: 'input' as const },
    { x: 170, y: 16, w: 100, h: 32, label: 'Python', type: 'input' as const },
    { x: 290, y: 16, w: 100, h: 32, label: 'Rust', type: 'input' as const },
    { x: 130, y: 72, w: 180, h: 36, label: 'Unified Planner', type: 'core' as const },
    { x: 130, y: 128, w: 180, h: 36, label: 'Arrow + DataFusion', type: 'core' as const },
    { x: 30, y: 190, w: 80, h: 30, label: 'Batch', type: 'mode' as const },
    { x: 130, y: 190, w: 80, h: 30, label: 'Streaming', type: 'mode' as const },
    { x: 230, y: 190, w: 100, h: 30, label: 'Incremental', type: 'mode' as const },
    { x: 20, y: 248, w: 70, h: 26, label: 'Iceberg', type: 'connector' as const },
    { x: 100, y: 248, w: 60, h: 26, label: 'Kafka', type: 'connector' as const },
    { x: 170, y: 248, w: 70, h: 26, label: 'Parquet', type: 'connector' as const },
    { x: 250, y: 248, w: 50, h: 26, label: 'S3', type: 'connector' as const },
    { x: 310, y: 248, w: 70, h: 26, label: 'ADLS', type: 'connector' as const },
  ];

  const edges: Array<[number, number]> = [
    [0, 3], [1, 3], [2, 3],
    [3, 4], [4, 5], [4, 6], [4, 7],
    [5, 8], [5, 9], [5, 10], [6, 8], [6, 9], [6, 11], [7, 8], [7, 9], [7, 10], [7, 11], [7, 12],
  ];

  const typeColors = {
    input: { fill: '#151515', stroke: '#2A2A2A', text: '#D4D4D4' },
    core: { fill: 'rgba(245,158,11,.10)', stroke: 'rgba(245,158,11,.45)', text: '#FFB52A' },
    mode: { fill: '#101010', stroke: '#343434', text: '#A3A3A3' },
    connector: { fill: '#0A0A0A', stroke: '#2A2A2A', text: '#A3A3A3' },
  };

  return (
    <div className="exec-graph" aria-label="Krishiv execution pipeline visualization">
      <svg viewBox="0 0 400 290" width="100%" role="img" aria-label="Execution graph showing SQL, Python, and Rust inputs flowing through unified planner and Arrow execution to batch, streaming, and incremental outputs">
        <defs>
          <marker id="eg-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#F59E0B" opacity=".5" />
          </marker>
        </defs>
        {edges.map(([from, to], i) => {
          const f = nodes[from];
          const t = nodes[to];
          return <line key={i} x1={f.x + f.w / 2} y1={f.y + f.h} x2={t.x + t.w / 2} y2={t.y} stroke="#F59E0B" strokeWidth=".7" opacity=".25" markerEnd="url(#eg-arrow)" />;
        })}
        {nodes.map((n, i) => {
          const c = typeColors[n.type];
          return (
            <g key={i}>
              <rect x={n.x} y={n.y} width={n.w} height={n.h} rx={n.type === 'connector' ? 6 : 8} fill={c.fill} stroke={c.stroke} strokeWidth={n.type === 'core' ? 1.2 : 1} />
              <text x={n.x + n.w / 2} y={n.y + n.h / 2 + 4} textAnchor="middle" fontSize={n.type === 'connector' ? 10 : 12} fontWeight={n.type === 'core' ? 700 : 600} fill={c.text}>{n.label}</text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

/* ── Why Krishiv ──────────────────────────────────────────────────────────── */

function WhyKrishiv() {
  const problems = [
    {
      icon: 'zap' as IconName,
      title: 'No separate engines',
      desc: 'Batch and streaming run on the same code path. No Spark for batch, Flink for streaming, glue code in between.',
    },
    {
      icon: 'delta' as IconName,
      title: 'Incremental by default',
      desc: 'DeltaBatch and IncrementalFlow maintain views incrementally. No full recomputation on every tick.',
    },
    {
      icon: 'cpu' as IconName,
      title: 'Rust performance',
      desc: 'Native Rust with Tokio async. Zero-copy Arrow batches between operators. Lower latency, smaller footprint.',
    },
    {
      icon: 'iceberg' as IconName,
      title: 'Lakehouse native',
      desc: 'Apache Iceberg is the primary lakehouse target. REST, Hive, and Glue catalogs. Parquet + manifest atomic commits.',
    },
    {
      icon: 'server' as IconName,
      title: 'Embedded to distributed',
      desc: 'One API. Start in-process, scale to a coordinator-plus-executors cluster. No rewrite when you outgrow your laptop.',
    },
    {
      icon: 'code' as IconName,
      title: 'Three APIs, one engine',
      desc: 'SQL for analysts, Python for data engineers, Rust for platform teams. Same planning, same execution, same results.',
    },
  ];

  return (
    <section className="section-dark" aria-label="Why Krishiv exists">
      <div className="section-container">
        <p className="section-eyebrow">Why Krishiv</p>
        <h2>The compute engine that does not make you choose</h2>
        <p className="section-subtitle">
          Most teams run separate systems for batch, streaming, and incremental work.
          Krishiv eliminates the seams between them.
        </p>
        <div className="problems-grid">
          {problems.map((p) => (
            <article className="problem-card" key={p.title}>
              <div className="problem-icon"><Icon name={p.icon} /></div>
              <h3>{p.title}</h3>
              <p>{p.desc}</p>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ── Code Examples ────────────────────────────────────────────────────────── */

function CodeExamples() {
  return (
    <section className="section-light" aria-label="Code examples">
      <div className="section-container">
        <p className="section-eyebrow">Developer Experience</p>
        <h2>Same workload. Three APIs. One engine.</h2>
        <p className="section-subtitle">
          Read data, run a query, get results. The exact same logic in SQL, Rust, and Python.
        </p>
        <div className="code-showcase">
          <CodeTabs
            tabs={[
              {
                id: 'sql',
                label: 'SQL',
                language: 'sql',
                code: `-- Read Iceberg table, aggregate, write back
SELECT
  customer_id,
  SUM(amount) AS total_spend,
  COUNT(*) AS order_count
FROM orders
WHERE event_time >= NOW() - INTERVAL '1' DAY
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 10;`,
              },
              {
                id: 'rust',
                label: 'Rust',
                language: 'rust',
                code: `use krishiv_api::{Session, col, lit, sum, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let session = Session::embedded().await?;

    let df = session
        .read_iceberg("catalog", "orders").await?
        .filter(col("event_time").gt(lit("2026-06-30")))?
        .group_by(vec![col("customer_id")])?
        .agg(vec![
            sum(col("amount")).alias("total_spend"),
            count(col("order_id")).alias("order_count"),
        ])?
        .sort(vec![col("total_spend").desc()])?
        .limit(10);

    df.show().await?;
    Ok(())
}`,
              },
              {
                id: 'python',
                label: 'Python',
                language: 'python',
                code: `import krishiv as ks
from krishiv.functions import col, lit, sum, count

session = ks.Session.embedded()

df = (session.read_iceberg("catalog", "orders")
        .filter(col("event_time") > lit("2026-06-30"))
        .group_by(["customer_id"])
        .agg([
            sum(col("amount")).alias("total_spend"),
            count(col("order_id")).alias("order_count"),
        ])
        .order_by(["total_spend"], ascending=False)
        .limit(10))

df.show()`,
              },
            ]}
          />
        </div>
      </div>
    </section>
  );
}

/* ── Comparison Table ─────────────────────────────────────────────────────── */

function ComparisonTable() {
  const features = [
    { name: 'Batch SQL', k: true, s: true, f: true },
    { name: 'Streaming', k: true, s: true, f: true },
    { name: 'Incremental Processing', k: true, s: false, f: false },
    { name: 'Rust API', k: true, s: false, f: false },
    { name: 'Python API', k: true, s: true, f: true },
    { name: 'SQL', k: true, s: true, f: true },
    { name: 'Embedded Mode', k: true, s: false, f: false },
    { name: 'Distributed Mode', k: true, s: true, f: true },
    { name: 'Lakehouse Native (Iceberg)', k: true, s: true, f: false },
    { name: 'Arrow Columnar Memory', k: true, s: false, f: false },
    { name: 'Zero-Copy Between Operators', k: true, s: false, f: false },
  ];

  function Cell({ v }: { v: boolean }) {
    return v
      ? <td className="cmp-yes"><Icon name="check" /></td>
      : <td className="cmp-no"><Icon name="minus" /></td>;
  }

  return (
    <section className="section-dark" aria-label="Comparison">
      <div className="section-container">
        <p className="section-eyebrow">Comparison</p>
        <h2>How Krishiv compares</h2>
        <p className="section-subtitle">
          Factual comparison with Apache Spark and Apache Flink. No marketing spin.
        </p>
        <div className="cmp-table-wrap">
          <table className="cmp-table">
            <thead>
              <tr>
                <th>Feature</th>
                <th className="cmp-highlight">Krishiv</th>
                <th>Spark</th>
                <th>Flink</th>
              </tr>
            </thead>
            <tbody>
              {features.map((f) => (
                <tr key={f.name}>
                  <td className="cmp-feature">{f.name}</td>
                  <Cell v={f.k} />
                  <Cell v={f.s} />
                  <Cell v={f.f} />
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

/* ── Deployment Modes ─────────────────────────────────────────────────────── */

function DeploymentModes() {
  const modes = [
    {
      icon: 'terminal' as IconName,
      title: 'Embedded',
      tag: 'Available',
      tagColor: 'green',
      desc: 'Run in your process. No daemon, no cluster. Ideal for notebooks, scripts, tests, and libraries.',
      code: 'Session::embedded().await?',
    },
    {
      icon: 'server' as IconName,
      title: 'Single Node',
      tag: 'Available',
      tagColor: 'green',
      desc: 'Local daemon with RocksDB state, checkpoints, and Flight SQL endpoint. One host, durable.',
      code: 'krishiv local start',
    },
    {
      icon: 'cluster' as IconName,
      title: 'Distributed',
      tag: 'Preview',
      tagColor: 'blue',
      desc: 'Coordinator schedules across N executors. Shared object store. Scale when you need to.',
      code: 'krishiv clusterd',
    },
  ];

  return (
    <section className="section-light" aria-label="Deployment modes">
      <div className="section-container">
        <p className="section-eyebrow">Deployment</p>
        <h2>Start small. Scale when ready.</h2>
        <p className="section-subtitle">
          One API. One engine. Three deployment shapes. No rewrite needed.
        </p>
        <div className="deploy-cards">
          {modes.map((m, i) => (
            <article className="deploy-card" key={m.title}>
              <div className="deploy-card-top">
                <Icon name={m.icon} />
                <span className={`badge badge-${m.tagColor}`}>{m.tag}</span>
              </div>
              <h3>{m.title}</h3>
              <p>{m.desc}</p>
              <code className="deploy-code">{m.code}</code>
              {i < modes.length - 1 && <span className="deploy-arrow" aria-hidden="true">→</span>}
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ── Ecosystem ────────────────────────────────────────────────────────────── */

function Ecosystem() {
  const items: Array<{ icon: IconName; name: string }> = [
    { icon: 'arrow-icon', name: 'Apache Arrow' },
    { icon: 'datafusion', name: 'DataFusion' },
    { icon: 'iceberg', name: 'Apache Iceberg' },
    { icon: 'kafka', name: 'Kafka' },
    { icon: 'parquet', name: 'Parquet' },
    { icon: 's3', name: 'S3 / ADLS / GCS' },
    { icon: 'tokio', name: 'Tokio' },
  ];

  return (
    <section className="section-dark" aria-label="Ecosystem">
      <div className="section-container">
        <p className="section-eyebrow">Ecosystem</p>
        <h2>Built on the open data stack</h2>
        <div className="eco-grid">
          {items.map((item) => (
            <div className="eco-item" key={item.name}>
              <Icon name={item.icon} />
              <span>{item.name}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ── Audience ─────────────────────────────────────────────────────────────── */

function Audience() {
  const roles = [
    { icon: 'database' as IconName, title: 'Data Engineers', desc: 'Unified batch and streaming pipelines with exactly-once semantics.' },
    { icon: 'cpu' as IconName, title: 'AI Engineers', desc: 'Feature pipelines, vector sinks, and incremental view maintenance for ML.' },
    { icon: 'users' as IconName, title: 'ML Platform Teams', desc: 'Embedded compute for feature stores, training data, and online inference.' },
    { icon: 'code' as IconName, title: 'Analytics Engineers', desc: 'SQL-first incremental views. Live dashboards without recomputation.' },
    { icon: 'cloud' as IconName, title: 'Infrastructure Teams', desc: 'Rust-native, Kubernetes-ready, CRD-driven deployment.' },
    { icon: 'shield' as IconName, title: 'SaaS Builders', desc: 'Embedded analytics engine. Add SQL to your product without a separate database.' },
  ];

  return (
    <section className="section-light" aria-label="Who Krishiv is for">
      <div className="section-container">
        <p className="section-eyebrow">Who It Is For</p>
        <h2>Built for the teams building the data stack</h2>
        <div className="audience-grid">
          {roles.map((r) => (
            <article className="audience-card" key={r.title}>
              <Icon name={r.icon} />
              <h3>{r.title}</h3>
              <p>{r.desc}</p>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ── Trust Signals ────────────────────────────────────────────────────────── */

function TrustSignals() {
  const signals = [
    { icon: 'shield' as IconName, label: 'Open Source', sub: 'Apache 2.0' },
    { icon: 'rust' as IconName, label: 'Rust Native', sub: 'No JVM' },
    { icon: 'arrow-icon' as IconName, label: 'Arrow Ecosystem', sub: 'Zero-copy' },
    { icon: 'database' as IconName, label: 'DataFusion', sub: 'SQL engine' },
    { icon: 'iceberg' as IconName, label: 'Iceberg First', sub: 'Lakehouse' },
    { icon: 'clock' as IconName, label: 'Benchmarks', sub: 'Coming Soon' },
  ];

  return (
    <section className="section-dark" aria-label="Trust signals">
      <div className="section-container">
        <div className="trust-grid">
          {signals.map((s) => (
            <div className="trust-item" key={s.label}>
              <Icon name={s.icon} />
              <div>
                <strong>{s.label}</strong>
                <span>{s.sub}</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ── FAQ ──────────────────────────────────────────────────────────────────── */

const faqItems = [
  {
    q: 'What is Krishiv?',
    a: 'Krishiv is a Rust-native compute engine that unifies batch SQL, streaming pipelines, and incremental view maintenance under one Apache Arrow / DataFusion runtime. It runs as an embedded library, a single-node daemon, or a distributed coordinator-plus-executors cluster.',
  },
  {
    q: 'How does Krishiv compare to Apache Spark?',
    a: 'Krishiv uses Apache Arrow as its in-memory data model (zero-copy between operators) and DataFusion for SQL planning, rather than Spark\'s JVM-based model. Krishiv runs natively in Rust with Tokio async, offering lower latency and smaller memory footprint. It supports embedded mode, single-node, and distributed deployment.',
  },
  {
    q: 'How does Krishiv compare to Apache Flink?',
    a: 'Krishiv unifies batch and streaming in one engine with shared Arrow batches and shared planning. It adds incremental view maintenance (DeltaBatch / IncrementalFlow) which Flink does not natively provide. Both support stateful processing, windowing, and exactly-once semantics.',
  },
  {
    q: 'Is Krishiv production-ready?',
    a: 'Batch SQL, the Rust/Python APIs, embedded and single-node modes, and Iceberg/Parquet connectors are Available. Distributed mode, Kafka, and checkpoint storage are Preview. Incremental view maintenance is Experimental. See the Feature Maturity page.',
  },
  {
    q: 'How do I install Krishiv?',
    a: 'Docker: docker pull ghcr.io/krishivai/krishiv:latest. Rust: krishiv = "0.1" on crates.io. Python: pip install krishiv. See the Getting Started guide for details.',
  },
];

function FaqSection() {
  const faqJsonLd = {
    '@context': 'https://schema.org',
    '@type': 'FAQPage',
    mainEntity: faqItems.map((item) => ({
      '@type': 'Question',
      name: item.q,
      acceptedAnswer: {
        '@type': 'Answer',
        text: item.a,
      },
    })),
  };

  return (
    <section className="section-light" aria-label="Frequently Asked Questions">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(faqJsonLd) }} />
      <div className="section-container">
        <p className="section-eyebrow">FAQ</p>
        <h2>Frequently Asked Questions</h2>
        <div className="faq-list">
          {faqItems.map((item) => (
            <details key={item.q} className="faq-item">
              <summary>{item.q}</summary>
              <p>{item.a}</p>
            </details>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ── Final CTA ────────────────────────────────────────────────────────────── */

function FinalCTA() {
  return (
    <section className="section-cta" aria-label="Get started">
      <div className="section-container" style={{ textAlign: 'center' }}>
        <h2>Ready to build?</h2>
        <p className="section-subtitle" style={{ maxWidth: 520, margin: '0 auto 28px' }}>
          Start with a single query. Scale to a cluster. No rewrite needed.
        </p>
        <div style={{ display: 'flex', gap: 12, justifyContent: 'center', flexWrap: 'wrap' }}>
          <Link className="btn btn-primary btn-lg" href="/docs/latest/getting-started">
            Get Started <Icon name="arrow-right" />
          </Link>
          <a className="btn btn-secondary btn-lg" href={githubUrl}>
            <GithubIcon /> Star on GitHub
          </a>
        </div>
      </div>
    </section>
  );
}

/* ── Page ─────────────────────────────────────────────────────────────────── */

export default function Home() {
  return (
    <SiteShell>
      <main>
        <Hero />
        <WhyKrishiv />
        <CodeExamples />
        <ComparisonTable />
        <DeploymentModes />
        <Ecosystem />
        <Audience />
        <TrustSignals />
        <FaqSection />
        <FinalCTA />
      </main>
    </SiteShell>
  );
}
