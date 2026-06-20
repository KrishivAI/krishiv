import Link from 'next/link';
import type { JSX, ReactNode } from 'react';
import { BrandLogo, SiteShell } from '@/components/Shell';
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
  | 'catalog';

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
};

function LineIcon({ name, className = '' }: { name: IconName; className?: string }) {
  return <svg className={className} viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.65" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{iconPaths[name]}</svg>;
}

export function DataFlowParticles() {
  return (
    <svg className="flow-field" viewBox="0 0 980 330" preserveAspectRatio="none" aria-hidden="true">
      <defs>
        <radialGradient id="goldParticle"><stop offset="0" stopColor="#FFB52A"/><stop offset="1" stopColor="#F59E0B" stopOpacity="0"/></radialGradient>
        <radialGradient id="whiteParticle"><stop offset="0" stopColor="#ECECEC"/><stop offset="1" stopColor="#ECECEC" stopOpacity="0"/></radialGradient>
      </defs>
      {Array.from({ length: 15 }).map((_, index) => {
        const y = 72 + index * 13;
        const curve = (index - 7) * 12;
        return <path key={`in-${index}`} className="flow-line flow-line-gold" d={`M0 ${y - curve} C 170 ${y - curve * 1.8}, 260 ${162 + curve * .4}, 438 165`} />;
      })}
      {Array.from({ length: 15 }).map((_, index) => {
        const y = 72 + index * 13;
        const curve = (index - 7) * 12;
        return <path key={`out-${index}`} className="flow-line flow-line-white" d={`M542 165 C 720 ${162 + curve * .4}, 810 ${y - curve * 1.8}, 980 ${y - curve}`} />;
      })}
      {Array.from({ length: 18 }).map((_, index) => <circle key={`g-${index}`} className={`particle particle-gold p-${index % 6}`} cx="0" cy="0" r={index % 3 === 0 ? 2.1 : 1.35} />)}
      {Array.from({ length: 18 }).map((_, index) => <circle key={`w-${index}`} className={`particle particle-white p-${index % 6}`} cx="0" cy="0" r={index % 3 === 0 ? 1.9 : 1.25} />)}
    </svg>
  );
}

function HeroContent() {
  return (
    <div className="hero-copy">
      <div className="hero-badge">Rust-native <span/> Unified Compute Engine</div>
      <h1>One engine for<br/><em>Batch</em>, Streaming,<br/>and <em>Incremental</em><br/>Processing</h1>
      <p className="hero-lead">Krishiv unifies batch, streaming, and incremental workloads in a single, high-performance engine — from local development to distributed scale.</p>
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
      <DataFlowParticles/>
      <div className="interface-row">{interfaces.map(([icon, label]) => <div className="interface-item" key={label}><LineIcon name={icon}/><span>{label}</span></div>)}</div>
      <div className="interface-lines" aria-hidden="true"><span/><span/><span/></div>
      <div className="runtime-stack">
        {layers.map(([icon, title, sub], index) => <div className="runtime-layer" key={title}><LineIcon name={icon}/><div><strong>{title}</strong>{sub && <p>{sub}</p>}</div>{index === 0 && <span className="layer-glow"/>}</div>)}
      </div>
    </div>
  );
}

const capabilities: Array<[IconName, string, string]> = [
  ['bolt', 'Unified Engine', 'One runtime for batch, streaming, and incremental processing.'],
  ['delta', 'Incremental by Design', 'Compute only what changes with IVM and delta processing.'],
  ['cube', 'Rust-Native Performance', 'Built for speed, safety, and predictable performance.'],
  ['snowflake', 'Iceberg First', 'Native table format support with ACID guarantees.'],
  ['server', 'Local to Distributed', 'Run locally, then scale to single-node or distributed clusters.'],
  ['shield', 'Reliable Foundations', 'Correctness, state management, and fault tolerance.'],
];

function CapabilityStrip() {
  return <section className="capability-strip" aria-label="Krishiv capabilities">{capabilities.map(([icon, title, text]) => <article key={title} className="capability-item"><LineIcon name={icon}/><div><h3>{title}</h3><p>{text}</p></div></article>)}</section>;
}

function ExecutionJourney() {
  const cards: Array<[IconName, string, string]> = [['laptop', 'Local Mode', 'Run and debug on your laptop.'], ['server', 'Single Node', 'Deploy to a server or VM for more power.'], ['cluster', 'Distributed Cluster', 'Scale out for massive data and high availability.']];
  return <div className="journey-cards">{cards.map(([icon, title, text], index) => <article className={`journey-card ${index === 0 ? 'active' : ''}`} key={title}><LineIcon name={icon}/><h3>{title}</h3><p>{text}</p>{index < cards.length - 1 && <span className="journey-connector"/>}</article>)}</div>;
}

function CodeExamplePanel() {
  return (
    <div className="code-panel">
      <div className="code-tabs"><button className="active">SQL</button><button>Rust</button><button>Python</button></div>
      <button className="copy-button" aria-label="Copy SQL example"><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6"><rect x="7" y="5" width="9" height="12" rx="1.5"/><path d="M4 13V4.5A1.5 1.5 0 0 1 5.5 3H13"/></svg></button>
      <pre aria-label="SQL example"><code>
        <span className="line"><span className="num">1</span><span><b>SELECT</b> customer_id, <i>SUM</i>(amount) <u>AS</u> total_spend</span></span>
        <span className="line"><span className="num">2</span><span><b>FROM</b> orders</span></span>
        <span className="line"><span className="num">3</span><span><b>WHERE</b> event_time <em>&gt;=</em> <i>NOW</i>() - <u>INTERVAL</u> <mark>'1'</mark> DAY</span></span>
        <span className="line"><span className="num">4</span><span><b>GROUP BY</b> customer_id;</span></span>
        <span className="line"><span className="num">5</span><span></span></span>
      </code></pre>
      <Link className="docs-link" href="/docs/latest/sql">→ More examples in the docs</Link>
    </div>
  );
}

function EcosystemRow() {
  const items: Array<[IconName, string]> = [['snowflake', 'Apache Iceberg'], ['kafka', 'Apache Kafka'], ['parquet', 'Parquet'], ['cloud', 'Amazon S3'], ['catalog', 'Azure Data Lake']];
  return <section className="ecosystem-row" aria-label="Connector ecosystem">{items.map(([icon, label]) => <span key={label}><LineIcon name={icon}/>{label}</span>)}<span>and more…</span></section>;
}

function GithubMark() { return <svg viewBox="0 0 20 20" width="18" height="18" fill="currentColor" aria-hidden="true"><path d="M10 .9a9.1 9.1 0 0 0-2.9 17.7c.46.08.63-.2.63-.44v-1.6c-2.57.56-3.11-1.1-3.11-1.1-.42-1.07-1.03-1.35-1.03-1.35-.84-.58.06-.57.06-.57.93.07 1.42.96 1.42.96.83 1.41 2.18 1 2.71.77.08-.6.32-1 .59-1.23-2.05-.23-4.2-1.02-4.2-4.55 0-1 .36-1.83.95-2.47-.1-.24-.41-1.18.09-2.44 0 0 .78-.25 2.5.94A8.7 8.7 0 0 1 10 5.2c.77 0 1.54.1 2.27.3 1.72-1.19 2.5-.94 2.5-.94.5 1.26.19 2.2.1 2.44.59.64.94 1.46.94 2.47 0 3.54-2.16 4.31-4.21 4.54.33.29.63.85.63 1.72v2.55c0 .25.17.53.64.44A9.1 9.1 0 0 0 10 .9Z"/></svg>; }

export default function Home() {
  return <SiteShell><main className="landing"><section className="hero"><HeroContent/><RuntimeArchitectureDiagram/></section><CapabilityStrip/><section className="developer-section"><div className="developer-copy"><p className="section-eyebrow">Developer Experience</p><h2>Start locally.<br/>Scale without limits.</h2><p>Same APIs. Same engine. From your laptop to a distributed cluster — Krishiv grows with your workload.</p><ExecutionJourney/></div><CodeExamplePanel/></section><EcosystemRow/></main></SiteShell>;
}
