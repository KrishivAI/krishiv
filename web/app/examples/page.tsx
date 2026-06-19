import { SiteHeader } from '@/components/SiteHeader';

const examples = [
  ['Batch SQL', 'Rust, Python, and CLI examples for finite SQL queries.'],
  ['Streaming', 'Windowing, watermarks, continuous jobs, and state TTL examples.'],
  ['DeltaBatch / IVM', 'Incremental view examples backed by per-tick deltas.'],
  ['Lakehouse', 'Iceberg-first and compatibility examples for Delta/Hudi paths.'],
  ['Kubernetes', 'Operator, direct manifests, Helm, and job examples.'],
  ['Pipelines', 'SQL and Python pipeline examples for multi-step workloads.'],
];

export const metadata = {
  title: 'Examples',
  description: 'Runnable Krishiv examples by language, mode, and topic.',
};

export default function ExamplesPage() {
  return (
    <main className="home-shell">
      <div className="home-container list-page">
        <SiteHeader />
        <span className="eyebrow">Examples</span>
        <h1>Runnable examples by workload.</h1>
        <p className="section-lead">This gallery is prepared for filters by language, runtime mode, and topic.</p>
        <div className="grid three">
          {examples.map(([title, body]) => (
            <article className="card" key={title}>
              <h3>{title}</h3>
              <p>{body}</p>
            </article>
          ))}
        </div>
      </div>
    </main>
  );
}
