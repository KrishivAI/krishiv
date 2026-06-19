import Link from 'next/link';
import { SiteHeader } from '@/components/SiteHeader';

const modes = [
  ['Batch SQL', 'Run finite DataFusion queries over Arrow data, files, and lakehouse tables.'],
  ['Streaming', 'Build event-time pipelines with windows, watermarks, state, and checkpointing.'],
  ['DeltaBatch / IVM', 'Maintain live derived views with tick-driven incremental deltas.'],
];

const runtimeModes = [
  ['Embedded', 'Run Krishiv inside a Rust or Python process for local use and tests.'],
  ['Single-node', 'Validate coordinator, executor, Flight, and UI paths on one host.'],
  ['Distributed', 'Run remote coordinators and replaceable executor workers.'],
  ['Kubernetes', 'Deploy with operator, direct manifests, or Helm.'],
];

export default function Home() {
  return (
    <main className="home-shell">
      <div className="home-container">
        <SiteHeader />
        <section className="home-hero">
          <div>
            <span className="eyebrow">Rust-native hybrid compute</span>
            <h1>One engine for batch SQL, streaming, and incremental views.</h1>
            <p>
              Krishiv unifies Apache Arrow, DataFusion, streaming pipelines, and lakehouse-oriented execution across
              embedded, single-node, distributed, and Kubernetes runtime modes.
            </p>
            <div className="actions">
              <Link href="/docs" className="button primary">Read the docs</Link>
              <Link href="/examples" className="button secondary">Explore examples</Link>
              <a href="https://github.com/KrishivAI/krishiv" className="button secondary" rel="noreferrer" target="_blank">
                View GitHub
              </a>
            </div>
          </div>
          <div className="panel">
            <pre className="code-card"><code>{`use krishiv_api::Session;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let session = Session::builder()
        .with_embedded_mode()
        .build()?;

    let result = session
        .sql("SELECT status, COUNT(*) FROM orders GROUP BY status")
        .await?;

    println!("{result:?}");
    Ok(())
}`}</code></pre>
          </div>
        </section>

        <section className="section">
          <h2>Compute modes</h2>
          <p className="section-lead">
            Krishiv documents batch, streaming, and IVM as modes on one runtime rather than separate engines.
          </p>
          <div className="grid three">
            {modes.map(([title, body]) => (
              <article className="card" key={title}>
                <h3>{title}</h3>
                <p>{body}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="section">
          <h2>Run where your workload starts</h2>
          <p className="section-lead">
            Start in-process, validate on one host, then move to distributed workers or Kubernetes without changing the
            core programming model.
          </p>
          <div className="grid two">
            {runtimeModes.map(([title, body]) => (
              <article className="card" key={title}>
                <h3>{title}</h3>
                <p>{body}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="section">
          <div className="panel">
            <span className="eyebrow">Docs are versioned with the repo</span>
            <h2>Public website content lives in web/. Development docs stay in docs/.</h2>
            <p className="section-lead">
              The Fumadocs site is set up for release-branch documentation, a blog, changelog pages, examples, and API
              references while preserving the existing development documentation tree.
            </p>
          </div>
        </section>
      </div>
    </main>
  );
}
