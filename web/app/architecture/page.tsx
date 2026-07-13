import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowIcon, DiagramFrame, InteriorHero, SectionIntro } from '@/components/InteriorPage';
import { SiteShell } from '@/components/Shell';
import {
  TopologyDiagram,
  RequestFlowDiagram,
  DataPlaneDiagram,
  LifecycleDiagram,
} from '@/components/ArchitectureDiagrams';

export const metadata: Metadata = {
  title: 'Engine Architecture',
  description:
    'How Krishiv Engine is organized across available embedded and single-node placements and a Preview distributed path.',
  openGraph: {
    title: 'Krishiv Engine Architecture',
    description:
      'The Rust, Arrow, and DataFusion foundations behind Krishiv Engine execution modes.',
  },
  alternates: {
    canonical: 'https://krishiv.ai/architecture',
  },
};

export default function Architecture() {
  return (
    <SiteShell>
      <main className="ip-page">
        <InteriorHero
          eyebrow="Engine architecture / Boundary map"
          title="One compute model. Three explicit placements."
          description={
            <p>
              Rust, Arrow, and DataFusion form the common execution spine. Embedded and
              single-node paths are available in the source workspace; distributed
              execution remains a Preview surface with a different operating boundary.
            </p>
          }
          aside={
            <div className="ip-panel">
              <div className="ip-panel-top">
                <span className="ip-panel-label">Placement map</span>
                <span className="ip-panel-badge"><i />Explicit</span>
              </div>
              <h2 className="ip-panel-title">Topology never silently changes.</h2>
              <ul className="ip-panel-list">
                <li className="ip-panel-row"><span>Embedded</span><strong>Available · in process</strong></li>
                <li className="ip-panel-row"><span>Single node</span><strong>Available · one host</strong></li>
                <li className="ip-panel-row"><span>Distributed</span><strong>Preview · remote workers</strong></li>
                <li className="ip-panel-row"><span>Incremental views</span><strong>Experimental · local first</strong></li>
              </ul>
              <p className="ip-panel-note">A topology label does not imply durability, recovery, or connector guarantees.</p>
            </div>
          }
        >
          <div className="ip-actions">
            <Link className="mk-button mk-button-primary" href="/docs/engine/concepts/architecture">Architecture reference <ArrowIcon /></Link>
            <Link className="mk-button mk-button-secondary" href="/product/maturity">Feature maturity</Link>
          </div>
        </InteriorHero>

        <section className="ip-section" id="placements">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="01 / Placement"
              title="Choose a shape, then verify its maturity."
              description={<p>Placement is an explicit configuration decision. State, checkpointing, storage, and recovery remain separate choices.</p>}
            />
            <DiagramFrame label="Deployment topology diagram; scroll horizontally to inspect all placements">
              <TopologyDiagram />
            </DiagramFrame>
            <div className="ip-card-grid">
              <article className="ip-card">
                <div className="ip-card-top"><span>01</span><span className="ip-tag ip-tag-accent">Available</span></div>
                <h3>Embedded</h3>
                <p>Runs Engine inside your process for local SQL, DataFrames, tests, and API evaluation. Batch results use Arrow RecordBatch values.</p>
              </article>
              <article className="ip-card">
                <div className="ip-card-top"><span>02</span><span className="ip-tag ip-tag-accent">Available</span></div>
                <h3>Single node</h3>
                <p>Places coordinator, executor, HTTP, and Flight boundaries on one host. Durability depends on the configured backends.</p>
              </article>
              <article className="ip-card">
                <div className="ip-card-top"><span>03</span><span className="ip-tag">Preview</span></div>
                <h3>Distributed</h3>
                <p>Coordinates work across remote executors. It is an evaluation path—not a promise of high availability, elastic scale, or production readiness.</p>
              </article>
            </div>
          </div>
        </section>

        <section className="ip-section ip-section-contrast" id="request-flow">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="02 / Request flow"
              title="A query moves through visible seams."
              description={<p>Batch begins with a session and DataFusion plan. Streaming and distributed paths add state and placement without changing the public entry point.</p>}
            />
            <DiagramFrame label="Query request-flow diagram; scroll horizontally to inspect every stage">
              <RequestFlowDiagram />
            </DiagramFrame>
            <ol className="ip-step-grid ip-step-grid-four">
              <li className="ip-step"><h3>Parse and bind</h3><p>The session resolves tables, UDFs, expressions, and types against its catalog.</p></li>
              <li className="ip-step"><h3>Plan and optimize</h3><p>Logical work is rewritten and lowered to a physical plan; remote paths add task boundaries.</p></li>
              <li className="ip-step"><h3>Execute</h3><p>Arrow operators run the plan while state, shuffle, and checkpoint interfaces connect where required.</p></li>
              <li className="ip-step"><h3>Return or continue</h3><p>Batch returns RecordBatch values; streaming surfaces remain active through their job and stream interfaces.</p></li>
            </ol>
          </div>
        </section>

        <section className="ip-section" id="control-data-plane">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="03 / Cluster boundary"
              title="Control plane above. Data plane below."
              description={<p>The Preview distributed design separates lifecycle and placement decisions from execution. This is a boundary map, not an SLA.</p>}
            />
            <DiagramFrame label="Control-plane and data-plane diagram; scroll horizontally to inspect all executors">
              <DataPlaneDiagram />
            </DiagramFrame>
            <div className="ip-card-grid ip-card-grid-two">
              <article className="ip-card">
                <div className="ip-card-top"><span>Control</span><span>Coordinator</span></div>
                <h3>Owns job and task lifecycle.</h3>
                <p>Scheduler and coordinator modules expose metadata, leadership, task assignment, and remote-control paths. Their presence does not certify a highly available control plane.</p>
              </article>
              <article className="ip-card">
                <div className="ip-card-top"><span>Data</span><span>Executors</span></div>
                <h3>Runs assigned fragments.</h3>
                <p>Executors connect work to shuffle, state, checkpoint, and transport interfaces. Failure behavior remains part of the distributed Preview boundary.</p>
              </article>
            </div>
          </div>
        </section>

        <section className="ip-section ip-section-contrast" id="lifecycle">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="04 / Lifecycle"
              title="Submission and recovery are separate concerns."
              description={<p>Batch and streaming do not share identical recovery semantics. The complete source, state, checkpoint, and sink combination determines behavior.</p>}
            />
            <DiagramFrame label="Execution lifecycle diagram; scroll horizontally to inspect every stage">
              <LifecycleDiagram />
            </DiagramFrame>
            <ol className="ip-step-grid">
              <li className="ip-step"><h3>Validate</h3><p>Resolve schema, types, and supported expressions before execution work is assigned.</p></li>
              <li className="ip-step"><h3>Plan and place</h3><p>Create executable fragments and assign them according to the selected runtime topology.</p></li>
              <li className="ip-step"><h3>Run and observe</h3><p>Execute the graph and evaluate failures against the configured state, checkpoint, source, and sink contracts.</p></li>
            </ol>
          </div>
        </section>

        <section className="ip-section" id="boundaries">
          <div className="mk-wrap">
            <SectionIntro
              eyebrow="05 / Honest boundaries"
              title="One architecture does not mean one maturity level."
              description={<p>Read capability status before treating an implementation path as a stable operating contract.</p>}
            />
            <div className="ip-card-grid">
              <article className="ip-card">
                <div className="ip-card-top"><span>Available</span><span>Current source</span></div>
                <h3>Batch and local placement</h3>
                <p>Batch SQL, DataFusion planning, Arrow data, embedded execution, single-node execution, and core source-built APIs.</p>
              </article>
              <article className="ip-card">
                <div className="ip-card-top"><span>Preview</span><span>Certification ongoing</span></div>
                <h3>Stateful and distributed paths</h3>
                <p>Streaming, remote execution, checkpoint and state integrations, and primary connector paths remain combination-specific.</p>
              </article>
              <article className="ip-card">
                <div className="ip-card-top"><span>Experimental</span><span>May change</span></div>
                <h3>Incremental view maintenance</h3>
                <p>Weighted deltas and IncrementalFlow exist as local-first evaluation surfaces, with distributed IVM still deferred.</p>
              </article>
            </div>
          </div>
        </section>

        <section className="ip-cta">
          <div className="mk-wrap ip-cta-inner">
            <div>
              <p className="ip-kicker">Go deeper</p>
              <h2>Use the docs for contracts, not the diagram alone.</h2>
              <p>Start with execution modes, then inspect the operational page for the topology you intend to evaluate.</p>
            </div>
            <div className="ip-actions">
              <Link className="mk-button mk-button-primary" href="/docs/engine/concepts/execution-modes">Execution modes <ArrowIcon /></Link>
              <Link className="mk-button mk-button-secondary" href="/docs/engine/operations/distributed">Distributed boundary</Link>
            </div>
          </div>
        </section>
      </main>
    </SiteShell>
  );
}
