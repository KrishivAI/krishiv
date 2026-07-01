import type { DocPage } from '../docs-data';
import { CRATE_MAP } from './crate-map';

const statusBadge = (s: string) => {
  const colors: Record<string, string> = {
    Available: 'badge-green',
    Preview: 'badge-blue',
    Experimental: 'badge-violet',
    'In Progress': 'badge-orange',
    Planned: 'badge-gray',
  };
  return `<span class="badge ${colors[s] ?? 'badge-gray'}">${s}</span>`;
};

function crateTable(): string {
  const rows = CRATE_MAP.map(
    (c) => `<tr>
      <td><code>${c.name}</code></td>
      <td>${c.responsibility}</td>
      <td>${statusBadge(c.maturity)}</td>
      <td>${c.keyApis ? c.keyApis.map((a) => `<code>${a}</code>`).join(', ') : '—'}</td>
      <td>${c.docsLink ? `<a href="${c.docsLink}">Docs</a>` : '—'}</td>
    </tr>`
  ).join('\n');
  return `<table class="api-table">
<thead><tr><th>Crate</th><th>Responsibility</th><th>Maturity</th><th>Key APIs</th><th>Docs</th></tr></thead>
<tbody>${rows}</tbody>
</table>`;
}

export const developmentPages: DocPage[] = [
  {
    slug: 'development/workspace-map',
    group: 'Development',
    title: 'Workspace Map',
    description: 'Every crate in the Krishiv workspace, its responsibility, maturity, and public API surface.',
    status: 'Available',
    body: `
<p>The Krishiv workspace contains ${CRATE_MAP.length} crates. This page is generated from <code>Cargo.toml</code> and <code>PRODUCT_FACTS.md</code>. Each crate is listed with its responsibility, maturity status, key public APIs, and link to relevant documentation.</p>

<h2 id="crate-map">Complete Crate Map</h2>
${crateTable()}

<h2 id="maturity-key">Maturity Key</h2>
<table class="api-table">
<thead><tr><th>Status</th><th>Meaning</th></tr></thead>
<tbody>
<tr><td>${statusBadge('Available')}</td><td>Implemented, tested, and used in core workflows. APIs are stable within minor versions.</td></tr>
<tr><td>${statusBadge('Preview')}</td><td>Scaffolding and initial implementation exist. End-to-end certification work is ongoing. Use with caution.</td></tr>
<tr><td>${statusBadge('Experimental')}</td><td>Implemented and functional. APIs and semantics may change. Not certified for production use.</td></tr>
<tr><td>${statusBadge('In Progress')}</td><td>Active development. Do not advertise as complete.</td></tr>
<tr><td>${statusBadge('Planned')}</td><td>On the roadmap but not yet implemented. Do not rely on these without maintainer confirmation.</td></tr>
</tbody>
</table>

<h2 id="architecture-invariants">Architecture Invariants</h2>
<ul>
  <li>Do not build separate engines for batch and streaming.</li>
  <li>One active job coordinator per job; executors are replaceable data-plane workers.</li>
  <li>Shuffle, state, checkpoint, metadata, and connector behavior live behind crate APIs.</li>
  <li>Prefer typed IDs, typed fragments, typed errors, and capability flags over stringly-routed public contracts.</li>
</ul>

<h2 id="build-commands">Build Commands</h2>
<pre><code class="language-bash">cargo check --workspace
cargo test --workspace --exclude krishiv-python
cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings
cargo fmt --check</code></pre>

<h2 id="feature-matrix">Feature Matrix</h2>
<table class="api-table">
<thead><tr><th>Feature</th><th>Purpose</th></tr></thead>
<tbody>
<tr><td><code>minimal</code></td><td>Smallest facade surface; no optional deployment capabilities.</td></tr>
<tr><td><code>local</code></td><td>Default developer build; embedded plus single-node capabilities.</td></tr>
<tr><td><code>embedded</code></td><td>In-process API use; no optional dependencies.</td></tr>
<tr><td><code>single-node</code></td><td>Local daemon/in-process cluster with Flight SQL, shuffle, and RocksDB metadata.</td></tr>
<tr><td><code>distributed</code></td><td>Bare remote cluster support with Flight SQL, shuffle, and etcd metadata.</td></tr>
<tr><td><code>k8s</code></td><td>Distributed support plus Kubernetes operator/CRD capability.</td></tr>
<tr><td><code>full</code></td><td>Standard compute-engine build: distributed/Kubernetes, Kafka, and primary Iceberg support.</td></tr>
</tbody>
</table>
`,
  },
  {
    slug: 'development/contributing',
    group: 'Development',
    title: 'Contributing',
    description: 'How to contribute to Krishiv — code, docs, and architecture decisions.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Krishiv is an open-source project under the Apache License 2.0. We welcome contributions of all kinds: code, documentation, bug reports, and feature requests.</p>

<h2 id="getting-started">Getting Started</h2>
<ol>
  <li>Fork the repository on GitHub.</li>
  <li>Clone your fork and create a feature branch.</li>
  <li>Make your changes following the coding standards below.</li>
  <li>Run the CI quality gates before submitting.</li>
  <li>Open a pull request with a clear description of the change.</li>
</ol>

<h2 id="coding-standards">Coding Standards</h2>
<ul>
  <li>Use Rust 2024 edition.</li>
  <li>Run <code>cargo fmt</code> before committing.</li>
  <li>Run <code>cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings</code>.</li>
  <li>All public crate boundaries must have explicit error types.</li>
  <li>Avoid panics in library code.</li>
  <li>Keep async boundaries clear; do not hide blocking work inside async tasks.</li>
</ul>

<h2 id="architecture-decisions">Architecture Decisions</h2>
<p>Significant changes should include an ADR (Architecture Decision Record) under <code>docs/decisions/</code>. See existing ADRs for the format.</p>

<h2 id="testing">Testing</h2>
<pre><code class="language-bash">cargo test --workspace --exclude krishiv-python
cargo test -p krishiv-delta
cargo test -p krishiv-api
cargo test -p krishiv-runtime</code></pre>
`,
  },
];
