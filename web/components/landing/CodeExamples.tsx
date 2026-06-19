'use client';

import { useState } from 'react';
import { motion } from 'framer-motion';

const tabs = [
  {
    id: 'sql',
    label: 'SQL',
    code: `<span class="code-comment">-- Batch query over Iceberg tables</span>
<span class="code-keyword">SELECT</span> country,
       <span class="code-fn">SUM</span>(price) <span class="code-keyword">AS</span> total_revenue,
       <span class="code-fn">COUNT</span>(<span class="code-keyword">*</span>) <span class="code-keyword">AS</span> order_count
<span class="code-keyword">FROM</span> orders
<span class="code-keyword">WHERE</span> status <span class="code-keyword">=</span> <span class="code-string">'completed'</span>
<span class="code-keyword">GROUP BY</span> country
<span class="code-keyword">ORDER BY</span> total_revenue <span class="code-keyword">DESC</span>;`,
  },
  {
    id: 'rust',
    label: 'Rust',
    code: `<span class="code-keyword">use</span> krishiv_api::Session;
<span class="code-keyword">use</span> krishiv_plan::expr::{col, lit};

<span class="code-keyword">let</span> session <span class="code-punct">=</span> Session<span class="code-punct">::</span><span class="code-fn">builder</span>()
    .<span class="code-fn">with_embedded_mode</span>()
    .<span class="code-fn">build</span>()<span class="code-punct">?</span>;

<span class="code-keyword">let</span> df <span class="code-punct">=</span> session
    .<span class="code-fn">sql</span>(<span class="code-string">"SELECT * FROM orders"</span>)
    .<span class="code-keyword">await</span><span class="code-punct">?</span>
    .<span class="code-fn">filter</span>(col(<span class="code-string">"status"</span>).<span class="code-fn">eq</span>(lit(<span class="code-string">"completed"</span>)))<span class="code-punct">?</span>
    .<span class="code-fn">aggregate</span>(<span class="code-keyword">vec</span>![col(<span class="code-string">"country"</span>)])<span class="code-punct">?</span>;

<span class="code-keyword">let</span> batches <span class="code-punct">=</span> df.<span class="code-fn">collect</span>().<span class="code-keyword">await</span><span class="code-punct">?</span>;
<span class="code-fn">println!</span>(<span class="code-string">"{batches:?}"</span>);`,
  },
  {
    id: 'python',
    label: 'Python',
    code: `<span class="code-keyword">import</span> krishiv <span class="code-keyword">as</span> ks

session <span class="code-punct">=</span> ks.<span class="code-fn">Session</span>.<span class="code-fn">connect</span>(<span class="code-string">"http://localhost:50051"</span>)

<span class="code-comment"># Read Iceberg table</span>
df <span class="code-punct">=</span> session.<span class="code-fn">read_iceberg</span>(<span class="code-string">"warehouse.orders"</span>)

<span class="code-comment"># Filter and aggregate</span>
result <span class="code-punct">=</span> (
    df
    .<span class="code-fn">filter</span>(df.status <span class="code-punct">==</span> <span class="code-string">"completed"</span>)
    .<span class="code-fn">group_by</span>(<span class="code-string">"country"</span>)
    .<span class="code-fn">agg</span>({<span class="code-string">"price"</span>: <span class="code-string">"sum"</span>})
    .<span class="code-fn">collect</span>()
)

<span class="code-fn">print</span>(result.<span class="code-fn">pretty</span>())`,
  },
];

export function CodeExamples() {
  const [active, setActive] = useState('sql');

  return (
    <section className="landing-section">
      <div className="landing-section-header">
        <motion.div
          className="landing-section-label"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
        >
          Code
        </motion.div>
        <motion.h2
          className="landing-section-title"
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          Your language, your choice
        </motion.h2>
        <motion.p
          className="landing-section-desc"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.1 }}
        >
          Same engine, same execution. Write in SQL, Rust, or Python.
        </motion.p>
      </div>

      <motion.div
        className="code-tabs"
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.6 }}
      >
        <div className="code-tabs-header">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              className={`code-tab ${active === tab.id ? 'code-tab-active' : ''}`}
              onClick={() => setActive(tab.id)}
            >
              {tab.label}
            </button>
          ))}
        </div>
        <div className="code-tab-panel">
          <pre dangerouslySetInnerHTML={{ __html: tabs.find((t) => t.id === active)?.code ?? '' }} />
        </div>
      </motion.div>
    </section>
  );
}
