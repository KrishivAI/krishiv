'use client';

import { motion } from 'framer-motion';

const features = [
  {
    icon: '⚡',
    iconClass: 'feature-card-icon-blue',
    title: 'Unified Engine',
    desc: 'Single engine for batch SQL, streaming pipelines, and incremental view maintenance.',
    items: ['Batch SQL execution', 'Event-time streaming', 'Incremental View Maintenance', 'CDC pipelines'],
  },
  {
    icon: '🏔',
    iconClass: 'feature-card-icon-green',
    title: 'Iceberg First',
    desc: 'Native Iceberg support with two-phase commit, REST catalog, and Polaris integration.',
    items: ['Iceberg tables', 'Polaris REST Catalog', 'Two-phase commit', 'Delta & Hudi support'],
  },
  {
    icon: '🧑‍💻',
    iconClass: 'feature-card-icon-purple',
    title: 'Developer First',
    desc: 'Same engine underneath with SQL, Rust API, and Python API.',
    items: ['SQL via DataFusion', 'Rust Session API', 'PyO3 Python bindings', 'UDFs in any language'],
  },
  {
    icon: '📐',
    iconClass: 'feature-card-icon-amber',
    title: 'Scale Anywhere',
    desc: 'Run embedded, single node, or distributed cluster. No code changes required.',
    items: ['Embedded in-process', 'Single-node daemon', 'Distributed cluster', 'Kubernetes operator'],
  },
  {
    icon: '🏎',
    iconClass: 'feature-card-icon-blue',
    title: 'Native Rust Performance',
    desc: 'Zero JVM overhead with Apache Arrow columnar execution and vectorized operators.',
    items: ['Apache Arrow RecordBatch', 'Columnar execution', 'Vectorized operators', 'Zero JVM'],
  },
  {
    icon: '🔒',
    iconClass: 'feature-card-icon-green',
    title: 'Fault Tolerant',
    desc: 'Checkpointing, exactly-once processing, durable state, and automatic recovery.',
    items: ['Checkpoint coordination', 'Exactly-once semantics', 'RocksDB state backend', 'Incremental snapshots'],
  },
];

const cardVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: (i: number) => ({
    opacity: 1,
    y: 0,
    transition: { duration: 0.5, delay: i * 0.08, ease: [0.22, 1, 0.36, 1] as [number, number, number, number] },
  }),
};

export function Features() {
  return (
    <section className="landing-section">
      <div className="landing-section-header">
        <motion.div
          className="landing-section-label"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
        >
          Features
        </motion.div>
        <motion.h2
          className="landing-section-title"
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          Built for modern data workloads
        </motion.h2>
        <motion.p
          className="landing-section-desc"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.1 }}
        >
          One runtime for batch SQL, streaming pipelines, and incremental views.
          Scale from your laptop to production clusters.
        </motion.p>
      </div>

      <div className="feature-grid">
        {features.map((f, i) => (
          <motion.div
            className="feature-card"
            key={f.title}
            custom={i}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
            variants={cardVariants}
          >
            <div className={`feature-card-icon ${f.iconClass}`}>{f.icon}</div>
            <h3>{f.title}</h3>
            <p>{f.desc}</p>
            <ul>
              {f.items.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </motion.div>
        ))}
      </div>
    </section>
  );
}
