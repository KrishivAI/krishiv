'use client';

import { motion } from 'framer-motion';

const items = [
  {
    title: 'Unified',
    desc: 'One engine for batch SQL, streaming, and incremental views. No separate systems for separate workloads.',
  },
  {
    title: 'Fast',
    desc: 'Rust-native execution with Apache Arrow and DataFusion. Zero JVM overhead. Columnar vectorized operators.',
  },
  {
    title: 'Open',
    desc: 'Apache 2.0 licensed. Developer-friendly APIs in SQL, Rust, and Python. Community-driven roadmap.',
  },
];

export function WhyKrishiv() {
  return (
    <section className="landing-section">
      <div className="landing-section-header">
        <motion.div
          className="landing-section-label"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
        >
          Why Krishiv
        </motion.div>
        <motion.h2
          className="landing-section-title"
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          Engineering excellence, not marketing
        </motion.h2>
      </div>

      <div className="why-grid">
        {items.map((item, i) => (
          <motion.div
            className="why-item"
            key={item.title}
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: i * 0.1 }}
          >
            <h3>{item.title}</h3>
            <p>{item.desc}</p>
          </motion.div>
        ))}
      </div>
    </section>
  );
}
