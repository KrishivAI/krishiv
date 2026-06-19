'use client';

import { motion } from 'framer-motion';

export function Cloud() {
  return (
    <section className="landing-section">
      <motion.div
        className="cloud-cta"
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.6 }}
      >
        <div style={{ position: 'relative', zIndex: 1 }}>
          <div
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 8,
              padding: '6px 14px',
              borderRadius: 999,
              border: '1px solid rgba(56, 189, 248, 0.2)',
              background: 'rgba(56, 189, 248, 0.06)',
              color: 'var(--accent-blue)',
              fontSize: 12,
              fontWeight: 600,
              letterSpacing: '0.06em',
              textTransform: 'uppercase',
              marginBottom: 20,
            }}
          >
            Coming Soon
          </div>
          <h2>Krishiv Cloud</h2>
          <p>Managed compute platform built on Krishiv. Scale without managing infrastructure.</p>
          <a
            href="https://github.com/KrishivAI/krishiv"
            className="landing-btn landing-btn-secondary"
            target="_blank"
            rel="noreferrer"
          >
            Follow Development
          </a>
        </div>
      </motion.div>
    </section>
  );
}
