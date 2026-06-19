'use client';

import { motion } from 'framer-motion';

const modes = [
  {
    label: 'Embedded',
    desc: 'In-process, no external deps. Best for tests and API usage.',
  },
  {
    label: 'Single Node',
    desc: 'All engine pieces on one host. Coordinator, Flight, UI.',
  },
  {
    label: 'Distributed',
    desc: 'Remote coordinator with replaceable executor workers.',
  },
];

export function Architecture() {
  return (
    <section className="landing-section">
      <div className="landing-section-header">
        <motion.div
          className="landing-section-label"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
        >
          Runtime
        </motion.div>
        <motion.h2
          className="landing-section-title"
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          One API, every scale
        </motion.h2>
        <motion.p
          className="landing-section-desc"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.1 }}
        >
          Start in-process, validate on one host, then move to distributed
          workers or Kubernetes without changing your code.
        </motion.p>
      </div>

      <div className="arch-progress">
        {modes.map((mode, i) => (
          <motion.div
            key={mode.label}
            style={{ display: 'contents' }}
            initial={{ opacity: 0, x: i === 0 ? -20 : i === 2 ? 20 : 0, y: i === 1 ? 20 : 0 }}
            whileInView={{ opacity: 1, x: 0, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: i * 0.12 }}
          >
            {i > 0 && (
              <div className="arch-progress-arrow">
                <svg width="32" height="16" viewBox="0 0 32 16" fill="none">
                  <path
                    d="M0 8h28m0 0l-6-6m6 6l-6 6"
                    stroke="rgba(56, 189, 248, 0.3)"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </div>
            )}
            <div className="arch-progress-node">
              <div style={{ fontSize: 24, marginBottom: 4 }}>
                {i === 0 ? '💻' : i === 1 ? '🖥' : '🌐'}
              </div>
              <div className="arch-progress-label">{mode.label}</div>
              <div className="arch-progress-desc">{mode.desc}</div>
            </div>
          </motion.div>
        ))}
      </div>
    </section>
  );
}
