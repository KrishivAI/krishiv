'use client';

import { motion } from 'framer-motion';
import Link from 'next/link';
import { basePath } from '@/lib/base-path';

const archNodes = [
  {
    layer: [
      { label: 'SQL', color: 'blue' },
      { label: 'Rust', color: 'amber' },
      { label: 'Python', color: 'green' },
    ],
  },
  {
    layer: [{ label: 'Unified Query Planner', color: 'purple' }],
  },
  {
    layer: [{ label: 'Apache DataFusion Execution', color: 'blue' }],
  },
  {
    layer: [
      { label: 'Batch Runtime', color: 'green' },
      { label: 'Streaming Runtime', color: 'blue' },
    ],
  },
  {
    layer: [
      { label: 'Scheduler', color: 'purple' },
      { label: 'Shuffle', color: 'amber' },
      { label: 'Checkpoint', color: 'green' },
    ],
  },
  {
    layer: [
      { label: 'Iceberg', color: 'blue' },
      { label: 'Kafka', color: 'green' },
      { label: 'S3', color: 'amber' },
      { label: 'ADLS', color: 'purple' },
      { label: 'Polaris', color: 'blue' },
    ],
  },
];

const colorMap: Record<string, string> = {
  blue: 'arch-node-blue',
  green: 'arch-node-green',
  purple: 'arch-node-purple',
  amber: 'arch-node-amber',
};

export function Hero() {
  return (
    <section className="landing-hero">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6, ease: [0.22, 1, 0.36, 1] as [number, number, number, number] }}
      >
        <div className="landing-hero-badge">
          <span className="landing-hero-badge-dot" />
          Open Source · Rust Native
        </div>

        <h1>Krishiv</h1>

        <p className="landing-hero-subtitle">
          Unified Batch + Streaming Compute Engine.
          <br />
          Build modern data pipelines using SQL, Rust, or Python.
          <br />
          From local development to distributed clusters.
        </p>

        <div className="landing-hero-actions">
          <Link href={`${basePath}/docs`} className="landing-btn landing-btn-primary">
            Get Started
          </Link>
          <Link href={`${basePath}/docs`} className="landing-btn landing-btn-secondary">
            Read Docs
          </Link>
          <a
            href="https://github.com/KrishivAI/krishiv"
            className="landing-btn landing-btn-secondary"
            target="_blank"
            rel="noreferrer"
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z" />
            </svg>
            GitHub
          </a>
        </div>
      </motion.div>

      <motion.div
        className="arch-diagram"
        initial={{ opacity: 0, y: 30 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.8, delay: 0.3, ease: [0.22, 1, 0.36, 1] as [number, number, number, number] }}
      >
        {archNodes.map((group, gi) => (
          <div key={gi}>
            {gi > 0 && (
              <div className="arch-connector">
                <svg width="2" height="24" viewBox="0 0 2 24">
                  <line
                    x1="1" y1="0" x2="1" y2="24"
                    stroke="rgba(56, 189, 248, 0.2)"
                    strokeWidth="2"
                    strokeDasharray="4 4"
                  />
                </svg>
              </div>
            )}
            <motion.div
              className="arch-layer"
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 0.4 + gi * 0.1 }}
            >
              {group.layer.map((node) => (
                <div
                  key={node.label}
                  className={`arch-node ${colorMap[node.color]}`}
                >
                  {node.label}
                </div>
              ))}
            </motion.div>
          </div>
        ))}
      </motion.div>
    </section>
  );
}
