'use client';

import { motion } from 'framer-motion';

const categories = [
  {
    label: 'Sources',
    items: [
      { name: 'Kafka', color: '#34d399' },
      { name: 'Iceberg', color: '#38bdf8' },
      { name: 'Parquet', color: '#a78bfa' },
      { name: 'CSV', color: '#fbbf24' },
      { name: 'JSON', color: '#fb923c' },
      { name: 'Kinesis', color: '#34d399' },
      { name: 'Pulsar', color: '#a78bfa' },
      { name: 'Debezium CDC', color: '#38bdf8' },
    ],
  },
  {
    label: 'Storage',
    items: [
      { name: 'S3', color: '#fbbf24' },
      { name: 'ADLS', color: '#38bdf8' },
      { name: 'Local FS', color: '#a78bfa' },
      { name: 'Object Store', color: '#34d399' },
    ],
  },
  {
    label: 'Catalogs',
    items: [
      { name: 'Iceberg REST', color: '#38bdf8' },
      { name: 'Polaris', color: '#a78bfa' },
      { name: 'Memory Catalog', color: '#34d399' },
      { name: 'Custom Catalogs', color: '#fbbf24' },
    ],
  },
  {
    label: 'Sinks',
    items: [
      { name: 'Iceberg', color: '#38bdf8' },
      { name: 'Kafka', color: '#34d399' },
      { name: 'Parquet', color: '#a78bfa' },
      { name: 'Cassandra', color: '#fbbf24' },
      { name: 'Elasticsearch', color: '#fb923c' },
      { name: 'HBase', color: '#38bdf8' },
    ],
  },
  {
    label: 'Vector Stores',
    items: [
      { name: 'Pinecone', color: '#34d399' },
      { name: 'Weaviate', color: '#a78bfa' },
      { name: 'Qdrant', color: '#38bdf8' },
      { name: 'LanceDB', color: '#fbbf24' },
      { name: 'pgvector', color: '#fb923c' },
    ],
  },
  {
    label: 'Lakehouse',
    items: [
      { name: 'Iceberg', color: '#38bdf8' },
      { name: 'Delta Lake', color: '#34d399' },
      { name: 'Apache Hudi', color: '#a78bfa' },
    ],
  },
];

const cardVariants = {
  hidden: { opacity: 0, y: 16 },
  visible: (i: number) => ({
    opacity: 1,
    y: 0,
    transition: { duration: 0.4, delay: i * 0.06, ease: [0.22, 1, 0.36, 1] as [number, number, number, number] },
  }),
};

export function Ecosystem() {
  return (
    <section className="landing-section">
      <div className="landing-section-header">
        <motion.div
          className="landing-section-label"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
        >
          Ecosystem
        </motion.div>
        <motion.h2
          className="landing-section-title"
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          Connect to everything
        </motion.h2>
        <motion.p
          className="landing-section-desc"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.1 }}
        >
          Kafka, Iceberg, Parquet, S3, ADLS, vector stores, and more.
          Built-in connectors with a registry for custom drivers.
        </motion.p>
      </div>

      <div className="eco-grid">
        {categories.map((cat, i) => (
          <motion.div
            className="eco-card"
            key={cat.label}
            custom={i}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
            variants={cardVariants}
          >
            <div className="eco-card-label">{cat.label}</div>
            <div className="eco-card-items">
              {cat.items.map((item) => (
                <div className="eco-pill" key={item.name}>
                  <span className="eco-pill-dot" style={{ background: item.color }} />
                  {item.name}
                </div>
              ))}
            </div>
          </motion.div>
        ))}
      </div>
    </section>
  );
}
