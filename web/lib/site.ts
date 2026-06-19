export const navItems = [
  { label: 'Product', href: '/product' },
  { label: 'Docs', href: '/docs/latest' },
  { label: 'Architecture', href: '/architecture' },
  { label: 'Blog', href: '/blog' },
  { label: 'GitHub', href: '/github' },
];

export const publicFacts = [
  { name: 'Batch SQL', status: 'Available', text: 'DataFusion-backed SQL over Arrow RecordBatches and registered sources.' },
  { name: 'Streaming execution', status: 'Available', text: 'Streaming sessions, windowed operators, stream jobs, and in-memory stream inputs exist in Rust and Python surfaces.' },
  { name: 'Delta batch / IVM', status: 'Experimental', text: 'DeltaBatch and IncrementalFlow are implemented with partitioning, snapshots, watches, and checkpoint hooks; distributed executor execution is deferred.' },
  { name: 'Local to distributed runtime', status: 'Available', text: 'Embedded, single-node, and remote distributed runtime placements are explicit.' },
  { name: 'Iceberg and catalogs', status: 'Preview', text: 'Iceberg is the primary lakehouse target with REST, Hive, and Glue catalog paths documented; certification work continues.' },
  { name: 'Kafka, Parquet, S3, ADLS', status: 'Preview', text: 'Connector contracts and implementations exist with maturity labels; end-to-end guarantees depend on certified combinations.' },
];

export const githubUrl = 'https://github.com/krishiv-data/krishiv';
