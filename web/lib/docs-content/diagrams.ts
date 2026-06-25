const gold = '#F59E0B';
const goldBright = '#FFB52A';
const goldSoft = 'rgba(245,158,11,.22)';
const goldFaint = 'rgba(245,158,11,.1)';
const border = '#2A2A2A';
const borderSoft = '#1f1f1f';
const text = '#F5F5F5';
const muted = '#A3A3A3';
const surface = '#101010';
const surfaceHi = '#151515';
const accent = '#7dd3fc';

function defs(): string {
  return `
    <defs>
      <marker id="dh" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
        <path d="M 0 0 L 10 5 L 0 10 z" fill="${gold}"/>
      </marker>
      <marker id="dh-muted" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
        <path d="M 0 0 L 10 5 L 0 10 z" fill="${muted}"/>
      </marker>
      <linearGradient id="diag-gold" x1="0" x2="0" y1="0" y2="1">
        <stop offset="0%" stop-color="${goldSoft}"/>
        <stop offset="100%" stop-color="${goldFaint}"/>
      </linearGradient>
    </defs>
  `;
}

function box(x: number, y: number, w: number, h: number, label: string, sub = '', opts: {
  fill?: string;
  stroke?: string;
  text?: string;
  accent?: boolean;
  radius?: number;
} = {}): string {
  const fill = opts.fill ?? surface;
  const stroke = opts.stroke ?? (opts.accent ? gold : border);
  const textColor = opts.text ?? (opts.accent ? goldBright : text);
  const r = opts.radius ?? 8;
  return `
    <rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${r}" fill="${fill}" stroke="${stroke}" stroke-width="${opts.accent ? 1.25 : 1}"/>
    <text x="${x + w / 2}" y="${sub ? y + h / 2 - 6 : y + h / 2 + 4}" text-anchor="middle" font-size="13" font-weight="600" fill="${textColor}">${label}</text>
    ${sub ? `<text x="${x + w / 2}" y="${y + h / 2 + 12}" text-anchor="middle" font-size="11" fill="${muted}">${sub}</text>` : ''}
  `;
}

function arrow(x1: number, y1: number, x2: number, y2: number, opts: {
  label?: string;
  dashed?: boolean;
  muted?: boolean;
  curve?: number;
} = {}): string {
  const stroke = opts.muted ? muted : gold;
  const marker = opts.muted ? 'dh-muted' : 'dh';
  const dash = opts.dashed ? ' stroke-dasharray="4 4"' : '';
  let path: string;
  let labelX: number;
  let labelY: number;
  if (opts.curve) {
    const mx = (x1 + x2) / 2;
    const my = (y1 + y2) / 2 + opts.curve;
    path = `M ${x1} ${y1} Q ${mx} ${my} ${x2} ${y2}`;
    labelX = mx;
    labelY = my - 6;
  } else {
    path = `M ${x1} ${y1} L ${x2} ${y2}`;
    labelX = (x1 + x2) / 2;
    labelY = (y1 + y2) / 2 - 6;
  }
  return `
    <path d="${path}" stroke="${stroke}" stroke-width="1.25" fill="none"${dash} marker-end="url(#${marker})"/>
    ${opts.label ? `<text x="${labelX}" y="${labelY}" text-anchor="middle" font-size="10.5" fill="${muted}">${opts.label}</text>` : ''}
  `;
}

function wrap(viewBox: string, inner: string, label: string, maxWidth = 900): string {
  return `
    <figure class="doc-figure">
      <div class="doc-diagram" role="img" aria-label="${label}">
        <svg viewBox="${viewBox}" xmlns="http://www.w3.org/2000/svg" style="max-width:${maxWidth}px">
          ${defs()}
          ${inner}
        </svg>
      </div>
      <figcaption class="doc-caption">${label}</figcaption>
    </figure>
  `;
}

// 1. Distributed mode — coordinator + 3 executors + object store
export const DIAGRAM_DISTRIBUTED_TOPOLOGY = wrap(
  '0 0 900 340',
  `
    <rect x="20" y="20" width="860" height="300" rx="14" fill="${surface}" stroke="${border}"/>
    <text x="40" y="46" font-size="11" font-weight="700" fill="${goldBright}" letter-spacing=".04em">CONTROL PLANE</text>
    ${box(140, 60, 620, 60, 'Coordinator', 'owns job state, schedules, recovers', { accent: true })}
    <text x="450" y="138" text-anchor="middle" font-size="10.5" fill="${muted}">task offers</text>
    ${arrow(450, 120, 450, 158)}

    <text x="40" y="190" font-size="11" font-weight="700" fill="${goldBright}" letter-spacing=".04em">DATA PLANE</text>
    ${box(60, 210, 240, 64, 'Executor 1', 'runs task slots')}
    ${box(330, 210, 240, 64, 'Executor 2', 'runs task slots')}
    ${box(600, 210, 240, 64, 'Executor N', 'runs task slots')}

    <text x="60" y="295" text-anchor="middle" font-size="10.5" fill="${muted}">state + checkpoints</text>
    <text x="330" y="295" text-anchor="middle" font-size="10.5" fill="${muted}">state + checkpoints</text>
    <text x="600" y="295" text-anchor="middle" font-size="10.5" fill="${muted}">state + checkpoints</text>
  `,
  'Distributed mode: a coordinator schedules tasks onto replaceable executors. State and checkpoints live on shared storage so executors can be added or removed without losing the job.'
);

// 2. IncrementalFlow — DAG of flows with tick propagation
export const DIAGRAM_INCREMENTAL_FLOW = wrap(
  '0 0 900 360',
  `
    <text x="450" y="30" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">IncrementalFlow graph</text>

    ${box(60, 80, 160, 60, 'orders', 'source')}
    ${box(280, 80, 160, 60, 'enriched', 'view')}
    ${box(500, 80, 160, 60, 'totals', 'view')}
    ${box(720, 80, 160, 60, 'alerts', 'sink', { accent: true })}

    ${arrow(220, 110, 280, 110)}
    ${arrow(440, 110, 500, 110)}
    ${arrow(660, 110, 720, 110)}

    <text x="450" y="170" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">Tick 1 — a delta arrives</text>

    ${box(60, 200, 160, 60, 'orders', '+1 row', { fill: surfaceHi })}
    ${box(280, 200, 160, 60, 'enriched', '+1 row', { fill: surfaceHi })}
    ${box(500, 200, 160, 60, 'totals', 'dirty', { fill: surfaceHi })}
    ${box(720, 200, 160, 60, 'alerts', 'idle', { fill: surfaceHi })}

    ${arrow(220, 230, 280, 230, { label: 'recompute' })}
    ${arrow(440, 230, 500, 230, { label: 'recompute' })}
    <text x="690" y="222" text-anchor="middle" font-size="10.5" fill="${muted}">skipped</text>
    <line x1="660" y1="230" x2="720" y2="230" stroke="${muted}" stroke-dasharray="3 3"/>

    <text x="450" y="300" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">After checkpoint</text>
    <text x="450" y="324" text-anchor="middle" font-size="11" fill="${muted}">all views consistent · next tick starts from here</text>
  `,
  'IncrementalFlow: a delta in the source triggers a recompute along the dirty path only. Downstream views stay consistent with the source of truth, and a checkpoint pins a known-good state.'
);

// 3. Pipeline builder — source → operations → sink
export const DIAGRAM_PIPELINE_BUILDER = wrap(
  '0 0 900 220',
  `
    ${box(20, 80, 140, 60, 'Source', 'Kafka / S3 / file')}
    ${box(200, 80, 130, 60, 'Filter')}
    ${box(360, 80, 130, 60, 'Map')}
    ${box(520, 80, 130, 60, 'Aggregate')}
    ${box(680, 80, 130, 60, 'Sink', 'Iceberg / Kafka', { accent: true })}

    ${arrow(160, 110, 200, 110)}
    ${arrow(330, 110, 360, 110)}
    ${arrow(490, 110, 520, 110)}
    ${arrow(650, 110, 680, 110)}

    <text x="20" y="170" font-size="11" font-weight="700" fill="${goldBright}">builder chain</text>
    <text x="20" y="190" font-size="11" fill="${muted}">source().filter(...).map(...).aggregate(...).sink(...)</text>
  `,
  'A pipeline is a chain of typed operations. Each step returns a new builder; nothing runs until you call .sink() or .show().'
);

// 4. Windows — tumbling / sliding / session
export const DIAGRAM_WINDOWS = wrap(
  '0 0 900 320',
  `
    <text x="150" y="30" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">Tumbling</text>
    <text x="150" y="48" text-anchor="middle" font-size="11" fill="${muted}">fixed-size, non-overlapping</text>
    <line x1="20" y1="100" x2="280" y2="100" stroke="${border}"/>
    ${[0,1,2,3,4].map(i => `<rect x="${20 + i*52}" y="80" width="50" height="40" fill="${goldFaint}" stroke="${gold}" stroke-width="1"/>`).join('')}
    <text x="40" y="138" font-size="10" fill="${muted}">0:00</text>
    <text x="240" y="138" font-size="10" fill="${muted}">0:10</text>

    <text x="450" y="30" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">Sliding</text>
    <text x="450" y="48" text-anchor="middle" font-size="11" fill="${muted}">overlapping, advances by slide</text>
    <line x1="320" y1="100" x2="580" y2="100" stroke="${border}"/>
    ${[0,1,2,3,4,5].map(i => `<rect x="${320 + i*42}" y="80" width="50" height="40" fill="${i%2===0?goldFaint:'rgba(125,211,252,.08)'}" stroke="${i%2===0?gold:accent}" stroke-width="1"/>`).join('')}

    <text x="750" y="30" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">Session</text>
    <text x="750" y="48" text-anchor="middle" font-size="11" fill="${muted}">data-driven gap</text>
    <line x1="620" y1="100" x2="880" y2="100" stroke="${border}"/>
    <rect x="620" y="80" width="60" height="40" fill="${goldFaint}" stroke="${gold}"/>
    <rect x="720" y="80" width="100" height="40" fill="${goldFaint}" stroke="${gold}"/>
    <text x="650" y="140" font-size="10" fill="${muted}">gap closes session</text>
    <path d="M 695 90 L 705 90" stroke="${muted}" stroke-width="1" marker-end="url(#dh-muted)"/>

    <text x="450" y="220" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">Watermark progress</text>
    <line x1="40" y1="280" x2="860" y2="280" stroke="${border}"/>
    ${[0,1,2,3,4,5,6,7].map(i => `<line x1="${40 + i*100}" y1="270" x2="${40 + i*100}" y2="290" stroke="${borderSoft}"/>`).join('')}
    <path d="M 40 280 L 320 280" stroke="${gold}" stroke-width="2" marker-end="url(#dh)"/>
    <path d="M 360 280 L 860 280" stroke="${borderSoft}" stroke-width="1" stroke-dasharray="3 3"/>
    <text x="180" y="265" text-anchor="middle" font-size="10.5" fill="${muted}">emitted (closed)</text>
    <text x="610" y="265" text-anchor="middle" font-size="10.5" fill="${muted}">pending (waiting for late events)</text>
  `,
  'Windows partition an unbounded stream into finite buckets. Watermarks mark the point past which only late events are expected.'
);

// 5. Joins — stream-table, stream-stream, regular
export const DIAGRAM_JOINS = wrap(
  '0 0 900 360',
  `
    <text x="450" y="28" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">Three join shapes</text>

    ${box(20, 80, 200, 60, 'orders stream', 'append-only')}
    ${box(280, 80, 200, 60, 'users table', 'versioned snapshot')}
    ${box(540, 80, 320, 60, 'enriched stream', 'joined', { accent: true })}
    ${arrow(220, 110, 280, 110, { label: 'as-of join' })}
    ${arrow(480, 110, 540, 110)}

    ${box(20, 170, 200, 60, 'clicks stream')}
    ${box(280, 170, 200, 60, 'impressions stream')}
    ${box(540, 170, 320, 60, 'attributed clicks', '', { accent: true })}
    ${arrow(220, 200, 280, 200, { label: 'interval join' })}
    ${arrow(480, 200, 540, 200)}

    ${box(20, 260, 200, 60, 'stream A')}
    ${box(280, 260, 200, 60, 'stream B')}
    ${box(540, 260, 320, 60, 'paired A-B', '', { accent: true })}
    ${arrow(220, 290, 280, 290, { label: 'windowed join' })}
    ${arrow(480, 290, 540, 290)}

    <text x="450" y="345" text-anchor="middle" font-size="11" fill="${muted}">All three produce an append-only output stream; intermediate state is held in keyed state.</text>
  `,
  'Krishiv supports three join shapes: stream-table (as-of), stream-stream (interval / windowed), and regular joins on streaming input.'
);

// 6. State types — value / list / map / reducing
export const DIAGRAM_STATE_TYPES = wrap(
  '0 0 900 320',
  `
    ${box(40, 60, 200, 60, 'ValueState&lt;T&gt;', 'one value per key')}
    ${box(280, 60, 200, 60, 'ListState&lt;T&gt;', 'append-only list')}
    ${box(520, 60, 200, 60, 'MapState&lt;K,V&gt;', 'per-key map')}
    ${box(760, 60, 120, 60, 'Reducing', 'merge')}
    <line x1="20" y1="180" x2="880" y2="180" stroke="${border}"/>
    <text x="40" y="170" font-size="10.5" fill="${muted}">key = "user_42"</text>
    <text x="280" y="170" font-size="10.5" fill="${muted}">key = "user_42"</text>
    <text x="520" y="170" font-size="10.5" fill="${muted}">key = "user_42"</text>
    <text x="760" y="170" font-size="10.5" fill="${muted}">key = "user_42"</text>

    ${box(40, 200, 200, 60, 'counter = 7', '')}
    ${box(280, 200, 200, 60, '[a, b, c]', '')}
    ${box(520, 200, 200, 60, 'fr → 3, es → 1', '')}
    ${box(760, 200, 120, 60, 'sum = 142', '')}

    <text x="450" y="295" text-anchor="middle" font-size="11" fill="${muted}">All four live in keyed state, partitioned by the key, persisted to RocksDB or in-memory, restored from the last checkpoint.</text>
  `,
  'Keyed state comes in four shapes: a single value, an append-only list, a per-key map, and a reducer that combines updates.'
);

// 7. Timers — event time + processing time
export const DIAGRAM_TIMERS = wrap(
  '0 0 900 280',
  `
    <text x="450" y="30" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">Two timer clocks</text>

    <text x="220" y="70" text-anchor="middle" font-size="12" font-weight="700" fill="${text}">Event time</text>
    <line x1="40" y1="120" x2="400" y2="120" stroke="${border}"/>
    ${[0,1,2,3,4,5].map(i => `<line x1="${40 + i*72}" y1="110" x2="${40 + i*72}" y2="130" stroke="${borderSoft}"/>`).join('')}
    ${[1,3,4].map(i => `<circle cx="${40 + i*72}" cy="120" r="4" fill="${gold}"/>`).join('')}
    <text x="40" y="105" font-size="10" fill="${muted}">events arrive out of order</text>
    <text x="40" y="160" font-size="10" fill="${muted}">timer fires when watermark ≥ 12:05</text>
    <path d="M 110 145 L 220 145" stroke="${accent}" stroke-width="1.5" marker-end="url(#dh)"/>

    <text x="680" y="70" text-anchor="middle" font-size="12" font-weight="700" fill="${text}">Processing time</text>
    <line x1="500" y1="120" x2="860" y2="120" stroke="${border}"/>
    ${[0,1,2,3,4,5].map(i => `<line x1="${500 + i*72}" y1="110" x2="${500 + i*72}" y2="130" stroke="${borderSoft}"/>`).join('')}
    <path d="M 500 120 L 720 120" stroke="${gold}" stroke-width="2" marker-end="url(#dh)"/>
    <text x="500" y="105" font-size="10" fill="${muted}">wall-clock, monotonic</text>
    <text x="500" y="160" font-size="10" fill="${muted}">timer fires N ms after registration</text>
  `,
  'Krishiv exposes two timer clocks: event time (driven by the watermark) and processing time (wall-clock). Timers are registered per key, fire once, and can re-register on firing.'
);

// 8. Savepoints — snapshot, restore, migration
export const DIAGRAM_SAVEPOINTS = wrap(
  '0 0 900 320',
  `
    <text x="450" y="30" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">Savepoint lifecycle</text>

    ${box(40, 80, 180, 60, 'Running job', '')}
    ${box(280, 80, 180, 60, 'Trigger savepoint', '')}
    ${box(480, 80, 180, 60, 'Cancel gracefully', '')}
    ${box(680, 80, 180, 60, 'Savepoint on S3', '', { accent: true })}
    ${arrow(220, 110, 280, 110)}
    ${arrow(460, 110, 480, 110, { label: 'drain' })}
    ${arrow(660, 110, 680, 110)}

    ${box(40, 180, 180, 60, 'New cluster', '')}
    ${box(280, 180, 180, 60, 'Restore from savepoint', '')}
    ${box(520, 180, 180, 60, 'Resumed job', '', { accent: true })}
    ${arrow(220, 210, 280, 210)}
    ${arrow(460, 210, 520, 210)}

    <text x="450" y="280" text-anchor="middle" font-size="11" fill="${muted}">Savepoints are portable across schema changes within a compatibility window — drop columns, widen types, re-attach.</text>
  `,
  'A savepoint is a point-in-time snapshot of all keyed state and source offsets. The same savepoint can be restored to a new cluster, on a new schema, or with a new parallelism.'
);

// 9. Incremental views — tick DAG
export const DIAGRAM_IVM_TICK = wrap(
  '0 0 900 280',
  `
    <text x="450" y="30" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">One IVM tick</text>

    ${box(40, 70, 200, 60, 'Source', 'delta arrives')}
    ${box(290, 70, 200, 60, 'Build', 'per-view delta')}
    ${box(540, 70, 200, 60, 'Apply', 'merge into sink')}
    ${box(790, 70, 90, 60, 'Commit', '', { accent: true })}

    ${arrow(240, 100, 290, 100)}
    ${arrow(490, 100, 540, 100)}
    ${arrow(740, 100, 790, 100)}

    <text x="150" y="170" text-anchor="middle" font-size="10.5" fill="${muted}">1. read source delta</text>
    <text x="390" y="170" text-anchor="middle" font-size="10.5" fill="${muted}">2. compute view delta</text>
    <text x="640" y="170" text-anchor="middle" font-size="10.5" fill="${muted}">3. apply to base table</text>
    <text x="835" y="170" text-anchor="middle" font-size="10.5" fill="${muted}">4. commit</text>

    <text x="450" y="240" text-anchor="middle" font-size="11" fill="${muted}">Steps 1–3 can be parallelised across shards; step 4 is the linearization point per view.</text>
  `,
  'Each IVM tick has four phases: read the source delta, compute the per-view delta, apply it to the base table, and commit. Step 4 is the only linearization point.'
);

// 10. MERGE INTO — match → delete/insert
export const DIAGRAM_MERGE_INTO = wrap(
  '0 0 900 260',
  `
    ${box(40, 70, 180, 60, 'Target', 'existing rows')}
    ${box(280, 70, 180, 60, 'Source', 'new rows')}
    ${box(540, 70, 180, 60, 'Matched →', 'update / delete')}
    ${box(740, 70, 140, 60, 'Not matched', 'insert', { accent: true })}

    ${arrow(220, 100, 280, 100, { label: 'join on key' })}
    ${arrow(460, 100, 540, 100)}
    ${arrow(720, 100, 740, 100)}

    <text x="450" y="200" text-anchor="middle" font-size="11" fill="${muted}">Rows are partitioned by the join key, joined locally on each executor, and the resulting changes are applied transactionally.</text>
  `,
  'MERGE INTO joins source to target on a key, then routes matched rows to update or delete and unmatched rows to insert.'
);

// 11. Iceberg snapshots — linear history with branches
export const DIAGRAM_ICEBERG_SNAPSHOTS = wrap(
  '0 0 900 280',
  `
    <text x="450" y="30" text-anchor="middle" font-size="13" font-weight="700" fill="${goldBright}">Iceberg snapshot timeline</text>

    <line x1="40" y1="100" x2="860" y2="100" stroke="${border}"/>
    ${[0,1,2,3,4].map(i => `
      <circle cx="${80 + i*160}" cy="100" r="14" fill="${i===4?gold:surfaceHi}" stroke="${i===4?goldBright:border}" stroke-width="1.5"/>
      <text x="${80 + i*160}" y="105" text-anchor="middle" font-size="10" fill="${text}" font-weight="700">${i+1}</text>
    `).join('')}

    ${arrow(94, 100, 226, 100, { muted: true })}
    ${arrow(226, 100, 386, 100, { muted: true })}
    ${arrow(386, 100, 546, 100, { muted: true })}
    ${arrow(546, 100, 706, 100, { muted: true })}

    <text x="80" y="155" text-anchor="middle" font-size="10.5" fill="${muted}">snapshot 1</text>
    <text x="240" y="155" text-anchor="middle" font-size="10.5" fill="${muted}">snapshot 2</text>
    <text x="400" y="155" text-anchor="middle" font-size="10.5" fill="${muted}">snapshot 3</text>
    <text x="560" y="155" text-anchor="middle" font-size="10.5" fill="${muted}">snapshot 4</text>
    <text x="720" y="155" text-anchor="middle" font-size="10.5" fill="${goldBright}">main (5)</text>

    <line x1="400" y1="200" x2="400" y2="220" stroke="${accent}" stroke-width="1.5" stroke-dasharray="3 3"/>
    <line x1="400" y1="220" x2="800" y2="220" stroke="${accent}" stroke-width="1.5" stroke-dasharray="3 3" marker-end="url(#dh)"/>
    <text x="600" y="240" text-anchor="middle" font-size="10.5" fill="${accent}">branch: experiment</text>
    <circle cx="800" cy="220" r="6" fill="${accent}" opacity=".4"/>

    <text x="450" y="270" text-anchor="middle" font-size="11" fill="${muted}">main advances linearly; branches are independent lines that can be merged with fast-forward or cherry-pick.</text>
  `,
  'Iceberg maintains a linear main timeline plus zero or more branches and tags. AS-OF reads can target a snapshot id, a branch, a tag, or a timestamp.'
);

// 12. CDC → Iceberg recipe
export const DIAGRAM_CDC_ICEBERG = wrap(
  '0 0 900 220',
  `
    ${box(20, 80, 150, 60, 'Postgres', '')}
    ${box(200, 80, 150, 60, 'Debezium', 'CDC source')}
    ${box(380, 80, 150, 60, 'Kafka', 'topic', { accent: true })}
    ${box(560, 80, 150, 60, 'Krishiv', 'merge + IVM')}
    ${box(740, 80, 140, 60, 'Iceberg', 'snapshots')}

    ${arrow(170, 110, 200, 110, { label: 'WAL' })}
    ${arrow(350, 110, 380, 110, { label: 'log' })}
    ${arrow(530, 110, 560, 110, { label: 'consume' })}
    ${arrow(710, 110, 740, 110, { label: 'MERGE' })}

    <text x="20" y="180" font-size="11" font-weight="700" fill="${goldBright}">End-to-end flow</text>
    <text x="20" y="200" font-size="11" fill="${muted}">WAL → CDC log → streaming source → IVM view → Iceberg snapshots (time-travelable)</text>
  `,
  'CDC to Iceberg: Postgres WAL → Debezium → Kafka → Krishiv MERGE / IVM → Iceberg snapshots. Every row in Iceberg is reproducible from the WAL.'
);
