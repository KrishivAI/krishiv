import type { JSX } from 'react';

const gold = '#F59E0B';
const goldBright = '#FFB52A';
const goldSoft = 'rgba(245,158,11,.18)';
const border = '#2A2A2A';
const text = '#F5F5F5';
const muted = '#A3A3A3';
const surface = '#101010';
const surfaceHi = '#151515';

function Box({
  x,
  y,
  w,
  h,
  label,
  sub,
  fill = surface,
  stroke = border,
  accent = false,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  label: string;
  sub?: string;
  fill?: string;
  stroke?: string;
  accent?: boolean;
}): JSX.Element {
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={w}
        height={h}
        rx={8}
        fill={fill}
        stroke={accent ? gold : stroke}
        strokeWidth={accent ? 1.25 : 1}
      />
      <text
        x={x + w / 2}
        y={sub ? y + h / 2 - 6 : y + h / 2 + 4}
        textAnchor="middle"
        fontSize={13}
        fontWeight={600}
        fill={accent ? goldBright : text}
      >
        {label}
      </text>
      {sub && (
        <text
          x={x + w / 2}
          y={y + h / 2 + 12}
          textAnchor="middle"
          fontSize={11}
          fill={muted}
        >
          {sub}
        </text>
      )}
    </g>
  );
}

function Arrow({
  x1,
  y1,
  x2,
  y2,
  label,
  curve = 0,
}: {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  label?: string;
  curve?: number;
}): JSX.Element {
  const mx = (x1 + x2) / 2;
  const my = (y1 + y2) / 2 + curve;
  const path = `M ${x1} ${y1} Q ${mx} ${my} ${x2} ${y2}`;
  const midX = curve === 0 ? (x1 + x2) / 2 : (x1 + x2) / 2 + curve * 0.3;
  const midY = (y1 + y2) / 2;
  return (
    <g>
      <path d={path} stroke={gold} strokeWidth={1.25} fill="none" markerEnd="url(#arrowhead)" />
      {label && (
        <text
          x={midX}
          y={midY - 6}
          textAnchor="middle"
          fontSize={10.5}
          fill={muted}
        >
          {label}
        </text>
      )}
    </g>
  );
}

function Defs(): JSX.Element {
  return (
    <defs>
      <marker id="arrowhead" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
        <path d="M 0 0 L 10 5 L 0 10 z" fill={gold} />
      </marker>
      <linearGradient id="bandGrad" x1="0" x2="0" y1="0" y2="1">
        <stop offset="0%" stopColor={goldSoft} />
        <stop offset="100%" stopColor="rgba(245,158,11,0)" />
      </linearGradient>
    </defs>
  );
}

export function TopologyDiagram(): JSX.Element {
  return (
    <svg
      viewBox="0 0 900 360"
      width="100%"
      role="img"
      aria-label="Three deployment topologies: embedded, single-node, and distributed"
      style={{ maxWidth: 900 }}
    >
      <Defs />

      <text x={150} y={28} textAnchor="middle" fontSize={13} fontWeight={700} fill={goldBright}>
        Embedded
      </text>
      <text x={150} y={46} textAnchor="middle" fontSize={11} fill={muted}>
        in-process
      </text>
      <Box x={50} y={70} w={200} h={56} label="Your app" sub="Rust / Python" />
      <Box x={50} y={150} w={200} h={56} label="Krishiv" sub="in-process runtime" accent />
      <Box x={50} y={230} w={200} h={56} label="Local files" sub="CSV · Parquet" />
      <Arrow x1={150} y1={126} x2={150} y2={150} />
      <Arrow x1={150} y1={206} x2={150} y2={230} />

      <line x1={290} x2={290} y1={20} y2={340} stroke={border} strokeDasharray="2 4" />

      <text x={450} y={28} textAnchor="middle" fontSize={13} fontWeight={700} fill={goldBright}>
        Single-node
      </text>
      <text x={450} y={46} textAnchor="middle" fontSize={11} fill={muted}>
        one host, durable
      </text>
      <Box x={350} y={70} w={200} h={56} label="Your app" sub="Rust / Python / SQL" />
      <Box x={350} y={150} w={200} h={84} label="Krishiv daemon" sub="coordinator + executor" accent />
      <Box x={350} y={258} w={200} h={56} label="Local disk / S3" sub="durable state" />
      <Arrow x1={450} y1={126} x2={450} y2={150} />
      <Arrow x1={450} y1={234} x2={450} y2={258} />

      <line x1={590} x2={590} y1={20} y2={340} stroke={border} strokeDasharray="2 4" />

      <text x={750} y={28} textAnchor="middle" fontSize={13} fontWeight={700} fill={goldBright}>
        Distributed
      </text>
      <text x={750} y={46} textAnchor="middle" fontSize={11} fill={muted}>
        coordinator + N executors
      </text>
      <Box x={650} y={70} w={200} h={56} label="Your app" sub="thin client" />
      <Box x={650} y={150} w={200} h={56} label="Coordinator" sub="job & task lifecycle" accent />
      <Box x={650} y={220} w={94} h={56} label="Executor 1" sub="data plane" />
      <Box x={756} y={220} w={94} h={56} label="Executor N" sub="data plane" />
      <Box x={650} y={290} w={200} h={40} label="Shared object store" sub="S3 / ADLS / GCS" />
      <Arrow x1={750} y1={126} x2={750} y2={150} />
      <Arrow x1={750} y1={206} x2={697} y2={220} />
      <Arrow x1={750} y1={206} x2={803} y2={220} />
      <Arrow x1={750} y1={276} x2={750} y2={290} />
    </svg>
  );
}

export function RequestFlowDiagram(): JSX.Element {
  return (
    <svg
      viewBox="0 0 900 220"
      width="100%"
      role="img"
      aria-label="How a query flows through Krishiv: API to plan to execution to result"
      style={{ maxWidth: 900 }}
    >
      <Defs />
      <Box x={10} y={80} w={130} h={60} label="SQL / Rust / Py" sub="your code" />
      <Box x={190} y={80} w={140} h={60} label="Session" sub="catalog + state" />
      <Box x={380} y={80} w={140} h={60} label="Plan" sub="logical + physical" accent />
      <Box x={570} y={80} w={140} h={60} label="Execute" sub="Arrow operators" />
      <Box x={760} y={80} w={130} h={60} label="Result" sub="RecordBatch" />

      <Arrow x1={140} y1={110} x2={190} y2={110} />
      <Arrow x1={330} y1={110} x2={380} y2={110} />
      <Arrow x1={520} y1={110} x2={570} y2={110} />
      <Arrow x1={710} y1={110} x2={760} y2={110} />

      <text x={260} y={70} textAnchor="middle" fontSize={10.5} fill={muted}>
        parse + bind
      </text>
      <text x={450} y={70} textAnchor="middle" fontSize={10.5} fill={muted}>
        optimize + fragment
      </text>
      <text x={640} y={70} textAnchor="middle" fontSize={10.5} fill={muted}>
        tasks on workers
      </text>
      <text x={825} y={70} textAnchor="middle" fontSize={10.5} fill={muted}>
        stream or batch
      </text>

      <text x={260} y={170} textAnchor="middle" fontSize={10.5} fill={muted}>
        schema, types, UDFs
      </text>
      <text x={450} y={170} textAnchor="middle" fontSize={10.5} fill={muted}>
        cost-based rules
      </text>
      <text x={640} y={170} textAnchor="middle" fontSize={10.5} fill={muted}>
        shuffle + state
      </text>
      <text x={825} y={170} textAnchor="middle" fontSize={10.5} fill={muted}>
        pull or push
      </text>
    </svg>
  );
}

export function DataPlaneDiagram(): JSX.Element {
  return (
    <svg
      viewBox="0 0 900 320"
      width="100%"
      role="img"
      aria-label="Data plane components: coordinator routes work to executors, which read state and write checkpoints"
      style={{ maxWidth: 900 }}
    >
      <Defs />

      <rect x={20} y={30} width={860} height={260} rx={14} fill={surface} stroke={border} />
      <text x={40} y={56} fontSize={12} fontWeight={700} fill={goldBright} letterSpacing=".04em">
        CONTROL PLANE
      </text>

      <Box x={140} y={70} w={620} h={70} label="Coordinator" sub="owns job and task state, schedules, recovers" accent />
      <text x={450} y={155} textAnchor="middle" fontSize={10.5} fill={muted}>
        task offers
      </text>
      <Arrow x1={450} y1={140} x2={450} y2={185} />

      <text x={40} y={190} fontSize={12} fontWeight={700} fill={goldBright} letterSpacing=".04em">
        DATA PLANE
      </text>

      <Box x={70} y={210} w={240} h={70} label="Executor 1" sub="runs task slots" />
      <Box x={340} y={210} w={240} h={70} label="Executor 2" sub="runs task slots" />
      <Box x={610} y={210} w={240} h={70} label="Executor N" sub="runs task slots" />

      <text x={70} y={300} textAnchor="middle" fontSize={10.5} fill={muted}>
        state + checkpoints
      </text>
      <text x={340} y={300} textAnchor="middle" fontSize={10.5} fill={muted}>
        state + checkpoints
      </text>
      <text x={610} y={300} textAnchor="middle" fontSize={10.5} fill={muted}>
        state + checkpoints
      </text>
    </svg>
  );
}

export function LifecycleDiagram(): JSX.Element {
  return (
    <svg
      viewBox="0 0 900 200"
      width="100%"
      role="img"
      aria-label="Pipeline lifecycle from submission to running to recovery"
      style={{ maxWidth: 900 }}
    >
      <Defs />
      <Box x={10} y={70} w={120} h={60} label="Submit" sub="pipeline / query" />
      <Box x={170} y={70} w={120} h={60} label="Validate" sub="schema + types" />
      <Box x={330} y={70} w={120} h={60} label="Plan" sub="fragment graph" accent />
      <Box x={490} y={70} w={120} h={60} label="Schedule" sub="tasks to workers" />
      <Box x={650} y={70} w={120} h={60} label="Run" sub="state + checkpoints" />
      <Box x={810} y={70} w={80} h={60} label="Result" sub="" />

      <Arrow x1={130} y1={100} x2={170} y2={100} />
      <Arrow x1={290} y1={100} x2={330} y2={100} />
      <Arrow x1={450} y1={100} x2={490} y2={100} />
      <Arrow x1={610} y1={100} x2={650} y2={100} />
      <Arrow x1={770} y1={100} x2={810} y2={100} />

      <text x={230} y={60} textAnchor="middle" fontSize={10.5} fill={muted}>
        catalog
      </text>
      <text x={390} y={60} textAnchor="middle" fontSize={10.5} fill={muted}>
        cost model
      </text>
      <text x={550} y={60} textAnchor="middle" fontSize={10.5} fill={muted}>
        placement
      </text>
      <text x={710} y={60} textAnchor="middle" fontSize={10.5} fill={muted}>
        streaming or batch
      </text>

      <path
        d="M 710 130 Q 550 170 390 130"
        stroke={gold}
        strokeWidth={1.25}
        strokeDasharray="4 4"
        fill="none"
        markerEnd="url(#arrowhead)"
      />
      <text x={550} y={172} textAnchor="middle" fontSize={10.5} fill={muted}>
        on failure: restore from last checkpoint
      </text>
    </svg>
  );
}
