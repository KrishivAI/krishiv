// Stage DAG from REAL dependency edges (upstream_stage_ids on the stages
// endpoint). Hand-rolled layered SVG in the house style — stage graphs are
// small (rarely more than a handful of nodes), so the chart philosophy
// applies: self-contained inline SVG themed through the tokens, no library.
//
// Layout: longest-path layering (a node sits one column right of its
// deepest upstream), nodes stacked within a column. Edges are cubic curves
// from a producer's right edge to the consumer's left edge.

import type { StageTimingView } from "../api/types";

const NODE_W = 168;
const NODE_H = 46;
const GAP_X = 64;
const GAP_Y = 18;

const STATE_STROKE: Record<string, string> = {
  Running: "var(--running)",
  Succeeded: "var(--success)",
  Failed: "var(--failed)",
  Scheduling: "var(--queued)",
  Pending: "var(--border-strong)",
};

export function StageDag({ stages }: { stages: StageTimingView[] }) {
  if (stages.length < 2) return null;

  // Longest-path layering over the real edges.
  const byId = new Map(stages.map((s) => [s.stage_id, s]));
  const layer = new Map<string, number>();
  const depth = (id: string, seen: Set<string>): number => {
    const cached = layer.get(id);
    if (cached !== undefined) return cached;
    if (seen.has(id)) return 0; // defensive: a cycle would be an engine bug
    seen.add(id);
    const ups = byId.get(id)?.upstream_stage_ids ?? [];
    const d = ups.length === 0 ? 0 : 1 + Math.max(...ups.map((u) => depth(u, seen)));
    layer.set(id, d);
    return d;
  };
  stages.forEach((s) => depth(s.stage_id, new Set()));

  const columns = new Map<number, string[]>();
  for (const s of stages) {
    const l = layer.get(s.stage_id) ?? 0;
    columns.set(l, [...(columns.get(l) ?? []), s.stage_id]);
  }
  const pos = new Map<string, { x: number; y: number }>();
  let maxRows = 0;
  for (const [l, ids] of columns) {
    maxRows = Math.max(maxRows, ids.length);
    ids.forEach((id, row) => {
      pos.set(id, { x: l * (NODE_W + GAP_X), y: row * (NODE_H + GAP_Y) });
    });
  }
  const width = (Math.max(...columns.keys()) + 1) * (NODE_W + GAP_X) - GAP_X;
  const height = maxRows * (NODE_H + GAP_Y) - GAP_Y;

  return (
    <div className="overflow-x-auto rounded border border-border bg-surface p-4">
      <svg width={width} height={height} role="img" aria-label="Stage dependency graph">
        {stages.flatMap((s) =>
          s.upstream_stage_ids.map((up) => {
            const a = pos.get(up);
            const b = pos.get(s.stage_id);
            if (!a || !b) return null;
            const x1 = a.x + NODE_W;
            const y1 = a.y + NODE_H / 2;
            const x2 = b.x;
            const y2 = b.y + NODE_H / 2;
            const mx = (x1 + x2) / 2;
            return (
              <path
                key={`${up}->${s.stage_id}`}
                d={`M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`}
                fill="none"
                stroke="var(--border-strong)"
                strokeWidth="1.5"
                markerEnd="url(#dag-arrow)"
              />
            );
          }),
        )}
        <defs>
          <marker id="dag-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
            <path d="M 0 1 L 7 4 L 0 7 z" fill="var(--border-strong)" />
          </marker>
        </defs>
        {stages.map((s) => {
          const p = pos.get(s.stage_id);
          if (!p) return null;
          const done = `${s.succeeded_task_count}/${s.task_count}`;
          return (
            <g key={s.stage_id} transform={`translate(${p.x}, ${p.y})`}>
              <rect
                width={NODE_W}
                height={NODE_H}
                rx="6"
                fill="var(--surface-2)"
                stroke={STATE_STROKE[s.state] ?? "var(--border-strong)"}
                strokeWidth="1.5"
              />
              <text x="10" y="19" fontSize="12" fontWeight="600" fill="var(--text)">
                {s.stage_id.length > 20 ? `${s.stage_id.slice(0, 19)}…` : s.stage_id}
              </text>
              <text x="10" y="35" fontSize="10" fill="var(--muted)">
                {s.state} · {done} tasks
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
