// The house chart (Phase 25 charter): one charting approach for every
// surface that draws series — Usage, reliability (#29), agent-pinned charts
// (#37), dashboards-lite (#46). Hand-rolled inline SVG: self-contained (the
// console ships offline, no CDN), themed through the true-black tokens
// (grid on border, series on accent/muted), and small. If a future surface
// outgrows it (brushing, zoom), the decision to adopt a library happens in
// docs/ui-design.md — not ad hoc in a page.

const PALETTE = ["#4f8cff", "#8a8f98", "#3dd68c", "#e5484d", "#f5a623"] as const;

export interface ChartSeries {
  /** Legend label. */
  name: string;
  /** Points in x order; x is a timestamp (ms) or ordinal. */
  points: { x: number; y: number }[];
}

/** Format a tick: timestamps as HH:MM or M/D, ordinals as-is. */
function tick(x: number, spanMs: number): string {
  if (x < 10_000_000) return String(x);
  const d = new Date(x);
  return spanMs > 36 * 3600 * 1000
    ? `${d.getMonth() + 1}/${d.getDate()}`
    : `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

/**
 * Multi-series line chart with an area fill on the first series. Renders
 * nothing (an honest empty note) when there is no data — no fake axes.
 */
export function Chart({
  series,
  height = 180,
  unit,
}: {
  series: ChartSeries[];
  height?: number;
  unit?: string;
}) {
  const all = series.flatMap((s) => s.points);
  if (all.length === 0) {
    return (
      <div className="flex h-24 items-center justify-center text-xs text-faint">
        No data in this window yet.
      </div>
    );
  }
  const w = 640;
  const h = height;
  const padL = 44;
  const padB = 22;
  const padT = 8;
  const xs = all.map((p) => p.x);
  const ys = all.map((p) => p.y);
  const xMin = Math.min(...xs);
  const xMax = Math.max(...xs);
  const yMax = Math.max(...ys, 1);
  const sx = (x: number) => padL + ((x - xMin) / Math.max(xMax - xMin, 1)) * (w - padL - 8);
  const sy = (y: number) => padT + (1 - y / yMax) * (h - padT - padB);

  const gridYs = [0, 0.5, 1].map((f) => yMax * f);
  const xTicks = [xMin, (xMin + xMax) / 2, xMax];

  return (
    <div className="overflow-x-auto">
      <svg
        viewBox={`0 0 ${w} ${h}`}
        className="w-full min-w-[420px]"
        role="img"
        aria-label={`chart: ${series.map((s) => s.name).join(", ")}`}
      >
        {gridYs.map((y) => (
          <g key={y}>
            <line
              x1={padL}
              x2={w - 8}
              y1={sy(y)}
              y2={sy(y)}
              className="stroke-border"
              strokeWidth="1"
            />
            <text
              x={padL - 6}
              y={sy(y) + 3}
              textAnchor="end"
              className="fill-faint text-[10px] tabular-nums"
            >
              {y >= 1000 ? `${(y / 1000).toFixed(y >= 10000 ? 0 : 1)}k` : Math.round(y)}
            </text>
          </g>
        ))}
        {xTicks.map((x) => (
          <text
            key={x}
            x={sx(x)}
            y={h - 6}
            textAnchor="middle"
            className="fill-faint text-[10px] tabular-nums"
          >
            {tick(x, xMax - xMin)}
          </text>
        ))}
        {series.map((s, i) => {
          if (s.points.length === 0) return null;
          const path = s.points
            .map((p, j) => `${j === 0 ? "M" : "L"}${sx(p.x).toFixed(1)},${sy(p.y).toFixed(1)}`)
            .join(" ");
          const color = PALETTE[i % PALETTE.length];
          const first = s.points[0];
          const last = s.points[s.points.length - 1];
          return (
            <g key={s.name}>
              {i === 0 && first && last && (
                <path
                  d={`${path} L${sx(last.x).toFixed(1)},${sy(0)} L${sx(first.x).toFixed(1)},${sy(0)} Z`}
                  fill={color}
                  opacity="0.08"
                />
              )}
              <path d={path} fill="none" stroke={color} strokeWidth="1.5" />
              {last && <circle cx={sx(last.x)} cy={sy(last.y)} r="2.5" fill={color} />}
            </g>
          );
        })}
      </svg>
      <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-muted">
        {series.map((s, i) => (
          <span key={s.name} className="inline-flex items-center gap-1.5">
            <span
              className="inline-block h-2 w-2 rounded-full"
              style={{ backgroundColor: PALETTE[i % PALETTE.length] }}
            />
            {s.name}
            {unit ? ` (${unit})` : ""}
          </span>
        ))}
      </div>
    </div>
  );
}
