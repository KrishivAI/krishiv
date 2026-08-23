// The "is anything wrong" page: unhealthy work first, then live trends from
// the client-side metrics history (server-clocked samples, ~10 min at the
// 3s poll), then totals. Every number is a coordinator value; trends only
// cover this browser session — the ring buffer starts when the tab opens.

import { Link } from "@tanstack/react-router";

import { useMetricsHistory } from "../api/metricsHistory";
import { useContinuousJobs, useExecutors, useJobs } from "../api/queries";
import { Chart } from "../components/Chart";
import { Freshness } from "../components/Freshness";
import { StateBadge } from "../components/StateBadge";
import { Card } from "../components/ui";

function Stat({ label, value, tone }: { label: string; value: string | number; tone?: string }) {
  return (
    <Card>
      <div className="text-xs uppercase tracking-wide text-faint">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tnum ${tone ?? ""}`}>{value}</div>
    </Card>
  );
}

function TrendCard({
  title,
  points,
  unit,
}: {
  title: string;
  points: { x: number; y: number }[];
  unit?: string;
}) {
  return (
    <Card>
      <div className="mb-1 text-xs uppercase tracking-wide text-faint">{title}</div>
      {points.length >= 2 ? (
        <Chart series={[{ name: title, points }]} height={90} unit={unit} />
      ) : (
        <div className="flex h-[90px] items-center justify-center text-xs text-faint">
          collecting samples…
        </div>
      )}
    </Card>
  );
}

export function DashboardPage() {
  const jobs = useJobs();
  const executors = useExecutors();
  const streams = useContinuousJobs();
  const metrics = useMetricsHistory();

  const all = jobs.data?.jobs ?? [];
  const running = all.filter((j) => j.state === "Running").length;
  const failedJobs = all.filter((j) => j.state === "Failed");
  const troubledJobs = all.filter(
    (j) => j.state !== "Failed" && j.failed_task_count > 0,
  );
  const troubledStreams = (streams.data?.streams ?? []).filter(
    (s) => s.failed_task_count > 0 || s.state === "Failed",
  );
  const unhealthy = failedJobs.length + troubledJobs.length + troubledStreams.length;

  const pts = (f: (m: (typeof metrics.history)[number]) => number) =>
    metrics.history.map((m) => ({ x: m.at_ms, y: f(m) }));

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <h1 className="text-lg font-semibold">Dashboard</h1>
        <Freshness dataUpdatedAt={metrics.dataUpdatedAt} error={metrics.error} />
      </div>

      {unhealthy > 0 && (
        <div className="mb-4 rounded border border-failed/40 bg-surface p-3">
          <div className="mb-2 text-sm font-semibold text-failed">
            Needs attention ({unhealthy})
          </div>
          <div className="space-y-1 text-sm">
            {failedJobs.map((j) => (
              <div key={j.job_id}>
                <Link to="/jobs/$jobId" params={{ jobId: j.job_id }} className="tnum underline decoration-border-strong hover:decoration-accent">
                  {j.job_id}
                </Link>{" "}
                <StateBadge state={j.state} />
              </div>
            ))}
            {troubledJobs.map((j) => (
              <div key={j.job_id}>
                <Link to="/jobs/$jobId" params={{ jobId: j.job_id }} className="tnum underline decoration-border-strong hover:decoration-accent">
                  {j.job_id}
                </Link>{" "}
                <span className="text-muted">
                  {j.failed_task_count} failed task{j.failed_task_count === 1 ? "" : "s"} (state {j.state})
                </span>
              </div>
            ))}
            {troubledStreams.map((s) => (
              <div key={s.job_id}>
                <Link to="/streaming/$jobId" params={{ jobId: s.job_id }} className="tnum underline decoration-border-strong hover:decoration-accent">
                  {s.job_id}
                </Link>{" "}
                <span className="text-muted">
                  stream: {s.failed_task_count} failed task{s.failed_task_count === 1 ? "" : "s"} (state {s.state})
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
        <Stat label="Jobs" value={all.length} />
        <Stat label="Running" value={running} tone="text-running" />
        <Stat label="Failed" value={failedJobs.length} tone={failedJobs.length ? "text-failed" : ""} />
        <Stat label="Executors" value={executors.data?.executors.length ?? "—"} />
        <Stat label="Streams" value={streams.data?.streams.length ?? "—"} />
      </div>

      <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
        <TrendCard title="Running tasks" points={pts((m) => m.running_task_count)} />
        <TrendCard title="Task retries (cumulative)" points={pts((m) => m.retry_count)} />
        <TrendCard title="Failed assignments (cumulative)" points={pts((m) => m.failed_assignments)} />
        <TrendCard
          title="Shuffle bytes written"
          points={pts((m) => m.shuffle_bytes_written)}
          unit="B"
        />
        <TrendCard title="Heartbeat lag (max ticks)" points={pts((m) => m.max_heartbeat_lag ?? 0)} />
        <TrendCard title="Executors" points={pts((m) => m.executor_count)} />
      </div>
    </div>
  );
}
