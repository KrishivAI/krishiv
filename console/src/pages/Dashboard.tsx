import { useContinuousJobs, useExecutors, useJobs } from "../api/queries";
import { Card } from "../components/ui";

function Stat({ label, value, tone }: { label: string; value: string | number; tone?: string }) {
  return (
    <Card>
      <div className="text-xs uppercase tracking-wide text-faint">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tnum ${tone ?? ""}`}>{value}</div>
    </Card>
  );
}

export function DashboardPage() {
  const jobs = useJobs();
  const executors = useExecutors();
  const streams = useContinuousJobs();
  const all = jobs.data?.jobs ?? [];
  const running = all.filter((j) => j.state === "Running").length;
  const failed = all.filter((j) => j.state === "Failed").length;
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">Dashboard</h1>
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
        <Stat label="Jobs" value={all.length} />
        <Stat label="Running" value={running} tone="text-running" />
        <Stat label="Failed" value={failed} tone={failed ? "text-failed" : ""} />
        <Stat label="Executors" value={executors.data?.executors.length ?? "—"} />
        <Stat label="Streams" value={streams.data?.streams.length ?? "—"} />
      </div>
    </div>
  );
}
