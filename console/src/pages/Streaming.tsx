// Continuous (run-loop / cycle) jobs — the surface none of the previous
// UIs had: registry state, watermarks, snapshot availability.

import { useContinuousJobs } from "../api/queries";
import { StateBadge } from "../components/StateBadge";
import { ErrorText } from "../components/ui";

function watermark(ms: number | null): string {
  if (ms === null || ms === -9223372036854776000) return "—";
  return new Date(ms).toISOString().replace("T", " ").replace("Z", "");
}

export function StreamingPage() {
  const { data, error } = useContinuousJobs();
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">Streaming</h1>
      {error && <ErrorText>{String(error)}</ErrorText>}
      <div className="overflow-x-auto rounded border border-border bg-surface">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
              <th className="px-3 py-2">Stream</th>
              <th className="px-3 py-2">State</th>
              <th className="px-3 py-2 text-right">Tasks</th>
              <th className="px-3 py-2">Watermark</th>
              <th className="px-3 py-2">Persisted</th>
              <th className="px-3 py-2">Snapshot</th>
              <th className="px-3 py-2">Delivery</th>
            </tr>
          </thead>
          <tbody>
            {(data?.streams ?? []).map((s) => (
              <tr key={s.job_id} className="border-b border-border last:border-0 hover:bg-surface-2">
                <td className="px-3 py-2 tnum">{s.job_id}</td>
                <td className="px-3 py-2">
                  <StateBadge state={s.state} />
                  {s.cycle_in_flight && <span className="ml-2 text-xs text-running">cycle in flight</span>}
                </td>
                <td className="px-3 py-2 text-right tnum">
                  {s.running_task_count}/{s.task_count}
                </td>
                <td className="px-3 py-2 tnum text-muted">{watermark(s.last_watermark_ms)}</td>
                <td className="px-3 py-2 tnum text-muted">{watermark(s.persisted_watermark_ms)}</td>
                <td className="px-3 py-2">{s.snapshot_available ? "yes" : "—"}</td>
                <td className="px-3 py-2 text-muted">{s.delivery_guarantee ?? "—"}</td>
              </tr>
            ))}
            {data && data.streams.length === 0 && (
              <tr><td colSpan={7} className="px-3 py-6 text-center text-faint">No continuous jobs</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
