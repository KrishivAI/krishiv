import { useHistory } from "../api/queries";
import { StateBadge } from "../components/StateBadge";
import { ErrorText } from "../components/ui";

function fmtBytes(n: number): string {
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(1)} GiB`;
  if (n >= 1 << 20) return `${(n / (1 << 20)).toFixed(1)} MiB`;
  if (n >= 1 << 10) return `${(n / (1 << 10)).toFixed(1)} KiB`;
  return `${n} B`;
}

export function HistoryPage() {
  const { data, error } = useHistory();
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">
        History{data && <span className="ml-2 text-sm font-normal text-faint tnum">{data.total} completed</span>}
      </h1>
      {error && <ErrorText>{String(error)}</ErrorText>}
      <div className="overflow-x-auto rounded border border-border bg-surface">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
              <th className="px-3 py-2">Job</th>
              <th className="px-3 py-2">Kind</th>
              <th className="px-3 py-2">Final state</th>
              <th className="px-3 py-2">Completed</th>
              <th className="px-3 py-2 text-right">Tasks (ok / fail)</th>
              <th className="px-3 py-2 text-right">CPU</th>
              <th className="px-3 py-2 text-right">Peak task mem</th>
            </tr>
          </thead>
          <tbody>
            {(data?.records ?? []).map((r) => (
              <tr key={r.job_id} className="border-b border-border last:border-0 hover:bg-surface-2">
                <td className="px-3 py-2 tnum">{r.job_id}</td>
                <td className="px-3 py-2 text-muted">{r.job_kind}</td>
                <td className="px-3 py-2"><StateBadge state={r.final_state} /></td>
                <td className="px-3 py-2 tnum text-muted">
                  {new Date(r.completed_at_ms).toISOString().replace("T", " ").slice(0, 19)}
                </td>
                <td className="px-3 py-2 text-right tnum">
                  {r.succeeded_task_count} / {r.failed_task_count}
                </td>
                <td className="px-3 py-2 text-right tnum">{(r.cpu_nanos / 1e9).toFixed(2)}s</td>
                <td className="px-3 py-2 text-right tnum">{fmtBytes(r.memory_peak_task_bytes)}</td>
              </tr>
            ))}
            {data && data.records.length === 0 && (
              <tr><td colSpan={7} className="px-3 py-6 text-center text-faint">No completed jobs yet</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
