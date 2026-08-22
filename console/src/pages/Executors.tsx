import { useExecutors } from "../api/queries";
import { ErrorText } from "../components/ui";

export function ExecutorsPage() {
  const { data, error } = useExecutors();
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">Executors</h1>
      {error && <ErrorText>{String(error)}</ErrorText>}
      <div className="overflow-x-auto rounded border border-border bg-surface">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
              <th className="px-3 py-2">Executor</th>
              <th className="px-3 py-2">Host</th>
              <th className="px-3 py-2">State</th>
              <th className="px-3 py-2 text-right">Slots</th>
              <th className="px-3 py-2 text-right">Running</th>
              <th className="px-3 py-2 text-right">Lease gen</th>
              <th className="px-3 py-2 text-right">Consecutive failures</th>
            </tr>
          </thead>
          <tbody>
            {(data?.executors ?? []).map((e) => (
              <tr key={e.executor_id} className="border-b border-border last:border-0 hover:bg-surface-2">
                <td className="px-3 py-2 tnum">{e.executor_id}</td>
                <td className="px-3 py-2 text-muted">{e.host}</td>
                <td className="px-3 py-2">{e.state}</td>
                <td className="px-3 py-2 text-right tnum">{e.slots}</td>
                <td className="px-3 py-2 text-right tnum">{e.running_task_count}</td>
                <td className="px-3 py-2 text-right tnum">{e.lease_generation}</td>
                <td className={`px-3 py-2 text-right tnum ${e.consecutive_task_failures ? "text-failed" : ""}`}>
                  {e.consecutive_task_failures}
                </td>
              </tr>
            ))}
            {data && data.executors.length === 0 && (
              <tr><td colSpan={7} className="px-3 py-6 text-center text-faint">No executors registered</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
