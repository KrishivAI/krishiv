import { useState } from "react";

import { useExecutors, useResetExecutorBreaker } from "../api/queries";
import { LogTable } from "../components/LogTable";
import { Button, ErrorText } from "../components/ui";

export function ExecutorsPage() {
  const { data, error } = useExecutors();
  const reset = useResetExecutorBreaker();
  const [logsFor, setLogsFor] = useState<string | null>(null);
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">Executors</h1>
      {error && <ErrorText>{String(error)}</ErrorText>}
      {reset.error && <ErrorText>{String(reset.error)}</ErrorText>}
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
              <th className="px-3 py-2 text-right">Heartbeat lag</th>
              <th className="px-3 py-2 text-right">Consecutive failures</th>
              <th className="px-3 py-2"></th>
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
                <td className="px-3 py-2 text-right tnum text-muted">
                  {data ? data.current_tick - e.last_heartbeat_tick : "—"}
                </td>
                <td className={`px-3 py-2 text-right tnum ${e.consecutive_task_failures ? "text-failed" : ""}`}>
                  {e.consecutive_task_failures}
                </td>
                <td className="px-3 py-2 text-right">
                  <Button
                    variant="ghost"
                    onClick={() => setLogsFor(logsFor === e.executor_id ? null : e.executor_id)}
                  >
                    {logsFor === e.executor_id ? "Hide logs" : "Logs"}
                  </Button>
                  {e.consecutive_task_failures > 0 && (
                    <Button
                      variant="ghost"
                      disabled={reset.isPending}
                      onClick={() => reset.mutate(e.executor_id)}
                    >
                      Reset breaker
                    </Button>
                  )}
                </td>
              </tr>
            ))}
            {data && data.executors.length === 0 && (
              <tr><td colSpan={9} className="px-3 py-6 text-center text-faint">No executors registered</td></tr>
            )}
          </tbody>
        </table>
      </div>
      {logsFor && (
        <div className="mt-6">
          <LogTable
            path={`/api/v1/executors/${encodeURIComponent(logsFor)}/logs`}
            title={`Executor logs · ${logsFor}`}
          />
        </div>
      )}
    </div>
  );
}
