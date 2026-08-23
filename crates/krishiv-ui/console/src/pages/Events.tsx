// The coordinator's decision feed, straight from the metadata store's event
// log. Entries are ordered but UNTIMED — the engine records no wall clock
// per event, and this page does not invent one; `seq` is the ordinal in the
// current buffer (old entries evict under memory pressure).

import { useEvents } from "../api/queries";
import { LogTable } from "../components/LogTable";
import { ErrorText } from "../components/ui";

const KIND_TONE: Record<string, string> = {
  task_failed: "text-failed",
  executor_lost: "text-failed",
  job_cancelled: "text-queued",
  task_succeeded: "text-success",
  job_completed: "text-success",
};

export function EventsPage() {
  const { data, error } = useEvents(300);
  return (
    <div>
      <h1 className="mb-1 text-lg font-semibold">Events</h1>
      <p className="mb-4 text-xs text-faint">
        Coordinator decision log, newest first — ordered, untimed (the engine records no
        per-event wall clock).{data && ` ${data.total} in buffer.`}
      </p>
      {error && <ErrorText>{String(error)}</ErrorText>}
      <div className="overflow-x-auto rounded border border-border bg-surface">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
              <th className="px-3 py-2 text-right">#</th>
              <th className="px-3 py-2">Event</th>
              <th className="px-3 py-2">Job</th>
              <th className="px-3 py-2">Task</th>
              <th className="px-3 py-2">Executor</th>
              <th className="px-3 py-2">Detail</th>
            </tr>
          </thead>
          <tbody>
            {(data?.events ?? []).map((e) => (
              <tr key={e.seq} className="border-b border-border last:border-0">
                <td className="px-3 py-1.5 text-right tnum text-faint">{e.seq}</td>
                <td className={`px-3 py-1.5 font-medium ${KIND_TONE[e.kind] ?? "text-text"}`}>
                  {e.kind.replaceAll("_", " ")}
                </td>
                <td className="px-3 py-1.5 tnum text-muted">{e.job_id ?? "—"}</td>
                <td className="px-3 py-1.5 tnum text-muted">
                  {e.task_id ?? "—"}
                  {e.attempt !== undefined && <span className="text-faint"> #{e.attempt}</span>}
                </td>
                <td className="px-3 py-1.5 tnum text-muted">{e.executor_id ?? "—"}</td>
                <td className="max-w-md truncate px-3 py-1.5 text-muted" title={e.detail}>
                  {e.detail ?? "—"}
                </td>
              </tr>
            ))}
            {data && data.events.length === 0 && (
              <tr><td colSpan={6} className="px-3 py-6 text-center text-faint">
                No events (the daemon needs a metadata store attached)
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="mt-6">
        <LogTable path="/api/v1/logs" title="Coordinator logs" />
      </div>
    </div>
  );
}
