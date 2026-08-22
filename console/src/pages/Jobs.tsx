import { Link } from "@tanstack/react-router";

import { useJobs } from "../api/queries";
import { StateBadge } from "../components/StateBadge";
import { ErrorText } from "../components/ui";

export function JobsPage() {
  const { data, error } = useJobs();
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">Jobs</h1>
      {error && <ErrorText>{String(error)}</ErrorText>}
      <div className="overflow-x-auto rounded border border-border bg-surface">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
              <th className="px-3 py-2">Job</th>
              <th className="px-3 py-2">Kind</th>
              <th className="px-3 py-2">State</th>
              <th className="px-3 py-2 text-right">Stages</th>
              <th className="px-3 py-2 text-right">Tasks (run / ok / fail)</th>
            </tr>
          </thead>
          <tbody>
            {(data?.jobs ?? []).map((j) => (
              <tr key={j.job_id} className="border-b border-border last:border-0 hover:bg-surface-2">
                <td className="px-3 py-2">
                  <Link
                    to="/jobs/$jobId"
                    params={{ jobId: j.job_id }}
                    className="text-text underline decoration-border-strong hover:decoration-accent"
                  >
                    {j.job_id}
                  </Link>
                </td>
                <td className="px-3 py-2 text-muted">{j.kind}</td>
                <td className="px-3 py-2"><StateBadge state={j.state} /></td>
                <td className="px-3 py-2 text-right tnum">{j.stage_count}</td>
                <td className="px-3 py-2 text-right tnum">
                  {j.running_task_count} / {j.succeeded_task_count} / {j.failed_task_count}
                </td>
              </tr>
            ))}
            {data && data.jobs.length === 0 && (
              <tr><td colSpan={5} className="px-3 py-6 text-center text-faint">No jobs</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
