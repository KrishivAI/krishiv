import { useNavigate, useParams } from "@tanstack/react-router";

import { useCancelJob, useJob, useJobDiagnose, useJobStages } from "../api/queries";
import { StateBadge } from "../components/StateBadge";
import { Button, Card, ErrorText } from "../components/ui";
import { fmtBytes, fmtMs, watermark } from "../lib/format";

export function JobDetailPage() {
  const { jobId } = useParams({ from: "/app/jobs/$jobId" });
  const navigate = useNavigate();
  const { data: job, error } = useJob(jobId);
  const stages = useJobStages(jobId);
  const diagnose = useJobDiagnose(jobId);
  const cancel = useCancelJob();
  const cancellable = job && (job.state === "Running" || job.state === "Pending");

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <h1 className="flex items-center gap-3 text-lg font-semibold">
          <span className="tnum">{jobId}</span>
          {job && <StateBadge state={job.state} />}
        </h1>
        {cancellable && (
          <Button
            variant="ghost"
            disabled={cancel.isPending}
            onClick={() => {
              if (window.confirm(`Cancel job ${jobId}?`)) {
                cancel.mutate(jobId, { onSuccess: () => void navigate({ to: "/jobs" }) });
              }
            }}
          >
            Cancel job
          </Button>
        )}
      </div>
      {error && <ErrorText>{String(error)}</ErrorText>}
      {cancel.error && <ErrorText>{String(cancel.error)}</ErrorText>}
      {job && (
        <div className="grid max-w-3xl grid-cols-2 gap-3 lg:grid-cols-4">
          <Card><div className="text-xs text-faint">Kind</div><div className="mt-1">{job.kind}</div></Card>
          <Card><div className="text-xs text-faint">Stages</div><div className="mt-1 tnum">{job.stage_count}</div></Card>
          <Card><div className="text-xs text-faint">Tasks</div><div className="mt-1 tnum">{job.task_count}</div></Card>
          <Card>
            <div className="text-xs text-faint">run / ok / fail</div>
            <div className="mt-1 tnum">
              {job.running_task_count} / {job.succeeded_task_count} / {job.failed_task_count}
            </div>
          </Card>
        </div>
      )}

      {stages.data && stages.data.stages.length > 0 && (
        <div className="mt-6">
          <h2 className="mb-2 text-sm font-semibold text-muted">Stages &amp; task placement</h2>
          {stages.data.stages.map((s) => (
            <div key={s.stage_id} className="mb-4 overflow-x-auto rounded border border-border bg-surface">
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-b border-border px-3 py-2 text-sm">
                <span className="font-medium tnum">{s.stage_id}</span>
                <StateBadge state={s.state} />
                <span className="text-xs text-faint tnum">
                  {s.succeeded_task_count}/{s.task_count} tasks · total {fmtMs(s.total_task_ms)}
                  {s.median_task_ms !== null &&
                    ` · min ${fmtMs(s.min_task_ms)} / med ${fmtMs(s.median_task_ms)} / max ${fmtMs(s.max_task_ms)}`}
                  {s.shuffle_bytes_written > 0 && ` · shuffle ${fmtBytes(s.shuffle_bytes_written)}`}
                </span>
              </div>
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
                    <th className="px-3 py-1.5">Task</th>
                    <th className="px-3 py-1.5">State</th>
                    <th className="px-3 py-1.5">Executor</th>
                    <th className="px-3 py-1.5 text-right">Attempt</th>
                    <th className="px-3 py-1.5 text-right">Failures</th>
                    <th className="px-3 py-1.5 text-right">Duration</th>
                    <th className="px-3 py-1.5">Watermark</th>
                  </tr>
                </thead>
                <tbody>
                  {s.tasks.map((t) => (
                    <tr key={t.task_id} className="border-b border-border last:border-0">
                      <td className="px-3 py-1.5 tnum">{t.task_id}</td>
                      <td className="px-3 py-1.5"><StateBadge state={t.state} /></td>
                      <td className="px-3 py-1.5 text-muted">{t.executor_id ?? "—"}</td>
                      <td className="px-3 py-1.5 text-right tnum">{t.attempt}</td>
                      <td className={`px-3 py-1.5 text-right tnum ${t.failure_count ? "text-failed" : ""}`}>
                        {t.failure_count}
                      </td>
                      <td className="px-3 py-1.5 text-right tnum">{fmtMs(t.completed_duration_ms)}</td>
                      <td className="px-3 py-1.5 tnum text-muted">{watermark(t.last_watermark_ms)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {s.tasks.some((t) => t.last_failure_reason) && (
                <div className="border-t border-border px-3 py-2 text-xs text-failed">
                  {s.tasks
                    .filter((t) => t.last_failure_reason)
                    .map((t) => (
                      <div key={t.task_id}>
                        {t.task_id}: {t.last_failure_reason}
                      </div>
                    ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {diagnose.data && (
        <div className="mt-6 max-w-4xl">
          <h2 className="mb-2 text-sm font-semibold text-muted">Diagnose report</h2>
          <pre className="overflow-x-auto rounded border border-border bg-surface p-3 text-xs">
            {JSON.stringify(diagnose.data, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}
