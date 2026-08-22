import { useParams } from "@tanstack/react-router";

import { useJob, useJobDiagnose } from "../api/queries";
import { StateBadge } from "../components/StateBadge";
import { Card, ErrorText } from "../components/ui";

export function JobDetailPage() {
  const { jobId } = useParams({ from: "/app/jobs/$jobId" });
  const { data: job, error } = useJob(jobId);
  const diagnose = useJobDiagnose(jobId);
  return (
    <div>
      <h1 className="mb-4 flex items-center gap-3 text-lg font-semibold">
        <span className="tnum">{jobId}</span>
        {job && <StateBadge state={job.state} />}
      </h1>
      {error && <ErrorText>{String(error)}</ErrorText>}
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
      {diagnose.data && (
        <div className="mt-6 max-w-3xl">
          <h2 className="mb-2 text-sm font-semibold text-muted">Diagnose</h2>
          <pre className="overflow-x-auto rounded border border-border bg-surface p-3 text-xs">
            {JSON.stringify(diagnose.data, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}
