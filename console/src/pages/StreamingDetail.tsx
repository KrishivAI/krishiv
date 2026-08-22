// One continuous job: registry state, delivery guarantee (as reported by
// the coordinator's capability derivation — never a hardcoded claim),
// per-task push targets, and the operational verbs (checkpoint, flush,
// stop-with-savepoint, deregister).

import { useNavigate, useParams } from "@tanstack/react-router";
import { useState } from "react";

import {
  useContinuousJob,
  useContinuousTargets,
  useDeregisterStream,
  useFlushStream,
  useStopWithSavepoint,
  useTriggerCheckpoint,
} from "../api/queries";
import { StateBadge } from "../components/StateBadge";
import { Button, Card, ErrorText, StatusText } from "../components/ui";
import { watermark } from "../lib/format";

export function StreamingDetailPage() {
  const { jobId } = useParams({ from: "/app/streaming/$jobId" });
  const navigate = useNavigate();
  const { data: job, error } = useContinuousJob(jobId);
  const targets = useContinuousTargets(jobId);
  const checkpoint = useTriggerCheckpoint();
  const savepoint = useStopWithSavepoint();
  const flush = useFlushStream();
  const deregister = useDeregisterStream();
  const [lastAction, setLastAction] = useState<string | null>(null);

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <h1 className="flex items-center gap-3 text-lg font-semibold">
          <span className="tnum">{jobId}</span>
          {job && <StateBadge state={job.state} />}
        </h1>
        <div className="flex gap-2">
          <Button
            variant="ghost"
            disabled={checkpoint.isPending}
            onClick={() =>
              checkpoint.mutate(jobId, {
                onSuccess: (r) =>
                  setLastAction(
                    r.snapshot_available
                      ? `checkpoint: snapshot captured, watermark ${watermark(r.watermark_ms)}`
                      : (r.snapshot_source ??
                          `checkpoint: no snapshot available, watermark ${watermark(r.watermark_ms)}`),
                  ),
              })
            }
          >
            Checkpoint
          </Button>
          <Button
            variant="ghost"
            disabled={flush.isPending}
            onClick={() =>
              flush.mutate(jobId, {
                onSuccess: (r) =>
                  setLastAction(
                    `flush: ${r.success ? "ok" : "failed"}${r.inline_record_batch_ipc_b64.length ? `, ${r.inline_record_batch_ipc_b64.length} payload(s)` : ""}`,
                  ),
              })
            }
          >
            Flush
          </Button>
          <Button
            variant="ghost"
            disabled={savepoint.isPending}
            onClick={() => {
              if (window.confirm(`Stop ${jobId} with a savepoint?`)) {
                savepoint.mutate(jobId, {
                  onSuccess: (r) => setLastAction(`stopping with savepoint epoch ${r.savepoint_epoch}`),
                });
              }
            }}
          >
            Stop with savepoint
          </Button>
          <Button
            variant="ghost"
            disabled={deregister.isPending}
            onClick={() => {
              if (window.confirm(`Deregister ${jobId}? This cancels the job and frees the id.`)) {
                deregister.mutate(jobId, {
                  onSuccess: () => void navigate({ to: "/streaming" }),
                });
              }
            }}
          >
            Deregister
          </Button>
        </div>
      </div>
      {error && <ErrorText>{String(error)}</ErrorText>}
      {(checkpoint.error ?? savepoint.error ?? flush.error ?? deregister.error) && (
        <ErrorText>
          {String(checkpoint.error ?? savepoint.error ?? flush.error ?? deregister.error)}
        </ErrorText>
      )}
      {lastAction && <StatusText>{lastAction}</StatusText>}

      {job && (
        <div className="mt-4 grid max-w-4xl grid-cols-2 gap-3 lg:grid-cols-4">
          <Card><div className="text-xs text-faint">Class</div><div className="mt-1">{job.class}</div></Card>
          <Card><div className="text-xs text-faint">Model</div><div className="mt-1">{job.delivery.model} ×{job.delivery.parallelism}</div></Card>
          <Card>
            <div className="text-xs text-faint">Delivery (effective)</div>
            <div className="mt-1">{job.delivery.effective}</div>
            {job.delivery.sink && (
              <div className="mt-1 text-xs text-faint">
                sink {job.delivery.sink}
                {job.delivery.sink_guarantee && ` (${job.delivery.sink_guarantee})`}
                {job.delivery.source_offsets_in_sink_transaction && " · offsets in txn"}
              </div>
            )}
          </Card>
          <Card>
            <div className="text-xs text-faint">Tasks run / ok / fail</div>
            <div className="mt-1 tnum">
              {job.running_task_count} / {job.succeeded_task_count} / {job.failed_task_count}
            </div>
          </Card>
          <Card><div className="text-xs text-faint">Watermark</div><div className="mt-1 tnum">{watermark(job.last_watermark_ms)}</div></Card>
          <Card><div className="text-xs text-faint">Persisted watermark</div><div className="mt-1 tnum">{watermark(job.persisted_watermark_ms)}</div></Card>
          <Card><div className="text-xs text-faint">Snapshot</div><div className="mt-1">{job.snapshot_available ? "available" : "none"}</div></Card>
          <Card><div className="text-xs text-faint">Cycle in flight</div><div className="mt-1">{job.cycle_in_flight ? "yes" : "no"}</div></Card>
        </div>
      )}

      <div className="mt-6 max-w-4xl">
        <h2 className="mb-2 text-sm font-semibold text-muted">Push targets</h2>
        {targets.error ? (
          <StatusText>targets unavailable ({String(targets.error)})</StatusText>
        ) : (
          <div className="overflow-x-auto rounded border border-border bg-surface">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
                  <th className="px-3 py-2">Task</th>
                  <th className="px-3 py-2">Executor endpoint</th>
                </tr>
              </thead>
              <tbody>
                {(targets.data?.targets ?? []).map((t) => (
                  <tr key={t.task_id} className="border-b border-border last:border-0">
                    <td className="px-3 py-1.5 tnum">{t.task_id}</td>
                    <td className="px-3 py-1.5 tnum text-muted">{t.endpoint}</td>
                  </tr>
                ))}
                {targets.data && targets.data.targets.length === 0 && (
                  <tr><td colSpan={2} className="px-3 py-4 text-center text-faint">No run-loop targets (cycle-model job)</td></tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
