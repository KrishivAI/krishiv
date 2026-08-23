// IVM (incremental view maintenance) jobs, in the same visual language as
// the Jobs/Streaming pages: a house-style table for the registry, a
// per-view stats table for the selection, and the dispatch state rendered
// as labeled cards (not raw JSON).

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api/client";
import { Card, ErrorText } from "../components/ui";
import { watermark } from "../lib/format";

interface IvmJobSummary {
  job_id: string;
  view_names: string[];
  partitioned: boolean;
  live: boolean;
}
interface ListJobsResponse {
  job_ids: string[];
  jobs: IvmJobSummary[];
}
interface DispatchState {
  attached: boolean;
  fence: number;
  last?: { tick: number; mode: string; reason: string; at_unix_ms: number };
}
interface ViewStats {
  num_rows: number;
  rows_inserted_total: number;
  rows_retracted_total: number;
  last_tick_inserts: number;
  last_tick_retracts: number;
}

function ViewStatsRow({ jobId, view }: { jobId: string; view: string }) {
  const stats = useQuery({
    queryKey: ["ivm-view-stats", jobId, view],
    queryFn: () =>
      api.get<ViewStats>(
        `/api/v1/ivm/jobs/${encodeURIComponent(jobId)}/views/${encodeURIComponent(view)}/stats`,
      ),
    refetchInterval: 5000,
    retry: 0,
  });
  const s = stats.data;
  return (
    <tr className="border-b border-border last:border-0">
      <td className="px-3 py-1.5 tnum">{view}</td>
      {stats.error ? (
        <td colSpan={5} className="px-3 py-1.5 text-xs text-failed">{String(stats.error)}</td>
      ) : (
        <>
          <td className="px-3 py-1.5 text-right tnum">{s?.num_rows ?? "…"}</td>
          <td className="px-3 py-1.5 text-right tnum">{s?.rows_inserted_total ?? "…"}</td>
          <td className="px-3 py-1.5 text-right tnum">{s?.rows_retracted_total ?? "…"}</td>
          <td className="px-3 py-1.5 text-right tnum">{s?.last_tick_inserts ?? "…"}</td>
          <td className="px-3 py-1.5 text-right tnum">{s?.last_tick_retracts ?? "…"}</td>
        </>
      )}
    </tr>
  );
}

export function IvmPage() {
  const [selected, setSelected] = useState<string | null>(null);
  const jobs = useQuery({
    queryKey: ["ivm-jobs"],
    queryFn: () => api.get<ListJobsResponse>("/api/v1/ivm/jobs"),
    refetchInterval: 5000,
  });
  const summary = jobs.data?.jobs.find((j) => j.job_id === selected);
  const dispatch = useQuery({
    queryKey: ["ivm-dispatch", selected],
    queryFn: () =>
      api.get<DispatchState>(
        `/api/v1/ivm/jobs/${encodeURIComponent(selected ?? "")}/dispatch`,
      ),
    enabled: selected !== null && (summary?.live ?? false),
    retry: 0,
  });
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">IVM</h1>
      {jobs.error && <ErrorText>{String(jobs.error)}</ErrorText>}
      <div className="overflow-x-auto rounded border border-border bg-surface">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
              <th className="px-3 py-2">Job</th>
              <th className="px-3 py-2">Views</th>
              <th className="px-3 py-2">Execution</th>
              <th className="px-3 py-2">Registry</th>
            </tr>
          </thead>
          <tbody>
            {(jobs.data?.jobs ?? []).map((j) => (
              <tr
                key={j.job_id}
                className={`cursor-pointer border-b border-border last:border-0 hover:bg-surface-2 ${selected === j.job_id ? "bg-surface-2" : ""}`}
                onClick={() => setSelected(j.job_id)}
              >
                <td className="px-3 py-2">
                  <span className="tnum underline decoration-border-strong">{j.job_id}</span>
                </td>
                <td className="px-3 py-2 text-muted">
                  {j.view_names.length > 0 ? j.view_names.join(", ") : "—"}
                </td>
                <td className="px-3 py-2 text-muted">{j.partitioned ? "partitioned" : "single"}</td>
                <td className="px-3 py-2">
                  <span
                    className={`inline-block rounded border px-1.5 py-0.5 text-xs ${j.live ? "border-success/40 text-success" : "border-border-strong text-faint"}`}
                  >
                    {j.live ? "live" : "snapshot-only"}
                  </span>
                </td>
              </tr>
            ))}
            {jobs.data && jobs.data.jobs.length === 0 && (
              <tr><td colSpan={4} className="px-3 py-6 text-center text-faint">No IVM jobs registered</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {summary && summary.view_names.length > 0 && (
        <div className="mt-6">
          <h2 className="mb-2 text-sm font-semibold text-muted">Views · {summary.job_id}</h2>
          <div className="max-w-3xl overflow-x-auto rounded border border-border bg-surface">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
                  <th className="px-3 py-2">View</th>
                  <th className="px-3 py-2 text-right">Rows</th>
                  <th className="px-3 py-2 text-right">Inserted</th>
                  <th className="px-3 py-2 text-right">Retracted</th>
                  <th className="px-3 py-2 text-right">Last tick +</th>
                  <th className="px-3 py-2 text-right">Last tick −</th>
                </tr>
              </thead>
              <tbody>
                {summary.view_names.map((v) => (
                  <ViewStatsRow key={v} jobId={summary.job_id} view={v} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      {summary && !summary.live && (
        <p className="mt-3 text-xs text-faint">
          Snapshot-only job: durable state exists but it is not live in the registry, so no
          view stats or dispatch state are available until it is rehydrated.
        </p>
      )}

      {summary?.live && dispatch.data && (
        <div className="mt-6">
          <h2 className="mb-2 text-sm font-semibold text-muted">Dispatch · {summary.job_id}</h2>
          {dispatch.error ? (
            <ErrorText>{String(dispatch.error)}</ErrorText>
          ) : (
            <div className="grid max-w-3xl grid-cols-2 gap-3 lg:grid-cols-4">
              <Card>
                <div className="text-xs text-faint">Executor-attached</div>
                <div className="mt-1">{dispatch.data.attached ? "yes" : "no (central)"}</div>
              </Card>
              <Card>
                <div className="text-xs text-faint">Fence</div>
                <div className="mt-1 tnum">{dispatch.data.fence}</div>
              </Card>
              <Card>
                <div className="text-xs text-faint">Last tick</div>
                <div className="mt-1 tnum">{dispatch.data.last?.tick ?? "—"}</div>
                {dispatch.data.last?.mode && (
                  <div className="mt-1 text-xs text-faint">{dispatch.data.last.mode}</div>
                )}
              </Card>
              <Card>
                <div className="text-xs text-faint">Last dispatch at</div>
                <div className="mt-1 tnum">{watermark(dispatch.data.last?.at_unix_ms)}</div>
                {dispatch.data.last?.reason && (
                  <div className="mt-1 text-xs text-muted">{dispatch.data.last.reason}</div>
                )}
              </Card>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
