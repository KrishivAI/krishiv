// IVM (incremental view maintenance) jobs. The list endpoint carries each
// job's registered view names, so selecting a job fetches REAL per-view
// stats (rows, inserts, retractions) alongside the dispatch state —
// snapshot-only jobs (durable but not live in the registry) say so.

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api/client";
import { Card, ErrorText } from "../components/ui";

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
    <tr className="border-t border-border">
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
      api.get<Record<string, unknown>>(
        `/api/v1/ivm/jobs/${encodeURIComponent(selected ?? "")}/dispatch`,
      ),
    enabled: selected !== null,
    retry: 0,
  });
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">IVM</h1>
      {jobs.error && <ErrorText>{String(jobs.error)}</ErrorText>}
      <div className="max-w-xl rounded border border-border bg-surface">
        {(jobs.data?.jobs ?? []).map((j) => (
          <button
            key={j.job_id}
            className={`flex w-full items-center gap-2 border-b border-border px-3 py-2 text-left text-sm last:border-0 hover:bg-surface-2 ${selected === j.job_id ? "bg-surface-2" : ""}`}
            onClick={() => setSelected(j.job_id)}
          >
            <span className="tnum">{j.job_id}</span>
            <span className="text-xs text-faint">
              {j.view_names.length} view{j.view_names.length === 1 ? "" : "s"}
              {j.partitioned && " · partitioned"}
              {!j.live && " · snapshot-only"}
            </span>
          </button>
        ))}
        {jobs.data && jobs.data.jobs.length === 0 && (
          <div className="px-3 py-6 text-center text-sm text-faint">No IVM jobs registered</div>
        )}
      </div>

      {summary && summary.view_names.length > 0 && (
        <div className="mt-4 max-w-3xl overflow-x-auto rounded border border-border bg-surface">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs uppercase tracking-wide text-faint">
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
      )}
      {summary && !summary.live && (
        <p className="mt-3 text-xs text-faint">
          Snapshot-only job: durable state exists but it is not live in the registry, so no
          view stats are available until it is rehydrated.
        </p>
      )}

      {selected && (
        <Card className="mt-4 max-w-3xl">
          <div className="mb-1 text-xs text-faint">dispatch state: {selected}</div>
          {dispatch.error ? (
            <ErrorText>{String(dispatch.error)}</ErrorText>
          ) : (
            <pre className="overflow-x-auto text-xs">
              {JSON.stringify(dispatch.data ?? {}, null, 2)}
            </pre>
          )}
        </Card>
      )}
    </div>
  );
}
