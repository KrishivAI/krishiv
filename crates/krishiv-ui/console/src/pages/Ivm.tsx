// IVM (incremental view maintenance) jobs — list from the coordinator's
// IVM registry; per-job dispatch state on selection. The registry only
// exposes job ids at the list level, so the table is honest about that:
// drill-in fetches the dispatch view.

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api/client";
import { Card, ErrorText } from "../components/ui";

interface ListJobsResponse {
  job_ids: string[];
}

export function IvmPage() {
  const [selected, setSelected] = useState<string | null>(null);
  const jobs = useQuery({
    queryKey: ["ivm-jobs"],
    queryFn: () => api.get<ListJobsResponse>("/api/v1/ivm/jobs"),
    refetchInterval: 5000,
  });
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
        {(jobs.data?.job_ids ?? []).map((id) => (
          <button
            key={id}
            className={`block w-full border-b border-border px-3 py-2 text-left text-sm tnum last:border-0 hover:bg-surface-2 ${selected === id ? "bg-surface-2" : ""}`}
            onClick={() => setSelected(id)}
          >
            {id}
          </button>
        ))}
        {jobs.data && jobs.data.job_ids.length === 0 && (
          <div className="px-3 py-6 text-center text-sm text-faint">No IVM jobs registered</div>
        )}
      </div>
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
