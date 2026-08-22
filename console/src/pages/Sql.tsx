// SQL runner over the batch-sql submit/poll endpoints. Results come back
// as base64 Arrow IPC stream payloads (serde_ipc_b64); apache-arrow
// decodes them client-side. CodeMirror editor is a follow-up — parity
// with the platform's editor is tracked, a textarea ships first.

import { tableFromIPC } from "apache-arrow";
import { useRef, useState } from "react";

import { api } from "../api/client";
import type { BatchSqlPollResponse, BatchSqlSubmitResponse } from "../api/types";
import { Button, ErrorText, StatusText } from "../components/ui";

interface ResultGrid {
  columns: string[];
  rows: unknown[][];
  stageCount: number;
  taskCount: number;
}

function decodeResults(poll: BatchSqlPollResponse): ResultGrid {
  const columns: string[] = [];
  const rows: unknown[][] = [];
  for (const b64 of poll.inline_record_batch_ipc ?? []) {
    const bytes = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
    const table = tableFromIPC(bytes);
    if (columns.length === 0) columns.push(...table.schema.fields.map((f) => f.name));
    for (const row of table.toArray()) {
      rows.push(columns.map((c) => (row as Record<string, unknown>)[c]));
    }
  }
  return { columns, rows, stageCount: poll.stage_count, taskCount: poll.task_count };
}

export function SqlPage() {
  const [sql, setSql] = useState("SELECT 1 AS one");
  const [state, setState] = useState<"idle" | "running">("idle");
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ResultGrid | null>(null);
  const cancelled = useRef(false);

  async function run() {
    setState("running");
    setError(null);
    setResult(null);
    cancelled.current = false;
    try {
      const submit = await api.post<BatchSqlSubmitResponse>("/api/v1/batch-sql/submit", { sql });
      for (;;) {
        if (cancelled.current) return;
        const poll = await api.get<BatchSqlPollResponse>(
          `/api/v1/batch-sql/${encodeURIComponent(submit.job_id)}`,
        );
        if (poll.state === "Succeeded") {
          setResult(decodeResults(poll));
          break;
        }
        if (poll.state === "Failed" || poll.state === "Cancelled") {
          setError(poll.error ?? poll.state);
          break;
        }
        await new Promise((r) => setTimeout(r, 300));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setState("idle");
    }
  }

  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">SQL</h1>
      <textarea
        value={sql}
        onChange={(e) => setSql(e.target.value)}
        rows={6}
        spellCheck={false}
        className="w-full rounded border border-border bg-surface p-3 font-mono text-sm focus:border-accent focus:outline-none"
      />
      <div className="mt-2 flex items-center gap-3">
        <Button disabled={state === "running" || !sql.trim()} onClick={() => void run()}>
          {state === "running" ? "Running…" : "Run"}
        </Button>
        {result && (
          <StatusText>
            {result.rows.length} rows · {result.stageCount} stages · {result.taskCount} tasks
          </StatusText>
        )}
      </div>
      {error && <ErrorText>{error}</ErrorText>}
      {result && result.columns.length > 0 && (
        <div className="mt-4 max-h-[32rem] overflow-auto rounded border border-border bg-surface">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-surface-2">
              <tr className="text-left text-xs uppercase tracking-wide text-faint">
                {result.columns.map((c) => (
                  <th key={c} className="px-3 py-2">{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, i) => (
                <tr key={i} className="border-t border-border">
                  {row.map((cell, k) => (
                    <td key={k} className="px-3 py-1.5 tnum">{String(cell)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
