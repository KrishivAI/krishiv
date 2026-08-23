// SQL runner over the batch-sql submit/poll endpoints. Results come back
// as base64 Arrow IPC stream payloads (serde_ipc_b64); apache-arrow is
// imported lazily so the editor route's chunk stays small until the first
// result actually needs decoding.

import { useRef, useState } from "react";
import { format as formatSql } from "sql-formatter";

import { api } from "../api/client";
import type { BatchSqlPollResponse, BatchSqlSubmitResponse } from "../api/types";
import { CodeMirrorSql } from "../components/editor/CodeMirrorSql";
import { Button, ErrorText, StatusText } from "../components/ui";

interface ResultGrid {
  columns: string[];
  rows: unknown[][];
  stageCount: number;
  taskCount: number;
}

async function decodeResults(poll: BatchSqlPollResponse): Promise<ResultGrid> {
  const { tableFromIPC } = await import("apache-arrow");
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
  const [sqlText, setSqlText] = useState("SELECT 1 AS one");
  const [state, setState] = useState<"idle" | "running">("idle");
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ResultGrid | null>(null);
  const cancelled = useRef(false);
  const sqlRef = useRef(sqlText);
  sqlRef.current = sqlText;

  async function run() {
    setState("running");
    setError(null);
    setResult(null);
    cancelled.current = false;
    try {
      const submit = await api.post<BatchSqlSubmitResponse>("/api/v1/batch-sql/submit", {
        query: sqlRef.current,
      });
      for (;;) {
        if (cancelled.current) return;
        const poll = await api.get<BatchSqlPollResponse>(
          `/api/v1/batch-sql/${encodeURIComponent(submit.job_id)}`,
        );
        if (poll.state === "Succeeded") {
          setResult(await decodeResults(poll));
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

  function format(): boolean {
    try {
      setSqlText(formatSql(sqlRef.current, { language: "postgresql" }));
    } catch {
      // Unparseable input: leave the text as typed.
    }
    return true;
  }

  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">SQL</h1>
      <CodeMirrorSql
        value={sqlText}
        onChange={setSqlText}
        onRun={() => {
          if (state === "idle" && sqlRef.current.trim()) void run();
          return true;
        }}
        onFormat={format}
      />
      <div className="mt-2 flex items-center gap-3">
        <Button disabled={state === "running" || !sqlText.trim()} onClick={() => void run()}>
          {state === "running" ? "Running…" : "Run  ⌘⏎"}
        </Button>
        <Button variant="ghost" onClick={format}>
          Format
        </Button>
        {result && (
          <StatusText>
            {result.rows.length} rows · {result.stageCount} stage{result.stageCount === 1 ? "" : "s"} ·{" "}
            {result.taskCount} task{result.taskCount === 1 ? "" : "s"}
            {result.stageCount === 1 && result.taskCount === 1 && " (single-task)"}
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
